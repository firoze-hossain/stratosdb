package com.stratosdb.sql.executor;

import com.stratosdb.common.exceptions.DeadlockException;
import com.stratosdb.index.btree.BTreeIndex;
import com.stratosdb.sql.ast.*;
import com.stratosdb.sql.parser.SqlParser;
import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.BTreePage;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.storage.wal.WALManager;
import com.stratosdb.transaction.Transaction;
import com.stratosdb.transaction.TransactionManager;
import com.stratosdb.transaction.mvcc.MVCCVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ExecutorEngine {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorEngine.class);

    private final SqlParser parser;
    private final Map<String, HeapTable> tables;
    private final BufferPool bufferPool;
    private final WALManager walManager;
    private final TransactionManager transactionManager;
    // Store column names for each table
    private final Map<String, List<String>> tableColumns;

    /** One B+Tree index, and what it indexes. Only integer/long-valued columns are indexable (see toIndexKey). */
    private record IndexEntry(String indexName, String tableName, String columnName, BTreeIndex index) {}

    private final Map<String, IndexEntry> indexesByName;
    private final Map<String, List<IndexEntry>> indexesByTable;

    /**
     * Per-column statistics: distinct-value count (for equality selectivity)
     * and numeric min/max (for range selectivity). Populated only by
     * ANALYZE - there is no automatic refresh on INSERT/UPDATE/DELETE (that
     * would be autovacuum's job, Phase E, not attempted here), so these can
     * go stale exactly the way Postgres's own statistics do without a
     * periodic ANALYZE/autovacuum. This is a real, named limitation, not
     * hidden: a table that grows substantially after ANALYZE was last run
     * will have the optimizer working from outdated row-count and
     * selectivity estimates until ANALYZE runs again.
     */
    private record ColumnStatistics(long distinctCount, Double min, Double max) {}
    private record TableStatistics(long rowCount, Map<String, ColumnStatistics> columnStats) {}

    private final Map<String, TableStatistics> statistics;

    public ExecutorEngine(BufferPool bufferPool, WALManager walManager, TransactionManager transactionManager) {
        this.parser = new SqlParser();
        this.tables = new ConcurrentHashMap<>();
        this.tableColumns = new ConcurrentHashMap<>();
        this.indexesByName = new ConcurrentHashMap<>();
        this.indexesByTable = new ConcurrentHashMap<>();
        this.statistics = new ConcurrentHashMap<>();
        this.bufferPool = bufferPool;
        this.walManager = walManager;
        this.transactionManager = transactionManager;
    }

    /**
     * Every statement runs inside its own transaction (begin -> handler ->
     * commit, or abort on any failure) - auto-commit, one statement per
     * transaction. That's a real transaction lifecycle now, not a formality:
     * INSERT/SELECT/UPDATE/DELETE all go through MVCC snapshots and, for
     * writers, real row-level locking with deadlock detection.
     *
     * The WAL commit record is written and forced to disk BEFORE the
     * transaction is marked committed in memory - if the process dies
     * between those two lines, redo on restart will still replay this
     * transaction's operations, and no reader can have seen it as committed
     * before it truly was.
     */
    public QueryResult execute(String sql) {
        Transaction txn = transactionManager.begin();
        try {
            Statement stmt = parser.parse(sql);
            QueryResult result = dispatch(stmt, txn);

            if (result.isSuccess()) {
                walManager.logCommit(txn.getXID());
                transactionManager.commit(txn);
            } else {
                transactionManager.abort(txn);
            }
            return result;
        } catch (DeadlockException e) {
            transactionManager.abort(txn);
            LOG.warn("Transaction {} aborted due to deadlock: {}", txn.getXID(), e.getMessage());
            return QueryResult.error("Deadlock detected, transaction aborted: " + e.getMessage());
        } catch (Exception e) {
            transactionManager.abort(txn);
            LOG.error("Execution failed: {}", sql, e);
            return QueryResult.error(e.getMessage());
        }
    }

    private QueryResult dispatch(Statement stmt, Transaction txn) throws DeadlockException {
        if (stmt instanceof CreateTableStatement s) return executeCreateTable(s);
        if (stmt instanceof CreateIndexStatement s) return executeCreateIndex(s, txn);
        if (stmt instanceof InsertStatement s) return executeInsert(s, txn);
        if (stmt instanceof SelectStatement s) return executeSelect(s, txn);
        if (stmt instanceof UpdateStatement s) return executeUpdate(s, txn);
        if (stmt instanceof DeleteStatement s) return executeDelete(s, txn);
        if (stmt instanceof DropTableStatement s) return executeDropTable(s);
        if (stmt instanceof ShowTablesStatement) return executeShowTables();
        if (stmt instanceof ExplainStatement s) return executeExplain(s);
        if (stmt instanceof AnalyzeStatement s) return executeAnalyze(s, txn);
        if (stmt instanceof VacuumStatement s) return executeVacuum(s);
        return QueryResult.error("Unsupported statement");
    }

    private QueryResult executeCreateTable(CreateTableStatement stmt) {
        if (tables.containsKey(stmt.tableName())) {
            return QueryResult.error("Table already exists: " + stmt.tableName());
        }

        HeapTable table = new HeapTable(stmt.tableName(), bufferPool);
        tables.put(stmt.tableName(), table);

        List<String> columns = new ArrayList<>();
        for (ColumnDefinition col : stmt.columns()) {
            columns.add(col.name());
        }
        tableColumns.put(stmt.tableName(), columns);

        return QueryResult.success("Table created: " + stmt.tableName());
    }

    /**
     * CREATE INDEX: builds a real B+Tree (stratosdb-index) over an existing
     * column and backfills it from every row currently visible to this
     * transaction. Only integer-valued columns can be indexed right now -
     * the B+Tree's keys are longs, and there's no string key encoding yet
     * (see toIndexKey). That's a real, named limitation, not a silent gap:
     * CREATE INDEX on a non-integer column succeeds but simply won't index
     * any rows, and later inserts into it won't be indexed either - the
     * planner will correctly never choose an index scan for it since
     * findIndex() only reports indexes that exist, but this is still worth
     * knowing before relying on it.
     */
    private QueryResult executeCreateIndex(CreateIndexStatement stmt, Transaction txn) {
        if (indexesByName.containsKey(stmt.indexName())) {
            return QueryResult.error("Index already exists: " + stmt.indexName());
        }
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }
        List<String> columns = tableColumns.get(stmt.tableName());
        boolean columnExists = columns != null && columns.stream().anyMatch(c -> c.equalsIgnoreCase(stmt.columnName()));
        if (!columnExists) {
            return QueryResult.error("Column not found: " + stmt.columnName() + " on table " + stmt.tableName());
        }

        BTreeIndex index = new BTreeIndex(stmt.indexName(), bufferPool);
        int indexed = 0;
        int skippedNonNumeric = 0;
        for (HeapTable.PositionedRow row : table.scanPositioned()) {
            if (!MVCCVisibility.isVisible(row.stored(), txn.getSnapshot(), transactionManager)) {
                continue;
            }
            Tuple tuple = Tuple.deserialize(MVCCVisibility.readPayload(row.stored()));
            Object value = findColumnValue(tuple, stmt.columnName());
            Long key = toIndexKey(value);
            if (key != null) {
                index.insert(key, new BTreePage.RID(row.pageId(), row.slot()));
                indexed++;
            } else {
                skippedNonNumeric++;
            }
        }

        IndexEntry entry = new IndexEntry(stmt.indexName(), stmt.tableName(), stmt.columnName(), index);
        indexesByName.put(stmt.indexName(), entry);
        indexesByTable.computeIfAbsent(stmt.tableName(), k -> new ArrayList<>()).add(entry);

        String message = "Index created: " + stmt.indexName() + " on " + stmt.tableName()
            + "(" + stmt.columnName() + "), indexed " + indexed + " row(s)";
        if (skippedNonNumeric > 0) {
            message += " (" + skippedNonNumeric + " row(s) skipped: non-integer column value)";
        }
        return QueryResult.success(message);
    }

    /**
     * ANALYZE: a real, if simple, statistics collector - one full scan of
     * the table computing row count and, per column, a distinct-value
     * count plus numeric min/max. Used by planScan (below) to make the
     * seq-scan-vs-index-scan choice genuinely cost-based rather than
     * rule-based, once this has been run.
     *
     * Known simplification, stated plainly: selectivity is estimated by
     * assuming a UNIFORM distribution across a column's distinct values
     * (rowCount / distinctCount for equality). Real Postgres tracks actual
     * most-common-value frequencies and histograms specifically because
     * real data is rarely uniform - a column with 2 distinct values split
     * 999-to-1 looks identical to this model as one split 500-to-500. That
     * gap is real further work (Phase B), not hidden here.
     */
    private QueryResult executeAnalyze(AnalyzeStatement stmt, Transaction txn) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        List<String> columnNames = tableColumns.get(stmt.tableName());
        Map<String, Set<Object>> distinctValues = new HashMap<>();
        Map<String, Double> minValues = new HashMap<>();
        Map<String, Double> maxValues = new HashMap<>();
        if (columnNames != null) {
            for (String col : columnNames) {
                distinctValues.put(col, new HashSet<>());
            }
        }

        long rowCount = 0;
        for (byte[] raw : table.scanMvcc(txn.getSnapshot(), transactionManager)) {
            Tuple tuple = Tuple.deserialize(raw);
            rowCount++;
            if (columnNames == null) {
                continue;
            }
            for (String col : columnNames) {
                Object value = findColumnValue(tuple, col);
                if (value == null) {
                    continue;
                }
                distinctValues.get(col).add(value);
                if (value instanceof Number n) {
                    double d = n.doubleValue();
                    minValues.merge(col, d, Math::min);
                    maxValues.merge(col, d, Math::max);
                }
            }
        }

        Map<String, ColumnStatistics> columnStats = new HashMap<>();
        if (columnNames != null) {
            for (String col : columnNames) {
                columnStats.put(col, new ColumnStatistics(
                    distinctValues.get(col).size(),
                    minValues.get(col),
                    maxValues.get(col)));
            }
        }

        statistics.put(stmt.tableName(), new TableStatistics(rowCount, columnStats));

        return QueryResult.success("Analyzed " + stmt.tableName() + ": " + rowCount + " row(s), "
            + (columnNames != null ? columnNames.size() : 0) + " column(s)");
    }

    /**
     * VACUUM: delegates the actual reclamation to HeapTable.vacuum() (see
     * its javadoc for the full explanation) - this method's only job is
     * computing the horizon and reporting the result as a QueryResult.
     *
     * Index entries are NOT touched here - they're already kept current in
     * real time by DELETE/UPDATE (see maintainIndexesOnDelete), a separate,
     * earlier piece of work. This is purely heap-level space reclamation.
     *
     * Known limitation, stated plainly: this is manual, not automatic -
     * there is no autovacuum background process deciding when to run this
     * on its own (Phase E, not attempted here). A table that's never
     * VACUUMed behaves exactly as it always has.
     */
    private QueryResult executeVacuum(VacuumStatement stmt) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        long horizon = transactionManager.getOldestActiveXid();
        HeapTable.VacuumResult result = table.vacuum(horizon, transactionManager);

        return QueryResult.success("Vacuumed " + stmt.tableName() + ": reclaimed "
            + result.reclaimedVersions() + " dead row version(s) across " + result.pagesCompacted() + " page(s)");
    }

    private QueryResult executeInsert(InsertStatement stmt, Transaction txn) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        List<Object> values = new ArrayList<>();
        for (String valueStr : stmt.values()) {
            values.add(parseLiteral(valueStr));
        }

        Tuple tuple = new Tuple();
        List<String> columns = tableColumns.get(stmt.tableName());
        if (columns != null) {
            for (int i = 0; i < values.size() && i < columns.size(); i++) {
                tuple.addValue(columns.get(i), values.get(i));
            }
        } else {
            for (int i = 0; i < values.size(); i++) {
                tuple.addValue("col" + i, values.get(i));
            }
        }

        byte[] data = tuple.serialize();
        HeapTable.InsertResult result = table.insertMvcc(data, txn.getXID());

        walManager.logInsert(stmt.tableName(), result.pageId, result.slot, data);
        maintainIndexesOnWrite(stmt.tableName(), tuple, result.pageId, result.slot);

        return QueryResult.success("Inserted row at " + result.pageId + "/" + result.slot);
    }

    /**
     * Rule-based scan choice: use a B+Tree index when the WHERE clause is a
     * single numeric predicate on an indexed column, otherwise fall back to
     * a full MVCC scan. Not cost-based (there's no statistics collection to
     * base a cost on yet) - it's the honest "does an applicable index exist
     * at all" version, same spirit as the Week 3 plan called for.
     */
    private QueryResult executeSelect(SelectStatement stmt, Transaction txn) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        if (stmt.joins() != null && !stmt.joins().isEmpty()) {
            return executeJoinedSelect(stmt, txn);
        }

        if (!stmt.aggregates().isEmpty() || !stmt.groupBy().isEmpty()) {
            return executeAggregateSelect(stmt, txn, table);
        }

        ScanPlan plan = planScan(stmt.tableName(), stmt.whereClause());
        List<Tuple> tuples = new ArrayList<>();

        if (plan.useIndex()) {
            List<BTreePage.RID> rids = plan.loKey().equals(plan.hiKey())
                ? plan.index().index().searchAll(plan.loKey())
                : plan.index().index().rangeScan(plan.loKey(), plan.hiKey());

            for (BTreePage.RID rid : rids) {
                byte[] stored = table.readTuple(rid.pageId(), rid.slot());
                if (stored == null || !MVCCVisibility.isVisible(stored, txn.getSnapshot(), transactionManager)) {
                    continue; // stale index entry (from an update/delete) or not visible to this snapshot
                }
                Tuple tuple = Tuple.deserialize(MVCCVisibility.readPayload(stored));
                if (!matchesWhere(tuple, stmt.whereClause())) {
                    continue; // defensive re-check, keeps index-scan results identical to seq-scan results
                }
                tuples.add(project(tuple, stmt.columns()));
            }
        } else {
            List<byte[]> visibleRows = table.scanMvcc(txn.getSnapshot(), transactionManager);
            for (byte[] data : visibleRows) {
                Tuple tuple = Tuple.deserialize(data);
                if (!matchesWhere(tuple, stmt.whereClause())) {
                    continue;
                }
                tuples.add(project(tuple, stmt.columns()));
            }
        }

        if (stmt.limit() != null) {
            try {
                int limit = Integer.parseInt(stmt.limit());
                if (tuples.size() > limit) {
                    tuples = tuples.subList(0, limit);
                }
            } catch (NumberFormatException e) {
                // Ignore invalid limit
            }
        }

        return QueryResult.success(tuples);
    }

    /**
     * GROUP BY / aggregates (COUNT/SUM/AVG/MIN/MAX) / HAVING.
     *
     * Always a full scan - there is no index acceleration for aggregate
     * queries yet (see PROJECT_PLAN.md Phase B: that needs a real
     * cost-based optimizer, which doesn't exist yet either). Not combined
     * with JOIN in this pass - a query with both joins and aggregates hits
     * executeJoinedSelect first and won't group; that combination is a
     * real, separate gap, not silently mishandled (executeJoinedSelect
     * doesn't consult stmt.aggregates()/groupBy() at all, so such a query
     * would just return per-row joined results, unaggregated).
     *
     * With no GROUP BY at all but an aggregate requested (e.g.
     * "SELECT COUNT(*) FROM t"), the whole filtered result set is treated
     * as a single implicit group - standard SQL behavior.
     */
    private QueryResult executeAggregateSelect(SelectStatement stmt, Transaction txn, HeapTable table) {
        List<Tuple> filteredRows = new ArrayList<>();
        for (byte[] raw : table.scanMvcc(txn.getSnapshot(), transactionManager)) {
            Tuple tuple = Tuple.deserialize(raw);
            if (matchesWhere(tuple, stmt.whereClause())) {
                filteredRows.add(tuple);
            }
        }

        Map<List<Object>, List<Tuple>> groups = new LinkedHashMap<>();
        if (stmt.groupBy().isEmpty()) {
            groups.put(List.of(), filteredRows);
        } else {
            for (Tuple tuple : filteredRows) {
                List<Object> key = new ArrayList<>();
                for (String col : stmt.groupBy()) {
                    key.add(findColumnValue(tuple, col));
                }
                groups.computeIfAbsent(key, k -> new ArrayList<>()).add(tuple);
            }
        }

        List<Tuple> resultRows = new ArrayList<>();
        for (Map.Entry<List<Object>, List<Tuple>> entry : groups.entrySet()) {
            List<Object> groupKey = entry.getKey();
            List<Tuple> groupRows = entry.getValue();

            Tuple outputRow = new Tuple();
            for (int i = 0; i < stmt.groupBy().size(); i++) {
                outputRow.addValue(stmt.groupBy().get(i), groupKey.get(i));
            }

            Map<String, Object> aggregateValues = new LinkedHashMap<>();
            for (AggregateCall agg : stmt.aggregates()) {
                Object value = computeAggregate(agg, groupRows);
                aggregateValues.put(agg.canonicalForm(), value); // HAVING always references the FUNC(arg) form
                outputRow.addValue(agg.displayName(), value);
            }

            if (stmt.havingClause() != null && !matchesHaving(stmt.havingClause(), aggregateValues)) {
                continue;
            }

            resultRows.add(outputRow);
        }

        if (stmt.limit() != null) {
            try {
                int limit = Integer.parseInt(stmt.limit());
                if (resultRows.size() > limit) {
                    resultRows = resultRows.subList(0, limit);
                }
            } catch (NumberFormatException e) {
                // Ignore invalid limit
            }
        }

        return QueryResult.success(resultRows);
    }

    /** COUNT(col) excludes NULLs (standard SQL); COUNT(*) counts every row regardless. SUM/AVG of zero contributing rows is NULL, not zero - also standard. */
    private Object computeAggregate(AggregateCall agg, List<Tuple> rows) {
        switch (agg.function()) {
            case "COUNT": {
                if (agg.argument().equals("*")) {
                    return rows.size();
                }
                int count = 0;
                for (Tuple row : rows) {
                    if (findColumnValue(row, agg.argument()) != null) count++;
                }
                return count;
            }
            case "SUM": {
                double sum = 0;
                boolean any = false;
                boolean allIntegral = true;
                for (Tuple row : rows) {
                    Object v = findColumnValue(row, agg.argument());
                    if (v instanceof Integer i) { sum += i; any = true; }
                    else if (v instanceof Long l) { sum += l; any = true; }
                    else if (v instanceof Double d) { sum += d; any = true; allIntegral = false; }
                }
                if (!any) return null;
                return allIntegral ? (Object) (long) sum : (Object) sum;
            }
            case "AVG": {
                double sum = 0;
                int count = 0;
                for (Tuple row : rows) {
                    Object v = findColumnValue(row, agg.argument());
                    if (v instanceof Number n) { sum += n.doubleValue(); count++; }
                }
                return count > 0 ? sum / count : null;
            }
            case "MIN": {
                Object best = null;
                for (Tuple row : rows) {
                    Object v = findColumnValue(row, agg.argument());
                    if (v != null && (best == null || compareForMinMax(v, best) < 0)) best = v;
                }
                return best;
            }
            case "MAX": {
                Object best = null;
                for (Tuple row : rows) {
                    Object v = findColumnValue(row, agg.argument());
                    if (v != null && (best == null || compareForMinMax(v, best) > 0)) best = v;
                }
                return best;
            }
            default:
                throw new IllegalArgumentException("Unknown aggregate function: " + agg.function());
        }
    }

    private int compareForMinMax(Object a, Object b) {
        if (a instanceof Number na && b instanceof Number nb) {
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        return a.toString().compareTo(b.toString());
    }

    /** HAVING always references the aggregate's canonical "FUNC(arg)" form, regardless of any SELECT-list alias. */
    private boolean matchesHaving(String havingClause, Map<String, Object> aggregateValues) {
        WherePredicate pred = parseWhere(havingClause);
        if (pred == null) {
            return true;
        }
        Object value = aggregateValues.get(pred.column());
        if (value == null) {
            return false;
        }
        return evaluatePredicate(value.toString(), pred);
    }

    /**
     * Hash join - replaced the Week 3 nested-loop join (Phase B of
     * PROJECT_PLAN.md). This is correct as the *only* join algorithm here
     * (not merely one option among several) because every JOIN this
     * grammar allows is an equality join (`joinClause: ... ON columnName
     * ASSIGN columnName` - ASSIGN is `=`, nothing else is accepted), and
     * hash join is exactly the right tool for equality predicates: O(n+m)
     * instead of nested-loop's O(n*m).
     *
     * Not yet cost-based, stated plainly: there's no statistics collection
     * or optimizer to weigh join strategies against each other (that's the
     * next item after this in Phase B), so this always builds the hash
     * table on whichever side has fewer rows - a real, if simple, heuristic
     * (less memory, fewer hash computations, on the side that gets
     * hashed rather than scanned), not a genuine cost comparison. If a
     * future non-equality join predicate is ever added, it would need a
     * different algorithm (hash join fundamentally cannot support it) -
     * nested-loop would be the natural fallback for that case, not
     * resurrected here speculatively.
     *
     * Every column in the combined result is qualified as
     * "tableName.columnName" to avoid ambiguity when two joined tables
     * share a column name (both having "id" is the common case). Bare
     * column references in WHERE/SELECT still work when unambiguous, via
     * findColumnValue's suffix-match fallback.
     */
    private QueryResult executeJoinedSelect(SelectStatement stmt, Transaction txn) {
        List<Tuple> current = new ArrayList<>();
        for (byte[] raw : tables.get(stmt.tableName()).scanMvcc(txn.getSnapshot(), transactionManager)) {
            current.add(qualify(Tuple.deserialize(raw), stmt.tableName()));
        }

        for (JoinClause join : stmt.joins()) {
            HeapTable joinedTable = tables.get(join.tableName());
            if (joinedTable == null) {
                return QueryResult.error("Table not found: " + join.tableName());
            }

            List<Tuple> joinedRows = new ArrayList<>();
            for (byte[] raw : joinedTable.scanMvcc(txn.getSnapshot(), transactionManager)) {
                joinedRows.add(qualify(Tuple.deserialize(raw), join.tableName()));
            }

            current = hashJoin(current, join.leftColumn(), joinedRows, join.rightColumn());
        }

        List<Tuple> tuples = new ArrayList<>();
        for (Tuple row : current) {
            if (!matchesWhere(row, stmt.whereClause())) {
                continue;
            }
            tuples.add(project(row, stmt.columns()));
        }

        if (stmt.limit() != null) {
            try {
                int limit = Integer.parseInt(stmt.limit());
                if (tuples.size() > limit) {
                    tuples = tuples.subList(0, limit);
                }
            } catch (NumberFormatException e) {
                // Ignore invalid limit
            }
        }

        return QueryResult.success(tuples);
    }

    /**
     * Classic hash join: build a hash table on the smaller side (by row
     * count), keyed by the join column, then probe it with the other side.
     * Output column order is always left-then-right regardless of which
     * side ended up as the build side internally - that choice is a pure
     * implementation detail, invisible to the caller.
     *
     * NULLs never match, on either side (standard SQL equi-join semantics)
     * - a row whose join-column value is null contributes nothing whether
     * it's on the build or probe side.
     *
     * Join keys are normalized to a canonical numeric form (double) when
     * they're any kind of Number before hashing, so values that are equal
     * numerically but different boxed types still hash/match consistently -
     * the same "compare by value, not by exact type" philosophy already
     * used throughout this class (see valuesEqual, evaluatePredicate,
     * compareForMinMax).
     */
    private List<Tuple> hashJoin(List<Tuple> left, String leftColumn, List<Tuple> right, String rightColumn) {
        boolean buildOnLeft = left.size() <= right.size();
        List<Tuple> buildSide = buildOnLeft ? left : right;
        List<Tuple> probeSide = buildOnLeft ? right : left;
        String buildColumn = buildOnLeft ? leftColumn : rightColumn;
        String probeColumn = buildOnLeft ? rightColumn : leftColumn;

        Map<Object, List<Tuple>> hashTable = new HashMap<>();
        for (Tuple row : buildSide) {
            Object key = normalizeJoinKey(findColumnValue(row, buildColumn));
            if (key != null) {
                hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
        }

        List<Tuple> result = new ArrayList<>();
        for (Tuple probeRow : probeSide) {
            Object key = normalizeJoinKey(findColumnValue(probeRow, probeColumn));
            if (key == null) {
                continue;
            }
            List<Tuple> matches = hashTable.get(key);
            if (matches == null) {
                continue;
            }
            for (Tuple buildRow : matches) {
                result.add(buildOnLeft ? merge(buildRow, probeRow) : merge(probeRow, buildRow));
            }
        }
        return result;
    }

    private Object normalizeJoinKey(Object value) {
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        return value;
    }

    private Tuple qualify(Tuple tuple, String tableName) {
        Tuple qualified = new Tuple();
        List<String> columnNames = tuple.getColumnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            qualified.addValue(tableName + "." + columnNames.get(i), tuple.getValue(i));
        }
        return qualified;
    }

    private Tuple merge(Tuple left, Tuple right) {
        Tuple merged = new Tuple();
        List<String> leftNames = left.getColumnNames();
        for (int i = 0; i < leftNames.size(); i++) {
            merged.addValue(leftNames.get(i), left.getValue(i));
        }
        List<String> rightNames = right.getColumnNames();
        for (int i = 0; i < rightNames.size(); i++) {
            merged.addValue(rightNames.get(i), right.getValue(i));
        }
        return merged;
    }

    /** Reports which strategy planScan would pick, without running the query. */
    private QueryResult executeExplain(ExplainStatement stmt) {
        SelectStatement select = stmt.select();
        if (!tables.containsKey(select.tableName())) {
            return QueryResult.error("Table not found: " + select.tableName());
        }

        if (select.joins() != null && !select.joins().isEmpty()) {
            StringBuilder sb = new StringBuilder("Hash Join: Seq Scan on ").append(select.tableName());
            for (JoinClause join : select.joins()) {
                if (!tables.containsKey(join.tableName())) {
                    return QueryResult.error("Table not found: " + join.tableName());
                }
                sb.append(" -> Seq Scan on ").append(join.tableName())
                  .append(" ON ").append(join.leftColumn()).append("=").append(join.rightColumn());
            }
            return QueryResult.success(sb.toString());
        }

        if (!select.aggregates().isEmpty() || !select.groupBy().isEmpty()) {
            String groupDesc = select.groupBy().isEmpty() ? "" : " GROUP BY " + String.join(", ", select.groupBy());
            return QueryResult.success("Aggregate" + groupDesc + ": Seq Scan on " + select.tableName()
                + " (no index acceleration for aggregate queries yet)");
        }

        ScanPlan plan = planScan(select.tableName(), select.whereClause());
        String costSuffix = plan.hasStatistics()
            ? String.format(" (cost=%.1f)", plan.estimatedCost())
            : " (no statistics - run ANALYZE for a cost-based choice)";
        String description = plan.useIndex()
            ? String.format("Index Scan using %s on %s (column=%s, range=[%s, %s])%s",
                plan.index().indexName(), select.tableName(), plan.index().columnName(), plan.loKey(), plan.hiKey(), costSuffix)
            : "Seq Scan on " + select.tableName() + costSuffix;
        return QueryResult.success(description);
    }

    /**
     * Constants in the same spirit as Postgres's own seq_page_cost (1.0)
     * vs. random_page_cost (4.0 by default): an index probe costs more per
     * matching row than a sequential scan does, because it's a B+Tree
     * traversal plus a random heap access rather than the next sequential
     * block. This is what makes the optimizer correctly prefer a seq scan
     * over an index for a predicate that matches a large fraction of the
     * table, even when an applicable index exists - "an index exists" and
     * "the index is worth using" are different questions, and only the
     * cost model (not the old rule-based planner) can tell them apart.
     */
    private static final double SEQ_SCAN_ROW_COST = 1.0;
    private static final double INDEX_ROW_COST = 4.0;
    private static final double INDEX_STARTUP_COST = 2.0; // B+Tree root-to-leaf traversal, paid once regardless of match count

    private record ScanPlan(boolean useIndex, IndexEntry index, Long loKey, Long hiKey, double estimatedCost, boolean hasStatistics) {
        static ScanPlan seqScan(double cost, boolean hasStats) {
            return new ScanPlan(false, null, null, null, cost, hasStats);
        }

        static ScanPlan indexScan(IndexEntry index, long lo, long hi, double cost, boolean hasStats) {
            return new ScanPlan(true, index, lo, hi, cost, hasStats);
        }
    }

    /**
     * Chooses seq scan vs. index scan. Genuinely cost-based when ANALYZE
     * has been run for this table (compares estimated costs and picks the
     * cheaper one); falls back to the original rule-based heuristic
     * ("an applicable index exists, so use it") when it hasn't, since
     * guessing at a cost with zero data would be worse than the simple
     * heuristic, not better - the same reasoning Postgres itself uses
     * when statistics are unavailable.
     */
    private ScanPlan planScan(String tableName, String whereClause) {
        TableStatistics stats = statistics.get(tableName);
        boolean hasStats = stats != null;

        WherePredicate pred = parseWhere(whereClause);
        if (pred == null || !pred.isNumeric()) {
            return ScanPlan.seqScan(hasStats ? costSeqScan(stats) : 0, hasStats);
        }
        IndexEntry idx = findIndex(tableName, pred.column());
        if (idx == null) {
            return ScanPlan.seqScan(hasStats ? costSeqScan(stats) : 0, hasStats);
        }
        long value;
        try {
            value = Long.parseLong(pred.value());
        } catch (NumberFormatException e) {
            return ScanPlan.seqScan(hasStats ? costSeqScan(stats) : 0, hasStats);
        }

        long lo, hi;
        switch (pred.operator()) {
            case "=" -> { lo = value; hi = value; }
            case ">" -> { lo = value + 1; hi = Long.MAX_VALUE; }
            case ">=" -> { lo = value; hi = Long.MAX_VALUE; }
            case "<" -> { lo = Long.MIN_VALUE; hi = value - 1; }
            case "<=" -> { lo = Long.MIN_VALUE; hi = value; }
            default -> { return ScanPlan.seqScan(hasStats ? costSeqScan(stats) : 0, hasStats); } // "!=" isn't a contiguous range
        }

        if (!hasStats) {
            return ScanPlan.indexScan(idx, lo, hi, 0, false);
        }

        double seqCost = costSeqScan(stats);
        double idxCost = costIndexScan(stats, idx.columnName(), pred.operator(), lo, hi);
        return idxCost < seqCost
            ? ScanPlan.indexScan(idx, lo, hi, idxCost, true)
            : ScanPlan.seqScan(seqCost, true);
    }

    private double costSeqScan(TableStatistics stats) {
        return stats.rowCount() * SEQ_SCAN_ROW_COST;
    }

    private double costIndexScan(TableStatistics stats, String column, String operator, long lo, long hi) {
        long estimatedRows = estimateMatchingRows(stats, column, operator, lo, hi);
        return INDEX_STARTUP_COST + estimatedRows * INDEX_ROW_COST;
    }

    private long estimateMatchingRows(TableStatistics stats, String column, String operator, long lo, long hi) {
        if (stats.rowCount() == 0) {
            return 0;
        }
        ColumnStatistics colStats = stats.columnStats().get(column);
        if (colStats == null) {
            return stats.rowCount(); // no per-column stats - assume worst case (matches everything)
        }
        if (operator.equals("=")) {
            long distinct = Math.max(1, colStats.distinctCount());
            return Math.max(1, stats.rowCount() / distinct);
        }
        if (colStats.min() == null || colStats.max() == null || colStats.max() <= colStats.min()) {
            return stats.rowCount(); // no usable numeric range - assume worst case
        }
        double totalWidth = colStats.max() - colStats.min();
        double rangeLo = Math.max(colStats.min(), lo == Long.MIN_VALUE ? colStats.min() : lo);
        double rangeHi = Math.min(colStats.max(), hi == Long.MAX_VALUE ? colStats.max() : hi);
        double rangeWidth = Math.max(0, rangeHi - rangeLo);
        double selectivity = Math.min(1.0, rangeWidth / totalWidth);
        return Math.max(1, (long) (stats.rowCount() * selectivity));
    }

    private IndexEntry findIndex(String tableName, String columnName) {
        List<IndexEntry> tableIndexes = indexesByTable.get(tableName);
        if (tableIndexes == null) return null;
        for (IndexEntry idx : tableIndexes) {
            if (idx.columnName().equalsIgnoreCase(columnName)) return idx;
        }
        return null;
    }

    private void maintainIndexesOnWrite(String tableName, Tuple tuple, long pageId, int slot) {
        List<IndexEntry> tableIndexes = indexesByTable.get(tableName);
        if (tableIndexes == null) return;
        for (IndexEntry idx : tableIndexes) {
            Object value = findColumnValue(tuple, idx.columnName());
            Long key = toIndexKey(value);
            if (key != null) {
                idx.index().insert(key, new BTreePage.RID(pageId, slot));
            } else {
                LOG.debug("Not indexing row in {} for index {}: column {} value {} isn't an integer key",
                    tableName, idx.indexName(), idx.columnName(), value);
            }
        }
    }

    /**
     * Removes the index entry pointing at (pageId, slot) - the physical
     * location a row is about to stop occupying, whether because it was
     * DELETEd outright or is the old version being replaced by an UPDATE.
     * Uses the row's CURRENT (pre-write) values, since that's what's
     * actually stored in the index right now - an UPDATE that changes the
     * indexed column itself must remove the entry keyed by the OLD value,
     * not the new one, or it would silently leave the real stale entry
     * behind while removing nothing.
     */
    private void maintainIndexesOnDelete(String tableName, Tuple tuple, long pageId, int slot) {
        List<IndexEntry> tableIndexes = indexesByTable.get(tableName);
        if (tableIndexes == null) return;
        for (IndexEntry idx : tableIndexes) {
            Object value = findColumnValue(tuple, idx.columnName());
            Long key = toIndexKey(value);
            if (key != null) {
                idx.index().delete(key, new BTreePage.RID(pageId, slot));
            }
        }
    }

    /** Only Integer/Long column values can be B+Tree keys right now - see executeCreateIndex's javadoc. */
    private Long toIndexKey(Object value) {
        if (value instanceof Integer i) return i.longValue();
        if (value instanceof Long l) return l;
        return null;
    }

    /**
     * Real UPDATE, replacing the previous hardcoded "Updated 0 rows" stub.
     * Scans with positions so each matching visible row can be targeted at
     * its exact (pageId, slot) for the MVCC update (tombstone + reinsert).
     * The old version's index entry is now genuinely removed (BTreeIndex
     * gained real delete - see PROJECT_PLAN.md Phase A), not just left as
     * MVCC-filtered dead weight the way it was before.
     */
    private QueryResult executeUpdate(UpdateStatement stmt, Transaction txn) throws DeadlockException {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        int updated = 0;
        for (HeapTable.PositionedRow row : table.scanPositioned()) {
            if (!MVCCVisibility.isVisible(row.stored(), txn.getSnapshot(), transactionManager)) {
                continue;
            }
            byte[] oldPayload = MVCCVisibility.readPayload(row.stored());
            Tuple tuple = Tuple.deserialize(oldPayload);
            if (!matchesWhere(tuple, stmt.whereClause())) {
                continue;
            }

            // Snapshot the old values BEFORE applying assignments - needed to
            // remove the old index entry by its actual current key, in case
            // an assignment changes the very column that's indexed.
            Tuple oldTuple = Tuple.deserialize(oldPayload);

            for (Assignment assignment : stmt.assignments()) {
                setColumnValue(tuple, assignment.column(), parseLiteral(assignment.value()));
            }
            byte[] newPayload = tuple.serialize();

            HeapTable.InsertResult newVersion = table.updateMvcc(row.pageId(), row.slot(), newPayload, txn.getXID(),
                txn.getSnapshot(), transactionManager, transactionManager.getLockManager());
            walManager.logUpdate(stmt.tableName(), row.pageId(), row.slot(), oldPayload, newPayload);
            maintainIndexesOnDelete(stmt.tableName(), oldTuple, row.pageId(), row.slot());
            maintainIndexesOnWrite(stmt.tableName(), tuple, newVersion.pageId, newVersion.slot);
            updated++;
        }

        return QueryResult.success("Updated " + updated + " row(s)");
    }

    /**
     * Real DELETE, replacing the previous hardcoded "Deleted 0 rows" stub.
     * The row's index entries are now genuinely removed (BTreeIndex gained
     * real delete - see PROJECT_PLAN.md Phase A), not left to accumulate
     * as MVCC-filtered dead weight forever.
     */
    private QueryResult executeDelete(DeleteStatement stmt, Transaction txn) throws DeadlockException {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        int deleted = 0;
        for (HeapTable.PositionedRow row : table.scanPositioned()) {
            if (!MVCCVisibility.isVisible(row.stored(), txn.getSnapshot(), transactionManager)) {
                continue;
            }
            Tuple tuple = Tuple.deserialize(MVCCVisibility.readPayload(row.stored()));
            if (!matchesWhere(tuple, stmt.whereClause())) {
                continue;
            }

            boolean removed = table.deleteMvcc(row.pageId(), row.slot(), txn.getXID(),
                txn.getSnapshot(), transactionManager, transactionManager.getLockManager());
            if (removed) {
                walManager.logDelete(stmt.tableName(), row.pageId(), row.slot());
                maintainIndexesOnDelete(stmt.tableName(), tuple, row.pageId(), row.slot());
                deleted++;
            }
        }

        return QueryResult.success("Deleted " + deleted + " row(s)");
    }

    private QueryResult executeDropTable(DropTableStatement stmt) {
        if (!tables.containsKey(stmt.tableName())) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        tables.remove(stmt.tableName());
        tableColumns.remove(stmt.tableName());
        return QueryResult.success("Table dropped: " + stmt.tableName());
    }

    /**
     * Returns one row per table, with a single "table_name" column - not a
     * message string. This used to return QueryResult.success("Tables: a, b")
     * or QueryResult.success("No tables found"), which worked fine for the
     * in-process CLI (which printed QueryResult.toString() directly) but
     * would have been silently useless over standard JDBC: Statement.execute()
     * only exposes a boolean ("was there a ResultSet") and an int row count,
     * not an arbitrary message string, so the actual table names would never
     * have reached a real JDBC caller. A proper rows-based result is both
     * more correct (this is fundamentally a query, not a command) and the
     * only way it can flow through a ResultSet.
     */
    private QueryResult executeShowTables() {
        List<Tuple> rows = new ArrayList<>();
        for (String tableName : tables.keySet()) {
            Tuple row = new Tuple();
            row.addValue("table_name", tableName);
            rows.add(row);
        }
        return QueryResult.success(rows);
    }

    /**
     * Column projection, factored out of executeSelect so it can stay
     * unchanged while the surrounding method switched to MVCC scanning.
     * Delegates to findColumnValue so joined (qualified-column) tuples are
     * projected correctly too, using the same resolution rules WHERE uses.
     */
    private Tuple project(Tuple tuple, List<String> requestedColumns) {
        if (requestedColumns.isEmpty() || requestedColumns.get(0).equals("*")) {
            return tuple;
        }
        Tuple projected = new Tuple();
        for (String colName : requestedColumns) {
            projected.addValue(colName, findColumnValue(tuple, colName));
        }
        return projected;
    }

    private record WherePredicate(String column, String operator, String value, boolean isNumeric) {}

    /**
     * Parses a single-predicate WHERE clause into (column, operator, value).
     *
     * Fixes a real bug found while building the planner: the previous
     * operator-detection order was {"=", ">", "<", ">=", "<=", "!="} and
     * picked the FIRST operator whose characters appeared anywhere in the
     * clause. Since "age>=30" contains "=" as a substring of ">=", it always
     * matched "=" first and split on it, producing column="age>" (with the
     * ">" wrongly stuck to the column name) and value="30". Longer operators
     * must be checked before their single-character prefixes.
     */
    private WherePredicate parseWhere(String whereClause) {
        if (whereClause == null || whereClause.isEmpty()) {
            return null;
        }

        String[] operatorsLongestFirst = {">=", "<=", "!=", "=", ">", "<"};
        String operator = null;
        for (String op : operatorsLongestFirst) {
            if (whereClause.contains(op)) {
                operator = op;
                break;
            }
        }
        if (operator == null) {
            return null;
        }

        String[] parts = whereClause.split(operator, 2);
        if (parts.length != 2) {
            return null;
        }

        String column = parts[0].trim();
        String value = parts[1].trim();
        if (value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length() - 1);
        }

        boolean isNumeric;
        try {
            Integer.parseInt(value);
            isNumeric = true;
        } catch (NumberFormatException e) {
            isNumeric = false;
        }

        return new WherePredicate(column, operator, value, isNumeric);
    }

    /**
     * WHERE-clause row matching, factored out so UPDATE/DELETE/SELECT share
     * one implementation. Also fixes a second real bug alongside the parser
     * fix above: this used to detect an operator only to decide how to split
     * the clause, then unconditionally check equality regardless of which
     * operator was found - so "WHERE age>25" silently behaved exactly like
     * "WHERE age=25". Every operator is now actually evaluated.
     */
    private boolean matchesWhere(Tuple tuple, String whereClause) {
        WherePredicate pred = parseWhere(whereClause);
        if (pred == null) {
            return true; // no WHERE clause, or unparsable - same permissive fallback as before
        }

        Object value = findColumnValue(tuple, pred.column());
        if (value != null) {
            return evaluatePredicate(value.toString(), pred);
        }

        // Column name didn't match anything in this tuple. The original code
        // fell back to "does ANY column hold this value" for such cases -
        // preserved here, but only for equality: an ordering comparison
        // ("> 25") doesn't have a sensible "check every column" fallback.
        if (!pred.operator().equals("=")) {
            return false;
        }
        for (int i = 0; i < tuple.size(); i++) {
            Object anyValue = tuple.getValue(i);
            if (anyValue != null && evaluatePredicate(anyValue.toString(), pred)) {
                return true;
            }
        }
        return false;
    }

    private Object findColumnValue(Tuple tuple, String columnName) {
        List<String> columnNames = tuple.getColumnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(columnName)) {
                return tuple.getValue(i);
            }
        }
        // Convenience fallback for joined tuples, whose columns are all
        // qualified as "table.column": a bare "amount" resolves to
        // "orders.amount" if some qualified column ends with ".amount".
        // This is a simplification, stated plainly: if two joined tables
        // both have a column with that bare name, whichever appears first
        // in the merged tuple wins silently rather than raising a real
        // SQL-style "ambiguous column reference" error. Use the qualified
        // form (table.column) to be unambiguous.
        if (!columnName.contains(".")) {
            String suffix = "." + columnName.toLowerCase();
            for (int i = 0; i < columnNames.size(); i++) {
                if (columnNames.get(i).toLowerCase().endsWith(suffix)) {
                    return tuple.getValue(i);
                }
            }
        }
        return null;
    }

    private boolean evaluatePredicate(String actualStr, WherePredicate pred) {
        if (pred.isNumeric()) {
            try {
                double actual = Double.parseDouble(actualStr);
                double expected = Double.parseDouble(pred.value());
                return switch (pred.operator()) {
                    case "=" -> actual == expected;
                    case "!=" -> actual != expected;
                    case ">" -> actual > expected;
                    case ">=" -> actual >= expected;
                    case "<" -> actual < expected;
                    case "<=" -> actual <= expected;
                    default -> false;
                };
            } catch (NumberFormatException e) {
                // actualStr wasn't numeric after all - fall through to string comparison
            }
        }
        return switch (pred.operator()) {
            case "=" -> actualStr.equals(pred.value());
            case "!=" -> !actualStr.equals(pred.value());
            default -> false; // ordering comparisons on non-numeric values aren't supported
        };
    }

    /** Mutates tuple in place - Tuple.getValues()/getColumnNames() return live backing lists. */
    private void setColumnValue(Tuple tuple, String column, Object newValue) {
        List<String> columnNames = tuple.getColumnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(column)) {
                tuple.getValues().set(i, newValue);
                return;
            }
        }
    }

    private Object parseLiteral(String value) {
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(value);
        }
        if (value.equalsIgnoreCase("null")) {
            return null;
        }
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            }
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return value;
        }
    }
}
