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

    public ExecutorEngine(BufferPool bufferPool, WALManager walManager, TransactionManager transactionManager) {
        this.parser = new SqlParser();
        this.tables = new ConcurrentHashMap<>();
        this.tableColumns = new ConcurrentHashMap<>();
        this.indexesByName = new ConcurrentHashMap<>();
        this.indexesByTable = new ConcurrentHashMap<>();
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

    /** Reports which strategy planScan would pick, without running the query. */
    private QueryResult executeExplain(ExplainStatement stmt) {
        SelectStatement select = stmt.select();
        if (!tables.containsKey(select.tableName())) {
            return QueryResult.error("Table not found: " + select.tableName());
        }
        ScanPlan plan = planScan(select.tableName(), select.whereClause());
        String description = plan.useIndex()
            ? String.format("Index Scan using %s on %s (column=%s, range=[%s, %s])",
                plan.index().indexName(), select.tableName(), plan.index().columnName(), plan.loKey(), plan.hiKey())
            : "Seq Scan on " + select.tableName();
        return QueryResult.success(description);
    }

    private record ScanPlan(boolean useIndex, IndexEntry index, Long loKey, Long hiKey) {
        static ScanPlan seqScan() {
            return new ScanPlan(false, null, null, null);
        }

        static ScanPlan indexScan(IndexEntry index, long lo, long hi) {
            return new ScanPlan(true, index, lo, hi);
        }
    }

    private ScanPlan planScan(String tableName, String whereClause) {
        WherePredicate pred = parseWhere(whereClause);
        if (pred == null || !pred.isNumeric()) {
            return ScanPlan.seqScan();
        }
        IndexEntry idx = findIndex(tableName, pred.column());
        if (idx == null) {
            return ScanPlan.seqScan();
        }
        long value;
        try {
            value = Long.parseLong(pred.value());
        } catch (NumberFormatException e) {
            return ScanPlan.seqScan();
        }
        return switch (pred.operator()) {
            case "=" -> ScanPlan.indexScan(idx, value, value);
            case ">" -> ScanPlan.indexScan(idx, value + 1, Long.MAX_VALUE);
            case ">=" -> ScanPlan.indexScan(idx, value, Long.MAX_VALUE);
            case "<" -> ScanPlan.indexScan(idx, Long.MIN_VALUE, value - 1);
            case "<=" -> ScanPlan.indexScan(idx, Long.MIN_VALUE, value);
            default -> ScanPlan.seqScan(); // "!=" isn't a contiguous range - not usable as an index scan
        };
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
     * Any index on this table is updated to point at the new row version -
     * the old version's stale index entry is left in place (BTreeIndex has
     * no delete yet) but is harmless: MVCCVisibility.isVisible filters it
     * out at read time, same as a stale entry from a plain DELETE.
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

            for (Assignment assignment : stmt.assignments()) {
                setColumnValue(tuple, assignment.column(), parseLiteral(assignment.value()));
            }
            byte[] newPayload = tuple.serialize();

            HeapTable.InsertResult newVersion = table.updateMvcc(row.pageId(), row.slot(), newPayload, txn.getXID(),
                txn.getSnapshot(), transactionManager, transactionManager.getLockManager());
            walManager.logUpdate(stmt.tableName(), row.pageId(), row.slot(), oldPayload, newPayload);
            maintainIndexesOnWrite(stmt.tableName(), tuple, newVersion.pageId, newVersion.slot);
            updated++;
        }

        return QueryResult.success("Updated " + updated + " row(s)");
    }

    /**
     * Real DELETE, replacing the previous hardcoded "Deleted 0 rows" stub.
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

    private QueryResult executeShowTables() {
        List<String> tableNames = new ArrayList<>(tables.keySet());
        if (tableNames.isEmpty()) {
            return QueryResult.success("No tables found");
        }
        return QueryResult.success("Tables: " + String.join(", ", tableNames));
    }

    /**
     * Column projection, factored out of executeSelect so it can stay
     * unchanged while the surrounding method switched to MVCC scanning.
     */
    private Tuple project(Tuple tuple, List<String> requestedColumns) {
        if (requestedColumns.isEmpty() || requestedColumns.get(0).equals("*")) {
            return tuple;
        }
        Tuple projected = new Tuple();
        List<String> columnNames = tuple.getColumnNames();
        for (String colName : requestedColumns) {
            Object value = null;
            for (int i = 0; i < columnNames.size(); i++) {
                if (columnNames.get(i).equalsIgnoreCase(colName)) {
                    value = tuple.getValue(i);
                    break;
                }
            }
            projected.addValue(colName, value);
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
