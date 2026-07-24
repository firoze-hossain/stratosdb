package com.stratosdb.sql.executor;

import com.stratosdb.common.exceptions.DeadlockException;
import com.stratosdb.sql.ast.*;
import com.stratosdb.sql.parser.SqlParser;
import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.heap.HeapTable;
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

    public ExecutorEngine(BufferPool bufferPool, WALManager walManager, TransactionManager transactionManager) {
        this.parser = new SqlParser();
        this.tables = new ConcurrentHashMap<>();
        this.tableColumns = new ConcurrentHashMap<>();
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
        if (stmt instanceof InsertStatement s) return executeInsert(s, txn);
        if (stmt instanceof SelectStatement s) return executeSelect(s, txn);
        if (stmt instanceof UpdateStatement s) return executeUpdate(s, txn);
        if (stmt instanceof DeleteStatement s) return executeDelete(s, txn);
        if (stmt instanceof DropTableStatement s) return executeDropTable(s);
        if (stmt instanceof ShowTablesStatement) return executeShowTables();
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

        return QueryResult.success("Inserted row at " + result.pageId + "/" + result.slot);
    }

    private QueryResult executeSelect(SelectStatement stmt, Transaction txn) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        List<byte[]> visibleRows = table.scanMvcc(txn.getSnapshot(), transactionManager);
        List<Tuple> tuples = new ArrayList<>();

        for (byte[] data : visibleRows) {
            Tuple tuple = Tuple.deserialize(data);
            if (!matchesWhere(tuple, stmt.whereClause())) {
                continue;
            }
            tuples.add(project(tuple, stmt.columns()));
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
     * Real UPDATE, replacing the previous hardcoded "Updated 0 rows" stub.
     * Scans with positions so each matching visible row can be targeted at
     * its exact (pageId, slot) for the MVCC update (tombstone + reinsert).
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

            table.updateMvcc(row.pageId(), row.slot(), newPayload, txn.getXID(),
                txn.getSnapshot(), transactionManager, transactionManager.getLockManager());
            walManager.logUpdate(stmt.tableName(), row.pageId(), row.slot(), oldPayload, newPayload);
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

    /**
     * WHERE-clause matching, factored out of executeSelect so UPDATE and
     * DELETE can share it instead of re-implementing (or worse, not
     * implementing) row filtering. Semantics are unchanged from the original
     * inline version - same simple single-predicate string matching, same
     * quirks - this is a straight extraction, not a rewrite. A real
     * WHERE-clause engine (AND/OR, proper AST, planner pushdown) is Week 3
     * (SQL engine + indexing) territory, not this pass.
     */
    private boolean matchesWhere(Tuple tuple, String whereClause) {
        if (whereClause == null || whereClause.isEmpty()) {
            return true;
        }

        String[] operators = {"=", ">", "<", ">=", "<=", "!="};
        String operator = "=";
        for (String op : operators) {
            if (whereClause.contains(op)) {
                operator = op;
                break;
            }
        }

        String[] parts = whereClause.split(operator);
        if (parts.length != 2) {
            return true; // unparsable WHERE - same permissive fallback as the original code
        }

        String whereColumn = parts[0].trim();
        String whereValue = parts[1].trim();
        if (whereValue.startsWith("'") && whereValue.endsWith("'")) {
            whereValue = whereValue.substring(1, whereValue.length() - 1);
        }

        boolean isNumericComparison;
        try {
            Integer.parseInt(whereValue);
            isNumericComparison = true;
        } catch (NumberFormatException e) {
            isNumericComparison = false;
        }

        List<String> columnNames = tuple.getColumnNames();
        int colIndex = -1;
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(whereColumn)) {
                colIndex = i;
                break;
            }
        }

        if (colIndex == -1) {
            for (int i = 0; i < tuple.size(); i++) {
                Object value = tuple.getValue(i);
                if (value == null) continue;
                String valueStr = value.toString();
                if (valueStr.equals(whereValue)) return true;
                if (isNumericComparison && numericEquals(valueStr, whereValue)) return true;
            }
            return false;
        }

        Object value = tuple.getValue(colIndex);
        if (value == null) return false;
        String valueStr = value.toString();
        if (valueStr.equals(whereValue)) return true;
        return isNumericComparison && numericEquals(valueStr, whereValue);
    }

    private boolean numericEquals(String a, String b) {
        try {
            return Double.parseDouble(a) == Double.parseDouble(b);
        } catch (NumberFormatException e) {
            return false;
        }
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
