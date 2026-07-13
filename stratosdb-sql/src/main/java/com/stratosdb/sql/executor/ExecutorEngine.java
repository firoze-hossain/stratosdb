package com.stratosdb.sql.executor;

import com.stratosdb.sql.ast.*;
import com.stratosdb.sql.parser.SqlParser;
import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.storage.wal.WALManager;
import com.stratosdb.transaction.TransactionManager;
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

    public ExecutorEngine(BufferPool bufferPool, WALManager walManager, TransactionManager transactionManager) {
        this.parser = new SqlParser();
        this.tables = new ConcurrentHashMap<>();
        this.bufferPool = bufferPool;
        this.walManager = walManager;
        this.transactionManager = transactionManager;
    }

    public QueryResult execute(String sql) {
        try {
            Statement stmt = parser.parse(sql);

            if (stmt instanceof CreateTableStatement) {
                return executeCreateTable((CreateTableStatement) stmt);
            } else if (stmt instanceof InsertStatement) {
                return executeInsert((InsertStatement) stmt);
            } else if (stmt instanceof SelectStatement) {
                return executeSelect((SelectStatement) stmt);
            } else if (stmt instanceof UpdateStatement) {
                return executeUpdate((UpdateStatement) stmt);
            } else if (stmt instanceof DeleteStatement) {
                return executeDelete((DeleteStatement) stmt);
            } else if (stmt instanceof DropTableStatement) {
                return executeDropTable((DropTableStatement) stmt);
            } else if (stmt instanceof ShowTablesStatement) {
                return executeShowTables();
            }

            return QueryResult.error("Unsupported statement");
        } catch (Exception e) {
            LOG.error("Execution failed: {}", sql, e);
            return QueryResult.error(e.getMessage());
        }
    }

    private QueryResult executeCreateTable(CreateTableStatement stmt) {
        if (tables.containsKey(stmt.tableName())) {
            return QueryResult.error("Table already exists: " + stmt.tableName());
        }

        HeapTable table = new HeapTable(stmt.tableName(), bufferPool);
        tables.put(stmt.tableName(), table);

        return QueryResult.success("Table created: " + stmt.tableName());
    }

    private QueryResult executeInsert(InsertStatement stmt) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        // Parse values
        List<Object> values = new ArrayList<>();
        for (String valueStr : stmt.values()) {
            values.add(parseLiteral(valueStr));
        }

        // Create tuple - use generic column names
        Tuple tuple = new Tuple();
        for (int i = 0; i < values.size(); i++) {
            tuple.addValue("col" + i, values.get(i));
        }

        byte[] data = tuple.serialize();
        HeapTable.InsertResult result = table.insert(data);

        // Log to WAL
        walManager.logInsert(stmt.tableName(), result.pageId, result.slot, data);

        return QueryResult.success("Inserted row at " + result.pageId + "/" + result.slot);
    }

    private QueryResult executeSelect(SelectStatement stmt) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        List<byte[]> rawTuples = table.scan();
        List<Tuple> tuples = new ArrayList<>();

        for (byte[] data : rawTuples) {
            Tuple tuple = Tuple.deserialize(data);

            // Apply WHERE filter (simple equality)
            if (stmt.whereClause() != null && !stmt.whereClause().isEmpty() &&
                    !matchesFilter(tuple, stmt.whereClause())) {
                continue;
            }

            // Project columns
            if (!stmt.columns().isEmpty() && !stmt.columns().get(0).equals("*")) {
                Tuple projected = new Tuple();
                for (String colName : stmt.columns()) {
                    projected.addValue(colName, tuple.getValue(colName));
                }
                tuples.add(projected);
            } else {
                tuples.add(tuple);
            }
        }

        // Apply LIMIT
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

    private QueryResult executeUpdate(UpdateStatement stmt) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        // Not fully implemented - would need to scan, filter, update
        return QueryResult.success("Updated 0 rows");
    }

    private QueryResult executeDelete(DeleteStatement stmt) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        // Simplified - would need to scan, filter, delete
        return QueryResult.success("Deleted 0 rows");
    }

    private QueryResult executeDropTable(DropTableStatement stmt) {
        if (!tables.containsKey(stmt.tableName())) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        tables.remove(stmt.tableName());
        return QueryResult.success("Table dropped: " + stmt.tableName());
    }

    private QueryResult executeShowTables() {
        List<String> tableNames = new ArrayList<>(tables.keySet());
        if (tableNames.isEmpty()) {
            return QueryResult.success("No tables found");
        }
        return QueryResult.success("Tables: " + String.join(", ", tableNames));
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

    private boolean matchesFilter(Tuple tuple, String whereClause) {
        // Simple parser for "column = value"
        if (whereClause == null || whereClause.isEmpty()) {
            return true;
        }

        String[] parts = whereClause.split("=");
        if (parts.length == 2) {
            String column = parts[0].trim();
            String value = parts[1].trim().replace("'", "");
            Object tupleValue = tuple.getValue(column);
            if (tupleValue == null) return false;
            return tupleValue.toString().equals(value);
        }
        return true;
    }
}