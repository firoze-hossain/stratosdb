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

        // Store column names
        List<String> columns = new ArrayList<>();
        for (ColumnDefinition col : stmt.columns()) {
            columns.add(col.name());
        }
        tableColumns.put(stmt.tableName(), columns);

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

        // Create tuple with column names
        Tuple tuple = new Tuple();
        List<String> columns = tableColumns.get(stmt.tableName());
        if (columns != null) {
            for (int i = 0; i < values.size() && i < columns.size(); i++) {
                tuple.addValue(columns.get(i), values.get(i));
            }
        } else {
            // Fallback to col0, col1, col2
            for (int i = 0; i < values.size(); i++) {
                tuple.addValue("col" + i, values.get(i));
            }
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

        // Parse WHERE clause to extract column name and value
        String whereColumn = null;
        String whereValue = null;
        boolean isNumericComparison = false;

        if (stmt.whereClause() != null && !stmt.whereClause().isEmpty()) {
            String whereClause = stmt.whereClause();
            LOG.debug("WHERE clause: {}", whereClause);

            // Handle different operators
            String[] operators = {"=", ">", "<", ">=", "<=", "!="};
            String operator = "=";

            for (String op : operators) {
                if (whereClause.contains(op)) {
                    operator = op;
                    break;
                }
            }

            String[] parts = whereClause.split(operator);
            if (parts.length == 2) {
                whereColumn = parts[0].trim();
                whereValue = parts[1].trim();

                // Remove quotes from value if present
                if (whereValue.startsWith("'") && whereValue.endsWith("'")) {
                    whereValue = whereValue.substring(1, whereValue.length() - 1);
                }

                // Check if it's a number
                try {
                    Integer.parseInt(whereValue);
                    isNumericComparison = true;
                } catch (NumberFormatException e) {
                    isNumericComparison = false;
                }

                LOG.debug("WHERE: column={}, value={}, isNumeric={}", whereColumn, whereValue, isNumericComparison);
            }
        }

        for (byte[] data : rawTuples) {
            Tuple tuple = Tuple.deserialize(data);
            boolean matches = true;

            // Apply WHERE filter
            if (whereColumn != null && whereValue != null) {
                matches = false;

                // Get all column names
                List<String> columnNames = tuple.getColumnNames();

                // Find the matching column index
                int colIndex = -1;
                for (int i = 0; i < columnNames.size(); i++) {
                    if (columnNames.get(i).equalsIgnoreCase(whereColumn)) {
                        colIndex = i;
                        break;
                    }
                }

                // If column not found by name, try all columns
                if (colIndex == -1) {
                    // Try to match value against any column
                    for (int i = 0; i < tuple.size(); i++) {
                        Object value = tuple.getValue(i);
                        if (value != null) {
                            String valueStr = value.toString();
                            if (valueStr.equals(whereValue)) {
                                matches = true;
                                break;
                            }
                            // Try numeric comparison
                            if (isNumericComparison) {
                                try {
                                    double num1 = Double.parseDouble(valueStr);
                                    double num2 = Double.parseDouble(whereValue);
                                    if (num1 == num2) {
                                        matches = true;
                                        break;
                                    }
                                } catch (NumberFormatException e) {
                                    // Not a number, skip
                                }
                            }
                        }
                    }
                } else {
                    // Column found, compare its value
                    Object value = tuple.getValue(colIndex);
                    if (value != null) {
                        String valueStr = value.toString();
                        if (valueStr.equals(whereValue)) {
                            matches = true;
                        } else if (isNumericComparison) {
                            try {
                                double num1 = Double.parseDouble(valueStr);
                                double num2 = Double.parseDouble(whereValue);
                                if (num1 == num2) {
                                    matches = true;
                                }
                            } catch (NumberFormatException e) {
                                // Not a number, skip
                            }
                        }
                    }
                }

                if (!matches) {
                    continue;
                }
            }

            // Project columns
            if (!stmt.columns().isEmpty() && !stmt.columns().get(0).equals("*")) {
                Tuple projected = new Tuple();
                for (String colName : stmt.columns()) {
                    Object value = null;
                    // Try to find by column name
                    List<String> columnNames = tuple.getColumnNames();
                    for (int i = 0; i < columnNames.size(); i++) {
                        if (columnNames.get(i).equalsIgnoreCase(colName)) {
                            value = tuple.getValue(i);
                            break;
                        }
                    }
                    projected.addValue(colName, value);
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

        return QueryResult.success("Updated 0 rows");
    }

    private QueryResult executeDelete(DeleteStatement stmt) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        return QueryResult.success("Deleted 0 rows");
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