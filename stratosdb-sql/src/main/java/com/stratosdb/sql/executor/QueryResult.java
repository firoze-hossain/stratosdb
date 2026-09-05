package com.stratosdb.sql.executor;

import com.stratosdb.storage.page.Tuple;

import java.util.List;

public class QueryResult {
    private final boolean success;
    private final String message;
    private final List<Tuple> rows;
    private final String error;
    /**
     * A result's own real, known column names, set explicitly by a caller
     * that knows them regardless of row count - null (the default) when
     * not given, meaning the wire layer falls back to deriving column
     * names from the first row (see StdWireServer.describeColumns), which
     * is impossible when there are genuinely zero rows to look at. Real,
     * previously-latent bug this fixes: a query with a static, always-
     * known shape (SHOW TABLES, SHOW DATABASES, ...) run against a
     * database that genuinely has zero matching objects - a perfectly
     * normal, common situation, not an error - would report zero
     * *columns* too, not just zero rows, since there was no first row
     * left to derive "table_name" (or whatever the real column was)
     * from. A real client (DatabaseMetaData.getTables(), for example)
     * that expects that column to exist regardless of row count would
     * then fail outright on an empty result, even though "zero tables"
     * is a completely valid answer.
     */
    private final List<String> columnNames;

    private QueryResult(boolean success, String message, List<Tuple> rows, String error, List<String> columnNames) {
        this.success = success;
        this.message = message;
        this.rows = rows;
        this.error = error;
        this.columnNames = columnNames;
    }

    public static QueryResult success(String message) {
        return new QueryResult(true, message, null, null, null);
    }

    public static QueryResult success(List<Tuple> rows) {
        return new QueryResult(true, rows.size() + " rows", rows, null, null);
    }

    /**
     * For a result whose own column shape is statically known regardless
     * of how many rows actually matched - see this class's own
     * columnNames javadoc for the real bug this exists to fix. Pass the
     * real, exact column names in the same order every row in
     * {@code rows} (if any) already uses.
     */
    public static QueryResult success(List<Tuple> rows, List<String> columnNames) {
        return new QueryResult(true, rows.size() + " rows", rows, null, columnNames);
    }

    public static QueryResult error(String error) {
        return new QueryResult(false, null, null, error, null);
    }

    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
    public List<Tuple> getRows() { return rows; }
    public String getError() { return error; }
    public List<String> getColumnNames() { return columnNames; }

    @Override
    public String toString() {
        if (!success) return "ERROR: " + error;
        if (rows != null && !rows.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("┌─────────────────────────────────────┐\n");
            for (Tuple tuple : rows) {
                sb.append("│ ").append(tuple).append("\n");
            }
            sb.append("└─────────────────────────────────────┘\n");
            sb.append("(").append(rows.size()).append(" rows)");
            return sb.toString();
        }
        return message != null ? message : "Success";
    }
}