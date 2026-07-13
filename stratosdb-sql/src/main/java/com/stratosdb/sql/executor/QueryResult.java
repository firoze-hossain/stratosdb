package com.stratosdb.sql.executor;

import com.stratosdb.storage.page.Tuple;

import java.util.List;

public class QueryResult {
    private final boolean success;
    private final String message;
    private final List<Tuple> rows;
    private final String error;

    private QueryResult(boolean success, String message, List<Tuple> rows, String error) {
        this.success = success;
        this.message = message;
        this.rows = rows;
        this.error = error;
    }

    public static QueryResult success(String message) {
        return new QueryResult(true, message, null, null);
    }

    public static QueryResult success(List<Tuple> rows) {
        return new QueryResult(true, rows.size() + " rows", rows, null);
    }

    public static QueryResult error(String error) {
        return new QueryResult(false, null, null, error);
    }

    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
    public List<Tuple> getRows() { return rows; }
    public String getError() { return error; }

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