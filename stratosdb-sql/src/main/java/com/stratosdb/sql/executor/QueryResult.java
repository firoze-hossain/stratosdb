package com.stratosdb.sql.executor;

import com.stratosdb.sql.execution.PhysicalOperator;
import com.stratosdb.sql.execution.TupleSlot;
import com.stratosdb.storage.page.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class QueryResult implements Iterable<Tuple>, AutoCloseable {
    private final boolean success;
    private final String message;
    private List<Tuple> rows;
    private final String error;
    private final List<String> columnNames;
    private final String sqlState;
    private final Exception exception;
    private final PhysicalOperator operator;

    private QueryResult(boolean success, String message, List<Tuple> rows, String error, List<String> columnNames) {
        this(success, message, rows, error, columnNames, "XX000", null, null);
    }

    private QueryResult(boolean success, String message, List<Tuple> rows, String error, List<String> columnNames, String sqlState, Exception exception) {
        this(success, message, rows, error, columnNames, sqlState, exception, null);
    }

    private QueryResult(boolean success, String message, List<Tuple> rows, String error, List<String> columnNames, String sqlState, Exception exception, PhysicalOperator operator) {
        this.success = success;
        this.message = message;
        this.rows = rows;
        this.error = error;
        this.columnNames = columnNames;
        this.sqlState = sqlState != null ? sqlState : (error != null ? "XX000" : null);
        this.exception = exception;
        this.operator = operator;
    }

    public static QueryResult success(String message) {
        return new QueryResult(true, message, null, null, null, null, null, null);
    }

    public static QueryResult success(List<Tuple> rows) {
        return new QueryResult(true, rows.size() + " rows", rows, null, null, null, null, null);
    }

    public static QueryResult success(List<Tuple> rows, List<String> columnNames) {
        return new QueryResult(true, rows.size() + " rows", rows, null, columnNames, null, null, null);
    }

    /**
     * Constructs a streaming QueryResult backed by a Volcano PhysicalOperator pipeline.
     * Rows are streamed on demand one by one without accumulating into an in-memory list.
     */
    public static QueryResult success(PhysicalOperator operator, List<String> columnNames) {
        return new QueryResult(true, "Volcano streaming query", null, null, columnNames, null, null, operator);
    }

    public static QueryResult error(String error) {
        return new QueryResult(false, null, null, error, null, "XX000", null, null);
    }

    public static QueryResult error(String error, String sqlState) {
        return new QueryResult(false, null, null, error, null, sqlState, null, null);
    }

    public static QueryResult error(String error, String sqlState, Exception exception) {
        return new QueryResult(false, null, null, error, null, sqlState, exception, null);
    }

    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }

    public synchronized List<Tuple> getRows() {
        if (rows != null) {
            return rows;
        }
        if (operator != null) {
            List<Tuple> collected = new ArrayList<>();
            try {
                operator.open();
                while (true) {
                    TupleSlot slot = operator.next();
                    if (slot == null || slot.isEmpty()) break;
                    collected.add(slot.getTuple());
                }
            } finally {
                operator.close();
            }
            this.rows = collected;
            return collected;
        }
        return null;
    }

    public String getError() { return error; }
    public List<String> getColumnNames() {
        if (columnNames != null) return columnNames;
        if (operator != null) return operator.getOutputColumns();
        return null;
    }
    public String getSqlState() { return sqlState != null ? sqlState : (error != null ? "XX000" : null); }
    public Exception getException() { return exception; }
    public PhysicalOperator getOperator() { return operator; }
    public boolean isStreaming() { return operator != null; }

    @Override
    public void close() {
        if (operator != null) {
            operator.close();
        }
    }

    @Override
    public Iterator<Tuple> iterator() {
        if (operator != null) {
            return new Iterator<>() {
                private TupleSlot current = null;
                private boolean opened = false;
                private boolean done = false;

                private void advance() {
                    if (!opened) {
                        operator.open();
                        opened = true;
                    }
                    if (done) return;
                    if (current == null) {
                        current = operator.next();
                        if (current == null || current.isEmpty()) {
                            done = true;
                            operator.close();
                        }
                    }
                }

                @Override
                public boolean hasNext() {
                    advance();
                    return !done && current != null && !current.isEmpty();
                }

                @Override
                public Tuple next() {
                    advance();
                    if (done || current == null || current.isEmpty()) throw new NoSuchElementException();
                    Tuple t = current.getTuple();
                    current = null;
                    return t;
                }
            };
        } else if (rows != null) {
            return rows.iterator();
        }
        return Collections.emptyIterator();
    }

    @Override
    public String toString() {
        if (!success) return "ERROR: " + error;
        List<Tuple> r = getRows();
        if (r != null && !r.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("┌─────────────────────────────────────┐\n");
            for (Tuple tuple : r) {
                sb.append("│ ").append(tuple).append("\n");
            }
            sb.append("└─────────────────────────────────────┘\n");
            sb.append("(").append(r.size()).append(" rows)");
            return sb.toString();
        }
        return message != null ? message : "Success";
    }
}
