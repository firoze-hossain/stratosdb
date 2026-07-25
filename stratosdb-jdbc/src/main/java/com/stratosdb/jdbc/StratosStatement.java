package com.stratosdb.jdbc;

import com.stratosdb.sql.executor.QueryResult;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.stratosdb.jdbc.JdbcSupport.notSupported;

/**
 * Real behavior: executeQuery, executeUpdate (with a best-effort affected-
 * row count parsed from the server's message - "Updated N row(s)" /
 * "Deleted N row(s)" map to N, a successful INSERT maps to 1, DDL maps to
 * 0), execute (the generic form, tracking whether the result was a row set
 * or an update for the getResultSet()/getUpdateCount() follow-up calls),
 * close, isClosed, getConnection.
 *
 * Everything else required by java.sql.Statement (61 methods on that
 * interface) throws SQLFeatureNotSupportedException via the shared
 * fallback - same dynamic-proxy approach as StratosConnection, for the
 * same reason (61 hand-written stubs vs. one fallback case).
 */
class StratosStatement implements InvocationHandler {
    private static final Pattern ROW_COUNT_PATTERN = Pattern.compile("(\\d+) row\\(s\\)");

    private final StratosConnection connection;
    private volatile boolean closed = false;
    private QueryResult lastResult;

    private StratosStatement(StratosConnection connection) {
        this.connection = connection;
    }

    static Statement create(StratosConnection connection) {
        StratosStatement handler = new StratosStatement(connection);
        return (Statement) Proxy.newProxyInstance(
            StratosStatement.class.getClassLoader(), new Class[]{Statement.class}, handler);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();
        switch (name) {
            case "executeQuery": {
                checkOpen();
                QueryResult result = runQuery((String) args[0]);
                if (result.getRows() == null) {
                    throw new SQLException("executeQuery() called with a statement that produced no result set "
                        + "(message: " + result.getMessage() + ") - use executeUpdate() for INSERT/UPDATE/DELETE/DDL");
                }
                return StratosResultSet.create(result.getRows());
            }
            case "executeUpdate": {
                checkOpen();
                QueryResult result = runQuery((String) args[0]);
                return parseUpdateCount(result);
            }
            case "execute": {
                checkOpen();
                lastResult = runQuery((String) args[0]);
                return lastResult.getRows() != null;
            }
            case "getResultSet":
                if (lastResult == null || lastResult.getRows() == null) {
                    return null;
                }
                return StratosResultSet.create(lastResult.getRows());
            case "getUpdateCount":
                return lastResult == null ? -1 : parseUpdateCount(lastResult);
            case "getMoreResults":
                return false; // this driver never produces multiple result sets from one statement
            case "close":
                closed = true;
                return null;
            case "isClosed":
                return closed;
            case "getConnection":
                return connection.asProxy();
            case "setMaxRows":
            case "setQueryTimeout":
            case "setFetchSize":
            case "setFetchDirection":
            case "setEscapeProcessing":
            case "setCursorName":
            case "setPoolable":
                return null; // accepted and ignored - honest no-ops, not thrown, since many tools call these defensively
            case "getMaxRows":
            case "getFetchSize":
            case "getQueryTimeout":
                return 0;
            case "getFetchDirection":
                return ResultSet.FETCH_FORWARD;
            case "getResultSetType":
                return ResultSet.TYPE_FORWARD_ONLY;
            case "getResultSetConcurrency":
                return ResultSet.CONCUR_READ_ONLY;
            case "getWarnings":
                return null;
            case "clearWarnings":
                return null;
            case "toString":
                return "StratosStatement[closed=" + closed + "]";
            case "equals":
                return proxy == args[0];
            case "hashCode":
                return System.identityHashCode(proxy);
            default:
                throw notSupported("Statement", name);
        }
    }

    private QueryResult runQuery(String sql) throws SQLException {
        QueryResult result = connection.execute(sql);
        if (!result.isSuccess()) {
            throw new SQLException(result.getError());
        }
        return result;
    }

    private int parseUpdateCount(QueryResult result) {
        String message = result.getMessage();
        if (message == null) {
            return 0;
        }
        if (message.startsWith("Inserted row at")) {
            return 1;
        }
        Matcher m = ROW_COUNT_PATTERN.matcher(message);
        if (m.find()) {
            return Integer.parseInt(m.group(1));
        }
        return 0; // DDL (CREATE TABLE, CREATE INDEX, DROP TABLE) or anything else with no row count
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }
}
