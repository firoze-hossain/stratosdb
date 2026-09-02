package com.stratosdb.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.stratosdb.jdbc.JdbcSupport.notSupported;

/**
 * Real behavior: executeQuery, executeUpdate, execute (the generic form,
 * tracking whether the result was a row set or an update for the
 * getResultSet()/getUpdateCount() follow-up calls), close, isClosed,
 * getConnection - all now over the real, current wire protocol via
 * {@code StratosConnection.runSimpleQuery} (see that class's own javadoc
 * for why this is a full rewrite, not a patch, of what used to be here).
 *
 * A row-returning statement is recognized by its own real CommandComplete
 * tag starting with "SELECT" - the real, current server tags every
 * row-returning statement this way (a genuine SELECT, and this engine's
 * own native SHOW TABLES/SHOW CATALOG/etc. commands alike - see
 * StdWireServer's own executeShowTables and buildCommandTag), which is a
 * more reliable signal than an empty column list, since a SELECT that
 * genuinely matches zero rows also has an empty column list (RowDescription
 * has no row to introspect column names from at all - a real, documented,
 * pre-existing limitation of this engine's own simple query protocol, not
 * something this driver can work around, but one it must not misinterpret
 * as "this wasn't a query at all").
 *
 * Forward-only, read-only. Everything else required by java.sql.Statement
 * throws SQLFeatureNotSupportedException via the shared fallback, same
 * dynamic-proxy approach as StratosConnection.
 */
class StratosStatement implements InvocationHandler {
    private final StratosConnection connection;
    private volatile boolean closed = false;
    private StratosConnection.WireResult lastResult;

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
                lastResult = connection.runSimpleQuery((String) args[0]);
                if (!isRowReturning(lastResult)) {
                    throw new SQLException("executeQuery() called with a statement that produced no result set "
                        + "(command tag: " + lastResult.commandTag() + ") - use executeUpdate() for INSERT/UPDATE/DELETE/DDL");
                }
                return StratosResultSet.create(lastResult.columns(), lastResult.rows());
            }
            case "executeUpdate": {
                checkOpen();
                lastResult = connection.runSimpleQuery((String) args[0]);
                return parseUpdateCount(lastResult.commandTag());
            }
            case "execute": {
                checkOpen();
                lastResult = connection.runSimpleQuery((String) args[0]);
                return isRowReturning(lastResult);
            }
            case "getResultSet":
                if (lastResult == null || !isRowReturning(lastResult)) {
                    return null;
                }
                return StratosResultSet.create(lastResult.columns(), lastResult.rows());
            case "getUpdateCount":
                return lastResult == null ? -1 : parseUpdateCount(lastResult.commandTag());
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

    static boolean isRowReturning(StratosConnection.WireResult result) {
        return result.commandTag().startsWith("SELECT");
    }

    /** Parses a real Postgres-style CommandComplete tag ("SELECT n", "INSERT 0 n", "UPDATE n", "DELETE n", "CREATE TABLE", "BEGIN", ...) into a real JDBC update count - -1 for a row-returning statement (per the JDBC contract: not an update count at all), the tag's own trailing number for INSERT/UPDATE/DELETE, 0 for anything else (DDL, transaction control). */
    static int parseUpdateCount(String commandTag) {
        if (commandTag.startsWith("SELECT")) {
            return -1;
        }
        String[] parts = commandTag.trim().split("\\s+");
        String last = parts[parts.length - 1];
        try {
            return Integer.parseInt(last);
        } catch (NumberFormatException e) {
            return 0; // DDL (CREATE TABLE, DROP TABLE, ...) or transaction control (BEGIN, COMMIT, ROLLBACK) - no row count at all
        }
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }
}
