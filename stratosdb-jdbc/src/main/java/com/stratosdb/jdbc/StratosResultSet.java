package com.stratosdb.jdbc;

import com.stratosdb.storage.page.Tuple;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.stratosdb.jdbc.JdbcSupport.notSupported;

/**
 * Real behavior: next, close, isClosed, wasNull, getMetaData, getRow,
 * isBeforeFirst/isAfterLast/isFirst/isLast, findColumn, and get* for the
 * types this engine actually produces (String, Integer, Long, Double,
 * Boolean, plus a generic getObject) - both by 1-based column index and by
 * column name, per the JDBC contract.
 *
 * Forward-only, read-only (TYPE_FORWARD_ONLY / CONCUR_READ_ONLY) - there is
 * no cursor scrolling and no updatable-result-set support. Everything else
 * required by java.sql.ResultSet (203 methods on that interface - the
 * largest of the JDBC interfaces this driver touches) throws
 * SQLFeatureNotSupportedException via the shared fallback, same
 * dynamic-proxy approach as StratosConnection/StratosStatement.
 */
class StratosResultSet implements InvocationHandler {
    private final List<Tuple> rows;
    private final StratosResultSetMetaData metaData;
    private int currentIndex = -1; // before-first
    private boolean closed = false;
    private boolean lastWasNull = false;

    private StratosResultSet(List<Tuple> rows) {
        this.rows = rows;
        this.metaData = StratosResultSetMetaData.fromRows(rows);
    }

    static ResultSet create(List<Tuple> rows) {
        StratosResultSet handler = new StratosResultSet(rows);
        return (ResultSet) Proxy.newProxyInstance(
            StratosResultSet.class.getClassLoader(), new Class[]{ResultSet.class}, handler);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();
        switch (name) {
            case "next":
                checkOpen();
                currentIndex++;
                return currentIndex < rows.size();
            case "close":
                closed = true;
                return null;
            case "isClosed":
                return closed;
            case "wasNull":
                return lastWasNull;
            case "getMetaData":
                return metaData;
            case "getRow":
                return currentIndex + 1;
            case "isBeforeFirst":
                return currentIndex < 0 && !rows.isEmpty();
            case "isAfterLast":
                return currentIndex >= rows.size() && !rows.isEmpty();
            case "isFirst":
                return currentIndex == 0;
            case "isLast":
                return !rows.isEmpty() && currentIndex == rows.size() - 1;
            case "findColumn": {
                String colName = (String) args[0];
                List<String> names = currentTuple() != null ? currentTuple().getColumnNames() : List.of();
                for (int i = 0; i < names.size(); i++) {
                    if (names.get(i).equalsIgnoreCase(colName)) return i + 1;
                }
                throw new SQLException("Column not found: " + colName);
            }
            case "getString":
                return asString(resolve(args[0]));
            case "getObject":
                return resolve(args[0]);
            case "getInt":
                return asInt(resolve(args[0]));
            case "getLong":
                return asLong(resolve(args[0]));
            case "getDouble":
                return asDouble(resolve(args[0]));
            case "getBoolean":
                return asBoolean(resolve(args[0]));
            case "getFetchSize":
                return 0;
            case "setFetchSize":
                return null;
            case "getFetchDirection":
                return ResultSet.FETCH_FORWARD;
            case "getType":
                return ResultSet.TYPE_FORWARD_ONLY;
            case "getConcurrency":
                return ResultSet.CONCUR_READ_ONLY;
            case "getWarnings":
                return null;
            case "clearWarnings":
                return null;
            case "getStatement":
                return null; // this ResultSet doesn't retain a back-reference to the Statement that produced it
            case "toString":
                return "StratosResultSet[row=" + (currentIndex + 1) + "/" + rows.size() + ", closed=" + closed + "]";
            case "equals":
                return proxy == args[0];
            case "hashCode":
                return System.identityHashCode(proxy);
            default:
                throw notSupported("ResultSet", name);
        }
    }

    private Tuple currentTuple() {
        return (currentIndex >= 0 && currentIndex < rows.size()) ? rows.get(currentIndex) : null;
    }

    /** arg is either a 1-based column index (Integer) or a column name (String), per the JDBC overloads. */
    private Object resolve(Object columnRef) throws SQLException {
        Tuple tuple = currentTuple();
        if (tuple == null) {
            throw new SQLException("No current row - call next() first");
        }
        Object value;
        if (columnRef instanceof Integer idx) {
            if (idx < 1 || idx > tuple.size()) {
                throw new SQLException("Column index out of range: " + idx);
            }
            value = tuple.getValue(idx - 1);
        } else {
            String colName = (String) columnRef;
            value = null;
            List<String> names = tuple.getColumnNames();
            boolean found = false;
            for (int i = 0; i < names.size(); i++) {
                if (names.get(i).equalsIgnoreCase(colName)) {
                    value = tuple.getValue(i);
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new SQLException("Column not found: " + colName);
            }
        }
        lastWasNull = (value == null);
        return value;
    }

    private static String asString(Object v) {
        return v == null ? null : v.toString();
    }

    private static int asInt(Object v) {
        if (v == null) return 0; // JDBC contract: getInt() on a null column returns 0, check wasNull() to distinguish
        if (v instanceof Number n) return n.intValue();
        return Integer.parseInt(v.toString());
    }

    private static long asLong(Object v) {
        if (v == null) return 0;
        if (v instanceof Number n) return n.longValue();
        return Long.parseLong(v.toString());
    }

    private static double asDouble(Object v) {
        if (v == null) return 0;
        if (v instanceof Number n) return n.doubleValue();
        return Double.parseDouble(v.toString());
    }

    private static boolean asBoolean(Object v) {
        if (v == null) return false;
        if (v instanceof Boolean b) return b;
        if (v instanceof Number n) return n.intValue() != 0;
        return Boolean.parseBoolean(v.toString());
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }
}
