package com.stratosdb.jdbc;

import com.stratosdb.network.stdwire.StdWireMessages;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static com.stratosdb.jdbc.JdbcSupport.notSupported;

/**
 * Real behavior: next, close, isClosed, wasNull, getMetaData, getRow,
 * isBeforeFirst/isAfterLast/isFirst/isLast, findColumn, and get* for
 * String/Integer/Long/Double/Boolean/Object - both by 1-based column
 * index and by column name, per the JDBC contract. Backed directly by
 * this engine's own real, wire-level row values (see StratosConnection's
 * own javadoc for why this is a full rewrite, not a patch: the old
 * driver held a list of in-process {@code Tuple} objects from the dead
 * legacy protocol; this one holds the real text values a real server
 * sent over a real socket, converted per the column's own real type OID
 * from RowDescription).
 *
 * Every wire value arrives as text (see StdWireMessages's own javadoc);
 * getInt/getLong/getDouble/getBoolean all parse from that text rather
 * than assuming a particular Java runtime type, so they work regardless
 * of a caller mixing up which getter "should" match a given column's
 * declared SQL type - the same forgiving behavior real JDBC drivers
 * offer.
 *
 * Forward-only, read-only. Everything else required by java.sql.ResultSet
 * throws SQLFeatureNotSupportedException via the shared fallback, same
 * dynamic-proxy approach as StratosConnection/StratosStatement.
 */
class StratosResultSet implements InvocationHandler {
    private final List<StdWireMessages.Column> columns;
    private final List<Object[]> rows;
    private final StratosResultSetMetaData metaData;
    private int currentIndex = -1; // before-first
    private boolean closed = false;
    private boolean lastWasNull = false;

    private StratosResultSet(List<StdWireMessages.Column> columns, List<Object[]> rows) {
        this.columns = columns;
        this.rows = rows;
        this.metaData = new StratosResultSetMetaData(columns);
    }

    static ResultSet create(List<StdWireMessages.Column> columns, List<Object[]> rows) {
        StratosResultSet handler = new StratosResultSet(columns, rows);
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
                int idx = columnIndexOf(colName);
                if (idx < 0) throw new SQLException("Column not found: " + colName);
                return idx + 1;
            }
            case "getString":
                return asString(resolve(args[0]));
            case "getObject":
                return asNaturalType(resolveWithColumn(args[0]));
            case "getInt":
                return asInt(resolve(args[0]));
            case "getLong":
                return asLong(resolve(args[0]));
            case "getDouble":
                return asDouble(resolve(args[0]));
            case "getBoolean":
                return asBoolean(resolve(args[0]));
            case "getDate":
                return asDate(resolve(args[0]));
            case "getTimestamp":
                return asTimestamp(resolve(args[0]));
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

    private int columnIndexOf(String colName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equalsIgnoreCase(colName)) return i;
        }
        return -1;
    }

    private Object[] currentRow() {
        return (currentIndex >= 0 && currentIndex < rows.size()) ? rows.get(currentIndex) : null;
    }

    /** arg is either a 1-based column index (Integer) or a column name (String), per the JDBC overloads. */
    private Object resolve(Object columnRef) throws SQLException {
        return resolveWithColumn(columnRef)[0];
    }

    /** Returns {value, columnIndex(0-based)} so getObject() can also consult the column's own type OID. */
    private Object[] resolveWithColumn(Object columnRef) throws SQLException {
        Object[] row = currentRow();
        if (row == null) {
            throw new SQLException("No current row - call next() first");
        }
        int idx;
        if (columnRef instanceof Integer i) {
            idx = i - 1;
            if (idx < 0 || idx >= row.length) {
                throw new SQLException("Column index out of range: " + i);
            }
        } else {
            idx = columnIndexOf((String) columnRef);
            if (idx < 0) {
                throw new SQLException("Column not found: " + columnRef);
            }
        }
        Object value = row[idx];
        lastWasNull = (value == null);
        return new Object[]{value, idx};
    }

    private static String asString(Object v) {
        return v == null ? null : v.toString();
    }

    private Object asNaturalType(Object[] valueAndIndex) {
        Object v = valueAndIndex[0];
        if (v == null) return null;
        int idx = (Integer) valueAndIndex[1];
        int typeOid = columns.get(idx).typeOid();
        String text = v.toString();
        try {
            return switch (typeOid) {
                case 23 -> Integer.parseInt(text);
                case 701 -> Double.parseDouble(text);
                case 16 -> Boolean.parseBoolean(text);
                default -> text;
            };
        } catch (NumberFormatException e) {
            return text; // the raw text is still a valid, honest answer even if it didn't parse as the inferred type
        }
    }

    private static int asInt(Object v) {
        if (v == null) return 0; // JDBC contract: getInt() on a null column returns 0, check wasNull() to distinguish
        return (int) Double.parseDouble(v.toString());
    }

    private static long asLong(Object v) {
        if (v == null) return 0;
        return (long) Double.parseDouble(v.toString());
    }

    private static double asDouble(Object v) {
        if (v == null) return 0;
        return Double.parseDouble(v.toString());
    }

    private static boolean asBoolean(Object v) {
        if (v == null) return false;
        return Boolean.parseBoolean(v.toString());
    }

    private static java.sql.Date asDate(Object v) throws SQLException {
        if (v == null) return null;
        try {
            return java.sql.Date.valueOf(v.toString());
        } catch (IllegalArgumentException e) {
            throw new SQLException("Column value is not a valid date (expected yyyy-MM-dd): " + v, e);
        }
    }

    private static java.sql.Timestamp asTimestamp(Object v) throws SQLException {
        if (v == null) return null;
        try {
            return java.sql.Timestamp.valueOf(v.toString());
        } catch (IllegalArgumentException e) {
            throw new SQLException("Column value is not a valid timestamp (expected yyyy-MM-dd HH:mm:ss[.f...]): " + v, e);
        }
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }
}
