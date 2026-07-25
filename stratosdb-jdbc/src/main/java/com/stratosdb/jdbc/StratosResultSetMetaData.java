package com.stratosdb.jdbc;

import com.stratosdb.storage.page.Tuple;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Column types are inferred from the first row's actual Java values, since
 * Tuple carries no declared column-type schema at all - a real limitation
 * worth naming rather than hiding: a column that's null in the first row
 * (or in every row) is reported as VARCHAR by default, and this engine
 * doesn't enforce a column's type to stay consistent across rows, so a
 * column whose actual type varies row to row only reports the first row's
 * type here. Good enough for typical read/display use with a JDBC tool;
 * not a substitute for real catalog-backed metadata.
 */
class StratosResultSetMetaData implements ResultSetMetaData {
    private final List<String> columnNames;
    private final List<Integer> sqlTypes;

    private StratosResultSetMetaData(List<String> columnNames, List<Integer> sqlTypes) {
        this.columnNames = columnNames;
        this.sqlTypes = sqlTypes;
    }

    static StratosResultSetMetaData fromRows(List<Tuple> rows) {
        if (rows.isEmpty()) {
            return new StratosResultSetMetaData(List.of(), List.of());
        }
        Tuple first = rows.get(0);
        List<String> names = new ArrayList<>(first.getColumnNames());
        List<Integer> types = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            types.add(sqlTypeOf(first.getValue(i)));
        }
        return new StratosResultSetMetaData(names, types);
    }

    private static int sqlTypeOf(Object value) {
        if (value instanceof Integer) return Types.INTEGER;
        if (value instanceof Long) return Types.BIGINT;
        if (value instanceof Double) return Types.DOUBLE;
        if (value instanceof Boolean) return Types.BOOLEAN;
        return Types.VARCHAR;
    }

    private String nameAt(int column) throws SQLException {
        if (column < 1 || column > columnNames.size()) {
            throw new SQLException("Column index out of range: " + column);
        }
        return columnNames.get(column - 1);
    }

    private int typeAt(int column) throws SQLException {
        if (column < 1 || column > sqlTypes.size()) {
            throw new SQLException("Column index out of range: " + column);
        }
        return sqlTypes.get(column - 1);
    }

    @Override public int getColumnCount() { return columnNames.size(); }
    @Override public String getColumnName(int column) throws SQLException { return nameAt(column); }
    @Override public String getColumnLabel(int column) throws SQLException { return nameAt(column); }
    @Override public int getColumnType(int column) throws SQLException { return typeAt(column); }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return switch (typeAt(column)) {
            case Types.INTEGER -> "INTEGER";
            case Types.BIGINT -> "BIGINT";
            case Types.DOUBLE -> "DOUBLE";
            case Types.BOOLEAN -> "BOOLEAN";
            default -> "VARCHAR";
        };
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return switch (typeAt(column)) {
            case Types.INTEGER -> "java.lang.Integer";
            case Types.BIGINT -> "java.lang.Long";
            case Types.DOUBLE -> "java.lang.Double";
            case Types.BOOLEAN -> "java.lang.Boolean";
            default -> "java.lang.String";
        };
    }

    @Override public boolean isAutoIncrement(int column) { return false; }
    @Override public boolean isCaseSensitive(int column) { return true; }
    @Override public boolean isSearchable(int column) { return true; }
    @Override public boolean isCurrency(int column) { return false; }
    @Override public int isNullable(int column) { return columnNullableUnknown; } // this engine doesn't track NOT NULL constraints yet
    @Override public boolean isSigned(int column) throws SQLException { int t = typeAt(column); return t == Types.INTEGER || t == Types.BIGINT || t == Types.DOUBLE; }
    @Override public int getColumnDisplaySize(int column) { return 128; }
    @Override public String getSchemaName(int column) { return ""; }
    @Override public int getPrecision(int column) { return 0; }
    @Override public int getScale(int column) { return 0; }
    @Override public String getTableName(int column) { return ""; } // joined-query columns are qualified in the name itself (see JOIN support); not tracked separately here
    @Override public String getCatalogName(int column) { return ""; }
    @Override public boolean isReadOnly(int column) { return true; }
    @Override public boolean isWritable(int column) { return false; }
    @Override public boolean isDefinitelyWritable(int column) { return false; }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return (T) this;
        throw new SQLException("Not a wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }
}
