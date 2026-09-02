package com.stratosdb.jdbc;

import com.stratosdb.network.stdwire.StdWireMessages;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Real column metadata from the real server's own RowDescription message -
 * a genuine improvement over the old driver's own approach (inferring
 * column types from the first row's actual Java values, since the old,
 * dead protocol carried whole in-process Tuple objects with no separate
 * wire-level column description at all): ResultSet and ResultSetMetaData
 * now both derive from the one, real, server-authored column list, rather
 * than ResultSetMetaData re-deriving its own answer from row content.
 *
 * A real, honestly-named limitation this does NOT fix: a genuinely
 * zero-row SELECT still has an empty RowDescription (see StratosStatement's
 * own javadoc) - this engine's own real, pre-existing limitation at the
 * server layer, not something a client-side rewrite can paper over. A
 * ResultSet built from a zero-row query reports zero columns here.
 */
class StratosResultSetMetaData implements ResultSetMetaData {
    private final List<StdWireMessages.Column> columns;

    StratosResultSetMetaData(List<StdWireMessages.Column> columns) {
        this.columns = columns;
    }

    /** Maps this engine's own real, wire-level type OIDs (see StdWireServer.inferTypeOid) to real java.sql.Types codes. */
    static int sqlTypeOf(int typeOid) {
        return switch (typeOid) {
            case 23 -> Types.INTEGER;  // int4
            case 701 -> Types.DOUBLE;  // float8
            case 16 -> Types.BOOLEAN;  // bool
            default -> Types.VARCHAR; // 25 (text) and the safe default for anything else
        };
    }

    private StdWireMessages.Column columnAt(int index) throws SQLException {
        if (index < 1 || index > columns.size()) {
            throw new SQLException("Column index out of range: " + index);
        }
        return columns.get(index - 1);
    }

    @Override public int getColumnCount() { return columns.size(); }
    @Override public String getColumnName(int column) throws SQLException { return columnAt(column).name(); }
    @Override public String getColumnLabel(int column) throws SQLException { return columnAt(column).name(); }
    @Override public int getColumnType(int column) throws SQLException { return sqlTypeOf(columnAt(column).typeOid()); }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return switch (getColumnType(column)) {
            case Types.INTEGER -> "INTEGER";
            case Types.DOUBLE -> "DOUBLE";
            case Types.BOOLEAN -> "BOOLEAN";
            default -> "VARCHAR";
        };
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return switch (getColumnType(column)) {
            case Types.INTEGER -> "java.lang.Integer";
            case Types.DOUBLE -> "java.lang.Double";
            case Types.BOOLEAN -> "java.lang.Boolean";
            default -> "java.lang.String";
        };
    }

    @Override public boolean isAutoIncrement(int column) { return false; }
    @Override public boolean isCaseSensitive(int column) { return true; }
    @Override public boolean isSearchable(int column) { return true; }
    @Override public boolean isCurrency(int column) { return false; }
    @Override public int isNullable(int column) { return columnNullableUnknown; } // this driver doesn't fetch NOT NULL constraints for a plain result set - see StratosDatabaseMetaData.getColumns() for that
    @Override public boolean isSigned(int column) throws SQLException { int t = getColumnType(column); return t == Types.INTEGER || t == Types.DOUBLE; }
    @Override public int getColumnDisplaySize(int column) { return 128; }
    @Override public String getSchemaName(int column) { return ""; }
    @Override public int getPrecision(int column) { return 0; }
    @Override public int getScale(int column) { return 0; }
    @Override public String getTableName(int column) { return ""; } // joined-query columns are qualified in the name itself; not tracked separately here
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
