package com.stratosdb.jdbc;

import com.stratosdb.network.stdwire.StdWireMessages;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A real, new DatabaseMetaData - the old driver never had one at all
 * (calling {@code Connection.getMetaData()} threw
 * SQLFeatureNotSupportedException). StratosDB has no pg_catalog or
 * information_schema emulation to delegate real catalog SQL to (a real,
 * separate, already-documented engine gap - see PROJECT_PLAN.md), so
 * this is built entirely from this engine's own real, existing, native
 * introspection commands instead: {@code SHOW TABLES} for the table
 * list, and {@code SHOW CATALOG} (which returns the exact, original
 * {@code CREATE TABLE}/{@code CREATE INDEX} DDL text this engine already
 * persists for its own restart survival) parsed for column/index detail.
 *
 * This is real, structural information, not a best-effort guess: column
 * names, declared types, {@code NOT NULL}, {@code DEFAULT}, and
 * {@code PRIMARY KEY} (both the column-level and table-level constraint
 * forms StratosDB's own CREATE TABLE grammar allows) are all parsed
 * directly from that real DDL text. What it can't tell a caller is
 * exactly what StratosDB itself doesn't track yet at all - e.g. foreign
 * keys (getImportedKeys/getExportedKeys - this engine has no FOREIGN KEY
 * support in its own CREATE TABLE grammar to report at all) - and those
 * honestly return an empty result rather than fabricating an answer.
 *
 * A deliberate, real difference from the rest of this driver's own
 * dynamic-proxy fallback: java.sql.Connection/Statement/ResultSet throw
 * SQLFeatureNotSupportedException for anything not explicitly
 * implemented, since callers gate advanced usage behind their own logic
 * and rarely invoke something exotic unconditionally. DatabaseMetaData
 * is different in practice - real tools (including DBNavigator itself)
 * routinely call dozens of cheap capability/limit methods
 * (supportsXxx()/getXxx() returning a fixed limit or an identifier
 * quote string) unconditionally during connection setup, before they've
 * decided whether to use any given feature at all. Throwing from any one
 * of those could break a tool's entire startup sequence just because
 * this class didn't explicitly enumerate that one method. So the
 * fallback here instead returns a safe, honest default based on the
 * method's own declared return type (false/0/""/null/an empty
 * ResultSet) rather than throwing - see the fallback() method below.
 */
class StratosDatabaseMetaData implements InvocationHandler {
    private final StratosConnection connection;

    private StratosDatabaseMetaData(StratosConnection connection) {
        this.connection = connection;
    }

    static DatabaseMetaData create(StratosConnection connection) {
        StratosDatabaseMetaData handler = new StratosDatabaseMetaData(connection);
        return (DatabaseMetaData) Proxy.newProxyInstance(
            StratosDatabaseMetaData.class.getClassLoader(), new Class[]{DatabaseMetaData.class}, handler);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();
        switch (name) {
            case "getConnection":
                return connection.asProxy();
            case "getDatabaseProductName":
                return "StratosDB";
            case "getDatabaseProductVersion":
                return "1.0.0";
            case "getDriverName":
                return "StratosDB JDBC Driver";
            case "getDriverVersion":
                return "1.0.0";
            case "getDriverMajorVersion":
                return 1;
            case "getDriverMinorVersion":
                return 0;
            case "getDatabaseMajorVersion":
                return 1;
            case "getDatabaseMinorVersion":
                return 0;
            case "getJDBCMajorVersion":
                return 4;
            case "getJDBCMinorVersion":
                return 2;
            case "getIdentifierQuoteString":
                return " "; // this engine has no quoted-identifier syntax at all yet - a single space is the real JDBC convention for "not supported"
            case "getSQLKeywords":
                return "";
            case "getNumericFunctions":
            case "getStringFunctions":
            case "getSystemFunctions":
            case "getTimeDateFunctions":
                return "";
            case "getSearchStringEscape":
                return "\\";
            case "getExtraNameCharacters":
                return "";
            case "getCatalogSeparator":
                return ".";
            case "getCatalogTerm":
                return "database";
            case "getSchemaTerm":
                return "schema";
            case "getProcedureTerm":
                return "procedure";
            case "getMaxConnections":
            case "getMaxStatements":
            case "getMaxTableNameLength":
            case "getMaxColumnNameLength":
            case "getMaxRowSize":
                return 0; // 0 = no limit, or limit unknown - the honest real JDBC convention for either case
            case "getResultSetHoldability":
                return ResultSet.CLOSE_CURSORS_AT_COMMIT;
            case "getDefaultTransactionIsolation":
                return java.sql.Connection.TRANSACTION_READ_COMMITTED;
            case "supportsTransactions":
                return true;
            case "supportsResultSetType":
                return args[0].equals(ResultSet.TYPE_FORWARD_ONLY);
            case "supportsResultSetConcurrency":
                return args[0].equals(ResultSet.TYPE_FORWARD_ONLY) && args[1].equals(ResultSet.CONCUR_READ_ONLY);
            case "getTables":
                return getTables((String) args[2]);
            case "getColumns":
                return getColumns((String) args[2], (String) args[3]);
            case "getPrimaryKeys":
                return getPrimaryKeys((String) args[2]);
            case "getIndexInfo":
                return getIndexInfo((String) args[2]);
            case "getSchemas":
            case "getCatalogs":
            case "getTableTypes":
            case "getImportedKeys":
            case "getExportedKeys":
            case "getCrossReference":
            case "getProcedures":
            case "getProcedureColumns":
            case "getUDTs":
            case "getSuperTypes":
            case "getSuperTables":
            case "getAttributes":
            case "getFunctions":
            case "getFunctionColumns":
            case "getClientInfoProperties":
            case "getTypeInfo":
                // Real, honest empty results - not fabricated data. Several of
                // these (foreign keys, stored procedures as a JDBC catalog
                // object, UDT hierarchies) have no real StratosDB counterpart
                // to report at all yet.
                return StratosResultSet.create(List.of(), List.of());
            case "toString":
                return "StratosDatabaseMetaData[]";
            case "equals":
                return proxy == args[0];
            case "hashCode":
                return System.identityHashCode(proxy);
            default:
                return fallback(method);
        }
    }

    /** See this class's own javadoc for why DatabaseMetaData's own fallback is permissive (a safe default per return type) rather than throwing, unlike the rest of this driver. */
    private Object fallback(Method method) {
        Class<?> returnType = method.getReturnType();
        if (returnType == boolean.class) return false;
        if (returnType == int.class) return 0;
        if (returnType == long.class) return 0L;
        if (returnType == short.class) return (short) 0;
        if (returnType == String.class) return "";
        if (returnType == ResultSet.class) return StratosResultSet.create(List.of(), List.of());
        return null;
    }

    // --- Real, native-introspection-backed schema information ---

    private ResultSet getTables(String tableNamePattern) throws SQLException {
        List<StdWireMessages.Column> columns = List.of(
            new StdWireMessages.Column("TABLE_CAT", 25, (short) -1),
            new StdWireMessages.Column("TABLE_SCHEM", 25, (short) -1),
            new StdWireMessages.Column("TABLE_NAME", 25, (short) -1),
            new StdWireMessages.Column("TABLE_TYPE", 25, (short) -1),
            new StdWireMessages.Column("REMARKS", 25, (short) -1),
            new StdWireMessages.Column("TYPE_CAT", 25, (short) -1),
            new StdWireMessages.Column("TYPE_SCHEM", 25, (short) -1),
            new StdWireMessages.Column("TYPE_NAME", 25, (short) -1),
            new StdWireMessages.Column("SELF_REFERENCING_COL_NAME", 25, (short) -1),
            new StdWireMessages.Column("REF_GENERATION", 25, (short) -1)
        );
        List<Object[]> rows = new ArrayList<>();
        for (String tableName : listTableNames()) {
            if (!matchesPattern(tableName, tableNamePattern)) continue;
            rows.add(new Object[]{null, null, tableName, "TABLE", null, null, null, null, null, null});
        }
        return StratosResultSet.create(columns, rows);
    }

    private ResultSet getColumns(String tableNamePattern, String columnNamePattern) throws SQLException {
        List<StdWireMessages.Column> columns = List.of(
            new StdWireMessages.Column("TABLE_CAT", 25, (short) -1),
            new StdWireMessages.Column("TABLE_SCHEM", 25, (short) -1),
            new StdWireMessages.Column("TABLE_NAME", 25, (short) -1),
            new StdWireMessages.Column("COLUMN_NAME", 25, (short) -1),
            new StdWireMessages.Column("DATA_TYPE", 23, (short) 4),
            new StdWireMessages.Column("TYPE_NAME", 25, (short) -1),
            new StdWireMessages.Column("COLUMN_SIZE", 23, (short) 4),
            new StdWireMessages.Column("BUFFER_LENGTH", 23, (short) 4),
            new StdWireMessages.Column("DECIMAL_DIGITS", 23, (short) 4),
            new StdWireMessages.Column("NUM_PREC_RADIX", 23, (short) 4),
            new StdWireMessages.Column("NULLABLE", 23, (short) 4),
            new StdWireMessages.Column("REMARKS", 25, (short) -1),
            new StdWireMessages.Column("COLUMN_DEF", 25, (short) -1),
            new StdWireMessages.Column("SQL_DATA_TYPE", 23, (short) 4),
            new StdWireMessages.Column("SQL_DATETIME_SUB", 23, (short) 4),
            new StdWireMessages.Column("CHAR_OCTET_LENGTH", 23, (short) 4),
            new StdWireMessages.Column("ORDINAL_POSITION", 23, (short) 4),
            new StdWireMessages.Column("IS_NULLABLE", 25, (short) -1),
            new StdWireMessages.Column("IS_AUTOINCREMENT", 25, (short) -1),
            new StdWireMessages.Column("IS_GENERATEDCOLUMN", 25, (short) -1)
        );
        List<Object[]> rows = new ArrayList<>();
        for (ParsedTable table : parseAllTables()) {
            if (!matchesPattern(table.name(), tableNamePattern)) continue;
            int position = 1;
            for (ParsedColumn col : table.columns()) {
                if (!matchesPattern(col.name(), columnNamePattern)) { position++; continue; }
                int jdbcType = jdbcTypeOf(col.type());
                boolean nullable = !col.notNull() && !col.primaryKey() && !table.primaryKeyColumns().contains(col.name());
                rows.add(new Object[]{
                    null, null, table.name(), col.name(),
                    String.valueOf(jdbcType), col.type().toUpperCase(),
                    "0", null, "0", "10",
                    String.valueOf(nullable ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls),
                    null, col.defaultValue(), null, null, null,
                    String.valueOf(position), nullable ? "YES" : "NO", "NO", "NO"
                });
                position++;
            }
        }
        return StratosResultSet.create(columns, rows);
    }

    private ResultSet getPrimaryKeys(String tableNamePattern) throws SQLException {
        List<StdWireMessages.Column> columns = List.of(
            new StdWireMessages.Column("TABLE_CAT", 25, (short) -1),
            new StdWireMessages.Column("TABLE_SCHEM", 25, (short) -1),
            new StdWireMessages.Column("TABLE_NAME", 25, (short) -1),
            new StdWireMessages.Column("COLUMN_NAME", 25, (short) -1),
            new StdWireMessages.Column("KEY_SEQ", 23, (short) 4),
            new StdWireMessages.Column("PK_NAME", 25, (short) -1)
        );
        List<Object[]> rows = new ArrayList<>();
        for (ParsedTable table : parseAllTables()) {
            if (!matchesPattern(table.name(), tableNamePattern)) continue;
            int seq = 1;
            for (String pkColumn : table.primaryKeyColumns()) {
                rows.add(new Object[]{null, null, table.name(), pkColumn, String.valueOf(seq++), null});
            }
        }
        return StratosResultSet.create(columns, rows);
    }

    private ResultSet getIndexInfo(String tableNamePattern) throws SQLException {
        List<StdWireMessages.Column> columns = List.of(
            new StdWireMessages.Column("TABLE_CAT", 25, (short) -1),
            new StdWireMessages.Column("TABLE_SCHEM", 25, (short) -1),
            new StdWireMessages.Column("TABLE_NAME", 25, (short) -1),
            new StdWireMessages.Column("NON_UNIQUE", 16, (short) 1),
            new StdWireMessages.Column("INDEX_QUALIFIER", 25, (short) -1),
            new StdWireMessages.Column("INDEX_NAME", 25, (short) -1),
            new StdWireMessages.Column("TYPE", 23, (short) 4),
            new StdWireMessages.Column("ORDINAL_POSITION", 23, (short) 4),
            new StdWireMessages.Column("COLUMN_NAME", 25, (short) -1),
            new StdWireMessages.Column("ASC_OR_DESC", 25, (short) -1),
            new StdWireMessages.Column("CARDINALITY", 23, (short) 4),
            new StdWireMessages.Column("PAGES", 23, (short) 4),
            new StdWireMessages.Column("FILTER_CONDITION", 25, (short) -1)
        );
        List<Object[]> rows = new ArrayList<>();
        for (ParsedIndex idx : parseAllIndexes()) {
            if (!matchesPattern(idx.tableName(), tableNamePattern)) continue;
            int position = 1;
            for (String col : idx.columnNames()) {
                rows.add(new Object[]{
                    null, null, idx.tableName(), "true", null, idx.indexName(),
                    String.valueOf(DatabaseMetaData.tableIndexOther), String.valueOf(position++),
                    col, "A", "0", "0", null
                });
            }
        }
        return StratosResultSet.create(columns, rows);
    }

    private boolean matchesPattern(String value, String pattern) {
        if (pattern == null || pattern.equals("%")) return true;
        String regex = "(?i)" + Pattern.quote(pattern).replace("%", "\\E.*\\Q").replace("_", "\\E.\\Q");
        return value.matches(regex);
    }

    private int jdbcTypeOf(String stratosType) {
        String t = stratosType.toUpperCase();
        if (t.startsWith("INT") || t.startsWith("SERIAL")) return Types.INTEGER;
        if (t.startsWith("BIGINT")) return Types.BIGINT;
        if (t.startsWith("FLOAT") || t.startsWith("DOUBLE") || t.startsWith("NUMERIC") || t.startsWith("DECIMAL")) return Types.DOUBLE;
        if (t.startsWith("BOOL")) return Types.BOOLEAN;
        if (t.startsWith("DATE")) return Types.DATE;
        if (t.startsWith("TIMESTAMP")) return Types.TIMESTAMP;
        return Types.VARCHAR;
    }

    // --- Native introspection: SHOW TABLES / SHOW CATALOG, parsed ---

    private List<String> listTableNames() throws SQLException {
        StratosConnection.WireResult result = connection.runSimpleQuery("SHOW TABLES");
        int nameIdx = columnIndex(result.columns(), "table_name");
        List<String> names = new ArrayList<>();
        for (Object[] row : result.rows()) {
            names.add((String) row[nameIdx]);
        }
        return names;
    }

    record ParsedColumn(String name, String type, boolean notNull, String defaultValue, boolean primaryKey) {}
    record ParsedTable(String name, List<ParsedColumn> columns, List<String> primaryKeyColumns) {}
    record ParsedIndex(String indexName, String tableName, List<String> columnNames) {}

    private List<ParsedTable> parseAllTables() throws SQLException {
        StratosConnection.WireResult result = connection.runSimpleQuery("SHOW CATALOG");
        int typeIdx = columnIndex(result.columns(), "object_type");
        int nameIdx = columnIndex(result.columns(), "object_name");
        int ddlIdx = columnIndex(result.columns(), "ddl_sql");
        List<ParsedTable> tables = new ArrayList<>();
        for (Object[] row : result.rows()) {
            if (!"TABLE".equals(row[typeIdx])) continue;
            tables.add(parseCreateTable((String) row[nameIdx], (String) row[ddlIdx]));
        }
        return tables;
    }

    private List<ParsedIndex> parseAllIndexes() throws SQLException {
        StratosConnection.WireResult result = connection.runSimpleQuery("SHOW CATALOG");
        int typeIdx = columnIndex(result.columns(), "object_type");
        int ddlIdx = columnIndex(result.columns(), "ddl_sql");
        List<ParsedIndex> indexes = new ArrayList<>();
        for (Object[] row : result.rows()) {
            if (!"INDEX".equals(row[typeIdx])) continue;
            ParsedIndex parsed = parseCreateIndex((String) row[ddlIdx]);
            if (parsed != null) indexes.add(parsed);
        }
        return indexes;
    }

    private int columnIndex(List<StdWireMessages.Column> columns, String name) throws SQLException {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equalsIgnoreCase(name)) return i;
        }
        throw new SQLException("Expected column not found in native introspection result: " + name);
    }

    /**
     * Real parsing of this engine's own real CREATE TABLE DDL text (see
     * StratosSQL.g4's own createTable/columnDef grammar rules) - not a
     * best-effort guess. Handles both the column-level PRIMARY KEY form
     * and the trailing, table-level PRIMARY KEY (col1, col2, ...) form,
     * NOT NULL, and DEFAULT value, with real paren-depth tracking so a
     * type like NUMERIC(10,2) doesn't get misread as multiple columns.
     */
    static ParsedTable parseCreateTable(String tableName, String ddl) {
        int openParen = ddl.indexOf('(');
        int closeParen = ddl.lastIndexOf(')');
        String body = (openParen >= 0 && closeParen > openParen) ? ddl.substring(openParen + 1, closeParen) : "";
        List<String> segments = splitTopLevel(body);

        List<ParsedColumn> columns = new ArrayList<>();
        List<String> tableLevelPk = new ArrayList<>();
        for (String segment : segments) {
            String trimmed = segment.trim();
            if (trimmed.isEmpty()) continue;
            if (trimmed.toUpperCase().startsWith("PRIMARY KEY")) {
                int pkOpen = trimmed.indexOf('(');
                int pkClose = trimmed.lastIndexOf(')');
                if (pkOpen >= 0 && pkClose > pkOpen) {
                    for (String col : trimmed.substring(pkOpen + 1, pkClose).split(",")) {
                        tableLevelPk.add(col.trim());
                    }
                }
                continue;
            }
            columns.add(parseColumnDef(trimmed));
        }

        List<String> primaryKeyColumns = new ArrayList<>(tableLevelPk);
        for (ParsedColumn col : columns) {
            if (col.primaryKey() && !primaryKeyColumns.contains(col.name())) {
                primaryKeyColumns.add(col.name());
            }
        }
        return new ParsedTable(tableName, columns, primaryKeyColumns);
    }

    private static final Pattern DEFAULT_PATTERN = Pattern.compile("(?i)DEFAULT\\s+('[^']*'|\\S+)");

    private static ParsedColumn parseColumnDef(String def) {
        String upper = def.toUpperCase();
        boolean notNull = upper.contains("NOT NULL");
        boolean primaryKey = upper.trim().endsWith("PRIMARY KEY");
        String defaultValue = null;
        Matcher m = DEFAULT_PATTERN.matcher(def);
        if (m.find()) {
            defaultValue = m.group(1);
        }

        // Column name is the first token; the type is everything up to (but not
        // including) NOT NULL / DEFAULT / a trailing PRIMARY KEY - real paren
        // groups in the type itself (e.g. NUMERIC(10,2)) are part of the type,
        // not a constraint, so only whole-word constraint keywords end it.
        String[] firstSplit = def.trim().split("\\s+", 2);
        String name = firstSplit[0];
        String rest = firstSplit.length > 1 ? firstSplit[1] : "";
        String type = rest
            .replaceAll("(?i)\\s+NOT\\s+NULL.*$", "")
            .replaceAll("(?i)\\s+DEFAULT\\s+('[^']*'|\\S+).*$", "")
            .replaceAll("(?i)\\s+PRIMARY\\s+KEY\\s*$", "")
            .trim();
        return new ParsedColumn(name, type, notNull, defaultValue, primaryKey);
    }

    /** Parses this engine's own real, already-reconstructed CREATE INDEX text (see ExecutorEngine.executeShowCatalog's own javadoc: real for every object type except INDEX, which this engine's own catalog reconstructs into real DDL text specifically for this method to read back). */
    static ParsedIndex parseCreateIndex(String ddl) {
        Pattern p = Pattern.compile("(?i)CREATE\\s+INDEX\\s+(\\S+)\\s+ON\\s+(\\S+)\\s*\\(([^)]*)\\)");
        Matcher m = p.matcher(ddl);
        if (!m.find()) return null;
        String indexName = m.group(1);
        String tableName = m.group(2);
        List<String> cols = new ArrayList<>();
        for (String c : m.group(3).split(",")) {
            cols.add(c.trim());
        }
        return new ParsedIndex(indexName, tableName, cols);
    }

    /** Splits a comma-separated list at paren-depth 0 only - a real, necessary distinction from a naive split(","), since a column's own type (e.g. NUMERIC(10,2)) or a table-level PRIMARY KEY(a, b) both contain commas that must NOT split the list at that point. */
    private static List<String> splitTopLevel(String s) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) {
                parts.add(s.substring(start, i));
                start = i + 1;
            }
        }
        if (start < s.length()) {
            parts.add(s.substring(start));
        }
        return parts;
    }
}
