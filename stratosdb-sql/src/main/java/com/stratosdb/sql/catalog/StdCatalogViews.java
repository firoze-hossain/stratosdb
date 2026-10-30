package com.stratosdb.sql.catalog;

import com.stratosdb.sql.ast.SelectStatement;
import com.stratosdb.sql.executor.ExecutorEngine;
import com.stratosdb.storage.page.Tuple;

import java.util.*;

/**
 * Standard system catalog views for StratosDB.
 *
 * Implements virtual system views:
 *   - std_class / stratos_class / pg_class / pg_catalog.pg_class
 *   - std_attribute / stratos_attribute / pg_attribute / pg_catalog.pg_attribute
 *   - std_type / stratos_type / pg_type / pg_catalog.pg_type
 *   - std_namespace / stratos_namespace / pg_namespace / pg_catalog.pg_namespace
 *   - std_index / stratos_index / pg_index / pg_catalog.pg_index
 *   - information_schema.tables / information_schema_tables / tables
 *   - std_database / stratos_database / pg_database / pg_catalog.pg_database
 *   - std_am / stratos_am / pg_am / pg_catalog.pg_am
 *   - std_tables / stratos_tables / pg_tables / pg_catalog.pg_tables
 *   - std_views / stratos_views / pg_views / pg_catalog.pg_views
 *   - std_indexes / stratos_indexes / pg_indexes / pg_catalog.pg_indexes
 *
 * These virtual views allow standard CLI tools (e.g. stdsql \d, \dt, \l, tab-completion,
 * JDBC metadata drivers, ORMs, and admin tools) and standard SQL queries to introspect
 * StratosDB schemas seamlessly with independent native naming.
 */
public final class StdCatalogViews {

    private StdCatalogViews() {}

    public static boolean isVirtualView(String tableName) {
        if (tableName == null) return false;
        String name = stripQuotes(tableName).toLowerCase(Locale.ROOT);
        return switch (canonicalViewName(name)) {
            case "class", "attribute", "type", "namespace", "index",
                 "information_schema.tables", "database", "am",
                 "tables", "views", "indexes" -> true;
            default -> false;
        };
    }

    public static String canonicalViewName(String tableName) {
        if (tableName == null) return "";
        String name = stripQuotes(tableName).toLowerCase(Locale.ROOT);
        if (name.startsWith("pg_catalog.")) {
            name = name.substring("pg_catalog.".length());
        } else if (name.startsWith("std_catalog.")) {
            name = name.substring("std_catalog.".length());
        } else if (name.startsWith("stratos_catalog.")) {
            name = name.substring("stratos_catalog.".length());
        }

        if (name.equals("information_schema_tables") || name.equals("information_schema.tables") || name.equals("tables")) {
            return "information_schema.tables";
        }

        return switch (name) {
            case "pg_class", "std_class", "stratos_class", "class" -> "class";
            case "pg_attribute", "std_attribute", "stratos_attribute", "attribute" -> "attribute";
            case "pg_type", "std_type", "stratos_type", "type" -> "type";
            case "pg_namespace", "std_namespace", "stratos_namespace", "namespace" -> "namespace";
            case "pg_index", "std_index", "stratos_index", "index" -> "index";
            case "pg_database", "std_database", "stratos_database", "database" -> "database";
            case "pg_am", "std_am", "stratos_am", "am" -> "am";
            case "pg_tables", "std_tables", "stratos_tables" -> "tables";
            case "pg_views", "std_views", "stratos_views" -> "views";
            case "pg_indexes", "std_indexes", "stratos_indexes" -> "indexes";
            default -> name;
        };
    }

    public static int getRelationOid(String relName) {
        if (relName == null) return 0;
        return 16384 + (Math.abs(relName.toLowerCase(Locale.ROOT).hashCode()) % 800000);
    }

    public static int getIndexOid(String indexName) {
        if (indexName == null) return 0;
        return 1000000 + (Math.abs(indexName.toLowerCase(Locale.ROOT).hashCode()) % 800000);
    }

    public static int getTypeOid(String typeName) {
        if (typeName == null) return 25; // TEXT
        String t = typeName.trim().toUpperCase(Locale.ROOT);
        if (t.startsWith("INT") || t.equals("INTEGER")) {
            if (t.contains("8") || t.contains("BIG")) return 20; // int8
            if (t.contains("2") || t.contains("SMALL")) return 21; // int2
            return 23; // int4
        }
        if (t.equals("VARCHAR") || t.startsWith("VARCHAR") || t.equals("CHAR") || t.equals("TEXT") || t.equals("STRING")) return 1043;
        if (t.equals("BOOLEAN") || t.equals("BOOL")) return 16;
        if (t.equals("FLOAT") || t.equals("DOUBLE") || t.equals("REAL") || t.equals("NUMERIC") || t.equals("DECIMAL")) return 701;
        if (t.equals("TIMESTAMP") || t.startsWith("TIMESTAMP")) return 1114;
        if (t.equals("DATE")) return 1082;
        if (t.equals("TIME")) return 1083;
        if (t.equals("JSON")) return 114;
        if (t.equals("JSONB")) return 3802;
        if (t.equals("BYTEA")) return 17;
        if (t.equals("INET")) return 869;
        if (t.equals("CIDR")) return 650;
        return 1043;
    }

    public static String formatType(int typeOid) {
        return switch (typeOid) {
            case 16 -> "boolean";
            case 17 -> "bytea";
            case 20 -> "bigint";
            case 21 -> "smallint";
            case 23 -> "integer";
            case 701 -> "double precision";
            case 1043 -> "character varying";
            case 25 -> "text";
            case 1082 -> "date";
            case 1083 -> "time without time zone";
            case 1114 -> "timestamp without time zone";
            case 114 -> "json";
            case 3802 -> "jsonb";
            case 869 -> "inet";
            case 650 -> "cidr";
            default -> "character varying";
        };
    }

    public static List<Tuple> generateRows(String viewName, ExecutorEngine engine) {
        String canonical = canonicalViewName(viewName);
        return switch (canonical) {
            case "class" -> generateClassRows(engine);
            case "attribute" -> generateAttributeRows(engine);
            case "type" -> generateTypeRows(engine);
            case "namespace" -> generateNamespaceRows(engine);
            case "index" -> generateIndexRows(engine);
            case "information_schema.tables" -> generateInformationSchemaTablesRows(engine);
            case "database" -> generateDatabaseRows(engine);
            case "am" -> generateAmRows(engine);
            case "tables" -> generateTablesRows(engine);
            case "views" -> generateViewsRows(engine);
            case "indexes" -> generateIndexesRows(engine);
            default -> Collections.emptyList();
        };
    }

    private static List<Tuple> generateClassRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "oid", "relname", "relnamespace", "reltype", "relowner", "relam",
            "relfilenode", "reltablespace", "relpages", "reltuples", "relallvisible",
            "reltoastrelid", "relhasindex", "relisshared", "relpersistence", "relkind",
            "relnatts", "relchecks", "relhasrules", "relhastriggers", "relhassubclass",
            "relrowsecurity", "relforcerowsecurity", "relispopulated", "relreplident", "relispartition"
        );
        List<Tuple> rows = new ArrayList<>();

        // 1. Tables (User + System tables)
        for (String table : engine.getAllTableNames()) {
            boolean isSys = SystemCatalog.isSystemTable(table);
            int nspOid = isSys ? 11 : 2200;
            int relOid = getRelationOid(table);
            List<String> tableCols = engine.getTableColumns(table);
            int natts = tableCols != null ? tableCols.size() : 0;
            boolean hasIndex = !engine.getIndexesForTable(table).isEmpty();

            rows.add(createTuple(cols,
                relOid, table, nspOid, relOid + 1, 10, 0,
                relOid, 0, 1, 0.0f, 0,
                0, hasIndex ? "t" : "f", "f", "p", "r",
                natts, 0, "f", "f", "f",
                "f", "f", "t", "d", "f"
            ));
        }

        // 2. Views
        for (String view : engine.getViewNames()) {
            int relOid = getRelationOid(view);
            rows.add(createTuple(cols,
                relOid, view, 2200, relOid + 1, 10, 0,
                relOid, 0, 0, 0.0f, 0,
                0, "f", "f", "p", "v",
                0, 0, "f", "f", "f",
                "f", "f", "t", "d", "f"
            ));
        }

        // 3. Indexes
        for (ExecutorEngine.IndexMetadata idx : engine.getAllIndexes()) {
            int idxOid = getIndexOid(idx.indexName());
            rows.add(createTuple(cols,
                idxOid, idx.indexName(), 2200, idxOid + 1, 10, 403,
                idxOid, 0, 1, 0.0f, 0,
                0, "f", "f", "p", "i",
                1, 0, "f", "f", "f",
                "f", "f", "t", "d", "f"
            ));
        }

        return rows;
    }

    private static List<Tuple> generateAttributeRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "attrelid", "attname", "atttypid", "attstattarget", "attlen", "attnum",
            "attndims", "attcacheoff", "atttypmod", "attbyval", "attalign", "attstorage",
            "attcompression", "attnotnull", "atthasdef", "atthasmissing", "attidentity",
            "attgenerated", "attisdropped", "attislocal", "attinhcount", "attcollation"
        );
        List<Tuple> rows = new ArrayList<>();

        for (String table : engine.getAllTableNames()) {
            int relOid = getRelationOid(table);
            List<String> tableCols = engine.getTableColumns(table);
            if (tableCols == null) continue;
            Map<String, String> types = engine.getTableColumnTypes(table);
            Map<String, String> defaults = engine.getTableColumnDefaults(table);
            List<String> pks = engine.getTablePrimaryKeys(table);

            for (int i = 0; i < tableCols.size(); i++) {
                String col = tableCols.get(i);
                String typeStr = types != null ? types.get(col) : "VARCHAR";
                int typOid = getTypeOid(typeStr);
                int attnum = i + 1;
                boolean isPk = pks != null && pks.contains(col);
                boolean hasDef = defaults != null && defaults.containsKey(col);

                int attlen = (typOid == 23 ? 4 : (typOid == 20 ? 8 : (typOid == 21 ? 2 : (typOid == 16 ? 1 : -1))));

                rows.add(createTuple(cols,
                    relOid, col, typOid, -1, attlen, attnum,
                    0, -1, -1, "t", "i", "x",
                    "", isPk ? "t" : "f", hasDef ? "t" : "f", "f", "",
                    "", "f", "t", 0, 0
                ));
            }
        }
        return rows;
    }

    private static List<Tuple> generateTypeRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "oid", "typname", "typnamespace", "typowner", "typlen", "typbyval",
            "typtype", "typcategory", "typispreferred", "typisdefined", "typdelim",
            "typrelid", "typelem", "typarray", "typinput", "typoutput"
        );
        List<Tuple> rows = new ArrayList<>();

        Object[][] standardTypes = new Object[][] {
            {16, "bool", 11, 10, 1, "t", "b", "B", "t", "t", ",", 0, 0, 1000, "boolin", "boolout"},
            {17, "bytea", 11, 10, -1, "f", "b", "U", "f", "t", ",", 0, 0, 1001, "byteain", "byteaout"},
            {18, "char", 11, 10, 1, "t", "b", "Z", "f", "t", ",", 0, 0, 1002, "charin", "charout"},
            {19, "name", 11, 10, 64, "f", "b", "S", "f", "t", ",", 0, 0, 1003, "namein", "nameout"},
            {20, "int8", 11, 10, 8, "t", "b", "N", "f", "t", ",", 0, 0, 1016, "int8in", "int8out"},
            {21, "int2", 11, 10, 2, "t", "b", "N", "f", "t", ",", 0, 0, 1005, "int2in", "int2out"},
            {23, "int4", 11, 10, 4, "t", "b", "N", "f", "t", ",", 0, 0, 1007, "int4in", "int4out"},
            {25, "text", 11, 10, -1, "f", "b", "S", "t", "t", ",", 0, 0, 1009, "textin", "textout"},
            {26, "oid", 11, 10, 4, "t", "b", "N", "t", "t", ",", 0, 0, 1028, "oidin", "oidout"},
            {114, "json", 11, 10, -1, "f", "b", "U", "f", "t", ",", 0, 0, 199, "json_in", "json_out"},
            {700, "float4", 11, 10, 4, "t", "b", "N", "f", "t", ",", 0, 0, 1021, "float4in", "float4out"},
            {701, "float8", 11, 10, 8, "t", "b", "N", "t", "t", ",", 0, 0, 1022, "float8in", "float8out"},
            {1043, "varchar", 11, 10, -1, "f", "b", "S", "f", "t", ",", 0, 0, 1015, "varcharin", "varcharout"},
            {1082, "date", 11, 10, 4, "t", "b", "D", "f", "t", ",", 0, 0, 1182, "date_in", "date_out"},
            {1083, "time", 11, 10, 8, "t", "b", "D", "f", "t", ",", 0, 0, 1183, "time_in", "time_out"},
            {1114, "timestamp", 11, 10, 8, "t", "b", "D", "f", "t", ",", 0, 0, 1115, "timestamp_in", "timestamp_out"},
            {1700, "numeric", 11, 10, -1, "f", "b", "N", "f", "t", ",", 0, 0, 1231, "numeric_in", "numeric_out"},
            {3802, "jsonb", 11, 10, -1, "f", "b", "U", "f", "t", ",", 0, 0, 3807, "jsonb_in", "jsonb_out"},
            {869, "inet", 11, 10, -1, "f", "b", "I", "f", "t", ",", 0, 0, 1041, "inet_in", "inet_out"},
            {650, "cidr", 11, 10, -1, "f", "b", "I", "f", "t", ",", 0, 0, 651, "cidr_in", "cidr_out"}
        };

        for (Object[] item : standardTypes) {
            rows.add(createTuple(cols, item));
        }

        // Custom enum types
        for (String enumType : engine.getEnumTypes()) {
            int oid = getTypeOid(enumType);
            rows.add(createTuple(cols,
                oid, enumType, 2200, 10, 4, "t",
                "e", "E", "f", "t", ",",
                0, 0, 0, "enum_in", "enum_out"
            ));
        }

        return rows;
    }

    private static List<Tuple> generateNamespaceRows(ExecutorEngine engine) {
        List<String> cols = List.of("oid", "nspname", "nspowner", "nspacl");
        List<Tuple> rows = new ArrayList<>();
        rows.add(createTuple(cols, 11, "pg_catalog", 10, null));
        rows.add(createTuple(cols, 11, "std_catalog", 10, null));
        rows.add(createTuple(cols, 12, "information_schema", 10, null));
        rows.add(createTuple(cols, 2200, "public", 10, null));
        rows.add(createTuple(cols, 13, "pg_toast", 10, null));
        return rows;
    }

    private static List<Tuple> generateIndexRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "indexrelid", "indrelid", "indnatts", "indnkeyatts", "indisunique",
            "indisprimary", "indisexclusion", "indimmediate", "indisclustered",
            "indisvalid", "indcheckxmin", "indisready", "indislive", "indisreplident", "indkey"
        );
        List<Tuple> rows = new ArrayList<>();

        for (ExecutorEngine.IndexMetadata idx : engine.getAllIndexes()) {
            int idxOid = getIndexOid(idx.indexName());
            int relOid = getRelationOid(idx.tableName());
            boolean isUnique = idx.isUnique() || idx.isPrimary();
            boolean isPrimary = idx.isPrimary();

            rows.add(createTuple(cols,
                idxOid, relOid, 1, 1, isUnique ? "t" : "f",
                isPrimary ? "t" : "f", "f", "t", "f",
                "t", "f", "t", "t", "f", "1"
            ));
        }
        return rows;
    }

    private static List<Tuple> generateInformationSchemaTablesRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "table_catalog", "table_schema", "table_name", "table_type",
            "self_referencing_column_name", "reference_generation",
            "user_defined_type_catalog", "user_defined_type_schema", "user_defined_type_name",
            "is_insertable_into", "is_typed", "commit_action"
        );
        List<Tuple> rows = new ArrayList<>();

        // User tables
        for (String t : engine.getTableNames()) {
            rows.add(createTuple(cols,
                "stratos", "public", t, "BASE TABLE",
                null, null, null, null, null,
                "YES", "NO", null
            ));
        }

        // Views
        for (String v : engine.getViewNames()) {
            rows.add(createTuple(cols,
                "stratos", "public", v, "VIEW",
                null, null, null, null, null,
                "NO", "NO", null
            ));
        }

        // System tables
        for (String t : engine.getAllTableNames()) {
            if (SystemCatalog.isSystemTable(t)) {
                rows.add(createTuple(cols,
                    "stratos", "std_catalog", t, "SYSTEM TABLE",
                    null, null, null, null, null,
                    "YES", "NO", null
                ));
            }
        }

        return rows;
    }

    private static List<Tuple> generateDatabaseRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "oid", "datname", "datdba", "encoding", "datcollate", "datctype",
            "datistemplate", "datallowconn", "datconnlimit", "datlastsysoid",
            "datfrozenxid", "datminmxid", "dattablespace", "datacl"
        );
        List<Tuple> rows = new ArrayList<>();
        rows.add(createTuple(cols,
            1, "stratos", 10, 6, "en_US.UTF-8", "en_US.UTF-8",
            "f", "t", -1, 10000,
            1, 1, 1663, null
        ));
        return rows;
    }

    private static List<Tuple> generateAmRows(ExecutorEngine engine) {
        List<String> cols = List.of("oid", "amname", "amhandler", "amtype");
        List<Tuple> rows = new ArrayList<>();
        rows.add(createTuple(cols, 403, "btree", "bthandler", "i"));
        rows.add(createTuple(cols, 405, "hash", "hashhandler", "i"));
        rows.add(createTuple(cols, 4000, "gin", "ginhandler", "i"));
        rows.add(createTuple(cols, 4001, "brin", "brinhandler", "i"));
        return rows;
    }

    private static List<Tuple> generateTablesRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "schemaname", "tablename", "tableowner", "tablespace",
            "hasindexes", "hasrules", "hastriggers", "rowsecurity"
        );
        List<Tuple> rows = new ArrayList<>();
        for (String t : engine.getAllTableNames()) {
            boolean isSys = SystemCatalog.isSystemTable(t);
            boolean hasIdx = !engine.getIndexesForTable(t).isEmpty();
            rows.add(createTuple(cols,
                isSys ? "std_catalog" : "public",
                t,
                engine.getTableOwner(t),
                null,
                hasIdx ? "t" : "f",
                "f",
                "f",
                "f"
            ));
        }
        return rows;
    }

    private static List<Tuple> generateViewsRows(ExecutorEngine engine) {
        List<String> cols = List.of("schemaname", "viewname", "viewowner", "definition");
        List<Tuple> rows = new ArrayList<>();
        for (String v : engine.getViewNames()) {
            SelectStatement viewDef = engine.getView(v);
            rows.add(createTuple(cols,
                "public",
                v,
                "stratos",
                viewDef != null ? viewDef.toString() : ""
            ));
        }
        return rows;
    }

    private static List<Tuple> generateIndexesRows(ExecutorEngine engine) {
        List<String> cols = List.of("schemaname", "tablename", "indexname", "tablespace", "indexdef");
        List<Tuple> rows = new ArrayList<>();
        for (ExecutorEngine.IndexMetadata idx : engine.getAllIndexes()) {
            String def = "CREATE INDEX " + idx.indexName() + " ON " + idx.tableName()
                + " USING " + idx.indexType().toLowerCase(Locale.ROOT) + " (" + idx.columnName() + ")";
            if (idx.isPrimary()) {
                def = "CREATE UNIQUE INDEX " + idx.indexName() + " ON " + idx.tableName()
                    + " USING btree (" + idx.columnName() + ")";
            }
            rows.add(createTuple(cols, "public", idx.tableName(), idx.indexName(), null, def));
        }
        return rows;
    }

    private static Tuple createTuple(List<String> colNames, Object... values) {
        List<Object> valList = new ArrayList<>(Arrays.asList(values));
        return new Tuple(valList, colNames);
    }

    private static String stripQuotes(String s) {
        if (s == null) return "";
        s = s.trim();
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }
}
