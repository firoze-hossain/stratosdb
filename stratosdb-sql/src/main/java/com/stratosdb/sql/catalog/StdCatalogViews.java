package com.stratosdb.sql.catalog;

import com.stratosdb.sql.ast.CheckConstraintDefinition;
import com.stratosdb.sql.ast.ForeignKeyDefinition;
import com.stratosdb.sql.ast.SelectStatement;
import com.stratosdb.sql.executor.ExecutorEngine;
import com.stratosdb.sql.executor.Sequence;
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
                 "information_schema.tables", "information_schema.columns",
                 "information_schema.table_constraints", "information_schema.key_column_usage",
                 "information_schema.referential_constraints", "information_schema.schemata",
                 "information_schema.sequences",
                 "database", "am", "tables", "views", "indexes", "statistic",
                 "constraint", "description", "proc", "roles", "enum", "inherits",
                 "partitioned_table", "sequence" -> true;
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

        if (name.startsWith("information_schema.")) {
            String sub = name.substring("information_schema.".length());
            return switch (sub) {
                case "tables" -> "information_schema.tables";
                case "columns" -> "information_schema.columns";
                case "table_constraints" -> "information_schema.table_constraints";
                case "key_column_usage" -> "information_schema.key_column_usage";
                case "referential_constraints" -> "information_schema.referential_constraints";
                case "schemata" -> "information_schema.schemata";
                case "sequences" -> "information_schema.sequences";
                default -> "information_schema." + sub;
            };
        }

        if (name.equals("information_schema_tables") || name.equals("tables")) {
            return "information_schema.tables";
        }
        if (name.equals("information_schema_columns") || name.equals("columns")) {
            return "information_schema.columns";
        }
        if (name.equals("information_schema_table_constraints") || name.equals("table_constraints")) {
            return "information_schema.table_constraints";
        }
        if (name.equals("information_schema_key_column_usage") || name.equals("key_column_usage")) {
            return "information_schema.key_column_usage";
        }
        if (name.equals("information_schema_referential_constraints") || name.equals("referential_constraints")) {
            return "information_schema.referential_constraints";
        }
        if (name.equals("information_schema_schemata") || name.equals("schemata")) {
            return "information_schema.schemata";
        }
        if (name.equals("information_schema_sequences") || name.equals("sequences")) {
            return "information_schema.sequences";
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
            case "pg_statistic", "std_statistic", "stratos_statistic", "statistic" -> "statistic";
            case "pg_constraint", "std_constraint", "stratos_constraint", "constraint" -> "constraint";
            case "pg_description", "std_description", "stratos_description", "description" -> "description";
            case "pg_proc", "std_proc", "stratos_proc", "proc" -> "proc";
            case "pg_roles", "std_roles", "stratos_roles", "roles", "pg_user", "std_user" -> "roles";
            case "pg_enum", "std_enum", "stratos_enum", "enum" -> "enum";
            case "pg_inherits", "std_inherits", "stratos_inherits", "inherits" -> "inherits";
            case "pg_partitioned_table", "std_partitioned_table", "stratos_partitioned_table", "partitioned_table" -> "partitioned_table";
            case "pg_sequence", "std_sequence", "stratos_sequence", "sequence" -> "sequence";
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
            case "information_schema.columns" -> generateColumnsRows(engine);
            case "information_schema.table_constraints" -> generateTableConstraintsRows(engine);
            case "information_schema.key_column_usage" -> generateKeyColumnUsageRows(engine);
            case "information_schema.referential_constraints" -> generateReferentialConstraintsRows(engine);
            case "information_schema.schemata" -> generateSchemataRows(engine);
            case "information_schema.sequences" -> generateSequencesRows(engine);
            case "database" -> generateDatabaseRows(engine);
            case "am" -> generateAmRows(engine);
            case "tables" -> generateTablesRows(engine);
            case "views" -> generateViewsRows(engine);
            case "indexes" -> generateIndexesRows(engine);
            case "statistic" -> generateStatisticRows(engine);
            case "constraint" -> generateConstraintRows(engine);
            case "description" -> generateDescriptionRows(engine);
            case "proc" -> generateProcRows(engine);
            case "roles" -> generateRolesRows(engine);
            case "enum" -> generateEnumRows(engine);
            case "inherits" -> generateInheritsRows(engine);
            case "partitioned_table" -> generatePartitionedTableRows(engine);
            case "sequence" -> generateSequenceRows(engine);
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

            int relpages = 1;
            float reltuples = 0.0f;
            com.stratosdb.sql.statistics.TableStatistics ts = engine.getStatistics() != null ? engine.getStatistics().get(table) : null;
            if (ts != null) {
                relpages = (int) Math.min(Integer.MAX_VALUE, ts.pageCount());
                reltuples = (float) ts.rowCount();
            }

            rows.add(createTuple(cols,
                relOid, table, nspOid, relOid + 1, 10, 0,
                relOid, 0, relpages, reltuples, 0,
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

        // 4. Sequences
        for (String seqName : engine.getSequenceNames()) {
            int seqOid = getRelationOid(seqName);
            rows.add(createTuple(cols,
                seqOid, seqName, 2200, seqOid + 1, 10, 0,
                seqOid, 0, 1, 1.0f, 0,
                0, "f", "f", "p", "S",
                0, 0, "f", "f", "f",
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

    private static List<Tuple> generateStatisticRows(ExecutorEngine engine) {
        List<String> cols = SystemCatalog.STRATOS_STATISTIC_COLS;
        List<Tuple> rows = new ArrayList<>();
        if (engine.getStatistics() == null) return rows;

        for (com.stratosdb.sql.statistics.TableStatistics ts : engine.getStatistics().values()) {
            for (com.stratosdb.sql.statistics.ColumnStatistics cs : ts.columnStats().values()) {
                rows.add(createTuple(cols,
                    cs.tableName(),
                    cs.columnName(),
                    (int) Math.min(Integer.MAX_VALUE, cs.rowCount()),
                    (int) Math.min(Integer.MAX_VALUE, cs.pageCount()),
                    cs.nullFraction(),
                    cs.avgWidth(),
                    cs.distinctCount(),
                    cs.mcv() != null ? cs.mcv().toJsonValues() : "[]",
                    cs.mcv() != null ? cs.mcv().toJsonFrequencies() : "[]",
                    cs.histogram() != null ? cs.histogram().toJson() : "[]",
                    cs.correlation()
                ));
            }
        }
        return rows;
    }

    private static List<Tuple> generateColumnsRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "table_catalog", "table_schema", "table_name", "column_name",
            "ordinal_position", "column_default", "is_nullable", "data_type",
            "character_maximum_length", "character_octet_length", "numeric_precision",
            "numeric_scale", "datetime_precision", "udt_name", "is_identity", "identity_generation"
        );
        List<Tuple> rows = new ArrayList<>();
        for (String table : engine.getAllTableNames()) {
            boolean isSys = SystemCatalog.isSystemTable(table);
            String schema = isSys ? "std_catalog" : "public";
            List<String> tableCols = engine.getTableColumns(table);
            if (tableCols == null) continue;
            Map<String, String> types = engine.getTableColumnTypes(table);
            Map<String, String> defaults = engine.getTableColumnDefaults(table);
            List<String> pks = engine.getTablePrimaryKeys(table);

            for (int i = 0; i < tableCols.size(); i++) {
                String col = tableCols.get(i);
                String typeStr = types != null ? types.get(col) : "VARCHAR";
                int typOid = getTypeOid(typeStr);
                String dType = formatType(typOid);
                boolean isPk = pks != null && pks.contains(col);
                String isNullable = isPk ? "NO" : "YES";
                String defVal = defaults != null ? defaults.get(col) : null;
                Integer charLen = (dType.contains("varying") || dType.equals("character")) ? Integer.valueOf(255) : null;
                Integer octetLen = charLen != null ? Integer.valueOf(charLen * 4) : null;
                Integer numPrec = typOid == 23 ? Integer.valueOf(32) : (typOid == 20 ? Integer.valueOf(64) : (typOid == 21 ? Integer.valueOf(16) : (typOid == 701 ? Integer.valueOf(53) : null)));
                Integer numScale = (numPrec != null && typOid != 701) ? Integer.valueOf(0) : null;
                Integer dtPrec = (typOid == 1114 || typOid == 1083) ? Integer.valueOf(6) : null;
                String udt = switch (typOid) {
                    case 16 -> "bool";
                    case 20 -> "int8";
                    case 21 -> "int2";
                    case 23 -> "int4";
                    case 701 -> "float8";
                    case 1114 -> "timestamp";
                    case 1082 -> "date";
                    case 1083 -> "time";
                    case 17 -> "bytea";
                    case 25 -> "text";
                    default -> "varchar";
                };

                rows.add(createTuple(cols,
                    "stratos", schema, table, col,
                    i + 1, defVal, isNullable, dType,
                    charLen, octetLen, numPrec,
                    numScale, dtPrec, udt, "NO", null
                ));
            }
        }
        return rows;
    }

    private static List<Tuple> generateTableConstraintsRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "constraint_catalog", "constraint_schema", "constraint_name",
            "table_catalog", "table_schema", "table_name", "constraint_type",
            "is_deferrable", "initially_deferred", "enforced"
        );
        List<Tuple> rows = new ArrayList<>();
        // 1. Primary keys
        for (String table : engine.getAllTableNames()) {
            List<String> pks = engine.getTablePrimaryKeys(table);
            if (pks != null && !pks.isEmpty()) {
                String schema = SystemCatalog.isSystemTable(table) ? "std_catalog" : "public";
                rows.add(createTuple(cols,
                    "stratos", schema, table + "_pkey",
                    "stratos", schema, table, "PRIMARY KEY",
                    "NO", "NO", "YES"
                ));
            }
        }
        // 2. Foreign keys
        for (Map.Entry<String, List<ForeignKeyDefinition>> entry : engine.getAllForeignKeys().entrySet()) {
            for (ForeignKeyDefinition fk : entry.getValue()) {
                rows.add(createTuple(cols,
                    "stratos", "public", fk.name(),
                    "stratos", "public", fk.childTable(), "FOREIGN KEY",
                    "NO", "NO", "YES"
                ));
            }
        }
        // 3. Check constraints
        for (Map.Entry<String, List<CheckConstraintDefinition>> entry : engine.getAllCheckConstraints().entrySet()) {
            for (CheckConstraintDefinition chk : entry.getValue()) {
                rows.add(createTuple(cols,
                    "stratos", "public", chk.name(),
                    "stratos", "public", entry.getKey(), "CHECK",
                    "NO", "NO", "YES"
                ));
            }
        }
        // 4. Unique indexes
        for (ExecutorEngine.IndexMetadata idx : engine.getAllIndexes()) {
            if (idx.isUnique() && !idx.isPrimary()) {
                rows.add(createTuple(cols,
                    "stratos", "public", idx.indexName(),
                    "stratos", "public", idx.tableName(), "UNIQUE",
                    "NO", "NO", "YES"
                ));
            }
        }
        return rows;
    }

    private static List<Tuple> generateKeyColumnUsageRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "constraint_catalog", "constraint_schema", "constraint_name",
            "table_catalog", "table_schema", "table_name",
            "column_name", "ordinal_position", "position_in_unique_constraint"
        );
        List<Tuple> rows = new ArrayList<>();
        // Primary keys
        for (String table : engine.getAllTableNames()) {
            List<String> pks = engine.getTablePrimaryKeys(table);
            if (pks != null && !pks.isEmpty()) {
                String schema = SystemCatalog.isSystemTable(table) ? "std_catalog" : "public";
                for (int i = 0; i < pks.size(); i++) {
                    rows.add(createTuple(cols,
                        "stratos", schema, table + "_pkey",
                        "stratos", schema, table,
                        pks.get(i), i + 1, null
                    ));
                }
            }
        }
        // Foreign keys
        for (Map.Entry<String, List<ForeignKeyDefinition>> entry : engine.getAllForeignKeys().entrySet()) {
            for (ForeignKeyDefinition fk : entry.getValue()) {
                for (int i = 0; i < fk.childColumns().size(); i++) {
                    rows.add(createTuple(cols,
                        "stratos", "public", fk.name(),
                        "stratos", "public", fk.childTable(),
                        fk.childColumns().get(i), i + 1, i + 1
                    ));
                }
            }
        }
        // Unique indexes
        for (ExecutorEngine.IndexMetadata idx : engine.getAllIndexes()) {
            if (idx.isUnique() && !idx.isPrimary()) {
                rows.add(createTuple(cols,
                    "stratos", "public", idx.indexName(),
                    "stratos", "public", idx.tableName(),
                    idx.columnName(), 1, null
                ));
            }
        }
        return rows;
    }

    private static List<Tuple> generateReferentialConstraintsRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "constraint_catalog", "constraint_schema", "constraint_name",
            "unique_constraint_catalog", "unique_constraint_schema", "unique_constraint_name",
            "match_option", "update_rule", "delete_rule"
        );
        List<Tuple> rows = new ArrayList<>();
        for (Map.Entry<String, List<ForeignKeyDefinition>> entry : engine.getAllForeignKeys().entrySet()) {
            for (ForeignKeyDefinition fk : entry.getValue()) {
                String upd = fk.onUpdate() != null ? fk.onUpdate().name().replace('_', ' ') : "NO ACTION";
                String del = fk.onDelete() != null ? fk.onDelete().name().replace('_', ' ') : "NO ACTION";
                rows.add(createTuple(cols,
                    "stratos", "public", fk.name(),
                    "stratos", "public", fk.parentTable() + "_pkey",
                    "NONE", upd, del
                ));
            }
        }
        return rows;
    }

    private static List<Tuple> generateSchemataRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "catalog_name", "schema_name", "schema_owner",
            "default_character_set_catalog", "default_character_set_schema",
            "default_character_set_name", "sql_path"
        );
        List<Tuple> rows = new ArrayList<>();
        List<String> schemas = List.of("public", "information_schema", "pg_catalog", "std_catalog", "pg_toast");
        for (String s : schemas) {
            rows.add(createTuple(cols, "stratos", s, "stratos", null, null, null, null));
        }
        return rows;
    }

    private static List<Tuple> generateSequencesRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "sequence_catalog", "sequence_schema", "sequence_name",
            "data_type", "numeric_precision", "numeric_precision_radix", "numeric_scale",
            "start_value", "minimum_value", "maximum_value", "increment", "cycle_option"
        );
        List<Tuple> rows = new ArrayList<>();
        for (Sequence seq : engine.getSequences().values()) {
            rows.add(createTuple(cols,
                "stratos", "public", seq.getName(),
                "bigint", 64, 2, 0,
                "1", "1", "9223372036854775807", String.valueOf(seq.getIncrementBy()), "NO"
            ));
        }
        return rows;
    }

    private static List<Tuple> generateConstraintRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "oid", "conname", "connamespace", "contype", "condeferrable", "condeferred",
            "convalidated", "conrelid", "contypid", "conindid", "confrelid", "confupdtype",
            "confdeltype", "confmatchtype", "conislocal", "coninhcount", "connoinherit",
            "conkey", "confkey", "conpfeqop", "conppeqop", "conffeqop", "conexclop", "conbin"
        );
        List<Tuple> rows = new ArrayList<>();
        int nextOid = 30000;

        // Primary keys
        for (String table : engine.getAllTableNames()) {
            List<String> pks = engine.getTablePrimaryKeys(table);
            if (pks != null && !pks.isEmpty()) {
                int relOid = getRelationOid(table);
                List<String> tableCols = engine.getTableColumns(table);
                List<Integer> attnums = new ArrayList<>();
                if (tableCols != null) {
                    for (String pk : pks) {
                        int idx = tableCols.indexOf(pk);
                        if (idx >= 0) attnums.add(idx + 1);
                    }
                }
                String conkey = "{" + String.join(",", attnums.stream().map(Object::toString).toList()) + "}";
                rows.add(createTuple(cols,
                    relOid + 100, table + "_pkey", 2200, "p", "f", "f",
                    "t", relOid, 0, 0, 0, "a",
                    "a", "u", "t", 0, "t",
                    conkey, null, null, null, null, null, null
                ));
            }
        }

        // Foreign keys
        for (Map.Entry<String, List<ForeignKeyDefinition>> entry : engine.getAllForeignKeys().entrySet()) {
            for (ForeignKeyDefinition fk : entry.getValue()) {
                int childOid = getRelationOid(fk.childTable());
                int parentOid = getRelationOid(fk.parentTable());
                List<String> childCols = engine.getTableColumns(fk.childTable());
                List<String> parentCols = engine.getTableColumns(fk.parentTable());
                List<Integer> childAttnums = new ArrayList<>();
                List<Integer> parentAttnums = new ArrayList<>();
                if (childCols != null) {
                    for (String c : fk.childColumns()) {
                        int idx = childCols.indexOf(c);
                        if (idx >= 0) childAttnums.add(idx + 1);
                    }
                }
                if (parentCols != null) {
                    for (String c : fk.parentColumns()) {
                        int idx = parentCols.indexOf(c);
                        if (idx >= 0) parentAttnums.add(idx + 1);
                    }
                }
                String conkey = "{" + String.join(",", childAttnums.stream().map(Object::toString).toList()) + "}";
                String confkey = "{" + String.join(",", parentAttnums.stream().map(Object::toString).toList()) + "}";
                String upd = fk.onUpdate() == ForeignKeyDefinition.ActionType.CASCADE ? "c" :
                    (fk.onUpdate() == ForeignKeyDefinition.ActionType.SET_NULL ? "n" : "a");
                String del = fk.onDelete() == ForeignKeyDefinition.ActionType.CASCADE ? "c" :
                    (fk.onDelete() == ForeignKeyDefinition.ActionType.SET_NULL ? "n" : "a");
                rows.add(createTuple(cols,
                    nextOid++, fk.name(), 2200, "f", "f", "f",
                    "t", childOid, 0, 0, parentOid, upd,
                    del, "u", "t", 0, "t",
                    conkey, confkey, null, null, null, null, null
                ));
            }
        }

        // Check constraints
        for (Map.Entry<String, List<CheckConstraintDefinition>> entry : engine.getAllCheckConstraints().entrySet()) {
            int relOid = getRelationOid(entry.getKey());
            for (CheckConstraintDefinition chk : entry.getValue()) {
                rows.add(createTuple(cols,
                    nextOid++, chk.name(), 2200, "c", "f", "f",
                    "t", relOid, 0, 0, 0, "a",
                    "a", "u", "t", 0, "t",
                    null, null, null, null, null, null, chk.expressionSql()
                ));
            }
        }

        // Unique constraints
        for (ExecutorEngine.IndexMetadata idx : engine.getAllIndexes()) {
            if (idx.isUnique() && !idx.isPrimary()) {
                int relOid = getRelationOid(idx.tableName());
                int idxOid = getIndexOid(idx.indexName());
                rows.add(createTuple(cols,
                    nextOid++, idx.indexName(), 2200, "u", "f", "f",
                    "t", relOid, 0, idxOid, 0, "a",
                    "a", "u", "t", 0, "t",
                    "{1}", null, null, null, null, null, null
                ));
            }
        }

        return rows;
    }

    private static List<Tuple> generateDescriptionRows(ExecutorEngine engine) {
        List<String> cols = List.of("objoid", "classoid", "objsubid", "description");
        return Collections.emptyList();
    }

    private static List<Tuple> generateProcRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "oid", "proname", "pronamespace", "proowner", "prolang", "procost",
            "prorows", "provariadic", "prosupport", "prokind", "prosecdef", "proleakproof",
            "proisstrict", "proretset", "provolatile", "proparallel", "pronargs",
            "pronargdefaults", "prorettype", "proargtypes", "proallargtypes", "proargmodes",
            "proargnames", "proargdefaults", "protrftypes", "prosrc", "probin", "prosqlbody",
            "proconfig", "proacl"
        );
        List<Tuple> rows = new ArrayList<>();
        int pOid = 50000;
        List<String> builtinFuncs = List.of(
            "version", "current_schema", "current_database", "nextval", "currval",
            "setval", "txid_current", "pg_total_relation_size", "pg_relation_size",
            "obj_description", "col_description", "shobj_description", "has_table_privilege",
            "has_database_privilege", "has_schema_privilege", "pg_get_serial_sequence"
        );
        for (String fn : builtinFuncs) {
            rows.add(createTuple(cols,
                pOid++, fn, 11, 10, 12, 1.0f,
                0.0f, 0, 0, "f", "f", "f",
                "f", "f", "v", "u", 0,
                0, 25, "", null, null,
                null, null, null, fn, null, null,
                null, null
            ));
        }
        for (String fn : engine.getFunctions().keySet()) {
            rows.add(createTuple(cols,
                pOid++, fn, 2200, 10, 12, 1.0f,
                0.0f, 0, 0, "f", "f", "f",
                "f", "f", "v", "u", 0,
                0, 25, "", null, null,
                null, null, null, fn, null, null,
                null, null
            ));
        }
        return rows;
    }

    private static List<Tuple> generateRolesRows(ExecutorEngine engine) {
        List<String> cols = List.of(
            "rolname", "rolsuper", "rolinherit", "rolcreaterole", "rolcreatedb",
            "rolcanlogin", "rolreplication", "rolconnlimit", "rolpassword",
            "rolvaliduntil", "rolbypassrls", "rolconfig", "oid"
        );
        List<Tuple> rows = new ArrayList<>();
        Set<String> added = new HashSet<>();
        for (String r : List.of("stratos", "postgres", "admin")) {
            rows.add(createTuple(cols,
                r, "t", "t", "t", "t",
                "t", "t", -1, "********",
                null, "f", null, 10 + added.size()
            ));
            added.add(r);
        }
        for (ExecutorEngine.Role role : engine.getRoles().values()) {
            if (!added.add(role.name())) continue;
            rows.add(createTuple(cols,
                role.name(), role.superuser() ? "t" : "f", "t", "f", "t",
                role.login() ? "t" : "f", "f", -1, "********",
                null, "f", null, 10 + added.size()
            ));
        }
        return rows;
    }

    private static List<Tuple> generateEnumRows(ExecutorEngine engine) {
        List<String> cols = List.of("oid", "enumtypid", "enumsortorder", "enumlabel");
        List<Tuple> rows = new ArrayList<>();
        int eOid = 60000;
        for (String enumName : engine.getEnumTypes()) {
            int typOid = getTypeOid(enumName);
            rows.add(createTuple(cols, eOid++, typOid, 1.0f, enumName));
        }
        return rows;
    }

    private static List<Tuple> generateInheritsRows(ExecutorEngine engine) {
        List<String> cols = List.of("inhrelid", "inhparent", "inhseqno");
        return Collections.emptyList();
    }

    private static List<Tuple> generatePartitionedTableRows(ExecutorEngine engine) {
        List<String> cols = List.of("partrelid", "partstrat", "partnatts", "partdefid", "partattrs", "partclass", "partcollation", "partexprs");
        return Collections.emptyList();
    }

    private static List<Tuple> generateSequenceRows(ExecutorEngine engine) {
        List<String> cols = List.of("seqrelid", "seqtypid", "seqstart", "seqincrement", "seqmax", "seqmin", "seqcache", "seqcycle");
        List<Tuple> rows = new ArrayList<>();
        for (Sequence seq : engine.getSequences().values()) {
            int seqOid = getRelationOid(seq.getName());
            rows.add(createTuple(cols,
                seqOid, 20, 1L, seq.getIncrementBy(), 9223372036854775807L, 1L, 1L, "f"
            ));
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
