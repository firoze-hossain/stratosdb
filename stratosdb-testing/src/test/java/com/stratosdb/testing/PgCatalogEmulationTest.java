package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sprint 3 Week 06: pg_catalog and information_schema System Views Emulation Test Suite.
 *
 * Verifies:
 *   1. Virtual system views (pg_class, pg_attribute, pg_type, pg_namespace, pg_index,
 *      information_schema.tables, pg_database, pg_am, pg_tables, pg_views, pg_indexes)
 *      are wired into query engine and execute seamlessly.
 *   2. System views dynamically reflect table creations, column types, nullability,
 *      defaults, indexes, and custom enum types.
 *   3. Aggregations (e.g. COUNT(*)) and WHERE filtering run cleanly on virtual views.
 *   4. Built-in PostgreSQL catalog functions (format_type, pg_table_is_visible,
 *      pg_get_userbyid, pg_encoding_to_char, quote_ident) execute accurately.
 */
public class PgCatalogEmulationTest {

    @Test
    void testPgClassVirtualView(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR, salary INT DEFAULT 50000)");
            db.execute("CREATE VIEW active_emp AS SELECT * FROM employees");

            // Query pg_class directly
            QueryResult res1 = db.execute("SELECT * FROM pg_class");
            assertTrue(res1.isSuccess(), "pg_class must be queryable: " + res1.getError());
            List<String> relNames = res1.getRows().stream()
                .map(r -> (String) r.getValue("relname"))
                .toList();
            assertTrue(relNames.contains("employees"), "pg_class must contain 'employees'");
            assertTrue(relNames.contains("active_emp"), "pg_class must contain view 'active_emp'");

            // Query with pg_catalog schema qualifier
            QueryResult res2 = db.execute("SELECT relname, relkind, relhasindex FROM pg_catalog.pg_class WHERE relname = 'employees'");
            assertTrue(res2.isSuccess(), "pg_catalog.pg_class must be queryable: " + res2.getError());
            assertEquals(1, res2.getRows().size());
            Tuple empRow = res2.getRows().get(0);
            assertEquals("employees", empRow.getValue("relname"));
            assertEquals("r", empRow.getValue("relkind"), "table relkind must be 'r'");
            assertEquals("t", empRow.getValue("relhasindex"), "table with primary key must have relhasindex='t'");

            // Check view relkind
            QueryResult resView = db.execute("SELECT relkind FROM pg_class WHERE relname = 'active_emp'");
            assertTrue(resView.isSuccess());
            assertEquals(1, resView.getRows().size());
            assertEquals("v", resView.getRows().get(0).getValue("relkind"), "view relkind must be 'v'");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testPgAttributeVirtualView(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE products (sku VARCHAR PRIMARY KEY, price INT, description VARCHAR DEFAULT 'N/A')");

            QueryResult res = db.execute("SELECT attname, atttypid, attnotnull, atthasdef FROM pg_catalog.pg_attribute");
            assertTrue(res.isSuccess(), "pg_attribute query failed: " + res.getError());

            List<Tuple> rows = res.getRows();
            Tuple skuRow = rows.stream().filter(r -> "sku".equals(r.getValue("attname"))).findFirst().orElse(null);
            Tuple priceRow = rows.stream().filter(r -> "price".equals(r.getValue("attname"))).findFirst().orElse(null);
            Tuple descRow = rows.stream().filter(r -> "description".equals(r.getValue("attname"))).findFirst().orElse(null);

            assertNotNull(skuRow, "column 'sku' must be in pg_attribute");
            assertNotNull(priceRow, "column 'price' must be in pg_attribute");
            assertNotNull(descRow, "column 'description' must be in pg_attribute");

            assertEquals("t", skuRow.getValue("attnotnull"), "primary key column must have attnotnull='t'");
            assertEquals(1043, ((Number) skuRow.getValue("atttypid")).intValue(), "varchar type OID is 1043");

            assertEquals(23, ((Number) priceRow.getValue("atttypid")).intValue(), "integer type OID is 23");

            assertEquals("t", descRow.getValue("atthasdef"), "default value column must have atthasdef='t'");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testPgTypeVirtualView(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TYPE mood_status AS ENUM ('happy', 'sad', 'relaxed')");

            QueryResult res = db.execute("SELECT typname, typtype FROM pg_catalog.pg_type");
            assertTrue(res.isSuccess(), "pg_type query failed: " + res.getError());

            List<String> typeNames = res.getRows().stream()
                .map(r -> (String) r.getValue("typname"))
                .toList();

            assertTrue(typeNames.contains("int4"), "pg_type must contain int4");
            assertTrue(typeNames.contains("varchar"), "pg_type must contain varchar");
            assertTrue(typeNames.contains("bool"), "pg_type must contain bool");
            assertTrue(typeNames.contains("float8"), "pg_type must contain float8");
            assertTrue(typeNames.contains("timestamp"), "pg_type must contain timestamp");
            assertTrue(typeNames.contains("mood_status"), "pg_type must contain custom enum 'mood_status'");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testPgNamespaceAndDatabaseAndAm(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            // pg_namespace
            QueryResult nsRes = db.execute("SELECT nspname, oid FROM pg_catalog.pg_namespace");
            assertTrue(nsRes.isSuccess());
            List<String> namespaces = nsRes.getRows().stream()
                .map(r -> (String) r.getValue("nspname"))
                .toList();
            assertTrue(namespaces.contains("public"), "must contain public namespace");
            assertTrue(namespaces.contains("pg_catalog"), "must contain pg_catalog namespace");
            assertTrue(namespaces.contains("information_schema"), "must contain information_schema namespace");

            // pg_database
            QueryResult dbRes = db.execute("SELECT datname, encoding FROM pg_catalog.pg_database");
            assertTrue(dbRes.isSuccess());
            assertFalse(dbRes.getRows().isEmpty());
            assertEquals("stratos", dbRes.getRows().get(0).getValue("datname"));

            // pg_am
            QueryResult amRes = db.execute("SELECT amname FROM pg_catalog.pg_am");
            assertTrue(amRes.isSuccess());
            List<String> ams = amRes.getRows().stream()
                .map(r -> (String) r.getValue("amname"))
                .toList();
            assertTrue(ams.contains("btree"));
            assertTrue(ams.contains("hash"));
            assertTrue(ams.contains("gin"));
            assertTrue(ams.contains("brin"));
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testPgIndexVirtualView(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE orders (order_id INT PRIMARY KEY, customer_id INT, amount INT)");
            db.execute("CREATE INDEX idx_orders_cust ON orders (customer_id)");

            QueryResult res = db.execute("SELECT * FROM pg_catalog.pg_index");
            assertTrue(res.isSuccess());
            assertFalse(res.getRows().isEmpty());

            // pg_indexes view
            QueryResult idxRes = db.execute("SELECT indexname, tablename, indexdef FROM pg_indexes");
            assertTrue(idxRes.isSuccess());
            List<String> idxNames = idxRes.getRows().stream()
                .map(r -> (String) r.getValue("indexname"))
                .toList();
            assertTrue(idxNames.contains("orders_pkey"), "must contain PK index orders_pkey");
            assertTrue(idxNames.contains("idx_orders_cust"), "must contain secondary index idx_orders_cust");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testInformationSchemaTablesVirtualView(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR)");
            db.execute("CREATE VIEW active_customers AS SELECT * FROM customers");

            QueryResult res = db.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public'");
            assertTrue(res.isSuccess(), "information_schema.tables failed: " + res.getError());

            List<Tuple> rows = res.getRows();
            Tuple custRow = rows.stream().filter(r -> "customers".equals(r.getValue("table_name"))).findFirst().orElse(null);
            Tuple viewRow = rows.stream().filter(r -> "active_customers".equals(r.getValue("table_name"))).findFirst().orElse(null);

            assertNotNull(custRow, "customers table must be present in information_schema.tables");
            assertEquals("BASE TABLE", custRow.getValue("table_type"));

            assertNotNull(viewRow, "active_customers view must be present in information_schema.tables");
            assertEquals("VIEW", viewRow.getValue("table_type"));
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testBuiltinCatalogFunctions(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            QueryResult res1 = db.execute("SELECT format_type(23, -1) AS ft");
            assertTrue(res1.isSuccess());
            assertEquals("integer", res1.getRows().get(0).getValue("ft"));

            QueryResult res2 = db.execute("SELECT format_type(1043, -1) AS ft");
            assertTrue(res2.isSuccess());
            assertEquals("character varying", res2.getRows().get(0).getValue("ft"));

            QueryResult res3 = db.execute("SELECT pg_table_is_visible(16385) AS vis");
            assertTrue(res3.isSuccess());
            assertEquals(true, res3.getRows().get(0).getValue("vis"));

            QueryResult res4 = db.execute("SELECT pg_get_userbyid(10) AS usr");
            assertTrue(res4.isSuccess());
            assertEquals("stratos", res4.getRows().get(0).getValue("usr"));

            QueryResult res5 = db.execute("SELECT pg_encoding_to_char(6) AS enc");
            assertTrue(res5.isSuccess());
            assertEquals("UTF8", res5.getRows().get(0).getValue("enc"));

            QueryResult res6 = db.execute("SELECT quote_ident('my_column') AS qi");
            assertTrue(res6.isSuccess());
            assertEquals("my_column", res6.getRows().get(0).getValue("qi"));
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testAggregationAndCountOverSystemViews(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE t1 (x INT)");
            db.execute("CREATE TABLE t2 (y INT)");

            QueryResult cnt = db.execute("SELECT COUNT(*) AS total FROM pg_class");
            assertTrue(cnt.isSuccess());
            Number total = (Number) cnt.getRows().get(0).getValue("total");
            assertTrue(total.longValue() >= 2, "count should be at least 2 tables");

            QueryResult cntTables = db.execute("SELECT COUNT(*) AS tbl_count FROM information_schema.tables WHERE table_type = 'BASE TABLE'");
            assertTrue(cntTables.isSuccess());
            Number tblCount = (Number) cntTables.getRows().get(0).getValue("tbl_count");
            assertTrue(tblCount.longValue() >= 2);
        } finally {
            db.shutdown();
        }
    }
}
