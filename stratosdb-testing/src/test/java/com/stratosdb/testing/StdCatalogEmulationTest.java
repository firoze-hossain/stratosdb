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
 * Standard Catalog & Information Schema System Views Emulation Test Suite.
 *
 * Verifies:
 *   1. Virtual system views (std_class / pg_class, std_attribute / pg_attribute,
 *      std_type / pg_type, std_namespace / pg_namespace, std_index / pg_index,
 *      information_schema.tables, std_database, std_am, std_tables, std_views, std_indexes)
 *      are wired into query engine and execute seamlessly.
 *   2. System views dynamically reflect table creations, column types, nullability,
 *      defaults, indexes, and custom enum types.
 *   3. Aggregations (e.g. COUNT(*)) and WHERE filtering run cleanly on virtual views.
 *   4. Built-in catalog introspection functions (format_type, std_format_type,
 *      table_is_visible, std_table_is_visible, get_userbyid, std_get_userbyid,
 *      encoding_to_char, quote_ident) execute accurately.
 */
public class StdCatalogEmulationTest {

    @Test
    void testClassVirtualView(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR, salary INT DEFAULT 50000)");
            db.execute("CREATE VIEW active_emp AS SELECT * FROM employees");

            // Query std_class directly
            QueryResult resStd = db.execute("SELECT * FROM std_class");
            assertTrue(resStd.isSuccess(), "std_class must be queryable: " + resStd.getError());
            List<String> stdRelNames = resStd.getRows().stream()
                .map(r -> (String) r.getValue("relname"))
                .toList();
            assertTrue(stdRelNames.contains("employees"), "std_class must contain 'employees'");
            assertTrue(stdRelNames.contains("active_emp"), "std_class must contain view 'active_emp'");

            // Query pg_class compatibility alias
            QueryResult res1 = db.execute("SELECT * FROM pg_class");
            assertTrue(res1.isSuccess(), "pg_class must be queryable: " + res1.getError());
            List<String> relNames = res1.getRows().stream()
                .map(r -> (String) r.getValue("relname"))
                .toList();
            assertTrue(relNames.contains("employees"), "pg_class must contain 'employees'");
            assertTrue(relNames.contains("active_emp"), "pg_class must contain view 'active_emp'");

            // Query with std_catalog schema qualifier
            QueryResult res2 = db.execute("SELECT relname, relkind, relhasindex FROM std_catalog.std_class WHERE relname = 'employees'");
            assertTrue(res2.isSuccess(), "std_catalog.std_class must be queryable: " + res2.getError());
            assertEquals(1, res2.getRows().size());
            Tuple empRow = res2.getRows().get(0);
            assertEquals("employees", empRow.getValue("relname"));
            assertEquals("r", empRow.getValue("relkind"), "table relkind must be 'r'");
            assertEquals("t", empRow.getValue("relhasindex"), "table with primary key must have relhasindex='t'");

            // Check view relkind
            QueryResult resView = db.execute("SELECT relkind FROM std_class WHERE relname = 'active_emp'");
            assertTrue(resView.isSuccess());
            assertEquals(1, resView.getRows().size());
            assertEquals("v", resView.getRows().get(0).getValue("relkind"), "view relkind must be 'v'");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testAttributeVirtualView(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE products (sku VARCHAR PRIMARY KEY, price INT, description VARCHAR DEFAULT 'N/A')");

            // Query std_attribute
            QueryResult resStd = db.execute("SELECT attname, atttypid, attnotnull, atthasdef FROM std_attribute");
            assertTrue(resStd.isSuccess(), "std_attribute query failed: " + resStd.getError());

            // Query pg_attribute compatibility alias
            QueryResult res = db.execute("SELECT attname, atttypid, attnotnull, atthasdef FROM pg_catalog.pg_attribute");
            assertTrue(res.isSuccess(), "pg_attribute query failed: " + res.getError());

            List<Tuple> rows = res.getRows();
            Tuple skuRow = rows.stream().filter(r -> "sku".equals(r.getValue("attname"))).findFirst().orElse(null);
            Tuple priceRow = rows.stream().filter(r -> "price".equals(r.getValue("attname"))).findFirst().orElse(null);
            Tuple descRow = rows.stream().filter(r -> "description".equals(r.getValue("attname"))).findFirst().orElse(null);

            assertNotNull(skuRow, "column 'sku' must be in attribute view");
            assertNotNull(priceRow, "column 'price' must be in attribute view");
            assertNotNull(descRow, "column 'description' must be in attribute view");

            assertEquals("t", skuRow.getValue("attnotnull"), "primary key column must have attnotnull='t'");
            assertEquals(1043, ((Number) skuRow.getValue("atttypid")).intValue(), "varchar type OID is 1043");

            assertEquals(23, ((Number) priceRow.getValue("atttypid")).intValue(), "integer type OID is 23");

            assertEquals("t", descRow.getValue("atthasdef"), "default value column must have atthasdef='t'");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testTypeVirtualView(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TYPE order_status AS ENUM ('PENDING', 'SHIPPED', 'DELIVERED')");

            QueryResult res = db.execute("SELECT typname, typtype FROM std_type");
            assertTrue(res.isSuccess(), "std_type query failed: " + res.getError());

            List<String> typeNames = res.getRows().stream()
                .map(r -> (String) r.getValue("typname"))
                .toList();

            assertTrue(typeNames.contains("bool"));
            assertTrue(typeNames.contains("int4"));
            assertTrue(typeNames.contains("int8"));
            assertTrue(typeNames.contains("varchar"));
            assertTrue(typeNames.contains("text"));
            assertTrue(typeNames.contains("timestamp"));
            assertTrue(typeNames.contains("jsonb"));
            assertTrue(typeNames.contains("order_status"), "custom enum must be present in type view");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testNamespaceVirtualView(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            QueryResult res = db.execute("SELECT nspname FROM std_namespace");
            assertTrue(res.isSuccess(), "std_namespace query failed: " + res.getError());

            List<String> nspNames = res.getRows().stream()
                .map(r -> (String) r.getValue("nspname"))
                .toList();

            assertTrue(nspNames.contains("public"));
            assertTrue(nspNames.contains("std_catalog"));
            assertTrue(nspNames.contains("information_schema"));
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testIndexVirtualView(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE orders (order_id INT PRIMARY KEY, customer_id INT, amount INT)");
            db.execute("CREATE INDEX idx_cust ON orders (customer_id)");

            QueryResult res = db.execute("SELECT indexrelid, indrelid, indisunique, indisprimary FROM std_index");
            assertTrue(res.isSuccess(), "std_index query failed: " + res.getError());
            assertEquals(2, res.getRows().size(), "two indexes expected (PK + secondary index)");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testInformationSchemaTables(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR)");
            db.execute("CREATE VIEW cust_names AS SELECT name FROM customers");

            QueryResult res = db.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public'");
            assertTrue(res.isSuccess(), "information_schema.tables query failed: " + res.getError());

            List<Tuple> rows = res.getRows();
            Tuple custRow = rows.stream().filter(r -> "customers".equals(r.getValue("table_name"))).findFirst().orElse(null);
            Tuple viewRow = rows.stream().filter(r -> "cust_names".equals(r.getValue("table_name"))).findFirst().orElse(null);

            assertNotNull(custRow);
            assertEquals("BASE TABLE", custRow.getValue("table_type"));

            assertNotNull(viewRow);
            assertEquals("VIEW", viewRow.getValue("table_type"));
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testCountOverVirtualViews(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE t1 (id INT)");
            db.execute("CREATE TABLE t2 (id INT)");

            QueryResult res = db.execute("SELECT COUNT(*) AS total_classes FROM std_class WHERE relkind = 'r'");
            assertTrue(res.isSuccess(), "COUNT(*) over std_class failed: " + res.getError());
            assertFalse(res.getRows().isEmpty());
            long count = ((Number) res.getRows().get(0).getValue("total_classes")).longValue();
            assertTrue(count >= 2, "must count at least the 2 newly created tables");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testCatalogIntrospectionFunctions(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            QueryResult res1 = db.execute("SELECT format_type(23, -1) AS type_name");
            assertTrue(res1.isSuccess());
            assertEquals("integer", res1.getRows().get(0).getValue("type_name"));

            QueryResult res1Std = db.execute("SELECT std_format_type(23, -1) AS type_name");
            assertTrue(res1Std.isSuccess());
            assertEquals("integer", res1Std.getRows().get(0).getValue("type_name"));

            QueryResult res2 = db.execute("SELECT table_is_visible(16384) AS is_vis");
            assertTrue(res2.isSuccess());
            assertEquals(true, res2.getRows().get(0).getValue("is_vis"));

            QueryResult res3 = db.execute("SELECT std_get_userbyid(10) AS uname");
            assertTrue(res3.isSuccess());
            assertEquals("stratos", res3.getRows().get(0).getValue("uname"));

            QueryResult res4 = db.execute("SELECT encoding_to_char(6) AS enc");
            assertTrue(res4.isSuccess());
            assertEquals("UTF8", res4.getRows().get(0).getValue("enc"));

            QueryResult res5 = db.execute("SELECT quote_ident('my_column') AS ident");
            assertTrue(res5.isSuccess());
            assertEquals("my_column", res5.getRows().get(0).getValue("ident"));
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testStratosCatalogAliases(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR)");
            db.execute("CREATE VIEW active_items AS SELECT * FROM items");
            db.execute("CREATE INDEX idx_item_name ON items (name)");

            QueryResult resClass = db.execute("SELECT relname FROM stratos_class WHERE relname = 'items'");
            assertTrue(resClass.isSuccess());
            assertEquals(1, resClass.getRows().size());

            QueryResult resTables = db.execute("SELECT tablename FROM std_tables WHERE tablename = 'items'");
            assertTrue(resTables.isSuccess());
            assertEquals(1, resTables.getRows().size());

            QueryResult resViews = db.execute("SELECT viewname FROM std_views WHERE viewname = 'active_items'");
            assertTrue(resViews.isSuccess());
            assertEquals(1, resViews.getRows().size());

            QueryResult resIndexes = db.execute("SELECT indexname FROM std_indexes WHERE indexname = 'idx_item_name'");
            assertTrue(resIndexes.isSuccess());
            assertEquals(1, resIndexes.getRows().size());
        } finally {
            db.shutdown();
        }
    }
}
