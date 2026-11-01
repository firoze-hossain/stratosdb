package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.disk.DiskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests for Sprint 7 Week 14:
 * REINDEX TABLE & REINDEX INDEX statements across all supported index types.
 */
class ReindexTableEndToEndTest {

    @TempDir
    Path tempDir;

    private StratosDB db;

    @BeforeEach
    void setUp() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setBufferPoolSize(256);
        db = new StratosDB(config);
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.shutdown();
        }
    }

    @Test
    void testReindexTableBTree() {
        QueryResult r1 = db.execute("CREATE TABLE products (id INT PRIMARY KEY, category_id INT, price INT)");
        assertTrue(r1.isSuccess());

        QueryResult r2 = db.execute("CREATE INDEX idx_products_cat ON products (category_id) USING BTREE");
        assertTrue(r2.isSuccess());

        for (int i = 1; i <= 50; i++) {
            QueryResult ins = db.execute("INSERT INTO products VALUES (" + i + ", " + (i % 5) + ", " + (i * 10) + ")");
            assertTrue(ins.isSuccess());
        }

        // Delete some rows
        db.execute("DELETE FROM products WHERE id <= 20");

        // Execute REINDEX TABLE
        QueryResult reindexRes = db.execute("REINDEX TABLE products;");
        assertTrue(reindexRes.isSuccess(), "REINDEX TABLE must succeed: " + reindexRes.getMessage());
        assertTrue(reindexRes.getMessage().contains("rebuilt"), "Message should mention rebuilt indexes");

        // Verify index lookups still return exactly the visible rows
        QueryResult q1 = db.execute("SELECT * FROM products WHERE id = 25;");
        assertTrue(q1.isSuccess());
        assertEquals(1, q1.getRows().size());

        QueryResult q2 = db.execute("SELECT * FROM products WHERE id = 10;");
        assertTrue(q2.isSuccess());
        assertEquals(0, q2.getRows().size(), "Deleted row must not be found via indexed lookup");

        QueryResult q3 = db.execute("SELECT * FROM products WHERE category_id = 0;");
        assertTrue(q3.isSuccess());
        // IDs 25, 30, 35, 40, 45, 50 have category_id = 0 (6 rows)
        assertEquals(6, q3.getRows().size());
    }

    @Test
    void testReindexSingleIndex() {
        db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)");
        db.execute("CREATE INDEX idx_accounts_bal ON accounts (balance) USING BTREE");

        for (int i = 1; i <= 30; i++) {
            db.execute("INSERT INTO accounts VALUES (" + i + ", " + (i * 100) + ")");
        }

        QueryResult reindex = db.execute("REINDEX INDEX idx_accounts_bal;");
        assertTrue(reindex.isSuccess(), "REINDEX INDEX must succeed: " + reindex.getMessage());
        assertTrue(reindex.getMessage().contains("completed"));

        QueryResult q = db.execute("SELECT * FROM accounts WHERE balance = 1500;");
        assertTrue(q.isSuccess());
        assertEquals(1, q.getRows().size());
    }

    @Test
    void testReindexTableWithMultipleIndexTypes() {
        db.execute("CREATE TABLE sensors (id INT PRIMARY KEY, device_id INT, temp INT)");
        db.execute("CREATE INDEX idx_sensors_dev_btree ON sensors (device_id) USING BTREE");
        db.execute("CREATE INDEX idx_sensors_dev_hash ON sensors (device_id) USING HASH");
        db.execute("CREATE INDEX idx_sensors_temp_brin ON sensors (temp) USING BRIN");

        for (int i = 1; i <= 40; i++) {
            db.execute("INSERT INTO sensors VALUES (" + i + ", " + (i % 4) + ", " + (20 + i) + ")");
        }

        QueryResult reindex = db.execute("REINDEX TABLE sensors;");
        assertTrue(reindex.isSuccess());
        // Primary key BTree + 3 secondary indexes = 4 indexes rebuilt
        assertTrue(reindex.getMessage().contains("4 index(es) rebuilt"));

        // Point query using PK BTree
        QueryResult qPk = db.execute("SELECT * FROM sensors WHERE id = 15;");
        assertTrue(qPk.isSuccess());
        assertEquals(1, qPk.getRows().size());

        // Equality query using Hash index
        QueryResult qHash = db.execute("SELECT * FROM sensors WHERE device_id = 2;");
        assertTrue(qHash.isSuccess());
        assertEquals(10, qHash.getRows().size());

        // Range query
        QueryResult qRange = db.execute("SELECT * FROM sensors WHERE id >= 35;");
        assertTrue(qRange.isSuccess());
        assertEquals(6, qRange.getRows().size());
    }

    @Test
    void testReindexSyntaxVariants() {
        db.execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)");
        db.execute("CREATE INDEX idx_orders_amt ON orders (amount) USING BTREE");
        db.execute("INSERT INTO orders VALUES (1, 100)");

        // 1. REINDEX TABLE <tableName>
        QueryResult r1 = db.execute("REINDEX TABLE orders;");
        assertTrue(r1.isSuccess());

        // 2. REINDEX <tableName> (Postgres shorthand)
        QueryResult r2 = db.execute("REINDEX orders;");
        assertTrue(r2.isSuccess());

        // 3. REINDEX INDEX <indexName>
        QueryResult r3 = db.execute("REINDEX INDEX idx_orders_amt;");
        assertTrue(r3.isSuccess());
    }

    @Test
    void testReindexNonExistentTargetFails() {
        QueryResult r1 = db.execute("REINDEX TABLE nonexistent_table;");
        assertFalse(r1.isSuccess());
        assertTrue(r1.getError().contains("Table not found"));

        QueryResult r2 = db.execute("REINDEX INDEX nonexistent_index;");
        assertFalse(r2.isSuccess());
        assertTrue(r2.getError().contains("Index not found"));
    }

    @Test
    void testReindexReclaimsSpaceAfterHeavyDelete() {
        db.execute("CREATE TABLE heavy_items (id INT PRIMARY KEY, val INT)");
        db.execute("CREATE INDEX idx_heavy_val ON heavy_items (val) USING BTREE");

        // Insert 1500 items
        for (int i = 1; i <= 1500; i++) {
            db.execute("INSERT INTO heavy_items VALUES (" + i + ", " + i + ")");
        }
        db.execute("CHECKPOINT;");

        DiskManager dm = new DiskManager(tempDir.toString());
        long pagesBefore = dm.getTablePageCount("idx_heavy_val");
        assertTrue(pagesBefore >= 5, "Index should span multiple pages: " + pagesBefore);

        // Delete 1400 items
        db.execute("DELETE FROM heavy_items WHERE id <= 1400");
        db.execute("CHECKPOINT;");

        // Run REINDEX TABLE
        QueryResult reindex = db.execute("REINDEX TABLE heavy_items;");
        assertTrue(reindex.isSuccess());
        db.execute("CHECKPOINT;");

        long pagesAfter = dm.getTablePageCount("idx_heavy_val");
        assertTrue(pagesAfter < pagesBefore, "Physical file page count should be compacted from " + pagesBefore + " to " + pagesAfter);

        // Verify remaining 100 items are valid
        QueryResult q = db.execute("SELECT * FROM heavy_items WHERE id >= 1450;");
        assertTrue(q.isSuccess());
        assertEquals(51, q.getRows().size());
    }
}
