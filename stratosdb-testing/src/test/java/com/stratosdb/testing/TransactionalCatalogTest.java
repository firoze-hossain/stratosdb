package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sprint 3 Week 05: Transactional Relational System Catalog Test Suite.
 *
 * Verifies:
 *  1. Internal system tables (stratos_tables, stratos_columns, stratos_indexes,
 *     stratos_types, stratos_constraints) exist and are queryable.
 *  2. DDL operations (CREATE TABLE, ALTER TABLE, DROP TABLE, CREATE INDEX, CREATE TYPE)
 *     execute as transactional ACID writes against system tables.
 *  3. Transaction ROLLBACK rolls back schema changes in both in-memory relcache
 *     and relational system tables.
 *  4. SAVEPOINT rollback rolls back DDL changes within a transaction.
 *  5. Catalog metadata persists reliably without catalog.txt across clean shutdown
 *     and restart.
 *  6. System tables are protected from unauthorized DROP or ALTER.
 */
public class TransactionalCatalogTest {

    @Test
    void testSystemTablesExistAndAreQueryable(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            // SHOW TABLES must filter out internal system tables (empty DB reports 0 user tables)
            QueryResult showTables = db.execute("SHOW TABLES");
            assertTrue(showTables.isSuccess());
            assertEquals(0, showTables.getRows().size(), "SHOW TABLES should not display internal system tables");

            // Query stratos_tables directly
            QueryResult tablesResult = db.execute("SELECT table_name FROM stratos_tables");
            assertTrue(tablesResult.isSuccess(), "stratos_tables must be queryable: " + tablesResult.getError());
            List<String> tableNames = tablesResult.getRows().stream()
                .map(r -> (String) r.getValue("table_name"))
                .toList();

            assertTrue(tableNames.contains("stratos_tables"), "stratos_tables must be registered");
            assertTrue(tableNames.contains("stratos_columns"), "stratos_columns must be registered");
            assertTrue(tableNames.contains("stratos_indexes"), "stratos_indexes must be registered");
            assertTrue(tableNames.contains("stratos_types"), "stratos_types must be registered");
            assertTrue(tableNames.contains("stratos_constraints"), "stratos_constraints must be registered");

            // Query stratos_columns directly
            QueryResult colsResult = db.execute("SELECT column_name FROM stratos_columns WHERE table_name = 'stratos_tables'");
            assertTrue(colsResult.isSuccess(), "stratos_columns must be queryable");
            List<String> colNames = colsResult.getRows().stream()
                .map(r -> (String) r.getValue("column_name"))
                .toList();
            assertTrue(colNames.contains("table_id"));
            assertTrue(colNames.contains("table_name"));
            assertTrue(colNames.contains("table_type"));
            assertTrue(colNames.contains("owner"));
            assertTrue(colNames.contains("ddl"));
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testCreateTableUpdatesSystemTables(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            QueryResult create = db.execute("CREATE TABLE users (id INT, name VARCHAR, active VARCHAR DEFAULT 'YES', PRIMARY KEY (id))");
            assertTrue(create.isSuccess(), "CREATE TABLE must succeed: " + create.getError());

            // Check stratos_tables
            QueryResult tableQuery = db.execute("SELECT table_name, table_type FROM stratos_tables WHERE table_name = 'users'");
            assertTrue(tableQuery.isSuccess());
            assertEquals(1, tableQuery.getRows().size());
            assertEquals("users", tableQuery.getRows().get(0).getValue("table_name"));
            assertEquals("BASE TABLE", tableQuery.getRows().get(0).getValue("table_type"));

            // Check stratos_columns
            QueryResult colQuery = db.execute("SELECT column_name, data_type, column_default FROM stratos_columns WHERE table_name = 'users'");
            assertTrue(colQuery.isSuccess());
            assertEquals(3, colQuery.getRows().size());

            // Check stratos_constraints
            QueryResult constraintQuery = db.execute("SELECT constraint_name, constraint_type, definition FROM stratos_constraints WHERE table_name = 'users'");
            assertTrue(constraintQuery.isSuccess());
            assertEquals(1, constraintQuery.getRows().size());
            assertEquals("PRIMARY KEY", constraintQuery.getRows().get(0).getValue("constraint_type"));
            assertEquals("id", constraintQuery.getRows().get(0).getValue("definition"));

            // Check SHOW TABLES
            QueryResult showTables = db.execute("SHOW TABLES");
            assertTrue(showTables.isSuccess());
            assertEquals(1, showTables.getRows().size());
            assertEquals("users", showTables.getRows().get(0).getValue("table_name"));
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testCreateTableRollback(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            assertTrue(db.execute("BEGIN").isSuccess());
            QueryResult create = db.execute("CREATE TABLE temp_tab (x INT, y VARCHAR)");
            assertTrue(create.isSuccess());

            // In-transaction visibility: temp_tab is visible
            QueryResult insideQuery = db.execute("SELECT table_name FROM stratos_tables WHERE table_name = 'temp_tab'");
            assertTrue(insideQuery.isSuccess());
            assertEquals(1, insideQuery.getRows().size());

            QueryResult insideCols = db.execute("SELECT column_name FROM stratos_columns WHERE table_name = 'temp_tab'");
            assertTrue(insideCols.isSuccess());
            assertEquals(2, insideCols.getRows().size());

            // Rollback the transaction
            assertTrue(db.execute("ROLLBACK").isSuccess());

            // After rollback: temp_tab must be absent from system tables and queryable tables
            QueryResult outsideQuery = db.execute("SELECT table_name FROM stratos_tables WHERE table_name = 'temp_tab'");
            assertTrue(outsideQuery.isSuccess());
            assertEquals(0, outsideQuery.getRows().size(), "temp_tab should be rolled back from stratos_tables");

            QueryResult outsideCols = db.execute("SELECT column_name FROM stratos_columns WHERE table_name = 'temp_tab'");
            assertTrue(outsideCols.isSuccess());
            assertEquals(0, outsideCols.getRows().size(), "columns of temp_tab should be rolled back from stratos_columns");

            QueryResult selectAttempt = db.execute("SELECT * FROM temp_tab");
            assertFalse(selectAttempt.isSuccess(), "Querying rolled-back table should fail: " + selectAttempt.getError());

            QueryResult showTables = db.execute("SHOW TABLES");
            assertEquals(0, showTables.getRows().size(), "SHOW TABLES should report 0 tables after rollback");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testDropTableRollback(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            assertTrue(db.execute("CREATE TABLE keep_me (x INT, info VARCHAR)").isSuccess());
            assertTrue(db.execute("INSERT INTO keep_me VALUES (42, 'secret')").isSuccess());

            assertTrue(db.execute("BEGIN").isSuccess());
            QueryResult drop = db.execute("DROP TABLE keep_me");
            assertTrue(drop.isSuccess());

            // In-transaction: keep_me tombstoned in stratos_tables and hidden in SHOW TABLES
            QueryResult inTxnSys = db.execute("SELECT table_name FROM stratos_tables WHERE table_name = 'keep_me'");
            assertTrue(inTxnSys.isSuccess(), "in-txn sys table query must succeed: " + inTxnSys.getError());
            assertEquals(0, inTxnSys.getRows().size(), "keep_me should be tombstoned in stratos_tables");

            QueryResult inTxnShow = db.execute("SHOW TABLES");
            assertTrue(inTxnShow.isSuccess());
            assertFalse(inTxnShow.getRows().stream().anyMatch(r -> "keep_me".equals(r.getValue("table_name"))),
                "keep_me should not appear in SHOW TABLES");

            // Rollback
            assertTrue(db.execute("ROLLBACK").isSuccess());

            // After rollback: keep_me must be completely restored with data!
            QueryResult restoredSelect = db.execute("SELECT * FROM keep_me");
            assertTrue(restoredSelect.isSuccess(), "keep_me should be restored after rollback: " + restoredSelect.getError());
            assertEquals(1, restoredSelect.getRows().size());
            assertEquals(42, restoredSelect.getRows().get(0).getValue("x"));
            assertEquals("secret", restoredSelect.getRows().get(0).getValue("info"));

            QueryResult restoredSys = db.execute("SELECT table_name FROM stratos_tables WHERE table_name = 'keep_me'");
            assertTrue(restoredSys.isSuccess());
            assertEquals(1, restoredSys.getRows().size(), "keep_me should be restored in stratos_tables");

            QueryResult restoredCols = db.execute("SELECT column_name FROM stratos_columns WHERE table_name = 'keep_me'");
            assertTrue(restoredCols.isSuccess());
            assertEquals(2, restoredCols.getRows().size(), "columns should be restored in stratos_columns");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testAlterTableAddColumnRollback(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            assertTrue(db.execute("CREATE TABLE items (id INT)").isSuccess());
            assertTrue(db.execute("INSERT INTO items VALUES (1)").isSuccess());

            assertTrue(db.execute("BEGIN").isSuccess());
            QueryResult alter = db.execute("ALTER TABLE items ADD COLUMN price VARCHAR");
            assertTrue(alter.isSuccess());

            // In-transaction: price column exists
            QueryResult inTxnCols = db.execute("SELECT column_name FROM stratos_columns WHERE table_name = 'items'");
            assertTrue(inTxnCols.isSuccess());
            assertEquals(2, inTxnCols.getRows().size());

            // Rollback
            assertTrue(db.execute("ROLLBACK").isSuccess());

            // After rollback: price column must be gone
            QueryResult postRollbackCols = db.execute("SELECT column_name FROM stratos_columns WHERE table_name = 'items'");
            assertTrue(postRollbackCols.isSuccess());
            assertEquals(1, postRollbackCols.getRows().size());
            assertEquals("id", postRollbackCols.getRows().get(0).getValue("column_name"));

            QueryResult insertAttempt = db.execute("INSERT INTO items (id, price) VALUES (2, '99.99')");
            assertFalse(insertAttempt.isSuccess(), "price column should no longer exist for INSERT: " + insertAttempt.getError());
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testSavepointRollbackDdl(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            assertTrue(db.execute("BEGIN").isSuccess());
            assertTrue(db.execute("CREATE TABLE t1 (id INT)").isSuccess());

            assertTrue(db.execute("SAVEPOINT sp1").isSuccess());
            assertTrue(db.execute("CREATE TABLE t2 (id INT)").isSuccess());

            // Inside savepoint: t1 and t2 both exist
            QueryResult q1 = db.execute("SELECT table_name FROM stratos_tables WHERE table_name = 't2'");
            assertEquals(1, q1.getRows().size());

            // Rollback to savepoint
            assertTrue(db.execute("ROLLBACK TO SAVEPOINT sp1").isSuccess());

            // After savepoint rollback: t2 is gone, t1 remains!
            QueryResult q2 = db.execute("SELECT table_name FROM stratos_tables WHERE table_name = 't2'");
            assertEquals(0, q2.getRows().size(), "t2 must be rolled back by savepoint");

            QueryResult q3 = db.execute("SELECT table_name FROM stratos_tables WHERE table_name = 't1'");
            assertEquals(1, q3.getRows().size(), "t1 must remain after rolling back to sp1");

            // Commit the transaction
            assertTrue(db.execute("COMMIT").isSuccess());

            QueryResult q4 = db.execute("SELECT table_name FROM stratos_tables WHERE table_name = 't1'");
            assertEquals(1, q4.getRows().size(), "t1 must be committed");

            QueryResult q5 = db.execute("SELECT table_name FROM stratos_tables WHERE table_name = 't2'");
            assertEquals(0, q5.getRows().size(), "t2 must remain absent");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testSystemCatalogPersistsWithoutCatalogTxt(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            assertTrue(db.execute("CREATE TABLE products (sku INT, title VARCHAR)").isSuccess());
            assertTrue(db.execute("CREATE INDEX idx_products_sku ON products (sku)").isSuccess());
            assertTrue(db.execute("CREATE TYPE order_status AS ENUM ('NEW', 'PROCESSING', 'SHIPPED')").isSuccess());
            assertTrue(db.execute("INSERT INTO products VALUES (101, 'Widget')").isSuccess());
        } finally {
            db.shutdown();
        }

        // Verification 1: catalog.txt MUST NOT exist!
        File catalogTxt = new File(tempDir.toFile(), "catalog.txt");
        assertFalse(catalogTxt.exists(), "catalog.txt is retired and must NOT be written to disk");

        // Verification 2: relational system table data files MUST exist
        File stratosTablesDat = new File(tempDir.toFile(), "stratos_tables.dat");
        assertTrue(stratosTablesDat.exists(), "stratos_tables.dat must exist on disk");
        assertTrue(stratosTablesDat.length() > 0, "stratos_tables.dat must not be empty");

        // Reopen database from disk
        StratosDB reopened = new StratosDB(config);
        try {
            // User table & data survived without catalog.txt
            QueryResult dataResult = reopened.execute("SELECT sku, title FROM products");
            assertTrue(dataResult.isSuccess(), "products table must survive without catalog.txt: " + dataResult.getError());
            assertEquals(1, dataResult.getRows().size());
            assertEquals(101, dataResult.getRows().get(0).getValue("sku"));
            assertEquals("Widget", dataResult.getRows().get(0).getValue("title"));

            // System table query shows products
            QueryResult sysTableResult = reopened.execute("SELECT table_name FROM stratos_tables WHERE table_name = 'products'");
            assertTrue(sysTableResult.isSuccess());
            assertEquals(1, sysTableResult.getRows().size());

            // System table query shows columns
            QueryResult sysColsResult = reopened.execute("SELECT column_name FROM stratos_columns WHERE table_name = 'products'");
            assertTrue(sysColsResult.isSuccess());
            assertEquals(2, sysColsResult.getRows().size());

            // System table query shows indexes
            QueryResult sysIdxResult = reopened.execute("SELECT index_name, table_name FROM stratos_indexes WHERE index_name = 'idx_products_sku'");
            assertTrue(sysIdxResult.isSuccess());
            assertEquals(1, sysIdxResult.getRows().size());

            // System table query shows types
            QueryResult sysTypeResult = reopened.execute("SELECT type_name FROM stratos_types WHERE type_name = 'order_status'");
            assertTrue(sysTypeResult.isSuccess());
            assertEquals(1, sysTypeResult.getRows().size());

            // SHOW CATALOG works correctly
            QueryResult showCatalog = reopened.execute("SHOW CATALOG");
            assertTrue(showCatalog.isSuccess());
            boolean hasProducts = showCatalog.getRows().stream().anyMatch(r -> "products".equals(r.getValue("object_name")));
            boolean hasIndex = showCatalog.getRows().stream().anyMatch(r -> "idx_products_sku".equals(r.getValue("object_name")));
            assertTrue(hasProducts, "SHOW CATALOG must include products table");
            assertTrue(hasIndex, "SHOW CATALOG must include idx_products_sku index");
        } finally {
            reopened.shutdown();
        }
    }

    @Test
    void testSystemTablesProtected(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            QueryResult dropResult = db.execute("DROP TABLE stratos_tables");
            assertFalse(dropResult.isSuccess(), "Dropping system table must be rejected");
            assertTrue(dropResult.getError().contains("system table"));

            QueryResult alterResult = db.execute("ALTER TABLE stratos_columns ADD COLUMN hack INT");
            assertFalse(alterResult.isSuccess(), "Altering system table must be rejected");
            assertTrue(alterResult.getError().contains("system table"));

            QueryResult createResult = db.execute("CREATE TABLE stratos_tables (id INT)");
            assertFalse(createResult.isSuccess(), "Creating table with reserved system name must be rejected");
        } finally {
            db.shutdown();
        }
    }
}
