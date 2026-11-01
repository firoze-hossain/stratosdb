package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests for Sprint 8 Week 15:
 * Statistics Engine (ANALYZE 2.0) with HyperLogLog, MCV lists,
 * equi-depth histograms, and stratos_statistic persistence.
 */
class StatisticsEngineEndToEndTest {

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
    void testAnalyzeSyntaxVariants() {
        db.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR)");
        db.execute("CREATE TABLE t2 (id INT PRIMARY KEY, val INT)");
        db.execute("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')");
        db.execute("INSERT INTO t2 VALUES (10, 100), (20, 200), (30, 300)");

        // 1. ANALYZE tableName
        QueryResult r1 = db.execute("ANALYZE t1;");
        assertTrue(r1.isSuccess());
        assertEquals("Analyzed t1: 2 row(s), 2 column(s)", r1.getMessage());

        // 2. ANALYZE TABLE tableName
        QueryResult r2 = db.execute("ANALYZE TABLE t2;");
        assertTrue(r2.isSuccess());
        assertEquals("Analyzed t2: 3 row(s), 2 column(s)", r2.getMessage());

        // 3. ANALYZE (all tables)
        QueryResult r3 = db.execute("ANALYZE;");
        assertTrue(r3.isSuccess());
        assertTrue(r3.getMessage().contains("ANALYZE completed for"));
    }

    @Test
    void testMcvAndSkewedDistribution() {
        db.execute("CREATE TABLE orders (id INT PRIMARY KEY, country VARCHAR, amount INT)");

        // Skewed country distribution:
        // 70 x 'US', 20 x 'CA', 10 x 'MX'
        for (int i = 1; i <= 70; i++) {
            db.execute("INSERT INTO orders VALUES (" + i + ", 'US', 100)");
        }
        for (int i = 71; i <= 90; i++) {
            db.execute("INSERT INTO orders VALUES (" + i + ", 'CA', 150)");
        }
        for (int i = 91; i <= 100; i++) {
            db.execute("INSERT INTO orders VALUES (" + i + ", 'MX', 200)");
        }

        QueryResult analyzeRes = db.execute("ANALYZE orders;");
        assertTrue(analyzeRes.isSuccess());

        // Query stratos_statistic
        QueryResult statRes = db.execute("SELECT column_name, row_count, n_distinct, most_common_vals, most_common_freqs FROM stratosdb_statistic WHERE table_name = 'orders' AND column_name = 'country';");
        if (!statRes.isSuccess()) {
            statRes = db.execute("SELECT column_name, row_count, n_distinct, most_common_vals, most_common_freqs FROM stratos_statistic WHERE table_name = 'orders' AND column_name = 'country';");
        }
        assertTrue(statRes.isSuccess(), "stat query failed: " + statRes.getError());
        assertEquals(1, statRes.getRows().size());

        var statRow = statRes.getRows().get(0);
        assertEquals("country", statRow.getValue("column_name"));
        assertEquals(100, ((Number) statRow.getValue("row_count")).intValue());
        assertEquals(3.0, ((Number) statRow.getValue("n_distinct")).doubleValue(), 1.0); // 3 distinct values

        String mcvVals = statRow.getValue("most_common_vals").toString();
        String mcvFreqs = statRow.getValue("most_common_freqs").toString();
        assertTrue(mcvVals.contains("US"));
        assertTrue(mcvFreqs.contains("0.7"));
    }

    @Test
    void testNullFractionAndAverageWidth() {
        db.execute("CREATE TABLE profiles (id INT PRIMARY KEY, bio VARCHAR)");

        // 100 rows: 25 NULL bios, 75 rows with bio of 10 chars
        for (int i = 1; i <= 25; i++) {
            db.execute("INSERT INTO profiles VALUES (" + i + ", NULL)");
        }
        for (int i = 26; i <= 100; i++) {
            db.execute("INSERT INTO profiles VALUES (" + i + ", '0123456789')");
        }

        db.execute("ANALYZE profiles;");

        QueryResult statRes = db.execute("SELECT column_name, null_frac, avg_width FROM stratos_statistic WHERE table_name = 'profiles' AND column_name = 'bio';");
        assertTrue(statRes.isSuccess(), statRes.getError());
        assertEquals(1, statRes.getRows().size());

        var row = statRes.getRows().get(0);
        assertEquals("bio", row.getValue("column_name"));
        double nullFrac = ((Number) row.getValue("null_frac")).doubleValue();
        assertEquals(0.25, nullFrac, 0.05);

        int avgWidth = ((Number) row.getValue("avg_width")).intValue();
        assertTrue(avgWidth >= 8, "Expected avgWidth to reflect string length, got " + avgWidth);
    }

    @Test
    void testEquiDepthHistogramClustering() {
        db.execute("CREATE TABLE sensor_data (id INT PRIMARY KEY, val INT)");

        // Clustered data: 80 values in [1..10], 20 values in [100..1000]
        for (int i = 1; i <= 80; i++) {
            db.execute("INSERT INTO sensor_data VALUES (" + i + ", " + (i % 10 + 1) + ")");
        }
        for (int i = 81; i <= 100; i++) {
            db.execute("INSERT INTO sensor_data VALUES (" + i + ", " + (i * 10) + ")");
        }

        db.execute("ANALYZE sensor_data;");

        QueryResult statRes = db.execute("SELECT column_name, histogram_bounds FROM stratos_statistic WHERE table_name = 'sensor_data' AND column_name = 'val';");
        assertTrue(statRes.isSuccess(), statRes.getError());
        assertEquals(1, statRes.getRows().size());

        String histBounds = statRes.getRows().get(0).getValue("histogram_bounds").toString();
        assertNotNull(histBounds);
        assertTrue(histBounds.startsWith("[") && histBounds.endsWith("]"));
    }

    @Test
    void testCatalogViewsStdStatisticAndPgClass() {
        db.execute("CREATE TABLE items (id INT PRIMARY KEY, price INT)");
        for (int i = 1; i <= 50; i++) {
            db.execute("INSERT INTO items VALUES (" + i + ", " + (i * 5) + ")");
        }

        db.execute("ANALYZE items;");

        // Test pg_statistic virtual view
        QueryResult pgStatRes = db.execute("SELECT tablename, attname, null_frac FROM pg_statistic WHERE tablename = 'items';");
        assertTrue(pgStatRes.isSuccess(), pgStatRes.getError());
        assertEquals(2, pgStatRes.getRows().size()); // id and price

        // Test pg_class reltuples and relpages updated by statistics
        QueryResult pgClassRes = db.execute("SELECT relname, reltuples, relpages FROM pg_class WHERE relname = 'items';");
        assertTrue(pgClassRes.isSuccess(), pgClassRes.getError());
        assertEquals(1, pgClassRes.getRows().size());

        var classRow = pgClassRes.getRows().get(0);
        assertEquals("items", classRow.getValue("relname"));
        assertEquals(50.0f, ((Number) classRow.getValue("reltuples")).floatValue(), 1e-3);
        assertTrue(((Number) classRow.getValue("relpages")).intValue() >= 1);
    }

    @Test
    void testPersistenceAcrossEngineRestart() {
        db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)");
        for (int i = 1; i <= 40; i++) {
            db.execute("INSERT INTO accounts VALUES (" + i + ", " + (i * 100) + ")");
        }

        QueryResult r = db.execute("ANALYZE accounts;");
        assertTrue(r.isSuccess());

        // Verify statistics exist
        QueryResult statBefore = db.execute("SELECT column_name, row_count FROM stratos_statistic WHERE table_name = 'accounts';");
        assertTrue(statBefore.isSuccess());
        assertEquals(2, statBefore.getRows().size());

        // Shutdown database
        db.shutdown();
        db = null;

        // Restart database using same directory
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setBufferPoolSize(256);
        db = new StratosDB(config);

        // Verify statistics were loaded upon restart
        QueryResult statAfter = db.execute("SELECT column_name, row_count FROM stratos_statistic WHERE table_name = 'accounts';");
        assertTrue(statAfter.isSuccess(), "stratos_statistic query failed after restart: " + statAfter.getMessage());
        assertEquals(2, statAfter.getRows().size());
        assertEquals(40, ((Number) statAfter.getRows().get(0).getValue("row_count")).intValue());
    }

    @Test
    void testDropTableCleansUpStatistics() {
        db.execute("CREATE TABLE doomed (id INT PRIMARY KEY, val INT)");
        db.execute("INSERT INTO doomed VALUES (1, 100), (2, 200)");
        db.execute("ANALYZE doomed;");

        QueryResult statBefore = db.execute("SELECT * FROM stratos_statistic WHERE table_name = 'doomed';");
        assertTrue(statBefore.isSuccess());
        assertEquals(2, statBefore.getRows().size());

        // DROP TABLE
        QueryResult dropRes = db.execute("DROP TABLE doomed;");
        assertTrue(dropRes.isSuccess());

        // Verify statistics are purged
        QueryResult statAfter = db.execute("SELECT * FROM stratos_statistic WHERE table_name = 'doomed';");
        assertTrue(statAfter.isSuccess());
        assertEquals(0, statAfter.getRows().size());
    }
}
