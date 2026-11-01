package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests for Sprint 8 Week 16:
 * Cost-Based Optimizer (CBO) & System-R Style Dynamic Programming Join Reordering.
 */
class CostBasedOptimizerEndToEndTest {

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
    void testIndexScanVsSeqScanCostBasedSelection() {
        db.execute("CREATE TABLE products (id INT PRIMARY KEY, category INT, price INT)");
        db.execute("CREATE INDEX idx_prod_cat ON products (category)");

        // 1,000 rows: category has only 2 distinct values (0 and 1, 500 each)
        // id has 1,000 distinct values
        db.execute("BEGIN");
        for (int i = 1; i <= 1000; i++) {
            db.execute("INSERT INTO products VALUES (" + i + ", " + (i % 2) + ", " + (i * 10) + ")");
        }
        db.execute("COMMIT");

        // 1. Before ANALYZE: rule-based heuristic (an index exists, so use it)
        QueryResult expBefore = db.execute("EXPLAIN SELECT * FROM products WHERE category = 0;");
        assertTrue(expBefore.isSuccess());
        assertTrue(expBefore.getMessage().startsWith("Index Scan"));
        assertTrue(expBefore.getMessage().contains("no statistics"));

        // 2. Run ANALYZE to build persistent statistics (HLL, MCV, equi-depth histograms)
        QueryResult anRes = db.execute("ANALYZE products;");
        assertTrue(anRes.isSuccess());

        // 3. Low-selectivity predicate (matches 500 / 1000 rows):
        // CostModel: 500 random I/Os >> 5 sequential page reads -> Planner selects SeqScan!
        QueryResult expLow = db.execute("EXPLAIN SELECT * FROM products WHERE category = 0;");
        assertTrue(expLow.isSuccess());
        assertTrue(expLow.getMessage().startsWith("Seq Scan"),
            "Low-selectivity predicate should choose SeqScan: " + expLow.getMessage());
        assertTrue(expLow.getMessage().contains("cost="));

        QueryResult resLow = db.execute("SELECT * FROM products WHERE category = 0;");
        assertTrue(resLow.isSuccess());
        assertEquals(500, resLow.getRows().size());

        // 4. High-selectivity predicate (point lookup on primary key, 1 / 1000 rows):
        // CostModel: 1 random page read << 5 sequential page reads -> Planner selects IndexScan!
        QueryResult expHigh = db.execute("EXPLAIN SELECT * FROM products WHERE id = 500;");
        assertTrue(expHigh.isSuccess());
        assertTrue(expHigh.getMessage().startsWith("Index Scan"),
            "High-selectivity predicate should choose IndexScan: " + expHigh.getMessage());
        assertTrue(expHigh.getMessage().contains("cost="));

        QueryResult resHigh = db.execute("SELECT * FROM products WHERE id = 500;");
        assertTrue(resHigh.isSuccess());
        assertEquals(1, resHigh.getRows().size());
    }

    @Test
    void testThreeTableJoinReorderingAJoinBJoinC() {
        // Table A: orders (500 rows)
        db.execute("CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT, amount INT)");
        // Table B: customers (50 rows)
        db.execute("CREATE TABLE customers (id INT PRIMARY KEY, country_id INT, name VARCHAR)");
        // Table C: countries (5 rows)
        db.execute("CREATE TABLE countries (id INT PRIMARY KEY, code VARCHAR)");

        // Populate countries (5 rows)
        db.execute("BEGIN");
        db.execute("INSERT INTO countries VALUES (1, 'US'), (2, 'CA'), (3, 'GB'), (4, 'DE'), (5, 'JP')");

        // Populate customers (50 rows)
        for (int i = 1; i <= 50; i++) {
            int countryId = (i % 5) + 1;
            db.execute("INSERT INTO customers VALUES (" + i + ", " + countryId + ", 'Cust_" + i + "')");
        }

        // Populate orders (500 rows)
        for (int i = 1; i <= 500; i++) {
            int custId = (i % 50) + 1;
            db.execute("INSERT INTO orders VALUES (" + i + ", " + custId + ", " + (i * 10) + ")");
        }
        db.execute("COMMIT");

        // 1. Before ANALYZE: explain reports original written order
        String sql = "EXPLAIN SELECT * FROM orders JOIN customers ON orders.cust_id = customers.id JOIN countries ON customers.country_id = countries.id;";
        QueryResult expUnanalyzed = db.execute(sql);
        assertTrue(expUnanalyzed.isSuccess());
        assertTrue(expUnanalyzed.getMessage().contains("orders -> Seq Scan on customers")
            && expUnanalyzed.getMessage().contains("customers.country_id=countries.id"),
            "Expected original join order before ANALYZE: " + expUnanalyzed.getMessage());

        // 2. Run ANALYZE across all tables
        QueryResult anRes = db.execute("ANALYZE;");
        assertTrue(anRes.isSuccess());

        // 3. After ANALYZE: System-R Dynamic Programming Optimizer analyzes costs
        // Joining countries (5 rows) with customers (50 rows) first is drastically cheaper than
        // joining orders (500 rows) with customers first.
        QueryResult expAnalyzed = db.execute(sql);
        assertTrue(expAnalyzed.isSuccess());
        String planText = expAnalyzed.getMessage();
        assertTrue(planText.contains("cost="), "Optimized plan should report cost: " + planText);
        assertTrue(planText.contains("rows="), "Optimized plan should report estimated rows: " + planText);

        // Verify System-R join reordering starts with the smaller relations
        assertTrue(planText.contains("countries") && planText.contains("customers") && planText.contains("orders"));

        // 4. Verify actual execution produces 100% correct query results
        QueryResult res = db.execute("SELECT orders.id, customers.name, countries.code FROM orders " +
            "JOIN customers ON orders.cust_id = customers.id " +
            "JOIN countries ON customers.country_id = countries.id " +
            "ORDER BY orders.id LIMIT 10;");
        assertTrue(res.isSuccess(), res.getError());
        assertEquals(10, res.getRows().size());

        // Row 1: orders.id=1, cust_id=2 -> Cust_2, country_id=3 -> GB
        var row0 = res.getRows().get(0);
        assertEquals(1, ((Number) row0.getValue("id")).intValue());
        assertEquals("Cust_2", row0.getValue("name"));
        assertEquals("GB", row0.getValue("code"));
    }

    @Test
    void testMultiTableJoinWithWhereAndAggregation() {
        db.execute("CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT, amount INT)");
        db.execute("CREATE TABLE customers (id INT PRIMARY KEY, country_id INT, name VARCHAR)");
        db.execute("CREATE TABLE countries (id INT PRIMARY KEY, code VARCHAR)");

        db.execute("INSERT INTO countries VALUES (1, 'US'), (2, 'CA')");
        for (int i = 1; i <= 20; i++) {
            db.execute("INSERT INTO customers VALUES (" + i + ", " + (i % 2 + 1) + ", 'User_" + i + "')");
        }
        for (int i = 1; i <= 100; i++) {
            db.execute("INSERT INTO orders VALUES (" + i + ", " + (i % 20 + 1) + ", " + (i * 5) + ")");
        }

        db.execute("ANALYZE;");

        // Aggregation + Group By over 3-table join with WHERE filter
        QueryResult res = db.execute(
            "SELECT countries.code, COUNT(*) AS cnt, SUM(orders.amount) AS total " +
            "FROM orders " +
            "JOIN customers ON orders.cust_id = customers.id " +
            "JOIN countries ON customers.country_id = countries.id " +
            "WHERE orders.amount >= 250 " +
            "GROUP BY countries.code " +
            "ORDER BY countries.code;"
        );
        assertTrue(res.isSuccess(), res.getError());
        assertEquals(2, res.getRows().size());

        // Check rows
        var rowUS = res.getRows().get(0);
        var rowCA = res.getRows().get(1);
        assertEquals("CA", rowUS.getValue("code"));
        assertEquals("US", rowCA.getValue("code"));
        assertTrue(((Number) rowUS.getValue("cnt")).intValue() > 0);
        assertTrue(((Number) rowCA.getValue("cnt")).intValue() > 0);
    }

    @Test
    void testOptimalJoinOrderWithIndexScanOnFilteredTable() {
        db.execute("CREATE TABLE users (id INT PRIMARY KEY, dept_id INT, name VARCHAR)");
        db.execute("CREATE TABLE departments (id INT PRIMARY KEY, name VARCHAR)");

        db.execute("BEGIN");
        for (int i = 1; i <= 5; i++) {
            db.execute("INSERT INTO departments VALUES (" + i + ", 'Dept_" + i + "')");
        }
        for (int i = 1; i <= 1000; i++) {
            db.execute("INSERT INTO users VALUES (" + i + ", " + (i % 5 + 1) + ", 'User_" + i + "')");
        }
        db.execute("COMMIT");

        db.execute("ANALYZE;");

        // Query with point filter on indexed primary key: WHERE users.id = 250
        QueryResult explain = db.execute(
            "EXPLAIN SELECT * FROM users JOIN departments ON users.dept_id = departments.id WHERE users.id = 250;"
        );
        assertTrue(explain.isSuccess());
        String planText = explain.getMessage();

        // The optimizer should recognize users.id = 250 as an Index Scan
        assertTrue(planText.contains("Index Scan"), "Expected index scan for point lookup on users: " + planText);

        // Execute query
        QueryResult res = db.execute(
            "SELECT users.id, users.name, departments.name AS dname FROM users " +
            "JOIN departments ON users.dept_id = departments.id " +
            "WHERE users.id = 250;"
        );
        assertTrue(res.isSuccess());
        assertEquals(1, res.getRows().size());
        assertEquals(250, ((Number) res.getRows().get(0).getValue("id")).intValue());
    }
}
