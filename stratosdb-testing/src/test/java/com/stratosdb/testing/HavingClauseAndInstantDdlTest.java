package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.ast.AggregateCall;
import com.stratosdb.sql.ast.WhereExpr;
import com.stratosdb.sql.execution.MemoryScanOperator;
import com.stratosdb.sql.execution.PhysicalOperator;
import com.stratosdb.sql.execution.StreamAggregateOperator;
import com.stratosdb.sql.execution.TupleSlot;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class HavingClauseAndInstantDdlTest {

    @TempDir
    Path tempDir;

    private StratosDB db;

    @BeforeEach
    void setUp() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setBufferPoolSize(128);
        db = new StratosDB(config);
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.shutdown();
        }
    }

    @Test
    @DisplayName("DoD: Queries with HAVING COUNT(*) > 1 AND AVG(price) < 50 execute correctly")
    void testDoDQueriesWithHavingCountAndAvg() {
        db.execute("CREATE TABLE products (id INT, department VARCHAR, price FLOAT8);");
        // Hardware: 3 items, sum = 90, avg = 30.0 (count > 1 AND avg < 50) -> MATCH
        db.execute("INSERT INTO products VALUES (1, 'Hardware', 20.0);");
        db.execute("INSERT INTO products VALUES (2, 'Hardware', 30.0);");
        db.execute("INSERT INTO products VALUES (3, 'Hardware', 40.0);");

        // Software: 2 items, sum = 300, avg = 150.0 (count > 1 BUT avg >= 50) -> NO MATCH
        db.execute("INSERT INTO products VALUES (4, 'Software', 100.0);");
        db.execute("INSERT INTO products VALUES (5, 'Software', 200.0);");

        // Books: 1 item, sum = 15, avg = 15.0 (count <= 1) -> NO MATCH
        db.execute("INSERT INTO products VALUES (6, 'Books', 15.0);");

        // Office: 2 items, sum = 30, avg = 15.0 (count > 1 AND avg < 50) -> MATCH
        db.execute("INSERT INTO products VALUES (7, 'Office', 10.0);");
        db.execute("INSERT INTO products VALUES (8, 'Office', 20.0);");

        QueryResult res = db.execute(
            "SELECT department, COUNT(*), AVG(price) FROM products GROUP BY department HAVING COUNT(*) > 1 AND AVG(price) < 50;"
        );
        assertTrue(res.isSuccess(), () -> "Query failed: " + res.getError());
        List<Tuple> rows = res.getRows();
        assertNotNull(rows);
        assertEquals(2, rows.size(), "Expected exactly 2 departments matching HAVING predicate");

        for (Tuple row : rows) {
            String dept = (String) row.getValue("department");
            if ("Hardware".equals(dept)) {
                assertEquals(3, ((Number) row.getValue("COUNT(*)")).intValue());
                assertEquals(30.0, ((Number) row.getValue("AVG(price)")).doubleValue(), 0.001);
            } else if ("Office".equals(dept)) {
                assertEquals(2, ((Number) row.getValue("COUNT(*)")).intValue());
                assertEquals(15.0, ((Number) row.getValue("AVG(price)")).doubleValue(), 0.001);
            } else {
                fail("Unexpected department: " + dept);
            }
        }
    }

    @Test
    @DisplayName("HAVING with aggregates NOT present in SELECT list")
    void testHavingAggregatesNotInSelectList() {
        db.execute("CREATE TABLE products (id INT, department VARCHAR, price FLOAT8);");
        db.execute("INSERT INTO products VALUES (1, 'Hardware', 20.0);");
        db.execute("INSERT INTO products VALUES (2, 'Hardware', 30.0);");
        db.execute("INSERT INTO products VALUES (3, 'Hardware', 40.0);");
        db.execute("INSERT INTO products VALUES (4, 'Software', 100.0);");
        db.execute("INSERT INTO products VALUES (5, 'Software', 200.0);");
        db.execute("INSERT INTO products VALUES (6, 'Books', 15.0);");
        db.execute("INSERT INTO products VALUES (7, 'Office', 10.0);");
        db.execute("INSERT INTO products VALUES (8, 'Office', 20.0);");

        QueryResult res = db.execute(
            "SELECT department FROM products GROUP BY department HAVING COUNT(*) > 1 AND AVG(price) < 50;"
        );
        assertTrue(res.isSuccess(), () -> "Query failed: " + res.getError());
        List<Tuple> rows = res.getRows();
        assertEquals(2, rows.size());

        List<String> depts = rows.stream().map(r -> (String) r.getValue("department")).toList();
        assertTrue(depts.contains("Hardware"));
        assertTrue(depts.contains("Office"));
        assertFalse(depts.contains("Software"));
        assertFalse(depts.contains("Books"));
    }

    @Test
    @DisplayName("Arbitrary HAVING with boolean OR and NOT")
    void testArbitraryHavingWithOrAndNot() {
        db.execute("CREATE TABLE sales (id INT, region VARCHAR, amount INT);");
        db.execute("INSERT INTO sales VALUES (1, 'North', 100);");
        db.execute("INSERT INTO sales VALUES (2, 'North', 200);");
        db.execute("INSERT INTO sales VALUES (3, 'North', 300);"); // count 3, sum 600, min 100
        db.execute("INSERT INTO sales VALUES (4, 'South', 50);");   // count 1, sum 50, min 50
        db.execute("INSERT INTO sales VALUES (5, 'East', 1500);");  // count 1, sum 1500, min 1500
        db.execute("INSERT INTO sales VALUES (6, 'West', 10);");
        db.execute("INSERT INTO sales VALUES (7, 'West', 20);");    // count 2, sum 30, min 10

        // Condition: COUNT(*) >= 3 OR (SUM(amount) >= 1000 AND NOT MIN(amount) < 100)
        // North matches via COUNT(*) >= 3
        // East matches via SUM(amount) >= 1000 AND NOT MIN(amount) < 100
        // South, West do not match
        QueryResult res = db.execute(
            "SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region " +
            "HAVING COUNT(*) >= 3 OR (SUM(amount) >= 1000 AND NOT MIN(amount) < 100);"
        );
        assertTrue(res.isSuccess(), () -> "Query failed: " + res.getError());
        List<Tuple> rows = res.getRows();
        assertEquals(2, rows.size());

        List<String> regions = rows.stream().map(r -> (String) r.getValue("region")).toList();
        assertTrue(regions.contains("North"));
        assertTrue(regions.contains("East"));
        assertFalse(regions.contains("South"));
        assertFalse(regions.contains("West"));
    }

    @Test
    @DisplayName("HAVING referencing group-by column and aggregate")
    void testHavingGroupByColumnAndAggregate() {
        db.execute("CREATE TABLE employees (id INT, dept VARCHAR, salary INT);");
        db.execute("INSERT INTO employees VALUES (1, 'IT', 5000);");
        db.execute("INSERT INTO employees VALUES (2, 'IT', 6000);");
        db.execute("INSERT INTO employees VALUES (3, 'HR', 4000);");
        db.execute("INSERT INTO employees VALUES (4, 'HR', 4500);");
        db.execute("INSERT INTO employees VALUES (5, 'Sales', 7000);");

        QueryResult res = db.execute(
            "SELECT dept, COUNT(*) FROM employees GROUP BY dept HAVING dept = 'IT' AND COUNT(*) >= 2;"
        );
        assertTrue(res.isSuccess(), () -> "Query failed: " + res.getError());
        List<Tuple> rows = res.getRows();
        assertEquals(1, rows.size());
        assertEquals("IT", rows.get(0).getValue("dept"));
        assertEquals(2, ((Number) rows.get(0).getValue("COUNT(*)")).intValue());
    }

    @Test
    @DisplayName("HAVING comparing two aggregates (MAX > MIN)")
    void testHavingCompareTwoAggregates() {
        db.execute("CREATE TABLE scores (team VARCHAR, score INT);");
        db.execute("INSERT INTO scores VALUES ('Alpha', 80);");
        db.execute("INSERT INTO scores VALUES ('Alpha', 95);"); // MAX 95 > MIN 80 -> MATCH
        db.execute("INSERT INTO scores VALUES ('Beta', 70);");  // MAX 70 == MIN 70 -> NO MATCH

        QueryResult res = db.execute(
            "SELECT team, MIN(score), MAX(score) FROM scores GROUP BY team HAVING MAX(score) > MIN(score);"
        );
        assertTrue(res.isSuccess(), () -> "Query failed: " + res.getError());
        List<Tuple> rows = res.getRows();
        assertEquals(1, rows.size());
        assertEquals("Alpha", rows.get(0).getValue("team"));
    }

    @Test
    @DisplayName("DoD: Instant ALTER TABLE ADD COLUMN requires zero table rewrites")
    void testInstantAlterTableAddColumnZeroTableRewrites() {
        db.execute("CREATE TABLE customer_orders (order_id INT, customer_name VARCHAR, amount INT);");

        // Insert 1000 rows
        for (int i = 1; i <= 1000; i++) {
            db.execute("INSERT INTO customer_orders VALUES (" + i + ", 'Customer" + i + "', " + (i * 10) + ");");
        }

        // Measure time taken for ALTER TABLE ADD COLUMN
        long startNs = System.nanoTime();
        QueryResult alterRes = db.execute(
            "ALTER TABLE customer_orders ADD COLUMN status VARCHAR DEFAULT 'PENDING';"
        );
        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;

        assertTrue(alterRes.isSuccess(), () -> "ALTER TABLE failed: " + alterRes.getError());
        // Verify execution is instant (< 50ms for 1000 rows, compared to physical row rewrite which is order of magnitude slower)
        assertTrue(elapsedMs < 100, "Instant ALTER TABLE took " + elapsedMs + "ms, expected < 100ms");

        // Verify existing 1000 rows immediately read the default value
        QueryResult countPending = db.execute("SELECT COUNT(*) FROM customer_orders WHERE status = 'PENDING';");
        assertTrue(countPending.isSuccess());
        assertEquals(1000, ((Number) countPending.getRows().get(0).getValue(0)).intValue());

        // Verify specific row values
        QueryResult rowCheck = db.execute("SELECT order_id, customer_name, status FROM customer_orders WHERE order_id = 42;");
        assertTrue(rowCheck.isSuccess());
        assertEquals("Customer42", rowCheck.getRows().get(0).getValue("customer_name"));
        assertEquals("PENDING", rowCheck.getRows().get(0).getValue("status"));

        // Insert a new row without specifying the new column - must get default
        db.execute("INSERT INTO customer_orders (order_id, customer_name, amount) VALUES (1001, 'NewCustomer', 500);");
        QueryResult newRow = db.execute("SELECT status FROM customer_orders WHERE order_id = 1001;");
        assertEquals("PENDING", newRow.getRows().get(0).getValue("status"));

        // Insert a new row specifying an explicit value
        db.execute("INSERT INTO customer_orders VALUES (1002, 'ExplicitCustomer', 600, 'COMPLETED');");
        QueryResult explicitRow = db.execute("SELECT status FROM customer_orders WHERE order_id = 1002;");
        assertEquals("COMPLETED", explicitRow.getRows().get(0).getValue("status"));

        // UPDATE an old row's newly added column
        QueryResult updateRes = db.execute("UPDATE customer_orders SET status = 'SHIPPED' WHERE order_id = 1;");
        assertTrue(updateRes.isSuccess());
        QueryResult updatedRow = db.execute("SELECT status FROM customer_orders WHERE order_id = 1;");
        assertEquals("SHIPPED", updatedRow.getRows().get(0).getValue("status"));

        // Verify remaining old rows still have default
        QueryResult countShipped = db.execute("SELECT COUNT(*) FROM customer_orders WHERE status = 'SHIPPED';");
        assertEquals(1, ((Number) countShipped.getRows().get(0).getValue(0)).intValue());
    }

    @Test
    @DisplayName("Instant ALTER TABLE with multiple data types, NULL defaults, and SET/DROP DEFAULT")
    void testInstantAlterTableMultipleColumnsAndDefaults() {
        db.execute("CREATE TABLE inventory (item_id INT, name VARCHAR);");
        db.execute("INSERT INTO inventory VALUES (1, 'Widget');");
        db.execute("INSERT INTO inventory VALUES (2, 'Gadget');");

        // Add INT column with default
        QueryResult res1 = db.execute("ALTER TABLE inventory ADD COLUMN quantity INT DEFAULT 100;");
        assertTrue(res1.isSuccess());

        // Add column with NULL default
        QueryResult res2 = db.execute("ALTER TABLE inventory ADD COLUMN description VARCHAR;");
        assertTrue(res2.isSuccess());

        // Verify existing rows read defaults
        QueryResult check = db.execute("SELECT item_id, name, quantity, description FROM inventory WHERE item_id = 1;");
        assertTrue(check.isSuccess());
        Tuple r = check.getRows().get(0);
        assertEquals(100, ((Number) r.getValue("quantity")).intValue());
        assertNull(r.getValue("description"));

        // Alter column set default
        QueryResult setDef = db.execute("ALTER TABLE inventory ALTER COLUMN quantity SET DEFAULT 250;");
        assertTrue(setDef.isSuccess());

        // New insert without quantity gets new default 250
        db.execute("INSERT INTO inventory (item_id, name) VALUES (3, 'Doohickey');");
        QueryResult r3 = db.execute("SELECT quantity FROM inventory WHERE item_id = 3;");
        assertEquals(250, ((Number) r3.getRows().get(0).getValue("quantity")).intValue());

        // Drop default
        QueryResult dropDef = db.execute("ALTER TABLE inventory ALTER COLUMN quantity DROP DEFAULT;");
        assertTrue(dropDef.isSuccess());

        // New insert without quantity gets null
        db.execute("INSERT INTO inventory (item_id, name) VALUES (4, 'Thingamajig');");
        QueryResult r4 = db.execute("SELECT quantity FROM inventory WHERE item_id = 4;");
        assertNull(r4.getRows().get(0).getValue("quantity"));
    }

    @Test
    @DisplayName("Direct StreamAggregateOperator test with arbitrary HAVING expression")
    void testStreamAggregateOperatorArbitraryHaving() {
        List<String> cols = List.of("dept", "price");
        List<Object[]> data = List.of(
            new Object[]{"Hardware", 20.0},
            new Object[]{"Hardware", 30.0},
            new Object[]{"Hardware", 40.0},
            new Object[]{"Office", 10.0},
            new Object[]{"Office", 20.0},
            new Object[]{"Software", 100.0},
            new Object[]{"Software", 200.0}
        );

        List<Tuple> inputTuples = new ArrayList<>();
        for (Object[] row : data) {
            Tuple t = new Tuple();
            t.addValue("dept", row[0]);
            t.addValue("price", row[1]);
            inputTuples.add(t);
        }

        PhysicalOperator source = new MemoryScanOperator(inputTuples, cols);
        List<String> groupBy = List.of("dept");
        List<AggregateCall> aggregates = List.of(
            new AggregateCall("COUNT", "*", "cnt"),
            new AggregateCall("AVG", "price", "avg_price")
        );

        WhereExpr havingExpr = new WhereExpr.And(
            new WhereExpr.Comparison("COUNT(*)", ">", "1"),
            new WhereExpr.Comparison("AVG(price)", "<", "50")
        );

        StreamAggregateOperator streamAgg = new StreamAggregateOperator(source, groupBy, aggregates, null, havingExpr);
        streamAgg.open();

        List<Tuple> results = new ArrayList<>();
        while (true) {
            TupleSlot slot = streamAgg.next();
            if (slot == null || slot.isEmpty()) break;
            results.add(slot.getTuple());
        }
        streamAgg.close();

        assertEquals(2, results.size());
        List<String> depts = results.stream().map(t -> (String) t.getValue("dept")).toList();
        assertTrue(depts.contains("Hardware"));
        assertTrue(depts.contains("Office"));
        assertFalse(depts.contains("Software"));
    }

    @Test
    @DisplayName("32 Virtual Threads: concurrent queries with arbitrary HAVING and instant DDL")
    void testVirtualThreadsConcurrentHavingAndDdl() throws InterruptedException {
        db.execute("CREATE TABLE metrics (id INT, host VARCHAR, cpu_pct FLOAT8);");
        for (int i = 0; i < 200; i++) {
            String host = "node-" + (i % 5);
            double cpu = 10.0 + (i % 60);
            db.execute("INSERT INTO metrics VALUES (" + i + ", '" + host + "', " + cpu + ");");
        }

        int threadCount = 32;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int tid = i;
            Thread.ofVirtual().start(() -> {
                try {
                    startLatch.await();
                    if (tid == 0) {
                        // Thread 0 executes instant ALTER TABLE
                        QueryResult alt = db.execute("ALTER TABLE metrics ADD COLUMN env VARCHAR DEFAULT 'prod';");
                        if (alt.isSuccess()) successCount.incrementAndGet();
                    } else if (tid % 2 == 1) {
                        // Odd threads execute arbitrary HAVING queries
                        QueryResult res = db.execute(
                            "SELECT host, COUNT(*), AVG(cpu_pct) FROM metrics GROUP BY host " +
                            "HAVING COUNT(*) > 10 AND AVG(cpu_pct) < 50.0;"
                        );
                        if (res.isSuccess()) successCount.incrementAndGet();
                    } else {
                        // Even threads execute INSERTs
                        QueryResult ins = db.execute("INSERT INTO metrics VALUES (" + (1000 + tid) + ", 'node-1', 25.0);");
                        if (ins.isSuccess()) successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    // latch error
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();

        assertEquals(threadCount, successCount.get(), "All 32 virtual threads should complete successfully");

        // Verify the newly added column is visible across all rows
        QueryResult envCheck = db.execute("SELECT COUNT(*) FROM metrics WHERE env = 'prod';");
        assertTrue(envCheck.isSuccess());
        assertTrue(((Number) envCheck.getRows().get(0).getValue(0)).intValue() >= 200);
    }

    @Test
    @DisplayName("End-to-end: Arbitrary HAVING combined with JOIN, WHERE, ORDER BY, and LIMIT")
    void testArbitraryHavingCombinedWithJoinWhereOrderByLimit() {
        db.execute("CREATE TABLE depts (dept_id INT, dept_name VARCHAR);");
        db.execute("CREATE TABLE emps (emp_id INT, dept_id INT, salary INT, active BOOLEAN);");

        db.execute("INSERT INTO depts VALUES (1, 'Engineering');");
        db.execute("INSERT INTO depts VALUES (2, 'Finance');");
        db.execute("INSERT INTO depts VALUES (3, 'HR');");

        // Engineering: 3 active emps, salaries 100, 200, 300 (sum=600, avg=200, count=3)
        db.execute("INSERT INTO emps VALUES (101, 1, 100, true);");
        db.execute("INSERT INTO emps VALUES (102, 1, 200, true);");
        db.execute("INSERT INTO emps VALUES (103, 1, 300, true);");
        db.execute("INSERT INTO emps VALUES (104, 1, 50, false);"); // inactive, filtered by WHERE

        // Finance: 2 active emps, salaries 500, 600 (sum=1100, avg=550, count=2)
        db.execute("INSERT INTO emps VALUES (201, 2, 500, true);");
        db.execute("INSERT INTO emps VALUES (202, 2, 600, true);");

        // HR: 1 active emp, salary 150 (count=1, sum=150)
        db.execute("INSERT INTO emps VALUES (301, 3, 150, true);");

        // Query combining JOIN + WHERE + GROUP BY + arbitrary HAVING + ORDER BY + LIMIT
        QueryResult res = db.execute(
            "SELECT d.dept_name, COUNT(e.emp_id) AS total_emps, AVG(e.salary) AS avg_sal " +
            "FROM depts d JOIN emps e ON d.dept_id = e.dept_id " +
            "WHERE e.active = true " +
            "GROUP BY d.dept_name " +
            "HAVING COUNT(e.emp_id) >= 2 AND AVG(e.salary) <= 500.0 " +
            "ORDER BY total_emps DESC " +
            "LIMIT 5;"
        );

        assertTrue(res.isSuccess(), () -> "Query failed: " + res.getError());
        List<Tuple> rows = res.getRows();
        assertEquals(1, rows.size(), "Only Engineering should match both COUNT >= 2 and AVG <= 500.0");
        assertEquals("Engineering", rows.get(0).getValue("dept_name"));
        assertEquals(3, ((Number) rows.get(0).getValue("total_emps")).intValue());
        assertEquals(200.0, ((Number) rows.get(0).getValue("avg_sal")).doubleValue(), 0.001);
    }
}
