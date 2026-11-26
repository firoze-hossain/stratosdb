package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.sql.ast.WhereExpr;
import com.stratosdb.sql.execution.*;
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
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class CompoundAndNonEquiJoinIntegrationTest {

    @TempDir
    Path tempDir;

    private StratosDB db;

    @BeforeEach
    void setUp() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setBufferPoolSize(1024);
        db = new StratosDB(config);
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.shutdown();
        }
    }

    private PhysicalOperator createMemoryOperator(String tableName, String[] colNames, Object[][] data) {
        List<Tuple> rows = new ArrayList<>();
        List<String> cols = new ArrayList<>();
        for (String c : colNames) {
            cols.add(tableName + "." + c);
        }
        for (Object[] rowData : data) {
            Tuple t = new Tuple();
            for (int i = 0; i < cols.size(); i++) {
                t.addValue(cols.get(i), rowData[i]);
            }
            rows.add(t);
        }
        return new MemoryScanOperator(rows, cols);
    }

    @Test
    @DisplayName("End-to-End SQL: Multi-column composite key INNER JOIN")
    void testMultiColumnCompositeKeyInnerJoin() {
        db.execute("CREATE TABLE accounts (tenant_id INT, account_id INT, name VARCHAR);");
        db.execute("CREATE TABLE balances (tenant_id INT, account_id INT, balance INT);");

        db.execute("INSERT INTO accounts VALUES (1, 101, 'Acct_1_101');");
        db.execute("INSERT INTO accounts VALUES (1, 102, 'Acct_1_102');");
        db.execute("INSERT INTO accounts VALUES (2, 101, 'Acct_2_101');");
        db.execute("INSERT INTO accounts VALUES (2, 999, 'Acct_2_999');"); // No matching balance

        db.execute("INSERT INTO balances VALUES (1, 101, 5000);");
        db.execute("INSERT INTO balances VALUES (1, 102, 8000);");
        db.execute("INSERT INTO balances VALUES (2, 101, 12000);");
        db.execute("INSERT INTO balances VALUES (3, 101, 99000);"); // Different tenant

        QueryResult res = db.execute(
            "SELECT a.tenant_id, a.account_id, a.name, b.balance " +
            "FROM accounts a JOIN balances b " +
            "ON a.tenant_id = b.tenant_id AND a.account_id = b.account_id;");

        assertTrue(res.isSuccess(), "Join query must succeed: " + res.getError());
        List<Tuple> rows = res.getRows();
        assertEquals(3, rows.size(), "Must match exactly 3 rows with identical (tenant_id, account_id)");

        for (Tuple r : rows) {
            Number tenant = (Number) r.getValue("a.tenant_id");
            Number acct = (Number) r.getValue("a.account_id");
            Number bal = (Number) r.getValue("b.balance");
            if (tenant.intValue() == 1 && acct.intValue() == 101) {
                assertEquals(5000, bal.intValue());
            } else if (tenant.intValue() == 1 && acct.intValue() == 102) {
                assertEquals(8000, bal.intValue());
            } else if (tenant.intValue() == 2 && acct.intValue() == 101) {
                assertEquals(12000, bal.intValue());
            } else {
                fail("Unexpected row: " + r);
            }
        }
    }

    @Test
    @DisplayName("End-to-End SQL: Multi-column composite key LEFT OUTER JOIN with NULL padding")
    void testMultiColumnCompositeKeyLeftOuterJoin() {
        db.execute("CREATE TABLE accounts (tenant_id INT, account_id INT, name VARCHAR);");
        db.execute("CREATE TABLE balances (tenant_id INT, account_id INT, balance INT);");

        db.execute("INSERT INTO accounts VALUES (1, 101, 'Acct_1_101');");
        db.execute("INSERT INTO accounts VALUES (1, 102, 'Acct_1_102');");
        db.execute("INSERT INTO accounts VALUES (2, 999, 'Acct_Unmatched');");

        db.execute("INSERT INTO balances VALUES (1, 101, 5000);");
        // account (1, 102) and (2, 999) have no balances

        QueryResult res = db.execute(
            "SELECT a.tenant_id, a.account_id, a.name, b.balance " +
            "FROM accounts a LEFT JOIN balances b " +
            "ON a.tenant_id = b.tenant_id AND a.account_id = b.account_id;");

        assertTrue(res.isSuccess());
        List<Tuple> rows = res.getRows();
        assertEquals(3, rows.size());

        Tuple matched = rows.stream().filter(r -> "Acct_1_101".equals(r.getValue("a.name"))).findFirst().orElse(null);
        assertNotNull(matched);
        assertEquals(5000, ((Number) matched.getValue("b.balance")).intValue());

        Tuple unmatched1 = rows.stream().filter(r -> "Acct_1_102".equals(r.getValue("a.name"))).findFirst().orElse(null);
        assertNotNull(unmatched1);
        assertNull(unmatched1.getValue("b.balance"), "Unmatched row in LEFT JOIN must have NULL for right table columns");

        Tuple unmatched2 = rows.stream().filter(r -> "Acct_Unmatched".equals(r.getValue("a.name"))).findFirst().orElse(null);
        assertNotNull(unmatched2);
        assertNull(unmatched2.getValue("b.balance"));
    }

    @Test
    @DisplayName("End-to-End SQL: Multi-column composite key RIGHT OUTER JOIN")
    void testMultiColumnCompositeKeyRightOuterJoin() {
        db.execute("CREATE TABLE departments (org_id INT, dept_id INT, dname VARCHAR);");
        db.execute("CREATE TABLE employees (org_id INT, dept_id INT, ename VARCHAR);");

        db.execute("INSERT INTO departments VALUES (10, 1, 'Engineering');");
        db.execute("INSERT INTO departments VALUES (10, 2, 'Marketing');");

        db.execute("INSERT INTO employees VALUES (10, 1, 'Alice');");
        db.execute("INSERT INTO employees VALUES (10, 99, 'Orphan');"); // Dept 99 doesn't exist

        QueryResult res = db.execute(
            "SELECT d.dname, e.ename " +
            "FROM departments d RIGHT JOIN employees e " +
            "ON d.org_id = e.org_id AND d.dept_id = e.dept_id;");

        assertTrue(res.isSuccess());
        List<Tuple> rows = res.getRows();
        assertEquals(2, rows.size());

        Tuple alice = rows.stream().filter(r -> "Alice".equals(r.getValue("e.ename"))).findFirst().orElse(null);
        assertNotNull(alice);
        assertEquals("Engineering", alice.getValue("d.dname"));

        Tuple orphan = rows.stream().filter(r -> "Orphan".equals(r.getValue("e.ename"))).findFirst().orElse(null);
        assertNotNull(orphan);
        assertNull(orphan.getValue("d.dname"), "Right join unmatched row must have NULL for left table");
    }

    @Test
    @DisplayName("End-to-End SQL: Multi-column composite key FULL OUTER JOIN")
    void testMultiColumnCompositeKeyFullOuterJoin() {
        db.execute("CREATE TABLE t1 (k1 INT, k2 VARCHAR, v1 VARCHAR);");
        db.execute("CREATE TABLE t2 (k1 INT, k2 VARCHAR, v2 VARCHAR);");

        db.execute("INSERT INTO t1 VALUES (1, 'A', 'LeftOnly');");
        db.execute("INSERT INTO t1 VALUES (2, 'B', 'BothMatched_1');");

        db.execute("INSERT INTO t2 VALUES (2, 'B', 'BothMatched_2');");
        db.execute("INSERT INTO t2 VALUES (3, 'C', 'RightOnly');");

        QueryResult res = db.execute(
            "SELECT t1.v1, t2.v2 FROM t1 FULL OUTER JOIN t2 ON t1.k1 = t2.k1 AND t1.k2 = t2.k2;");

        assertTrue(res.isSuccess());
        List<Tuple> rows = res.getRows();
        assertEquals(3, rows.size(), "Full outer join must emit 1-null, matched-matched, and null-3");

        boolean leftOnlyFound = false, matchedFound = false, rightOnlyFound = false;
        for (Tuple r : rows) {
            if ("LeftOnly".equals(r.getValue("t1.v1"))) {
                leftOnlyFound = true;
                assertNull(r.getValue("t2.v2"));
            } else if ("BothMatched_1".equals(r.getValue("t1.v1"))) {
                matchedFound = true;
                assertEquals("BothMatched_2", r.getValue("t2.v2"));
            } else if ("RightOnly".equals(r.getValue("t2.v2"))) {
                rightOnlyFound = true;
                assertNull(r.getValue("t1.v1"));
            }
        }
        assertTrue(leftOnlyFound && matchedFound && rightOnlyFound);
    }

    @Test
    @DisplayName("End-to-End SQL: Compound join with non-equi comparison filter (equi + inequality)")
    void testCompoundJoinWithNonEquiInequality() {
        db.execute("CREATE TABLE customers (id INT, tier VARCHAR);");
        db.execute("CREATE TABLE discounts (tier VARCHAR, min_amount INT, rate INT);");

        db.execute("INSERT INTO customers VALUES (1, 'GOLD');");
        db.execute("INSERT INTO customers VALUES (2, 'SILVER');");
        db.execute("INSERT INTO customers VALUES (3, 'BRONZE');");

        db.execute("INSERT INTO discounts VALUES ('GOLD', 100, 20);");
        db.execute("INSERT INTO discounts VALUES ('GOLD', 500, 30);");
        db.execute("INSERT INTO discounts VALUES ('SILVER', 100, 10);");

        // Join on tier equality AND min_amount >= 200
        QueryResult res = db.execute(
            "SELECT c.id, c.tier, d.rate " +
            "FROM customers c JOIN discounts d " +
            "ON c.tier = d.tier AND d.min_amount >= 200;");

        assertTrue(res.isSuccess());
        List<Tuple> rows = res.getRows();
        assertEquals(1, rows.size(), "Only GOLD tier with min_amount >= 200 should match");
        assertEquals(1, ((Number) rows.get(0).getValue("c.id")).intValue());
        assertEquals(30, ((Number) rows.get(0).getValue("d.rate")).intValue());
    }

    @Test
    @DisplayName("End-to-End SQL: Cross-table pure non-equi join inequality (a.salary > b.threshold)")
    void testPureNonEquiJoinInequality() {
        db.execute("CREATE TABLE workers (id INT, salary INT);");
        db.execute("CREATE TABLE pay_grades (grade VARCHAR, min_salary INT);");

        db.execute("INSERT INTO workers VALUES (1, 1500);");
        db.execute("INSERT INTO workers VALUES (2, 3500);");
        db.execute("INSERT INTO workers VALUES (3, 7500);");

        db.execute("INSERT INTO pay_grades VALUES ('Executive', 5000);");

        QueryResult res = db.execute(
            "SELECT w.id, w.salary, p.grade " +
            "FROM workers w JOIN pay_grades p " +
            "ON w.salary > p.min_salary;");

        assertTrue(res.isSuccess());
        List<Tuple> rows = res.getRows();
        assertEquals(1, rows.size(), "Only worker 3 has salary > 5000");
        assertEquals(3, ((Number) rows.get(0).getValue("w.id")).intValue());
        assertEquals("Executive", rows.get(0).getValue("p.grade"));
    }

    @Test
    @DisplayName("End-to-End SQL: 3-way join with composite keys")
    void testThreeWayCompositeKeyJoin() {
        db.execute("CREATE TABLE orders (tenant INT, order_id INT, customer VARCHAR);");
        db.execute("CREATE TABLE line_items (tenant INT, order_id INT, item_id INT, qty INT);");
        db.execute("CREATE TABLE items (item_id INT, item_name VARCHAR);");

        db.execute("INSERT INTO orders VALUES (1, 10, 'CorpA');");
        db.execute("INSERT INTO orders VALUES (2, 10, 'CorpB');");

        db.execute("INSERT INTO line_items VALUES (1, 10, 501, 5);");
        db.execute("INSERT INTO line_items VALUES (1, 10, 502, 2);");
        db.execute("INSERT INTO line_items VALUES (2, 10, 501, 99);");

        db.execute("INSERT INTO items VALUES (501, 'Widget');");
        db.execute("INSERT INTO items VALUES (502, 'Gadget');");

        QueryResult res = db.execute(
            "SELECT o.customer, i.item_name, li.qty " +
            "FROM orders o " +
            "JOIN line_items li ON o.tenant = li.tenant AND o.order_id = li.order_id " +
            "JOIN items i ON li.item_id = i.item_id;");

        assertTrue(res.isSuccess());
        List<Tuple> rows = res.getRows();
        assertEquals(3, rows.size());
    }

    @Test
    @DisplayName("End-to-End SQL: Compound join combined with WHERE, ORDER BY, and LIMIT")
    void testCompoundJoinWithWhereOrderByAndLimit() {
        db.execute("CREATE TABLE t1 (k1 INT, k2 INT, v VARCHAR);");
        db.execute("CREATE TABLE t2 (k1 INT, k2 INT, score INT);");

        for (int i = 1; i <= 20; i++) {
            db.execute(String.format("INSERT INTO t1 VALUES (%d, %d, 'Val_%d');", i, i % 2, i));
            db.execute(String.format("INSERT INTO t2 VALUES (%d, %d, %d);", i, i % 2, i * 10));
        }

        QueryResult res = db.execute(
            "SELECT t1.v, t2.score " +
            "FROM t1 JOIN t2 ON t1.k1 = t2.k1 AND t1.k2 = t2.k2 " +
            "WHERE t2.score >= 50 " +
            "ORDER BY t2.score DESC " +
            "LIMIT 5 OFFSET 2;");

        assertTrue(res.isSuccess());
        List<Tuple> rows = res.getRows();
        assertEquals(5, rows.size());
        // Scores >= 50 descending are 200, 190, 180, 170, 160...
        // OFFSET 2 skips 200 and 190, so first is 180
        assertEquals(180, ((Number) rows.get(0).getValue("t2.score")).intValue());
        assertEquals(140, ((Number) rows.get(4).getValue("t2.score")).intValue());
    }

    @Test
    @DisplayName("Direct Operator: MergeJoinOperator with 2-column composite keys and non-equi filter")
    void testMergeJoinOperatorWithCompositeKeysAndNonEqui() {
        // Presorted on (tenant, id)
        Object[][] leftData = new Object[][]{
            {1, 10, "A_1_10", 100},
            {1, 20, "A_1_20", 250},
            {2, 10, "A_2_10", 300}
        };
        Object[][] rightData = new Object[][]{
            {1, 10, "B_1_10", 50},
            {1, 20, "B_1_20", 500}, // Left amount 250 < right amount 500
            {2, 10, "B_2_10", 150}
        };

        String[] cols = new String[]{"tenant", "id", "name", "amount"};
        PhysicalOperator leftOp = createMemoryOperator("l", cols, leftData);
        PhysicalOperator rightOp = createMemoryOperator("r", cols, rightData);

        // Join on l.tenant = r.tenant AND l.id = r.id AND l.amount >= r.amount
        WhereExpr nonEqui = new WhereExpr.ColumnComparison("l.amount", ">=", "r.amount");
        MergeJoinOperator join = new MergeJoinOperator(
            leftOp, rightOp,
            List.of("l.tenant", "l.id"),
            List.of("r.tenant", "r.id"),
            nonEqui,
            JoinClause.JoinType.INNER
        );

        join.open();
        List<Tuple> rows = new ArrayList<>();
        while (true) {
            TupleSlot s = join.next();
            if (s == null || s.isEmpty()) break;
            rows.add(s.getTuple());
        }
        join.close();

        // Row (1, 10): 100 >= 50 -> match
        // Row (1, 20): 250 >= 500 -> false
        // Row (2, 10): 300 >= 150 -> match
        assertEquals(2, rows.size(), "Only rows satisfying both composite key and l.amount >= r.amount must match");
    }

    @Test
    @DisplayName("Direct Operator: HashJoinOperator with 2-column composite keys and non-equi filter")
    void testHashJoinOperatorWithCompositeKeysAndNonEqui() {
        Object[][] leftData = new Object[][]{
            {1, 10, "L1", 50},
            {1, 20, "L2", 90},
            {2, 10, "L3", 200}
        };
        Object[][] rightData = new Object[][]{
            {1, 10, "R1", 100},
            {1, 20, "R2", 80},
            {2, 10, "R3", 50}
        };

        String[] cols = new String[]{"tenant", "id", "tag", "val"};
        PhysicalOperator leftOp = createMemoryOperator("l", cols, leftData);
        PhysicalOperator rightOp = createMemoryOperator("r", cols, rightData);

        WhereExpr nonEqui = new WhereExpr.ColumnComparison("l.val", "<", "r.val");
        HashJoinOperator join = new HashJoinOperator(
            leftOp, rightOp,
            List.of("l.tenant", "l.id"),
            List.of("r.tenant", "r.id"),
            nonEqui,
            JoinClause.JoinType.INNER
        );

        join.open();
        List<Tuple> rows = new ArrayList<>();
        while (true) {
            TupleSlot s = join.next();
            if (s == null || s.isEmpty()) break;
            rows.add(s.getTuple());
        }
        join.close();

        // (1, 10): 50 < 100 -> match!
        // (1, 20): 90 < 80 -> false
        // (2, 10): 200 < 50 -> false
        assertEquals(1, rows.size());
        assertEquals("L1", rows.get(0).getValue("l.tag"));
        assertEquals("R1", rows.get(0).getValue("r.tag"));
    }

    @Test
    @DisplayName("Concurrency: Concurrent compound joins scale across 32 virtual threads")
    void testConcurrentCompoundJoinsUnderVirtualThreads() throws Exception {
        db.execute("CREATE TABLE base_t1 (tenant INT, id INT, payload VARCHAR);");
        db.execute("CREATE TABLE base_t2 (tenant INT, id INT, rating INT);");

        for (int i = 1; i <= 50; i++) {
            db.execute(String.format("INSERT INTO base_t1 VALUES (%d, %d, 'Data_%d');", i % 5, i, i));
            db.execute(String.format("INSERT INTO base_t2 VALUES (%d, %d, %d);", i % 5, i, i * 2));
        }

        int threadCount = 32;
        int queriesPerThread = 25;
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            futures.add(executor.submit(() -> {
                for (int q = 0; q < queriesPerThread; q++) {
                    QueryResult res = db.execute(
                        "SELECT a.payload, b.rating " +
                        "FROM base_t1 a JOIN base_t2 b " +
                        "ON a.tenant = b.tenant AND a.id = b.id " +
                        "WHERE b.rating >= 20;");
                    assertTrue(res.isSuccess());
                    List<Tuple> rows = res.getRows();
                    assertEquals(41, rows.size(), "41 rows have rating >= 20 (id 10 to 50)");
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }

        executor.shutdown();
    }
}
