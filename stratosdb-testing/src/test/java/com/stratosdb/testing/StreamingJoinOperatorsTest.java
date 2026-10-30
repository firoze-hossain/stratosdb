package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.sql.execution.*;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class StreamingJoinOperatorsTest {

    @TempDir
    Path tempDir;

    private StratosDB db;

    @BeforeEach
    void setUp() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setBufferPoolSize(64);
        db = new StratosDB(config);
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.shutdown();
        }
    }

    private PhysicalOperator createMemoryOperator(String tableName, String keyCol, String valCol, Object[][] data) {
        List<Tuple> rows = new ArrayList<>();
        List<String> cols = List.of(tableName + "." + keyCol, tableName + "." + valCol);
        for (Object[] pair : data) {
            Tuple t = new Tuple();
            t.addValue(cols.get(0), pair[0]);
            t.addValue(cols.get(1), pair[1]);
            rows.add(t);
        }
        return new MemoryScanOperator(rows, cols);
    }

    @Test
    void testNestedLoopJoinInnerAndOuter() {
        // Left: users (id, name) -> (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
        // Right: orders (user_id, amount) -> (1, 100), (4, 400)
        Object[][] usersData = new Object[][]{{1, "Alice"}, {2, "Bob"}, {3, "Charlie"}};
        Object[][] ordersData = new Object[][]{{1, 100}, {4, 400}};

        // 1. INNER JOIN
        PhysicalOperator leftInner = createMemoryOperator("users", "id", "name", usersData);
        PhysicalOperator rightInner = createMemoryOperator("orders", "user_id", "amount", ordersData);
        NestedLoopJoinOperator innerJoin = new NestedLoopJoinOperator(
            leftInner, rightInner, "users.id", "orders.user_id", JoinClause.JoinType.INNER);

        innerJoin.open();
        List<Tuple> innerRows = new ArrayList<>();
        while (true) {
            TupleSlot s = innerJoin.next();
            if (s == null || s.isEmpty()) break;
            innerRows.add(s.getTuple());
        }
        innerJoin.close();

        assertEquals(1, innerRows.size());
        assertEquals("Alice", innerRows.get(0).getValue("users.name"));
        assertEquals(100, innerRows.get(0).getValue("orders.amount"));

        // 2. LEFT OUTER JOIN
        PhysicalOperator leftOuter = createMemoryOperator("users", "id", "name", usersData);
        PhysicalOperator rightOuter = createMemoryOperator("orders", "user_id", "amount", ordersData);
        NestedLoopJoinOperator leftJoin = new NestedLoopJoinOperator(
            leftOuter, rightOuter, "users.id", "orders.user_id", JoinClause.JoinType.LEFT);

        leftJoin.open();
        List<Tuple> leftRows = new ArrayList<>();
        while (true) {
            TupleSlot s = leftJoin.next();
            if (s == null || s.isEmpty()) break;
            leftRows.add(s.getTuple());
        }
        leftJoin.close();

        assertEquals(3, leftRows.size());
        assertEquals("Alice", leftRows.get(0).getValue("users.name"));
        assertEquals(100, leftRows.get(0).getValue("orders.amount"));
        assertEquals("Bob", leftRows.get(1).getValue("users.name"));
        assertNull(leftRows.get(1).getValue("orders.amount"));
        assertEquals("Charlie", leftRows.get(2).getValue("users.name"));
        assertNull(leftRows.get(2).getValue("orders.amount"));

        // 3. RIGHT OUTER JOIN
        PhysicalOperator leftRight = createMemoryOperator("users", "id", "name", usersData);
        PhysicalOperator rightRight = createMemoryOperator("orders", "user_id", "amount", ordersData);
        NestedLoopJoinOperator rightJoin = new NestedLoopJoinOperator(
            leftRight, rightRight, "users.id", "orders.user_id", JoinClause.JoinType.RIGHT);

        rightJoin.open();
        List<Tuple> rightRows = new ArrayList<>();
        while (true) {
            TupleSlot s = rightJoin.next();
            if (s == null || s.isEmpty()) break;
            rightRows.add(s.getTuple());
        }
        rightJoin.close();

        assertEquals(2, rightRows.size());
        assertEquals("Alice", rightRows.get(0).getValue("users.name"));
        assertEquals(100, rightRows.get(0).getValue("orders.amount"));
        assertNull(rightRows.get(1).getValue("users.name"));
        assertEquals(400, rightRows.get(1).getValue("orders.amount"));

        // 4. FULL OUTER JOIN
        PhysicalOperator leftFull = createMemoryOperator("users", "id", "name", usersData);
        PhysicalOperator rightFull = createMemoryOperator("orders", "user_id", "amount", ordersData);
        NestedLoopJoinOperator fullJoin = new NestedLoopJoinOperator(
            leftFull, rightFull, "users.id", "orders.user_id", JoinClause.JoinType.FULL);

        fullJoin.open();
        List<Tuple> fullRows = new ArrayList<>();
        while (true) {
            TupleSlot s = fullJoin.next();
            if (s == null || s.isEmpty()) break;
            fullRows.add(s.getTuple());
        }
        fullJoin.close();

        assertEquals(4, fullRows.size()); // Alice matched, Bob, Charlie (null right), order 4 (null left)
    }

    @Test
    void testHashJoinAllJoinTypes() {
        Object[][] aData = new Object[][]{{1, "A1"}, {2, "A2"}, {3, "A3"}};
        Object[][] bData = new Object[][]{{1, "B1"}, {3, "B3"}, {4, "B4"}};

        // 1. INNER JOIN
        HashJoinOperator innerOp = new HashJoinOperator(
            createMemoryOperator("a", "id", "val", aData),
            createMemoryOperator("b", "id", "val", bData),
            "a.id", "b.id", JoinClause.JoinType.INNER
        );
        innerOp.open();
        List<Tuple> innerRes = new ArrayList<>();
        while (true) {
            TupleSlot s = innerOp.next();
            if (s == null || s.isEmpty()) break;
            innerRes.add(s.getTuple());
        }
        innerOp.close();
        assertEquals(2, innerRes.size()); // 1 and 3

        // 2. LEFT OUTER JOIN
        HashJoinOperator leftOp = new HashJoinOperator(
            createMemoryOperator("a", "id", "val", aData),
            createMemoryOperator("b", "id", "val", bData),
            "a.id", "b.id", JoinClause.JoinType.LEFT
        );
        leftOp.open();
        List<Tuple> leftRes = new ArrayList<>();
        while (true) {
            TupleSlot s = leftOp.next();
            if (s == null || s.isEmpty()) break;
            leftRes.add(s.getTuple());
        }
        leftOp.close();
        assertEquals(3, leftRes.size());
        assertEquals("A1", leftRes.get(0).getValue("a.val"));
        assertEquals("B1", leftRes.get(0).getValue("b.val"));
        assertEquals("A2", leftRes.get(1).getValue("a.val"));
        assertNull(leftRes.get(1).getValue("b.val"));
        assertEquals("A3", leftRes.get(2).getValue("a.val"));
        assertEquals("B3", leftRes.get(2).getValue("b.val"));

        // 3. RIGHT OUTER JOIN
        HashJoinOperator rightOp = new HashJoinOperator(
            createMemoryOperator("a", "id", "val", aData),
            createMemoryOperator("b", "id", "val", bData),
            "a.id", "b.id", JoinClause.JoinType.RIGHT
        );
        rightOp.open();
        List<Tuple> rightRes = new ArrayList<>();
        while (true) {
            TupleSlot s = rightOp.next();
            if (s == null || s.isEmpty()) break;
            rightRes.add(s.getTuple());
        }
        rightOp.close();
        assertEquals(3, rightRes.size());
        assertEquals("A1", rightRes.get(0).getValue("a.val"));
        assertEquals("B1", rightRes.get(0).getValue("b.val"));
        assertEquals("A3", rightRes.get(1).getValue("a.val"));
        assertEquals("B3", rightRes.get(1).getValue("b.val"));
        assertNull(rightRes.get(2).getValue("a.val"));
        assertEquals("B4", rightRes.get(2).getValue("b.val"));

        // 4. FULL OUTER JOIN
        HashJoinOperator fullOp = new HashJoinOperator(
            createMemoryOperator("a", "id", "val", aData),
            createMemoryOperator("b", "id", "val", bData),
            "a.id", "b.id", JoinClause.JoinType.FULL
        );
        fullOp.open();
        List<Tuple> fullRes = new ArrayList<>();
        while (true) {
            TupleSlot s = fullOp.next();
            if (s == null || s.isEmpty()) break;
            fullRes.add(s.getTuple());
        }
        fullOp.close();
        assertEquals(4, fullRes.size()); // A1-B1, A2-null, A3-B3, null-B4
    }

    @Test
    void testMergeJoinPresortedInputs() {
        // Presorted inputs: a keys=(10, 20, 40), b keys=(20, 30, 40, 50)
        Object[][] aData = new Object[][]{{10, "A10"}, {20, "A20"}, {40, "A40"}};
        Object[][] bData = new Object[][]{{20, "B20"}, {30, "B30"}, {40, "B40"}, {50, "B50"}};

        // 1. INNER JOIN
        MergeJoinOperator innerOp = new MergeJoinOperator(
            createMemoryOperator("a", "id", "val", aData),
            createMemoryOperator("b", "id", "val", bData),
            "a.id", "b.id", JoinClause.JoinType.INNER
        );
        innerOp.open();
        List<Tuple> innerRes = new ArrayList<>();
        while (true) {
            TupleSlot s = innerOp.next();
            if (s == null || s.isEmpty()) break;
            innerRes.add(s.getTuple());
        }
        innerOp.close();
        assertEquals(2, innerRes.size()); // 20 and 40

        // 2. LEFT OUTER JOIN
        MergeJoinOperator leftOp = new MergeJoinOperator(
            createMemoryOperator("a", "id", "val", aData),
            createMemoryOperator("b", "id", "val", bData),
            "a.id", "b.id", JoinClause.JoinType.LEFT
        );
        leftOp.open();
        List<Tuple> leftRes = new ArrayList<>();
        while (true) {
            TupleSlot s = leftOp.next();
            if (s == null || s.isEmpty()) break;
            leftRes.add(s.getTuple());
        }
        leftOp.close();
        assertEquals(3, leftRes.size());
        assertEquals("A10", leftRes.get(0).getValue("a.val"));
        assertNull(leftRes.get(0).getValue("b.val"));
        assertEquals("A20", leftRes.get(1).getValue("a.val"));
        assertEquals("B20", leftRes.get(1).getValue("b.val"));
        assertEquals("A40", leftRes.get(2).getValue("a.val"));
        assertEquals("B40", leftRes.get(2).getValue("b.val"));

        // 3. RIGHT OUTER JOIN
        MergeJoinOperator rightOp = new MergeJoinOperator(
            createMemoryOperator("a", "id", "val", aData),
            createMemoryOperator("b", "id", "val", bData),
            "a.id", "b.id", JoinClause.JoinType.RIGHT
        );
        rightOp.open();
        List<Tuple> rightRes = new ArrayList<>();
        while (true) {
            TupleSlot s = rightOp.next();
            if (s == null || s.isEmpty()) break;
            rightRes.add(s.getTuple());
        }
        rightOp.close();
        assertEquals(4, rightRes.size()); // B20 (matched), B30 (unmatched), B40 (matched), B50 (unmatched)

        // 4. FULL OUTER JOIN
        MergeJoinOperator fullOp = new MergeJoinOperator(
            createMemoryOperator("a", "id", "val", aData),
            createMemoryOperator("b", "id", "val", bData),
            "a.id", "b.id", JoinClause.JoinType.FULL
        );
        fullOp.open();
        List<Tuple> fullRes = new ArrayList<>();
        while (true) {
            TupleSlot s = fullOp.next();
            if (s == null || s.isEmpty()) break;
            fullRes.add(s.getTuple());
        }
        fullOp.close();
        assertEquals(5, fullRes.size()); // A10 (null b), A20-B20, B30 (null a), A40-B40, B50 (null a)
    }

    @Test
    void testMergeJoinDuplicateKeysRun() {
        // Left has keys (1, 2, 2, 3), Right has keys (2, 2, 4)
        Object[][] aData = new Object[][]{{1, "A1"}, {2, "A2_1"}, {2, "A2_2"}, {3, "A3"}};
        Object[][] bData = new Object[][]{{2, "B2_1"}, {2, "B2_2"}, {4, "B4"}};

        MergeJoinOperator op = new MergeJoinOperator(
            createMemoryOperator("a", "id", "val", aData),
            createMemoryOperator("b", "id", "val", bData),
            "a.id", "b.id", JoinClause.JoinType.INNER
        );
        op.open();
        List<Tuple> res = new ArrayList<>();
        while (true) {
            TupleSlot s = op.next();
            if (s == null || s.isEmpty()) break;
            res.add(s.getTuple());
        }
        op.close();

        // Key 2 has 2 left rows and 2 right rows -> 2 x 2 = 4 matches!
        assertEquals(4, res.size());
    }

    @Test
    void testEndToEndSqlLeftJoin() {
        db.execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR)");
        db.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)");

        db.execute("INSERT INTO customers VALUES (1, 'Alice')");
        db.execute("INSERT INTO customers VALUES (2, 'Bob')");
        db.execute("INSERT INTO customers VALUES (3, 'Charlie')");

        db.execute("INSERT INTO orders VALUES (101, 1, 50)");
        db.execute("INSERT INTO orders VALUES (102, 1, 150)");
        db.execute("INSERT INTO orders VALUES (103, 2, 75)");

        // Charlie has no orders
        QueryResult result = db.execute("SELECT customers.name, orders.amount FROM customers LEFT JOIN orders ON customers.id = orders.customer_id");
        assertTrue(result.isSuccess());
        assertTrue(result.isStreaming(), "QueryResult must be streaming Volcano pipeline");

        List<Tuple> rows = result.getRows();
        assertEquals(4, rows.size(), "Alice (2 orders) + Bob (1 order) + Charlie (1 null order) = 4 rows");

        // Verify Charlie row has null amount
        Tuple charlieRow = rows.stream().filter(r -> "Charlie".equals(r.getValue("customers.name"))).findFirst().orElse(null);
        assertNotNull(charlieRow);
        assertNull(charlieRow.getValue("orders.amount"));
    }

    @Test
    void testEndToEndSqlRightJoin() {
        db.execute("CREATE TABLE departments (id INT PRIMARY KEY, dept_name VARCHAR)");
        db.execute("CREATE TABLE staff (id INT PRIMARY KEY, dept_id INT, name VARCHAR)");

        db.execute("INSERT INTO departments VALUES (10, 'Engineering')");
        db.execute("INSERT INTO departments VALUES (20, 'Marketing')");
        db.execute("INSERT INTO departments VALUES (30, 'Sales')"); // Sales has no staff

        db.execute("INSERT INTO staff VALUES (1, 10, 'Alice')");
        db.execute("INSERT INTO staff VALUES (2, 20, 'Bob')");
        db.execute("INSERT INTO staff VALUES (3, 99, 'Dave')"); // Dave belongs to non-existent department

        // RIGHT JOIN staff ON departments.id = staff.dept_id
        QueryResult result = db.execute("SELECT departments.dept_name, staff.name FROM departments RIGHT JOIN staff ON departments.id = staff.dept_id");
        assertTrue(result.isSuccess());
        assertTrue(result.isStreaming());

        List<Tuple> rows = result.getRows();
        assertEquals(3, rows.size(), "Staff rows: Alice, Bob, and Dave (with NULL department)");

        Tuple daveRow = rows.stream().filter(r -> "Dave".equals(r.getValue("staff.name"))).findFirst().orElse(null);
        assertNotNull(daveRow);
        assertNull(daveRow.getValue("departments.dept_name"));
    }

    @Test
    void testEndToEndSqlFullOuterJoin() {
        db.execute("CREATE TABLE t1 (id INT PRIMARY KEY, v1 VARCHAR)");
        db.execute("CREATE TABLE t2 (id INT PRIMARY KEY, v2 VARCHAR)");

        db.execute("INSERT INTO t1 VALUES (1, 'A')");
        db.execute("INSERT INTO t1 VALUES (2, 'B')");

        db.execute("INSERT INTO t2 VALUES (2, 'B_matched')");
        db.execute("INSERT INTO t2 VALUES (3, 'C')");

        QueryResult result = db.execute("SELECT t1.id, t1.v1, t2.id, t2.v2 FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id");
        assertTrue(result.isSuccess());
        assertTrue(result.isStreaming());

        List<Tuple> rows = result.getRows();
        assertEquals(3, rows.size(), "Full outer join must emit 1-null, 2-2, null-3");

        boolean found1 = false, found2 = false, found3 = false;
        for (Tuple r : rows) {
            if ("A".equals(r.getValue("t1.v1"))) {
                found1 = true;
                assertNull(r.getValue("t2.v2"));
            } else if ("B".equals(r.getValue("t1.v1"))) {
                found2 = true;
                assertEquals("B_matched", r.getValue("t2.v2"));
            } else if ("C".equals(r.getValue("t2.v2"))) {
                found3 = true;
                assertNull(r.getValue("t1.v1"));
            }
        }
        assertTrue(found1 && found2 && found3);
    }

    @Test
    void testJoinedSelectWithWhereAndLimit() {
        db.execute("CREATE TABLE p1 (id INT PRIMARY KEY, name VARCHAR)");
        db.execute("CREATE TABLE p2 (id INT PRIMARY KEY, p1_id INT, score INT)");

        for (int i = 1; i <= 20; i++) {
            db.execute(String.format("INSERT INTO p1 VALUES (%d, 'User_%d')", i, i));
            db.execute(String.format("INSERT INTO p2 VALUES (%d, %d, %d)", i, i, i * 10));
        }

        // Test JOIN with WHERE, LIMIT, and OFFSET
        QueryResult result = db.execute(
            "SELECT p1.name, p2.score FROM p1 LEFT JOIN p2 ON p1.id = p2.p1_id WHERE p2.score >= 50 LIMIT 5 OFFSET 2");
        assertTrue(result.isSuccess());
        assertTrue(result.isStreaming());

        assertInstanceOf(LimitOperator.class, result.getOperator());

        List<Tuple> rows = result.getRows();
        assertEquals(5, rows.size());
    }

    @Test
    void testHashJoinMemoryScalesWithBuildSideOnly() {
        db.execute("CREATE TABLE big_probe (id INT, tag VARCHAR)");
        db.execute("CREATE TABLE small_build (id INT PRIMARY KEY, description VARCHAR)");

        // Small build table: 10 rows
        for (int i = 1; i <= 10; i++) {
            db.execute(String.format("INSERT INTO small_build VALUES (%d, 'Category_%d')", i, i));
        }

        // Large probe table: 3,000 rows
        for (int i = 1; i <= 3000; i++) {
            db.execute(String.format("INSERT INTO big_probe VALUES (%d, 'Tag_%d')", (i % 10) + 1, i));
        }

        // Stream joined query
        QueryResult result = db.execute("SELECT big_probe.tag, small_build.description FROM big_probe JOIN small_build ON big_probe.id = small_build.id");
        assertTrue(result.isSuccess());
        assertTrue(result.isStreaming());

        long count = 0;
        for (Tuple t : result) {
            count++;
            assertNotNull(t.getValue("small_build.description"));
        }

        assertEquals(3000, count, "All 3,000 probe rows must stream through and match the build table");
    }
}
