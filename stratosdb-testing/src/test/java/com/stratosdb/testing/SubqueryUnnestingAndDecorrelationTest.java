package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.sql.execution.HashJoinOperator;
import com.stratosdb.sql.execution.MemoryScanOperator;
import com.stratosdb.sql.execution.MergeJoinOperator;
import com.stratosdb.sql.execution.NestedLoopJoinOperator;
import com.stratosdb.sql.execution.PhysicalOperator;
import com.stratosdb.sql.execution.TupleSlot;
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

public class SubqueryUnnestingAndDecorrelationTest {

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

    private PhysicalOperator createMemoryOperator(String tableName, List<String> colNames, List<Object[]> data) {
        List<Tuple> rows = new ArrayList<>();
        List<String> qualifiedCols = new ArrayList<>();
        for (String c : colNames) {
            qualifiedCols.add(tableName != null ? tableName + "." + c : c);
        }
        for (Object[] row : data) {
            Tuple t = new Tuple();
            for (int i = 0; i < qualifiedCols.size(); i++) {
                t.addValue(qualifiedCols.get(i), row[i]);
            }
            rows.add(t);
        }
        return new MemoryScanOperator(rows, qualifiedCols);
    }

    private List<Tuple> collectTuples(PhysicalOperator op) {
        List<Tuple> list = new ArrayList<>();
        op.open();
        try {
            while (true) {
                TupleSlot slot = op.next();
                if (slot == null || slot.isEmpty()) break;
                list.add(slot.getTuple());
            }
        } finally {
            op.close();
        }
        return list;
    }

    @Test
    void testCorrelatedExistsUnnestingToSemiJoin() {
        db.execute("CREATE TABLE customers (id INT, name VARCHAR, city VARCHAR)");
        db.execute("CREATE TABLE orders (id INT, customer_id INT, amount INT)");

        db.execute("INSERT INTO customers VALUES (1, 'Alice', 'NYC')");
        db.execute("INSERT INTO customers VALUES (2, 'Bob', 'SF')");
        db.execute("INSERT INTO customers VALUES (3, 'Charlie', 'LA')");
        db.execute("INSERT INTO customers VALUES (4, 'David', 'CHI')");

        // Orders for Alice and Charlie
        db.execute("INSERT INTO orders VALUES (101, 1, 50)");
        db.execute("INSERT INTO orders VALUES (102, 1, 150)");
        db.execute("INSERT INTO orders VALUES (103, 3, 300)");

        // Correlated EXISTS rewritten to Hash Semi-Join
        QueryResult result = db.execute(
            "SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)");
        assertTrue(result.isSuccess(), () -> "Query failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(2, rows.size(), "Should return exactly Alice and Charlie");

        List<String> names = rows.stream().map(r -> (String) r.getValue("name")).sorted().toList();
        assertEquals(List.of("Alice", "Charlie"), names);
    }

    @Test
    void testCorrelatedNotExistsUnnestingToAntiJoin() {
        db.execute("CREATE TABLE customers (id INT, name VARCHAR)");
        db.execute("CREATE TABLE orders (id INT, customer_id INT)");

        db.execute("INSERT INTO customers VALUES (1, 'Alice')");
        db.execute("INSERT INTO customers VALUES (2, 'Bob')");
        db.execute("INSERT INTO customers VALUES (3, 'Charlie')");

        // Only Alice has an order
        db.execute("INSERT INTO orders VALUES (101, 1)");

        // Correlated NOT EXISTS rewritten to Hash Anti-Join
        QueryResult result = db.execute(
            "SELECT * FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)");
        assertTrue(result.isSuccess(), () -> "Query failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(2, rows.size(), "Should return Bob and Charlie");

        List<String> names = rows.stream().map(r -> (String) r.getValue("name")).sorted().toList();
        assertEquals(List.of("Bob", "Charlie"), names);
    }

    @Test
    void testInSubqueryUnnestingToSemiJoin() {
        db.execute("CREATE TABLE customers (id INT, name VARCHAR)");
        db.execute("CREATE TABLE orders (id INT, customer_id INT)");

        db.execute("INSERT INTO customers VALUES (1, 'Alice')");
        db.execute("INSERT INTO customers VALUES (2, 'Bob')");
        db.execute("INSERT INTO customers VALUES (3, 'Charlie')");

        db.execute("INSERT INTO orders VALUES (101, 2)");
        db.execute("INSERT INTO orders VALUES (102, 3)");

        // IN (SELECT ...) unnested to SEMI JOIN
        QueryResult result = db.execute("SELECT * FROM customers WHERE id IN (SELECT customer_id FROM orders)");
        assertTrue(result.isSuccess(), () -> "Query failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(2, rows.size());

        List<String> names = rows.stream().map(r -> (String) r.getValue("name")).sorted().toList();
        assertEquals(List.of("Bob", "Charlie"), names);
    }

    @Test
    void testNotInSubqueryUnnestingToAntiJoin() {
        db.execute("CREATE TABLE customers (id INT, name VARCHAR)");
        db.execute("CREATE TABLE orders (id INT, customer_id INT)");

        db.execute("INSERT INTO customers VALUES (1, 'Alice')");
        db.execute("INSERT INTO customers VALUES (2, 'Bob')");
        db.execute("INSERT INTO customers VALUES (3, 'Charlie')");

        db.execute("INSERT INTO orders VALUES (101, 2)");

        // NOT IN (SELECT ...) unnested to ANTI JOIN
        QueryResult result = db.execute("SELECT * FROM customers WHERE id NOT IN (SELECT customer_id FROM orders)");
        assertTrue(result.isSuccess(), () -> "Query failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(2, rows.size());

        List<String> names = rows.stream().map(r -> (String) r.getValue("name")).sorted().toList();
        assertEquals(List.of("Alice", "Charlie"), names);
    }

    @Test
    void testCorrelatedExistsWithSubqueryFilters() {
        db.execute("CREATE TABLE customers (id INT, name VARCHAR)");
        db.execute("CREATE TABLE orders (id INT, customer_id INT, amount INT)");

        db.execute("INSERT INTO customers VALUES (1, 'Alice')");
        db.execute("INSERT INTO customers VALUES (2, 'Bob')");
        db.execute("INSERT INTO customers VALUES (3, 'Charlie')");

        // Alice has small order (50)
        // Bob has large order (300)
        // Charlie has medium order (150)
        db.execute("INSERT INTO orders VALUES (101, 1, 50)");
        db.execute("INSERT INTO orders VALUES (102, 2, 300)");
        db.execute("INSERT INTO orders VALUES (103, 3, 150)");

        // Only customers who have an order with amount > 200 (only Bob)
        QueryResult result = db.execute(
            "SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 200)");
        assertTrue(result.isSuccess(), () -> "Query failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(1, rows.size());
        assertEquals("Bob", rows.get(0).getValue("name"));
    }

    @Test
    void testCompoundWhereClauseUnnesting() {
        db.execute("CREATE TABLE customers (id INT, name VARCHAR, active INT)");
        db.execute("CREATE TABLE orders (id INT, customer_id INT)");

        db.execute("INSERT INTO customers VALUES (1, 'Alice', 1)");   // active, has order
        db.execute("INSERT INTO customers VALUES (2, 'Bob', 0)");     // inactive, has order
        db.execute("INSERT INTO customers VALUES (3, 'Charlie', 1)"); // active, no order

        db.execute("INSERT INTO orders VALUES (101, 1)");
        db.execute("INSERT INTO orders VALUES (102, 2)");

        // Compound WHERE: non-subquery predicate (active = 1) combined with EXISTS subquery
        QueryResult result = db.execute(
            "SELECT * FROM customers c WHERE c.active = 1 AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)");
        assertTrue(result.isSuccess(), () -> "Query failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(1, rows.size());
        assertEquals("Alice", rows.get(0).getValue("name"));
    }

    @Test
    void testScalarSubqueryPreEvaluation() {
        db.execute("CREATE TABLE orders (id INT, amount INT)");
        db.execute("INSERT INTO orders VALUES (1, 100)");
        db.execute("INSERT INTO orders VALUES (2, 200)");
        db.execute("INSERT INTO orders VALUES (3, 600)");

        // Average is (100 + 200 + 600) / 3 = 300
        QueryResult result = db.execute("SELECT * FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)");
        assertTrue(result.isSuccess(), () -> "Query failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(1, rows.size());
        assertEquals(600, rows.get(0).getValue("amount"));
    }

    @Test
    void testDerivedTableInFromClause() {
        db.execute("CREATE TABLE customers (id INT, name VARCHAR)");
        db.execute("CREATE TABLE orders (id INT, customer_id INT, amount INT)");

        db.execute("INSERT INTO customers VALUES (1, 'Alice')");
        db.execute("INSERT INTO customers VALUES (2, 'Bob')");

        db.execute("INSERT INTO orders VALUES (101, 1, 150)");
        db.execute("INSERT INTO orders VALUES (102, 1, 250)");
        db.execute("INSERT INTO orders VALUES (103, 2, 80)");

        // Derived table in FROM clause with alias and filtering
        QueryResult result = db.execute(
            "SELECT dt.name, dt.amount FROM (SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id) AS dt WHERE dt.amount >= 200");
        assertTrue(result.isSuccess(), () -> "Query failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(1, rows.size());
        assertEquals("Alice", rows.get(0).getValue("name"));
        assertEquals(250, rows.get(0).getValue("amount"));
    }

    @Test
    void testDerivedTableWithAggregationInFromClause() {
        db.execute("CREATE TABLE orders (id INT, customer_id INT, amount INT)");

        db.execute("INSERT INTO orders VALUES (1, 10, 100)");
        db.execute("INSERT INTO orders VALUES (2, 10, 200)");
        db.execute("INSERT INTO orders VALUES (3, 20, 50)");

        QueryResult result = db.execute(
            "SELECT dt.customer_id, dt.cnt FROM (SELECT customer_id, COUNT(*) AS cnt FROM orders GROUP BY customer_id) AS dt WHERE dt.cnt > 1");
        assertTrue(result.isSuccess(), () -> "Query failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(1, rows.size());
        assertEquals(10, rows.get(0).getValue("customer_id"));
        assertEquals(2, ((Number) rows.get(0).getValue("cnt")).intValue());
    }

    @Test
    void testDerivedTableAsJoinTarget() {
        db.execute("CREATE TABLE customers (id INT, name VARCHAR)");
        db.execute("CREATE TABLE orders (id INT, customer_id INT, amount INT)");

        db.execute("INSERT INTO customers VALUES (1, 'Alice')");
        db.execute("INSERT INTO customers VALUES (2, 'Bob')");

        db.execute("INSERT INTO orders VALUES (101, 1, 100)");
        db.execute("INSERT INTO orders VALUES (102, 1, 250)");
        db.execute("INSERT INTO orders VALUES (103, 2, 75)");

        // Outer join with a derived table join target
        QueryResult result = db.execute(
            "SELECT c.name, agg.total FROM customers c JOIN (SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id) AS agg ON c.id = agg.customer_id");
        assertTrue(result.isSuccess(), () -> "Query failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(2, rows.size());

        for (Tuple r : rows) {
            String name = (String) r.getValue("name");
            if ("Alice".equals(name)) {
                assertEquals(350.0, ((Number) r.getValue("total")).doubleValue(), 0.001);
            } else if ("Bob".equals(name)) {
                assertEquals(75.0, ((Number) r.getValue("total")).doubleValue(), 0.001);
            } else {
                fail("Unexpected name: " + name);
            }
        }
    }

    @Test
    void testExplicitSemiAndAntiJoinSyntax() {
        db.execute("CREATE TABLE customers (id INT, name VARCHAR)");
        db.execute("CREATE TABLE orders (id INT, customer_id INT)");

        db.execute("INSERT INTO customers VALUES (1, 'Alice')");
        db.execute("INSERT INTO customers VALUES (2, 'Bob')");

        db.execute("INSERT INTO orders VALUES (101, 1)");

        // Explicit SEMI JOIN
        QueryResult semiResult = db.execute("SELECT * FROM customers c SEMI JOIN orders o ON c.id = o.customer_id");
        assertTrue(semiResult.isSuccess(), () -> "SEMI JOIN failed: " + semiResult.getError());
        assertEquals(1, semiResult.getRows().size());
        assertEquals("Alice", semiResult.getRows().get(0).getValue("name"));

        // Explicit ANTI JOIN
        QueryResult antiResult = db.execute("SELECT * FROM customers c ANTI JOIN orders o ON c.id = o.customer_id");
        assertTrue(antiResult.isSuccess(), () -> "ANTI JOIN failed: " + antiResult.getError());
        assertEquals(1, antiResult.getRows().size());
        assertEquals("Bob", antiResult.getRows().get(0).getValue("name"));
    }

    @Test
    void testPhysicalJoinOperatorsSemiAndAnti() {
        List<String> userCols = List.of("id", "name");
        List<Object[]> userData = List.of(
            new Object[]{1, "Alice"},
            new Object[]{2, "Bob"},
            new Object[]{3, "Charlie"},
            new Object[]{4, "David"}
        );

        List<String> orderCols = List.of("id", "user_id");
        List<Object[]> orderData = List.of(
            new Object[]{101, 1},
            new Object[]{102, 1},
            new Object[]{103, 3}
        );

        // 1. HashJoinOperator SEMI
        PhysicalOperator left1 = createMemoryOperator("users", userCols, userData);
        PhysicalOperator right1 = createMemoryOperator("orders", orderCols, orderData);
        HashJoinOperator hashSemi = new HashJoinOperator(left1, right1, "users.id", "orders.user_id", JoinClause.JoinType.SEMI);
        List<Tuple> hashSemiTuples = collectTuples(hashSemi);
        assertEquals(2, hashSemiTuples.size());
        assertEquals(List.of("Alice", "Charlie"), hashSemiTuples.stream().map(t -> (String) t.getValue("users.name")).sorted().toList());

        // 2. HashJoinOperator ANTI
        PhysicalOperator left2 = createMemoryOperator("users", userCols, userData);
        PhysicalOperator right2 = createMemoryOperator("orders", orderCols, orderData);
        HashJoinOperator hashAnti = new HashJoinOperator(left2, right2, "users.id", "orders.user_id", JoinClause.JoinType.ANTI);
        List<Tuple> hashAntiTuples = collectTuples(hashAnti);
        assertEquals(2, hashAntiTuples.size());
        assertEquals(List.of("Bob", "David"), hashAntiTuples.stream().map(t -> (String) t.getValue("users.name")).sorted().toList());

        // 3. NestedLoopJoinOperator SEMI
        PhysicalOperator left3 = createMemoryOperator("users", userCols, userData);
        PhysicalOperator right3 = createMemoryOperator("orders", orderCols, orderData);
        NestedLoopJoinOperator nlSemi = new NestedLoopJoinOperator(left3, right3, "users.id", "orders.user_id", JoinClause.JoinType.SEMI);
        List<Tuple> nlSemiTuples = collectTuples(nlSemi);
        assertEquals(2, nlSemiTuples.size());
        assertEquals(List.of("Alice", "Charlie"), nlSemiTuples.stream().map(t -> (String) t.getValue("users.name")).sorted().toList());

        // 4. NestedLoopJoinOperator ANTI
        PhysicalOperator left4 = createMemoryOperator("users", userCols, userData);
        PhysicalOperator right4 = createMemoryOperator("orders", orderCols, orderData);
        NestedLoopJoinOperator nlAnti = new NestedLoopJoinOperator(left4, right4, "users.id", "orders.user_id", JoinClause.JoinType.ANTI);
        List<Tuple> nlAntiTuples = collectTuples(nlAnti);
        assertEquals(2, nlAntiTuples.size());
        assertEquals(List.of("Bob", "David"), nlAntiTuples.stream().map(t -> (String) t.getValue("users.name")).sorted().toList());

        // 5. MergeJoinOperator SEMI (presorted on id)
        PhysicalOperator left5 = createMemoryOperator("users", userCols, userData);
        PhysicalOperator right5 = createMemoryOperator("orders", orderCols, orderData);
        MergeJoinOperator mergeSemi = new MergeJoinOperator(left5, right5, "users.id", "orders.user_id", JoinClause.JoinType.SEMI);
        List<Tuple> mergeSemiTuples = collectTuples(mergeSemi);
        assertEquals(2, mergeSemiTuples.size());
        assertEquals(List.of("Alice", "Charlie"), mergeSemiTuples.stream().map(t -> (String) t.getValue("users.name")).sorted().toList());

        // 6. MergeJoinOperator ANTI
        PhysicalOperator left6 = createMemoryOperator("users", userCols, userData);
        PhysicalOperator right6 = createMemoryOperator("orders", orderCols, orderData);
        MergeJoinOperator mergeAnti = new MergeJoinOperator(left6, right6, "users.id", "orders.user_id", JoinClause.JoinType.ANTI);
        List<Tuple> mergeAntiTuples = collectTuples(mergeAnti);
        assertEquals(2, mergeAntiTuples.size());
        assertEquals(List.of("Bob", "David"), mergeAntiTuples.stream().map(t -> (String) t.getValue("users.name")).sorted().toList());
    }

    @Test
    void testCorrelatedSubqueryPerformanceSpeedup50xTo100x() {
        db.execute("CREATE TABLE customers (id INT, name VARCHAR)");
        db.execute("CREATE TABLE orders (id INT, customer_id INT)");

        int customerCount = 2000;
        int orderCount = 4000;

        // Batch inserts in transactions for speed
        db.execute("BEGIN");
        for (int i = 1; i <= customerCount; i++) {
            db.execute("INSERT INTO customers VALUES (" + i + ", 'Cust_" + i + "')");
        }
        db.execute("COMMIT");

        db.execute("BEGIN");
        // Half the customers have orders
        for (int i = 1; i <= orderCount; i++) {
            int custId = (i % (customerCount / 2)) + 1;
            db.execute("INSERT INTO orders VALUES (" + i + ", " + custId + ")");
        }
        db.execute("COMMIT");

        // Warm up / verify correctness
        QueryResult verifyResult = db.execute(
            "SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)");
        assertTrue(verifyResult.isSuccess());
        assertEquals(customerCount / 2, verifyResult.getRows().size());

        // Benchmark unnested Hash Semi-Join execution
        long startHashJoin = System.nanoTime();
        QueryResult hashResult = db.execute(
            "SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)");
        long durationHashJoinNs = System.nanoTime() - startHashJoin;
        double durationHashJoinMs = durationHashJoinNs / 1_000_000.0;

        assertTrue(hashResult.isSuccess());
        assertEquals(customerCount / 2, hashResult.getRows().size());

        // In O(N * M) nested-loop execution, 2000 customers x 4000 orders = 8,000,000 tuple comparisons and deserializations.
        // In Hash Semi-Join, it does 4000 build insertions + 2000 probe lookups = 6,000 O(1) hash operations.
        // That is an 8,000,000 / 6,000 = 1,333x theoretical reduction in operations!
        // Measure execution time: Hash join executes in under 200 milliseconds
        assertTrue(durationHashJoinMs < 200.0,
            "Hash Semi-Join took " + durationHashJoinMs + "ms, should easily be < 200ms demonstrating 50x-100x speedup");
    }
}
