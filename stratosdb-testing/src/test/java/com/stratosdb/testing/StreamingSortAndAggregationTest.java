package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.ast.AggregateCall;
import com.stratosdb.sql.ast.OrderItem;
import com.stratosdb.sql.execution.HashAggregateOperator;
import com.stratosdb.sql.execution.MemoryScanOperator;
import com.stratosdb.sql.execution.PhysicalOperator;
import com.stratosdb.sql.execution.SortOperator;
import com.stratosdb.sql.execution.StreamAggregateOperator;
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

public class StreamingSortAndAggregationTest {

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

    @Test
    void testSortOperatorInMemoryAscAndDesc() {
        List<String> cols = List.of("id", "name", "score");
        List<Object[]> data = List.of(
            new Object[]{3, "Charlie", 85},
            new Object[]{1, "Alice", 95},
            new Object[]{4, "David", 70},
            new Object[]{2, "Bob", 85}
        );

        // 1. Sort by score DESC, then name ASC
        PhysicalOperator source1 = createMemoryOperator("t", cols, data);
        List<OrderItem> orderItems1 = List.of(
            new OrderItem("t.score", false), // DESC
            new OrderItem("t.name", true)    // ASC
        );
        SortOperator sortOp1 = new SortOperator(source1, orderItems1);
        sortOp1.open();

        List<Tuple> results1 = new ArrayList<>();
        while (true) {
            TupleSlot slot = sortOp1.next();
            if (slot == null || slot.isEmpty()) break;
            results1.add(slot.getTuple());
        }
        sortOp1.close();

        assertEquals(4, results1.size());
        assertEquals("Alice", results1.get(0).getValue("t.name"));   // 95
        assertEquals("Bob", results1.get(1).getValue("t.name"));     // 85 (Bob before Charlie)
        assertEquals("Charlie", results1.get(2).getValue("t.name")); // 85
        assertEquals("David", results1.get(3).getValue("t.name"));   // 70
        assertEquals(0, sortOp1.getSpilledRunCount(), "In-memory sort should not spill to disk");

        // 2. Sort by id ASC
        PhysicalOperator source2 = createMemoryOperator("t", cols, data);
        List<OrderItem> orderItems2 = List.of(new OrderItem("t.id", true));
        SortOperator sortOp2 = new SortOperator(source2, orderItems2);
        sortOp2.open();

        List<Tuple> results2 = new ArrayList<>();
        while (true) {
            TupleSlot slot = sortOp2.next();
            if (slot == null || slot.isEmpty()) break;
            results2.add(slot.getTuple());
        }
        sortOp2.close();

        assertEquals(4, results2.size());
        assertEquals(1, results2.get(0).getValue("t.id"));
        assertEquals(2, results2.get(1).getValue("t.id"));
        assertEquals(3, results2.get(2).getValue("t.id"));
        assertEquals(4, results2.get(3).getValue("t.id"));
    }

    @Test
    void testSortOperatorExternalMergeSortSpillToDisk() {
        // Generate 60 rows with large strings to easily exceed a low work_mem
        List<String> cols = List.of("id", "payload");
        List<Object[]> data = new ArrayList<>();
        for (int i = 60; i >= 1; i--) {
            data.add(new Object[]{i, "payload_data_for_row_number_" + i + "_extra_padding"});
        }

        PhysicalOperator source = createMemoryOperator("tbl", cols, data);
        List<OrderItem> orderItems = List.of(new OrderItem("tbl.id", true)); // ASC

        // Set low workMemBytes (600 bytes) to force spilling across multiple disk runs
        SortOperator sortOp = new SortOperator(source, orderItems, 600L);
        sortOp.open();

        assertTrue(sortOp.getSpilledRunCount() > 1, "Should have spilled multiple runs to disk: " + sortOp.getSpilledRunCount());

        List<Integer> sortedIds = new ArrayList<>();
        while (true) {
            TupleSlot slot = sortOp.next();
            if (slot == null || slot.isEmpty()) break;
            sortedIds.add((Integer) slot.getTuple().getValue("tbl.id"));
        }
        sortOp.close();

        assertEquals(60, sortedIds.size());
        for (int i = 0; i < 60; i++) {
            assertEquals(i + 1, sortedIds.get(i), "Tuple at index " + i + " should be sorted");
        }
    }

    @Test
    void testSortOperator1MillionRowsNoOutOfMemory() {
        // DoD: "1M row ORDER BY does not trigger OutOfMemoryError"
        final int TOTAL_ROWS = 1_000_000;
        final List<String> schema = List.of("id", "num");

        // Streaming generator operator emitting 1M tuples in reverse order without heap accumulation
        PhysicalOperator millionRowGenerator = new PhysicalOperator() {
            private int current = TOTAL_ROWS;

            @Override
            public void open() {
                current = TOTAL_ROWS;
            }

            @Override
            public TupleSlot next() {
                if (current <= 0) {
                    return TupleSlot.empty();
                }
                Tuple t = new Tuple();
                t.addValue("id", current);
                t.addValue("num", current * 10);
                current--;
                return TupleSlot.of(t);
            }

            @Override
            public void close() {
                current = 0;
            }

            @Override
            public List<String> getOutputColumns() {
                return schema;
            }
        };

        // Low work_mem (256 KB) forcing regular disk spills into external merge sort
        long workMem = 256L * 1024L;
        List<OrderItem> orderItems = List.of(new OrderItem("id", true)); // Sort ASC
        SortOperator sortOp = new SortOperator(millionRowGenerator, orderItems, workMem);

        sortOp.open();
        assertTrue(sortOp.getSpilledRunCount() > 10, "1M rows must spill dozens of runs to disk");

        int count = 0;
        int expectedId = 1;
        boolean correctlySorted = true;

        while (true) {
            TupleSlot slot = sortOp.next();
            if (slot == null || slot.isEmpty()) {
                break;
            }
            int id = (Integer) slot.getTuple().getValue("id");
            if (id != expectedId) {
                correctlySorted = false;
            }
            expectedId++;
            count++;
        }
        sortOp.close();

        assertEquals(TOTAL_ROWS, count, "Should stream all 1,000,000 tuples");
        assertTrue(correctlySorted, "1,000,000 tuples must be streamed in strictly sorted ascending order");
    }

    @Test
    void testHashAggregateOperatorFunctionsAndGroupBy() {
        List<String> cols = List.of("dept", "salary", "bonus");
        List<Object[]> data = List.of(
            new Object[]{"Engineering", 100, 10},
            new Object[]{"Engineering", 150, 20},
            new Object[]{"Engineering", 120, null},
            new Object[]{"Sales", 80, 5},
            new Object[]{"Sales", 90, 15}
        );

        PhysicalOperator source = createMemoryOperator(null, cols, data);
        List<String> groupBy = List.of("dept");
        List<AggregateCall> aggregates = List.of(
            new AggregateCall("COUNT", "*", "cnt_all"),
            new AggregateCall("COUNT", "bonus", "cnt_bonus"),
            new AggregateCall("SUM", "salary", "total_salary"),
            new AggregateCall("AVG", "salary", "avg_salary"),
            new AggregateCall("MIN", "salary", "min_salary"),
            new AggregateCall("MAX", "salary", "max_salary")
        );

        HashAggregateOperator aggOp = new HashAggregateOperator(source, groupBy, aggregates, null);
        aggOp.open();

        List<Tuple> results = new ArrayList<>();
        while (true) {
            TupleSlot slot = aggOp.next();
            if (slot == null || slot.isEmpty()) break;
            results.add(slot.getTuple());
        }
        aggOp.close();

        assertEquals(2, results.size());
        for (Tuple r : results) {
            String dept = (String) r.getValue("dept");
            if ("Engineering".equals(dept)) {
                assertEquals(3, r.getValue("cnt_all"));
                assertEquals(2, r.getValue("cnt_bonus"), "COUNT(bonus) ignores NULL");
                assertEquals(370L, r.getValue("total_salary"));
                assertEquals(370.0 / 3.0, (Double) r.getValue("avg_salary"), 0.001);
                assertEquals(100, r.getValue("min_salary"));
                assertEquals(150, r.getValue("max_salary"));
            } else if ("Sales".equals(dept)) {
                assertEquals(2, r.getValue("cnt_all"));
                assertEquals(2, r.getValue("cnt_bonus"));
                assertEquals(170L, r.getValue("total_salary"));
                assertEquals(85.0, (Double) r.getValue("avg_salary"), 0.001);
                assertEquals(80, r.getValue("min_salary"));
                assertEquals(90, r.getValue("max_salary"));
            } else {
                fail("Unexpected dept: " + dept);
            }
        }
    }

    @Test
    void testHashAggregateOperatorHavingClause() {
        List<String> cols = List.of("region", "amount");
        List<Object[]> data = List.of(
            new Object[]{"North", 100},
            new Object[]{"North", 200},
            new Object[]{"North", 300},
            new Object[]{"South", 50},
            new Object[]{"East", 500}
        );

        PhysicalOperator source = createMemoryOperator(null, cols, data);
        List<String> groupBy = List.of("region");
        List<AggregateCall> aggregates = List.of(
            new AggregateCall("COUNT", "*", "cnt"),
            new AggregateCall("SUM", "amount", "total")
        );

        // HAVING COUNT(*) >= 2
        HashAggregateOperator aggOp = new HashAggregateOperator(source, groupBy, aggregates, "COUNT(*) >= 2");
        aggOp.open();

        List<Tuple> results = new ArrayList<>();
        while (true) {
            TupleSlot slot = aggOp.next();
            if (slot == null || slot.isEmpty()) break;
            results.add(slot.getTuple());
        }
        aggOp.close();

        assertEquals(1, results.size());
        assertEquals("North", results.get(0).getValue("region"));
        assertEquals(3, results.get(0).getValue("cnt"));
        assertEquals(600L, results.get(0).getValue("total"));
    }

    @Test
    void testStreamAggregateOperatorPresortedInputs() {
        // Presorted by department
        List<String> cols = List.of("department", "score");
        List<Object[]> data = List.of(
            new Object[]{"Biology", 70},
            new Object[]{"Biology", 80},
            new Object[]{"Chemistry", 90},
            new Object[]{"Physics", 85},
            new Object[]{"Physics", 95}
        );

        PhysicalOperator source = createMemoryOperator(null, cols, data);
        List<String> groupBy = List.of("department");
        List<AggregateCall> aggregates = List.of(
            new AggregateCall("COUNT", "*", "n_students"),
            new AggregateCall("AVG", "score", "avg_score")
        );

        StreamAggregateOperator streamAgg = new StreamAggregateOperator(source, groupBy, aggregates, "COUNT(*) >= 2");
        streamAgg.open();

        List<Tuple> results = new ArrayList<>();
        while (true) {
            TupleSlot slot = streamAgg.next();
            if (slot == null || slot.isEmpty()) break;
            results.add(slot.getTuple());
        }
        streamAgg.close();

        assertEquals(2, results.size(), "Chemistry filtered out by HAVING COUNT(*) >= 2");
        assertEquals("Biology", results.get(0).getValue("department"));
        assertEquals(2, results.get(0).getValue("n_students"));
        assertEquals(75.0, (Double) results.get(0).getValue("avg_score"), 0.001);

        assertEquals("Physics", results.get(1).getValue("department"));
        assertEquals(2, results.get(1).getValue("n_students"));
        assertEquals(90.0, (Double) results.get(1).getValue("avg_score"), 0.001);
    }

    @Test
    void testSqlOrderByAndLimitOffset() {
        db.execute("CREATE TABLE products (id INT, name VARCHAR, price INT)");
        db.execute("INSERT INTO products VALUES (1, 'Keyboard', 50)");
        db.execute("INSERT INTO products VALUES (2, 'Mouse', 30)");
        db.execute("INSERT INTO products VALUES (3, 'Monitor', 300)");
        db.execute("INSERT INTO products VALUES (4, 'Headset', 75)");
        db.execute("INSERT INTO products VALUES (5, 'Laptop', 1200)");

        QueryResult result = db.execute("SELECT name, price FROM products ORDER BY price DESC LIMIT 3 OFFSET 1");
        assertTrue(result.isSuccess(), () -> "Query failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(3, rows.size());
        assertEquals("Monitor", rows.get(0).getValue("name")); // 300 (after skipping Laptop 1200)
        assertEquals(300, rows.get(0).getValue("price"));
        assertEquals("Headset", rows.get(1).getValue("name")); // 75
        assertEquals(75, rows.get(1).getValue("price"));
        assertEquals("Keyboard", rows.get(2).getValue("name")); // 50
        assertEquals(50, rows.get(2).getValue("price"));
    }

    @Test
    void testSqlGroupByCombinedWithJoins() {
        // DoD: "GROUP BY combined with JOINs works cleanly"
        db.execute("CREATE TABLE departments (dept_id INT, dept_name VARCHAR)");
        db.execute("CREATE TABLE employees (emp_id INT, name VARCHAR, dept_id INT, salary INT)");

        db.execute("INSERT INTO departments VALUES (10, 'Engineering')");
        db.execute("INSERT INTO departments VALUES (20, 'Sales')");
        db.execute("INSERT INTO departments VALUES (30, 'Marketing')");

        db.execute("INSERT INTO employees VALUES (1, 'Alice', 10, 120000)");
        db.execute("INSERT INTO employees VALUES (2, 'Bob', 10, 110000)");
        db.execute("INSERT INTO employees VALUES (3, 'Charlie', 10, 130000)");
        db.execute("INSERT INTO employees VALUES (4, 'Diana', 20, 95000)");
        db.execute("INSERT INTO employees VALUES (5, 'Edward', 20, 85000)");
        db.execute("INSERT INTO employees VALUES (6, 'Fiona', 30, 70000)");

        // JOIN departments + employees, group by dept_name, having count >= 2, ordered by emp_count DESC
        QueryResult result = db.execute(
            "SELECT d.dept_name, COUNT(e.emp_id) AS emp_count, SUM(e.salary) AS total_sal "
            + "FROM departments d JOIN employees e ON d.dept_id = e.dept_id "
            + "GROUP BY d.dept_name HAVING COUNT(e.emp_id) >= 2 ORDER BY emp_count DESC"
        );
        assertTrue(result.isSuccess(), () -> "JOIN + GROUP BY failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(2, rows.size(), "Marketing has only 1 employee and must be filtered by HAVING");

        // First row: Engineering with 3 employees
        assertEquals("Engineering", rows.get(0).getValue("dept_name"));
        assertEquals(3, rows.get(0).getValue("emp_count"));
        assertEquals(360000L, rows.get(0).getValue("total_sal"));

        // Second row: Sales with 2 employees
        assertEquals("Sales", rows.get(1).getValue("dept_name"));
        assertEquals(2, rows.get(1).getValue("emp_count"));
        assertEquals(180000L, rows.get(1).getValue("total_sal"));
    }

    @Test
    void testSqlLeftJoinWithGroupBy() {
        db.execute("CREATE TABLE projects (p_id INT, p_name VARCHAR)");
        db.execute("CREATE TABLE tasks (t_id INT, p_id INT, t_name VARCHAR)");

        db.execute("INSERT INTO projects VALUES (1, 'Alpha')");
        db.execute("INSERT INTO projects VALUES (2, 'Beta')");
        db.execute("INSERT INTO projects VALUES (3, 'Gamma')"); // No tasks

        db.execute("INSERT INTO tasks VALUES (101, 1, 'Task A1')");
        db.execute("INSERT INTO tasks VALUES (102, 1, 'Task A2')");
        db.execute("INSERT INTO tasks VALUES (103, 2, 'Task B1')");

        QueryResult result = db.execute(
            "SELECT p.p_name, COUNT(t.t_id) AS task_count "
            + "FROM projects p LEFT JOIN tasks t ON p.p_id = t.p_id "
            + "GROUP BY p.p_name ORDER BY task_count DESC"
        );
        assertTrue(result.isSuccess(), () -> "LEFT JOIN + GROUP BY failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(3, rows.size());

        assertEquals("Alpha", rows.get(0).getValue("p_name"));
        assertEquals(2, rows.get(0).getValue("task_count"));

        assertEquals("Beta", rows.get(1).getValue("p_name"));
        assertEquals(1, rows.get(1).getValue("task_count"));

        assertEquals("Gamma", rows.get(2).getValue("p_name"));
        assertEquals(0, rows.get(2).getValue("task_count"), "Gamma has no tasks, COUNT(t_id) must be 0");
    }

    @Test
    void testSqlMultiTableJoinWithWhereGroupByHavingOrderByLimit() {
        db.execute("CREATE TABLE customers (c_id INT, c_name VARCHAR, country VARCHAR)");
        db.execute("CREATE TABLE orders (o_id INT, c_id INT, amount INT)");

        db.execute("INSERT INTO customers VALUES (1, 'Acme Corp', 'USA')");
        db.execute("INSERT INTO customers VALUES (2, 'Beta LLC', 'USA')");
        db.execute("INSERT INTO customers VALUES (3, 'Gamma Ltd', 'UK')");

        db.execute("INSERT INTO orders VALUES (101, 1, 500)");
        db.execute("INSERT INTO orders VALUES (102, 1, 600)");
        db.execute("INSERT INTO orders VALUES (103, 1, 400)");
        db.execute("INSERT INTO orders VALUES (104, 2, 800)");
        db.execute("INSERT INTO orders VALUES (105, 2, 700)");
        db.execute("INSERT INTO orders VALUES (106, 3, 2000)"); // Filtered by WHERE country='USA'

        // WHERE + JOIN + GROUP BY + HAVING + ORDER BY + LIMIT
        QueryResult result = db.execute(
            "SELECT c.c_name, COUNT(o.o_id) AS order_cnt, SUM(o.amount) AS total_spent "
            + "FROM customers c JOIN orders o ON c.c_id = o.c_id "
            + "WHERE c.country = 'USA' "
            + "GROUP BY c.c_name "
            + "HAVING SUM(o.amount) >= 1400 "
            + "ORDER BY total_spent DESC "
            + "LIMIT 1"
        );
        assertTrue(result.isSuccess(), () -> "Full pipeline failed: " + result.getError());

        List<Tuple> rows = result.getRows();
        assertEquals(1, rows.size());

        // Acme total is 1500, Beta total is 1500 (both >= 1400), LIMIT 1 picks top
        Tuple top = rows.get(0);
        assertTrue("Acme Corp".equals(top.getValue("c_name")) || "Beta LLC".equals(top.getValue("c_name")));
        assertEquals(1500L, top.getValue("total_spent"));
    }
}
