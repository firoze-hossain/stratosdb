package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.execution.*;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class VolcanoIteratorFrameworkTest {

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

    @Test
    void testTupleSlotOperations() {
        TupleSlot slot = new TupleSlot();
        assertTrue(slot.isEmpty(), "New slot must be empty");
        assertNull(slot.getTuple(), "New slot tuple must be null");
        assertEquals(0, slot.size());

        Tuple tuple = new Tuple();
        tuple.addValue("id", 42);
        tuple.addValue("name", "Stratos");

        slot.set(tuple);
        assertFalse(slot.isEmpty(), "Slot with tuple must not be empty");
        assertEquals(tuple, slot.getTuple());
        assertEquals(2, slot.size());
        assertEquals(42, slot.getValue(0));
        assertEquals(42, slot.getValue("id"));
        assertEquals("Stratos", slot.getValue(1));
        assertEquals("Stratos", slot.getValue("name"));
        assertEquals(List.of("id", "name"), slot.getColumnNames());

        slot.clear();
        assertTrue(slot.isEmpty(), "Cleared slot must be empty");
        assertNull(slot.getTuple());
        assertNull(slot.getValue(0));
        assertNull(slot.getValue("id"));

        TupleSlot ofSlot = TupleSlot.of(tuple);
        assertFalse(ofSlot.isEmpty());
        assertEquals(42, ofSlot.getValue("id"));

        TupleSlot emptySlot = TupleSlot.empty();
        assertTrue(emptySlot.isEmpty());
    }

    @Test
    void testPhysicalOperatorsDirectPipeline() {
        db.execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR, price INT)");
        for (int i = 1; i <= 20; i++) {
            db.execute(String.format("INSERT INTO products VALUES (%d, 'Product_%d', %d)", i, i, i * 10));
        }

        HeapTable table = db.getTable("products");
        assertNotNull(table, "Table 'products' must exist");

        // 1. SeqScanOperator
        SeqScanOperator seqScan = new SeqScanOperator(
            "products",
            table,
            null,
            null,
            List.of("id", "name", "price"),
            (tbl, data) -> Tuple.deserialize(data, List.of("id", "name", "price"))
        );

        seqScan.open();
        int count = 0;
        while (true) {
            TupleSlot slot = seqScan.next();
            if (slot == null || slot.isEmpty()) break;
            count++;
            assertEquals(3, slot.size());
        }
        assertEquals(20, count, "SeqScanOperator should yield all 20 rows");
        seqScan.close();

        // 2. FilterOperator (price > 100 -> i > 10 -> 10 rows)
        SeqScanOperator seqScan2 = new SeqScanOperator(
            "products",
            table,
            null,
            null,
            List.of("id", "name", "price"),
            (tbl, data) -> Tuple.deserialize(data, List.of("id", "name", "price"))
        );
        FilterOperator filterOp = new FilterOperator(
            seqScan2,
            t -> ((Number) t.getValue("price")).intValue() > 100
        );

        filterOp.open();
        List<Integer> prices = new ArrayList<>();
        while (true) {
            TupleSlot slot = filterOp.next();
            if (slot == null || slot.isEmpty()) break;
            prices.add(((Number) slot.getValue("price")).intValue());
        }
        filterOp.close();
        assertEquals(10, prices.size(), "FilterOperator should emit exactly 10 rows with price > 100");
        for (int p : prices) {
            assertTrue(p > 100, "Emitted price must be > 100");
        }

        // 3. ProjectOperator (project name and price, compute discount)
        SeqScanOperator seqScan3 = new SeqScanOperator(
            "products",
            table,
            null,
            null,
            List.of("id", "name", "price"),
            (tbl, data) -> Tuple.deserialize(data, List.of("id", "name", "price"))
        );
        ProjectOperator projectOp = new ProjectOperator(
            seqScan3,
            t -> {
                Tuple proj = new Tuple();
                proj.addValue("item_name", t.getValue("name"));
                proj.addValue("sale_price", ((Number) t.getValue("price")).intValue() - 5);
                return proj;
            },
            List.of("item_name", "sale_price")
        );

        projectOp.open();
        TupleSlot firstProjected = projectOp.next();
        assertNotNull(firstProjected);
        assertFalse(firstProjected.isEmpty());
        assertEquals("Product_1", firstProjected.getValue("item_name"));
        assertEquals(5, firstProjected.getValue("sale_price"));
        assertEquals(List.of("item_name", "sale_price"), projectOp.getOutputColumns());
        projectOp.close();

        // 4. LimitOperator (limit = 5, offset = 3 -> 5 rows: rows 4, 5, 6, 7, 8)
        SeqScanOperator seqScan4 = new SeqScanOperator(
            "products",
            table,
            null,
            null,
            List.of("id", "name", "price"),
            (tbl, data) -> Tuple.deserialize(data, List.of("id", "name", "price"))
        );
        LimitOperator limitOp = new LimitOperator(seqScan4, 5, 3);
        limitOp.open();
        List<Integer> limitIds = new ArrayList<>();
        while (true) {
            TupleSlot slot = limitOp.next();
            if (slot == null || slot.isEmpty()) break;
            limitIds.add(((Number) slot.getValue("id")).intValue());
        }
        limitOp.close();
        assertEquals(5, limitIds.size(), "LimitOperator should emit exactly 5 rows");
        assertEquals(List.of(4, 5, 6, 7, 8), limitIds);
    }

    @Test
    void testEndToEndSelectPipelineWithVolcanoStreaming() {
        db.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT, score INT)");
        for (int i = 1; i <= 30; i++) {
            db.execute(String.format("INSERT INTO users VALUES (%d, 'User_%d', %d, %d)", i, i, 20 + (i % 10), i * 100));
        }

        // Query with WHERE, projection with alias, and LIMIT + OFFSET
        QueryResult result = db.execute("SELECT name AS full_name, score FROM users WHERE age >= 25 LIMIT 5 OFFSET 2");
        assertTrue(result.isSuccess(), "Select query must succeed");
        assertTrue(result.isStreaming(), "QueryResult must be backed by Volcano streaming pipeline");
        assertNotNull(result.getOperator(), "PhysicalOperator root pipeline must be present");

        // Verify root operator is LimitOperator
        PhysicalOperator root = result.getOperator();
        assertInstanceOf(LimitOperator.class, root, "Root operator for query with LIMIT must be LimitOperator");

        LimitOperator limitOp = (LimitOperator) root;
        assertEquals(5, limitOp.getLimit());
        assertEquals(2, limitOp.getOffset());

        // Verify pipeline streaming execution via Iterator (constant memory)
        List<String> names = new ArrayList<>();
        List<Integer> scores = new ArrayList<>();
        for (Tuple row : result) {
            names.add((String) row.getValue("full_name"));
            scores.add(((Number) row.getValue("score")).intValue());
        }

        assertEquals(5, names.size(), "Iterator must stream exactly 5 rows");
        assertEquals(List.of("full_name", "score"), result.getColumnNames());

        // Verify lazy materialization into List<Tuple> is also supported and idempotent
        List<Tuple> materialized = result.getRows();
        assertNotNull(materialized);
        assertEquals(5, materialized.size());
        assertSame(materialized, result.getRows(), "Subsequent getRows() must return cached list");
    }

    @Test
    void testIndexScanOperatorViaSelect() {
        db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)");
        for (int i = 1; i <= 50; i++) {
            db.execute(String.format("INSERT INTO accounts VALUES (%d, %d)", i, i * 50));
        }

        // 1. Point equality index scan
        QueryResult eqResult = db.execute("SELECT id, balance FROM accounts WHERE id = 25");
        assertTrue(eqResult.isSuccess());
        assertTrue(eqResult.isStreaming());

        List<Tuple> eqRows = new ArrayList<>();
        for (Tuple t : eqResult) {
            eqRows.add(t);
        }
        assertEquals(1, eqRows.size());
        assertEquals(25, eqRows.get(0).getValue("id"));
        assertEquals(1250, eqRows.get(0).getValue("balance"));

        // 2. Range index scan
        QueryResult rangeResult = db.execute("SELECT id, balance FROM accounts WHERE id >= 10 AND id <= 15");
        assertTrue(rangeResult.isSuccess());
        assertTrue(rangeResult.isStreaming());

        List<Integer> rangeIds = new ArrayList<>();
        for (Tuple t : rangeResult) {
            rangeIds.add(((Number) t.getValue("id")).intValue());
        }
        assertEquals(List.of(10, 11, 12, 13, 14, 15), rangeIds);

        // 3. Index-only scan (SELECT id FROM accounts WHERE id = 10)
        QueryResult indexOnlyResult = db.execute("SELECT id FROM accounts WHERE id = 10");
        assertTrue(indexOnlyResult.isSuccess());
        assertTrue(indexOnlyResult.isStreaming());
        List<Tuple> indexOnlyRows = indexOnlyResult.getRows();
        assertEquals(1, indexOnlyRows.size());
        assertEquals(10, indexOnlyRows.get(0).getValue("id"));
    }

    @Test
    void testVolcanoLimitShortCircuitExecution() {
        db.execute("CREATE TABLE big_data (id INT PRIMARY KEY, info VARCHAR)");

        // Insert 1000 rows
        for (int i = 1; i <= 1000; i++) {
            db.execute(String.format("INSERT INTO big_data VALUES (%d, 'Payload_Data_%d')", i, i));
        }

        // Run query with LIMIT 3
        QueryResult result = db.execute("SELECT id, info FROM big_data LIMIT 3");
        assertTrue(result.isSuccess());
        assertTrue(result.isStreaming());

        PhysicalOperator root = result.getOperator();
        assertInstanceOf(LimitOperator.class, root);
        LimitOperator limitOp = (LimitOperator) root;

        int rowCount = 0;
        for (Tuple t : result) {
            rowCount++;
            assertNotNull(t.getValue("id"));
        }

        assertEquals(3, rowCount, "LIMIT 3 must emit exactly 3 rows");
        assertEquals(3, limitOp.getRowsEmitted(), "LimitOperator must have short-circuited after 3 rows");
    }

    @Test
    void testStreamingSequentialScanConstantMemory() {
        db.execute("CREATE TABLE sensor_readings (id INT, temp INT, humidity INT)");

        // Insert 2,000 rows
        for (int i = 1; i <= 2000; i++) {
            db.execute(String.format("INSERT INTO sensor_readings VALUES (%d, %d, %d)", i, 20 + (i % 15), 50 + (i % 30)));
        }

        QueryResult streamResult = db.execute("SELECT id, temp, humidity FROM sensor_readings");
        assertTrue(streamResult.isSuccess());
        assertTrue(streamResult.isStreaming());

        // Stream rows one-by-one without calling getRows()
        long streamedCount = 0;
        long sumTemp = 0;
        for (Tuple row : streamResult) {
            streamedCount++;
            sumTemp += ((Number) row.getValue("temp")).longValue();
        }

        assertEquals(2000, streamedCount, "Streaming iterator must process all 2000 rows");
        assertTrue(sumTemp > 0, "Sum of temperatures should be positive");
    }

    @Test
    void testLimitAndOffsetCombinations() {
        db.execute("CREATE TABLE numbers (num INT PRIMARY KEY)");
        for (int i = 1; i <= 10; i++) {
            db.execute("INSERT INTO numbers VALUES (" + i + ")");
        }

        // Limit 0
        QueryResult r0 = db.execute("SELECT num FROM numbers LIMIT 0");
        assertTrue(r0.isStreaming());
        assertEquals(0, r0.getRows().size());

        // Offset 10 (at boundary)
        QueryResult rBoundary = db.execute("SELECT num FROM numbers OFFSET 10");
        assertTrue(rBoundary.isStreaming());
        assertEquals(0, rBoundary.getRows().size());

        // Offset 7
        QueryResult rOffset = db.execute("SELECT num FROM numbers OFFSET 7");
        List<Integer> offsetNums = new ArrayList<>();
        for (Tuple t : rOffset) {
            offsetNums.add(((Number) t.getValue("num")).intValue());
        }
        assertEquals(List.of(8, 9, 10), offsetNums);

        // Limit 2 Offset 3
        QueryResult rLimOff = db.execute("SELECT num FROM numbers LIMIT 2 OFFSET 3");
        List<Integer> limOffNums = new ArrayList<>();
        for (Tuple t : rLimOff) {
            limOffNums.add(((Number) t.getValue("num")).intValue());
        }
        assertEquals(List.of(4, 5), limOffNums);
    }
}
