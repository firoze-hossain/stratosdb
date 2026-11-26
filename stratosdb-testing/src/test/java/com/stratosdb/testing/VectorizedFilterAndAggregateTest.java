package com.stratosdb.testing;

import com.stratosdb.sql.ast.AggregateCall;
import com.stratosdb.sql.ast.WhereExpr;
import com.stratosdb.sql.execution.FilterOperator;
import com.stratosdb.sql.execution.HashAggregateOperator;
import com.stratosdb.sql.execution.PhysicalOperator;
import com.stratosdb.sql.execution.SeqScanOperator;
import com.stratosdb.sql.execution.TupleSlot;
import com.stratosdb.sql.execution.VectorizedAggregateOperator;
import com.stratosdb.sql.execution.VectorizedFilterOperator;
import com.stratosdb.sql.execution.VectorizedOperator;
import com.stratosdb.sql.execution.VectorizedSeqScanOperator;
import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.storage.vector.DoubleVector;
import com.stratosdb.storage.vector.IntVector;
import com.stratosdb.storage.vector.LongVector;
import com.stratosdb.storage.vector.VectorBatch;
import com.stratosdb.storage.vector.VectorType;
import com.stratosdb.transaction.Transaction;
import com.stratosdb.transaction.TransactionManager;
import com.stratosdb.transaction.mvcc.Snapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration and benchmark tests for Sprint 05 Week 10:
 * VectorizedFilterOperator and VectorizedAggregateOperator powered by
 * Java 25 jdk.incubator.vector SIMD compiler intrinsics.
 */
public class VectorizedFilterAndAggregateTest {

    @TempDir
    Path tempDir;

    private DiskManager diskManager;
    private BufferPoolManager bufferPool;
    private TransactionManager txnManager;
    private HeapTable table;

    @BeforeEach
    void setUp() {
        diskManager = new DiskManager(tempDir.toString());
        bufferPool = new BufferPoolManager(128, diskManager);
        txnManager = new TransactionManager();
        table = new HeapTable("sales", bufferPool);
    }

    @AfterEach
    void tearDown() {
        bufferPool.close();
        diskManager.close();
    }

    private void populateSales(int count) {
        Transaction tx = txnManager.begin();
        for (int i = 0; i < count; i++) {
            int dept = i % 5;
            int qty = (i % 10) + 1;
            Double price = (i % 20 == 0) ? null : ((i % 100) * 1.0);
            Tuple t = new Tuple(
                java.util.Arrays.asList(i, dept, qty, price),
                List.of("id", "dept", "qty", "price"));
            table.insertMvcc(t.serialize(), tx.getXID());
        }
        txnManager.commit(tx);
    }

    @Test
    void testVectorizedFilterWithAndOrNotAndNulls() {
        populateSales(2500);
        Transaction readTx = txnManager.begin();
        Snapshot snap = txnManager.createSnapshot(readTx.getXID());

        // Predicate: (price > 50.0 AND qty >= 5) OR dept = 0
        WhereExpr pred = new WhereExpr.Or(
            new WhereExpr.And(
                new WhereExpr.Comparison("price", ">", "50.0"),
                new WhereExpr.Comparison("qty", ">=", "5")),
            new WhereExpr.Comparison("dept", "=", "0"));

        try (VectorBatch batch = new VectorBatch(
                List.of("id", "dept", "qty", "price"),
                List.of(VectorType.INT, VectorType.INT, VectorType.INT, VectorType.DOUBLE));
             VectorizedOperator scan = new VectorizedSeqScanOperator(
                table, snap, txnManager,
                List.of("id", "dept", "qty", "price"),
                List.of(VectorType.INT, VectorType.INT, VectorType.INT, VectorType.DOUBLE),
                new int[]{0, 1, 2, 3});
             VectorizedFilterOperator filter = new VectorizedFilterOperator(scan, pred)) {

            filter.open();
            int matched = 0;
            while (filter.nextBatch(batch)) {
                int[] sel = batch.getSelectionVector();
                int selCount = batch.getSelectedCount();
                IntVector depts = batch.getIntVector(1);
                IntVector qtys = batch.getIntVector(2);
                DoubleVector prices = batch.getDoubleVector(3);
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    boolean ok = (!prices.isNull(idx) && prices.get(idx) > 50.0 && qtys.get(idx) >= 5)
                        || depts.get(idx) == 0;
                    assertTrue(ok, "Filtered row must satisfy predicate");
                    matched++;
                }
            }

            // Compute exact expected count
            int expected = 0;
            for (int i = 0; i < 2500; i++) {
                int dept = i % 5;
                int qty = (i % 10) + 1;
                Double price = (i % 20 == 0) ? null : ((i % 100) * 1.0);
                if ((price != null && price > 50.0 && qty >= 5) || dept == 0) {
                    expected++;
                }
            }
            assertEquals(expected, matched);
        } finally {
            txnManager.commit(readTx);
        }
    }

    @Test
    void testVectorizedGlobalAggregatesParityWithVolcano() {
        populateSales(3000);
        Transaction readTx = txnManager.begin();
        Snapshot snap = txnManager.createSnapshot(readTx.getXID());

        WhereExpr pred = new WhereExpr.Comparison("price", ">=", "25.0");
        List<AggregateCall> aggs = List.of(
            new AggregateCall("COUNT", "*", "cnt_all"),
            new AggregateCall("COUNT", "price", "cnt_price"),
            new AggregateCall("SUM", "qty", "sum_qty"),
            new AggregateCall("SUM", "price", "sum_price"),
            new AggregateCall("AVG", "price", "avg_price"),
            new AggregateCall("MIN", "qty", "min_qty"),
            new AggregateCall("MAX", "price", "max_price")
        );

        try {
            // 1. Vectorized pipeline
            Tuple vecTuple;
            try (VectorizedOperator scan = new VectorizedSeqScanOperator(
                    table, snap, txnManager,
                    List.of("id", "dept", "qty", "price"),
                    List.of(VectorType.INT, VectorType.INT, VectorType.INT, VectorType.DOUBLE),
                    new int[]{0, 1, 2, 3});
                 VectorizedFilterOperator filter = new VectorizedFilterOperator(scan, pred);
                 VectorizedAggregateOperator vecAgg = new VectorizedAggregateOperator(
                    filter, List.of(), aggs, null)) {
                vecAgg.open();
                TupleSlot slot = vecAgg.next();
                assertFalse(slot.isEmpty());
                vecTuple = slot.getTuple();
                assertTrue(vecAgg.next().isEmpty());
            }

            // 2. Volcano row-by-row pipeline
            Tuple volTuple;
            try (SeqScanOperator seq = new SeqScanOperator(
                    "sales", table, snap, txnManager,
                    List.of("id", "dept", "qty", "price"), null);
                 FilterOperator f = new FilterOperator(seq, t -> {
                     Object p = t.getValue("price");
                     return p instanceof Number n && n.doubleValue() >= 25.0;
                 });
                 HashAggregateOperator volAgg = new HashAggregateOperator(f, List.of(), aggs, (String) null)) {
                volAgg.open();
                TupleSlot slot = volAgg.next();
                assertFalse(slot.isEmpty());
                volTuple = slot.getTuple();
            }

            assertEquals(volTuple.getValue(0), vecTuple.getValue(0));
            assertEquals(volTuple.getValue(1), vecTuple.getValue(1));
            assertEquals(((Number) volTuple.getValue(2)).longValue(), ((Number) vecTuple.getValue(2)).longValue());
            assertEquals(((Number) volTuple.getValue(3)).doubleValue(), ((Number) vecTuple.getValue(3)).doubleValue(), 1e-6);
            assertEquals(((Number) volTuple.getValue(4)).doubleValue(), ((Number) vecTuple.getValue(4)).doubleValue(), 1e-6);
            assertEquals(((Number) volTuple.getValue(5)).intValue(), ((Number) vecTuple.getValue(5)).intValue());
            assertEquals(((Number) volTuple.getValue(6)).doubleValue(), ((Number) vecTuple.getValue(6)).doubleValue(), 1e-6);
        } finally {
            txnManager.commit(readTx);
        }
    }

    @Test
    void testVectorizedGroupByAndHavingParity() {
        populateSales(2000);
        Transaction readTx = txnManager.begin();
        Snapshot snap = txnManager.createSnapshot(readTx.getXID());

        List<AggregateCall> aggs = List.of(
            new AggregateCall("COUNT", "*", "cnt"),
            new AggregateCall("SUM", "qty", "total_qty"),
            new AggregateCall("AVG", "price", "avg_price")
        );
        // Arbitrary HAVING: COUNT(*) > 100 AND AVG(price) > 40
        WhereExpr having = new WhereExpr.And(
            new WhereExpr.Comparison("COUNT(*)", ">", "100"),
            new WhereExpr.Comparison("AVG(price)", ">", "40"));

        try {
            Map<Integer, Tuple> vecResults = new HashMap<>();
            try (VectorizedOperator scan = new VectorizedSeqScanOperator(
                    table, snap, txnManager,
                    List.of("id", "dept", "qty", "price"),
                    List.of(VectorType.INT, VectorType.INT, VectorType.INT, VectorType.DOUBLE),
                    new int[]{0, 1, 2, 3});
                 VectorizedAggregateOperator vecAgg = new VectorizedAggregateOperator(
                    scan, List.of("dept"), aggs, having, null)) {
                vecAgg.open();
                TupleSlot slot;
                while ((slot = vecAgg.next()) != null && !slot.isEmpty()) {
                    Tuple t = slot.getTuple();
                    vecResults.put((Integer) t.getValue(0), t);
                }
            }

            Map<Integer, Tuple> volResults = new HashMap<>();
            try (SeqScanOperator seq = new SeqScanOperator(
                    "sales", table, snap, txnManager,
                    List.of("id", "dept", "qty", "price"), null);
                 HashAggregateOperator volAgg = new HashAggregateOperator(
                    seq, List.of("dept"), aggs, null, having)) {
                volAgg.open();
                TupleSlot slot;
                while ((slot = volAgg.next()) != null && !slot.isEmpty()) {
                    Tuple t = slot.getTuple();
                    volResults.put((Integer) t.getValue(0), t);
                }
            }

            assertFalse(volResults.isEmpty());
            assertEquals(volResults.keySet(), vecResults.keySet());
            for (Integer dept : volResults.keySet()) {
                Tuple vt = volResults.get(dept);
                Tuple st = vecResults.get(dept);
                assertEquals(vt.getValue(1), st.getValue(1));
                assertEquals(((Number) vt.getValue(2)).longValue(), ((Number) st.getValue(2)).longValue());
                assertEquals(((Number) vt.getValue(3)).doubleValue(), ((Number) st.getValue(3)).doubleValue(), 1e-6);
            }
        } finally {
            txnManager.commit(readTx);
        }
    }

    @Test
    void test10MRowsAnalyticalAggregation5xFasterThanVolcano() {
        final int totalRows = 10_000_000;
        final int batchSize = VectorBatch.DEFAULT_BATCH_SIZE; // 1024

        // Pre-populate a 1024-row native VectorBatch pattern that streams 10M rows cleanly
        // without disk I/O bottlenecking CPU execution comparison.
        WhereExpr pred = new WhereExpr.And(
            new WhereExpr.Comparison("price", ">", "25.0"),
            new WhereExpr.Comparison("qty", ">=", "3"));
        List<AggregateCall> aggs = List.of(
            new AggregateCall("COUNT", "*", "cnt"),
            new AggregateCall("SUM", "qty", "sum_qty"),
            new AggregateCall("SUM", "price", "sum_price")
        );

        // Warmup both engines on 100k rows
        runVectorized10M(100_000, batchSize, pred, aggs);
        runVolcano10M(100_000);

        // Measure Vectorized SIMD pipeline on 10,000,000 rows
        long vecStart = System.nanoTime();
        Tuple vecResult = runVectorized10M(totalRows, batchSize, pred, aggs);
        long vecNs = System.nanoTime() - vecStart;

        // Measure Volcano row-by-row pipeline on 10,000,000 rows
        long volStart = System.nanoTime();
        Tuple volResult = runVolcano10M(totalRows);
        long volNs = System.nanoTime() - volStart;

        double speedup = (double) volNs / (double) vecNs;
        System.out.printf(
            "=== Sprint 05 Week 10 DoD (10M rows) ===%n"
            + "Volcano row-by-row: %.2f ms%n"
            + "Vectorized SIMD:    %.2f ms%n"
            + "Speedup:            %.2fx (Goal: >= 5.0x)%n",
            volNs / 1e6, vecNs / 1e6, speedup);

        assertEquals(volResult.getValue(0), vecResult.getValue(0), "COUNT(*) must match on 10M rows");
        assertEquals(((Number) volResult.getValue(1)).longValue(),
            ((Number) vecResult.getValue(1)).longValue(), "SUM(qty) must match on 10M rows");
        assertEquals(((Number) volResult.getValue(2)).doubleValue(),
            ((Number) vecResult.getValue(2)).doubleValue(), 1e-3, "SUM(price) must match on 10M rows");
        assertTrue(speedup >= 5.0,
            "Expected Vectorized SIMD >= 5.0x faster than Volcano on 10M rows, got " + speedup + "x");
    }

    private Tuple runVectorized10M(int totalRows, int batchSize, WhereExpr pred, List<AggregateCall> aggs) {
        int[] baseQty = new int[batchSize];
        double[] basePrice = new double[batchSize];
        long[] baseId = new long[batchSize];
        for (int i = 0; i < batchSize; i++) {
            baseId[i] = i;
            baseQty[i] = (i % 10) + 1;
            basePrice[i] = (i % 100) * 1.0;
        }

        VectorizedOperator source = new VectorizedOperator() {
            int remaining = totalRows;

            @Override
            public void open() {
                remaining = totalRows;
            }

            @Override
            public boolean nextBatch(VectorBatch batch) {
                if (remaining <= 0) return false;
                batch.reset();
                int take = Math.min(batchSize, remaining);
                ((LongVector) batch.getColumn(0)).copyFrom(baseId, 0, 0, take);
                ((IntVector) batch.getColumn(1)).copyFrom(baseQty, 0, 0, take);
                ((DoubleVector) batch.getColumn(2)).copyFrom(basePrice, 0, 0, take);
                batch.setNumRows(take);
                remaining -= take;
                return true;
            }

            @Override
            public void close() {}

            @Override
            public List<String> getOutputColumns() {
                return List.of("id", "qty", "price");
            }

            @Override
            public List<VectorType> getOutputTypes() {
                return List.of(VectorType.LONG, VectorType.INT, VectorType.DOUBLE);
            }
        };

        try (VectorizedFilterOperator filter = new VectorizedFilterOperator(source, pred);
             VectorizedAggregateOperator agg = new VectorizedAggregateOperator(filter, List.of(), aggs, null)) {
            agg.open();
            return agg.next().getTuple();
        }
    }

    private Tuple runVolcano10M(int totalRows) {
        List<String> cols = List.of("id", "qty", "price");
        PhysicalOperator source = new PhysicalOperator() {
            int cursor = 0;
            final TupleSlot slot = new TupleSlot();

            @Override
            public void open() {
                cursor = 0;
            }

            @Override
            public TupleSlot next() {
                if (cursor >= totalRows) {
                    slot.clear();
                    return slot;
                }
                int i = (cursor++) % VectorBatch.DEFAULT_BATCH_SIZE;
                List<Object> vals = new ArrayList<>(3);
                vals.add((long) i);
                vals.add((i % 10) + 1);
                vals.add((i % 100) * 1.0);
                slot.set(new Tuple(vals, cols));
                return slot;
            }

            @Override
            public void close() {}

            @Override
            public List<String> getOutputColumns() {
                return cols;
            }
        };

        List<AggregateCall> aggs = List.of(
            new AggregateCall("COUNT", "*", "cnt"),
            new AggregateCall("SUM", "qty", "sum_qty"),
            new AggregateCall("SUM", "price", "sum_price")
        );

        try (FilterOperator filter = new FilterOperator(source, t -> {
                Object p = t.getValue("price");
                Object q = t.getValue("qty");
                return ((Double) p) > 25.0 && ((Integer) q) >= 3;
            });
             HashAggregateOperator agg = new HashAggregateOperator(filter, List.of(), aggs, (String) null)) {
            agg.open();
            return agg.next().getTuple();
        }
    }
}
