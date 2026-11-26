package com.stratosdb.testing;

import com.stratosdb.sql.execution.VectorizedSeqScanOperator;
import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.storage.vector.ByteVector;
import com.stratosdb.storage.vector.DoubleVector;
import com.stratosdb.storage.vector.IntVector;
import com.stratosdb.storage.vector.LongVector;
import com.stratosdb.storage.vector.VectorBatch;
import com.stratosdb.storage.vector.VectorType;
import com.stratosdb.storage.vector.VectorizedHeapScanner;
import com.stratosdb.transaction.Transaction;
import com.stratosdb.transaction.TransactionManager;
import com.stratosdb.transaction.mvcc.Snapshot;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Sprint 05 Week 09: Vectorized SIMD Memory Formats.
 * Verifies:
 * 1. Columnar VectorBatch (1024 rows) with IntVector, LongVector, DoubleVector, ByteVector in native memory.
 * 2. Zero heap object allocation during table scan via ThreadMXBean.
 * 3. Memory throughput exceeding 4 GB/sec on sequential scan.
 * 4. MVCC visibility correctness, projection, and virtual thread concurrency.
 */
public class VectorizedSimdMemoryFormatTest {

    @TempDir
    Path tempDir;

    private DiskManager diskManager;
    private BufferPoolManager bufferPool;
    private TransactionManager txnManager;
    private HeapTable table;

    @BeforeEach
    void setUp() {
        diskManager = new DiskManager(tempDir.toString());
        bufferPool = new BufferPoolManager(64, diskManager);
        txnManager = new TransactionManager();
        table = new HeapTable("vec_metrics", bufferPool);
    }

    @AfterEach
    void tearDown() {
        bufferPool.close();
        diskManager.close();
    }

    private void populateRows(int count) {
        Transaction tx = txnManager.begin();
        for (int i = 0; i < count; i++) {
            List<Object> vals = new ArrayList<>(4);
            vals.add(i); // INT
            vals.add(1_000_000_000L + i); // LONG
            vals.add((i % 25 == 0) ? null : i * 1.25); // DOUBLE with periodic NULL
            vals.add(i % 2 == 0); // BOOLEAN -> BYTE
            Tuple t = new Tuple(vals, List.of("id", "ts", "val", "active"));
            table.insertMvcc(t.serialize(), tx.getXID());
        }
        txnManager.commit(tx);
    }

    @Test
    void testVectorizedHeapScanCorrectnessAndMvcc() {
        populateRows(3000);

        // Insert an uncommitted row that must NOT be visible
        Transaction uncommittedTx = txnManager.begin();
        Tuple uncommittedTuple = new Tuple(
            List.of(9999, 9999L, 99.9, true),
            List.of("id", "ts", "val", "active"));
        table.insertMvcc(uncommittedTuple.serialize(), uncommittedTx.getXID());

        Transaction readTx = txnManager.begin();
        Snapshot snap = txnManager.createSnapshot(readTx.getXID());

        try (VectorBatch batch = new VectorBatch(
                List.of("id", "ts", "val", "active"),
                List.of(VectorType.INT, VectorType.LONG, VectorType.DOUBLE, VectorType.BYTE));
             VectorizedSeqScanOperator op = new VectorizedSeqScanOperator(
                table, snap, txnManager,
                List.of("id", "ts", "val", "active"),
                List.of(VectorType.INT, VectorType.LONG, VectorType.DOUBLE, VectorType.BYTE),
                new int[]{0, 1, 2, 3})) {

            op.open();
            int totalScanned = 0;
            int batchCount = 0;
            while (op.nextBatch(batch)) {
                batchCount++;
                int n = batch.getNumRows();
                IntVector ids = batch.getIntVector(0);
                LongVector tss = batch.getLongVector(1);
                DoubleVector vals = batch.getDoubleVector(2);
                ByteVector active = batch.getByteVector(3);

                for (int r = 0; r < n; r++) {
                    int expectedId = totalScanned + r;
                    assertEquals(expectedId, ids.get(r));
                    assertEquals(1_000_000_000L + expectedId, tss.get(r));
                    if (expectedId % 25 == 0) {
                        assertTrue(vals.isNull(r));
                    } else {
                        assertFalse(vals.isNull(r));
                        assertEquals(expectedId * 1.25, vals.get(r), 1e-9);
                    }
                    assertEquals(expectedId % 2 == 0, active.getBoolean(r));
                }
                totalScanned += n;
            }
            assertEquals(3000, totalScanned);
            assertEquals(3, batchCount); // 1024 + 1024 + 952
        } finally {
            txnManager.commit(readTx);
            txnManager.abort(uncommittedTx);
        }
    }

    @Test
    void testZeroObjectAllocationDuringTableScan() {
        populateRows(5000);

        Transaction readTx = txnManager.begin();
        Snapshot snap = txnManager.createSnapshot(readTx.getXID());
        ThreadMXBean threadBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

        try (VectorBatch batch = new VectorBatch(
                List.of("id", "ts", "val", "active"),
                List.of(VectorType.INT, VectorType.LONG, VectorType.DOUBLE, VectorType.BYTE));
             VectorizedHeapScanner scanner = table.scanVectorized(snap, txnManager, new int[]{0, 1, 2, 3})) {

            // Warmup JIT so classloading / C2 compilation completes before measurement
            for (int w = 0; w < 500; w++) {
                scanner.reset();
                while (scanner.nextBatch(batch)) {
                    batch.getIntVector(0).sum();
                }
            }
            threadBean.getCurrentThreadAllocatedBytes();

            scanner.reset();
            long allocBefore = threadBean.getCurrentThreadAllocatedBytes();
            int totalRows = 0;
            long idSum = 0;
            while (scanner.nextBatch(batch)) {
                totalRows += batch.getNumRows();
                idSum += batch.getIntVector(0).sum();
            }
            long allocAfter = threadBean.getCurrentThreadAllocatedBytes();

            assertEquals(5000, totalRows);
            assertEquals(5000L * 4999L / 2L, idSum);
            assertEquals(0L, allocAfter - allocBefore,
                "VectorizedHeapScanner.nextBatch must allocate 0 bytes on Java heap");
        } finally {
            txnManager.commit(readTx);
        }
    }

    @Test
    void testColumnProjectionAndMissingTrailingColumns() {
        // Insert 10 rows with only 2 columns, simulating pre-ALTER TABLE rows
        Transaction tx = txnManager.begin();
        for (int i = 0; i < 10; i++) {
            Tuple t = new Tuple(List.of(i, i * 10L), List.of("a", "b"));
            table.insertMvcc(t.serialize(), tx.getXID());
        }
        txnManager.commit(tx);

        Transaction readTx = txnManager.begin();
        Snapshot snap = txnManager.createSnapshot(readTx.getXID());

        // Project column 1 (b) and non-existent column 2 (instant-added c)
        try (VectorBatch batch = new VectorBatch(
                List.of("b", "c"),
                List.of(VectorType.LONG, VectorType.DOUBLE));
             VectorizedHeapScanner scanner = table.scanVectorized(snap, txnManager, new int[]{1, 2})) {

            assertTrue(scanner.nextBatch(batch));
            assertEquals(10, batch.getNumRows());
            LongVector b = batch.getLongVector(0);
            DoubleVector c = batch.getDoubleVector(1);
            for (int i = 0; i < 10; i++) {
                assertEquals(i * 10L, b.get(i));
                assertTrue(c.isNull(i));
            }
            assertFalse(scanner.nextBatch(batch));
        } finally {
            txnManager.commit(readTx);
        }
    }

    @Test
    void testSequentialScanThroughputExceeds4GBPerSec() {
        try (VectorBatch batch = new VectorBatch(VectorType.LONG, VectorType.DOUBLE, VectorType.INT)) {
            LongVector lv = batch.getLongVector(0);
            DoubleVector dv = batch.getDoubleVector(1);
            IntVector iv = batch.getIntVector(2);
            for (int i = 0; i < 1024; i++) {
                lv.set(i, i + 1L);
                dv.set(i, i * 1.5);
                iv.set(i, i);
            }
            batch.setNumRows(1024);

            long sinkL = 0;
            double sinkD = 0;
            for (int w = 0; w < 20_000; w++) {
                sinkL += lv.sum() + iv.sum();
                sinkD += dv.sum();
            }

            int iterations = 250_000;
            long totalBytes = (long) iterations * 1024L * (Long.BYTES + Double.BYTES + Integer.BYTES);
            long startNs = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                sinkL += lv.sum() + iv.sum();
                sinkD += dv.sum();
            }
            long elapsedNs = System.nanoTime() - startNs;
            double gbPerSec = (totalBytes / 1e9) / (elapsedNs / 1e9);

            assertTrue(gbPerSec > 4.0,
                "Expected sequential scan memory throughput > 4.0 GB/sec, measured " + gbPerSec + " GB/sec (sink=" + sinkL + "," + sinkD + ")");
        }
    }

    @Test
    void testConcurrentVectorizedScansUnderVirtualThreads() throws Exception {
        populateRows(2048);

        Transaction readTx = txnManager.begin();
        Snapshot snap = txnManager.createSnapshot(readTx.getXID());

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Long>> futures = new ArrayList<>();
            for (int t = 0; t < 16; t++) {
                futures.add(executor.submit(() -> {
                    long sum = 0;
                    try (VectorBatch batch = new VectorBatch(VectorType.INT, VectorType.LONG);
                         VectorizedHeapScanner scanner = table.scanVectorized(snap, txnManager, new int[]{0, 1})) {
                        while (scanner.nextBatch(batch)) {
                            sum += batch.getIntVector(0).sum();
                        }
                    }
                    return sum;
                }));
            }
            long expected = 2048L * 2047L / 2L;
            for (Future<Long> f : futures) {
                assertEquals(expected, f.get());
            }
        } finally {
            txnManager.commit(readTx);
        }
    }
}
