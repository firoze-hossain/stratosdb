package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.index.btree.BTreeIndex;
import com.stratosdb.sql.execution.IndexOnlyScanOperator;
import com.stratosdb.sql.execution.IndexScanOperator;
import com.stratosdb.sql.execution.TupleSlot;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.BTreePage;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.transaction.Transaction;
import com.stratosdb.transaction.TransactionManager;
import com.stratosdb.transaction.mvcc.Snapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Sprint 06 Week 12:
 * Persistent 2-bit Visibility Map, Index-Only Scans, EXPLAIN ANALYZE 0 heap page reads,
 * and >= 3x throughput speedup on indexed queries.
 */
public class VisibilityMapAndIndexOnlyScanTest {

    @Test
    void testIndexOnlyScanReportsZeroHeapPageReadsInExplainAnalyze(@TempDir Path tempDir) {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setBufferPoolSize(64);
        StratosDB db = new StratosDB(config);
        try {
            assertTrue(db.execute("CREATE TABLE orders (id INT, payload VARCHAR)").isSuccess());
            assertTrue(db.execute("CREATE INDEX idx_orders_id ON orders (id)").isSuccess());

            String pad = "P".repeat(180);
            for (int i = 1; i <= 1000; i++) {
                assertTrue(db.execute("INSERT INTO orders VALUES (" + i + ", '" + pad + i + "')").isSuccess());
            }

            // VACUUM marks all pages all-visible & all-frozen, ANALYZE collects table stats
            assertTrue(db.execute("VACUUM orders").isSuccess());
            assertTrue(db.execute("ANALYZE orders").isSuccess());

            // 1. Point lookup Index-Only Scan
            QueryResult explainPoint = db.execute("EXPLAIN ANALYZE SELECT id FROM orders WHERE id = 150");
            assertTrue(explainPoint.isSuccess(), explainPoint.getMessage());
            String msgPoint = explainPoint.getMessage();
            assertTrue(msgPoint.contains("Index Only Scan"), "Expected Index Only Scan, got: " + msgPoint);
            assertTrue(msgPoint.contains("heap_page_reads=0"), "Expected 0 heap page reads, got: " + msgPoint);
            assertTrue(msgPoint.contains("rows=1"), "Expected rows=1, got: " + msgPoint);

            // 2. Range Index-Only Scan (selective range so planner picks index)
            QueryResult explainRange = db.execute("EXPLAIN ANALYZE SELECT id FROM orders WHERE id <= 5");
            assertTrue(explainRange.isSuccess(), explainRange.getMessage());
            String msgRange = explainRange.getMessage();
            assertTrue(msgRange.contains("Index Only Scan"), "Expected Index Only Scan, got: " + msgRange);
            assertTrue(msgRange.contains("heap_page_reads=0"), "Expected 0 heap page reads, got: " + msgRange);

            // Verify actual range query returns exact matching IDs
            QueryResult rangeData = db.execute("SELECT id FROM orders WHERE id <= 5");
            assertTrue(rangeData.isSuccess());
            assertEquals(5, rangeData.getRows().size());
        } finally {
            db.shutdown();
        }

        // Restart StratosDB and verify persistent .vm file still yields 0 heap page reads without re-VACUUM
        DatabaseConfig config2 = new DatabaseConfig();
        config2.setDataDirectory(tempDir.toString());
        config2.setBufferPoolSize(64);
        StratosDB db2 = new StratosDB(config2);
        try {
            QueryResult explainAfterRestart = db2.execute("EXPLAIN ANALYZE SELECT id FROM orders WHERE id = 150");
            assertTrue(explainAfterRestart.isSuccess(), explainAfterRestart.getMessage());
            String msg = explainAfterRestart.getMessage();
            assertTrue(msg.contains("Index Only Scan"), "Expected Index Only Scan after restart, got: " + msg);
            assertTrue(msg.contains("heap_page_reads=0"), "Expected 0 heap page reads after restart, got: " + msg);
        } finally {
            db2.shutdown();
        }
    }

    @Test
    void testMutationClearsVisibilityBitAndTriggersSafeHeapFallback(@TempDir Path tempDir) {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setBufferPoolSize(64);
        StratosDB db = new StratosDB(config);
        try {
            assertTrue(db.execute("CREATE TABLE items (id INT, tag VARCHAR)").isSuccess());
            assertTrue(db.execute("CREATE INDEX idx_items_id ON items (id)").isSuccess());

            String pad = "T".repeat(180);
            for (int i = 1; i <= 1000; i++) {
                assertTrue(db.execute("INSERT INTO items VALUES (" + i + ", '" + pad + i + "')").isSuccess());
            }
            assertTrue(db.execute("VACUUM items").isSuccess());
            assertTrue(db.execute("ANALYZE items").isSuccess());

            QueryResult beforeInsert = db.execute("EXPLAIN ANALYZE SELECT id FROM items WHERE id = 999");
            assertTrue(beforeInsert.getMessage().contains("heap_page_reads=0"), beforeInsert.getMessage());

            // Insert a new row -> lands on the last page (where id=999 lives) and clears ALL_VISIBLE
            assertTrue(db.execute("INSERT INTO items VALUES (1001, '" + pad + "1001')").isSuccess());

            QueryResult afterInsert = db.execute("EXPLAIN ANALYZE SELECT id FROM items WHERE id = 999");
            assertTrue(afterInsert.getMessage().contains("heap_page_reads=1"),
                "Expected heap fallback (heap_page_reads=1) on modified page, got: " + afterInsert.getMessage());

            // Re-VACUUM restores 0 heap page reads
            assertTrue(db.execute("VACUUM items").isSuccess());
            QueryResult afterVacuum = db.execute("EXPLAIN ANALYZE SELECT id FROM items WHERE id = 999");
            assertTrue(afterVacuum.getMessage().contains("heap_page_reads=0"), afterVacuum.getMessage());
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testIndexOnlyScan3xThroughputSpeedup(@TempDir Path tempDir) {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(64, dm);
        TransactionManager txnMgr = new TransactionManager();

        try {
            HeapTable table = new HeapTable("bench_items", bp);
            BTreeIndex index = new BTreeIndex("idx_bench_id", bp, false);

            // Populate 20,000 rows with wide payload so heap tuples span ~1,000+ pages
            // (exceeding the 64-page buffer pool to reflect real heap-fetch vs index-only I/O + deserialization cost)
            String pad = "X".repeat(250);
            Transaction insertTx = txnMgr.begin();
            for (int i = 0; i < 20_000; i++) {
                byte[] tupleBytes = new Tuple(
                    List.of(i, pad + i),
                    List.of("id", "payload")).serialize();
                HeapTable.InsertResult ins = table.insertMvcc(tupleBytes, insertTx.getXID());
                index.insert(i, new BTreePage.RID(ins.pageId, ins.slot));
            }
            txnMgr.commit(insertTx);

            // Vacuum marks all pages ALL_VISIBLE
            Transaction vacTx = txnMgr.begin();
            table.vacuum(vacTx.getXID(), txnMgr);
            txnMgr.commit(vacTx);

            Transaction readTx = txnMgr.begin();
            Snapshot snap = txnMgr.createSnapshot(readTx.getXID());
            List<String> cols = List.of("id", "payload");

            try (IndexOnlyScanOperator indexOnlyOp = new IndexOnlyScanOperator(
                    "bench_items", table, snap, txnMgr, index, 0L, 19_999L, false, "id", cols, null);
                 IndexScanOperator regularIndexOp = new IndexScanOperator(
                    "bench_items", table, snap, txnMgr, index, 0L, 19_999L, false, "id", cols, null, false)) {

                // Warmup JIT
                for (int w = 0; w < 2; w++) {
                    indexOnlyOp.open();
                    while (!indexOnlyOp.next().isEmpty()) {}
                    regularIndexOp.open();
                    while (!regularIndexOp.next().isEmpty()) {}
                }

                // Benchmark Index-Only Scan (all-visible -> 0 heap reads)
                long ioStart = System.nanoTime();
                long ioSum = 0;
                int ioCount = 0;
                indexOnlyOp.open();
                TupleSlot slot;
                while ((slot = indexOnlyOp.next()) != null && !slot.isEmpty()) {
                    ioSum += ((Number) slot.getTuple().getValue(0)).longValue();
                    ioCount++;
                }
                long ioNs = System.nanoTime() - ioStart;
                assertEquals(0L, indexOnlyOp.getHeapPageReads());
                assertEquals(20_000L, indexOnlyOp.getIndexOnlyHits());

                // Benchmark Regular Index Scan (heap tuple fetch + MVCC + deserialization)
                long regStart = System.nanoTime();
                long regSum = 0;
                int regCount = 0;
                regularIndexOp.open();
                while ((slot = regularIndexOp.next()) != null && !slot.isEmpty()) {
                    regSum += ((Number) slot.getTuple().getValue(0)).longValue();
                    regCount++;
                }
                long regNs = System.nanoTime() - regStart;

                double speedup = (double) regNs / (double) ioNs;
                System.out.printf(
                    "=== Sprint 06 Week 12 DoD (Index-Only Scan vs Index Scan, 20k rows) ===%n"
                    + "Regular Index Scan: %.2f ms (heap_reads=%d)%n"
                    + "Index-Only Scan:    %.2f ms (heap_reads=%d)%n"
                    + "Speedup:            %.2fx (Goal: >= 3.0x)%n",
                    regNs / 1e6, regularIndexOp.getHeapPageReads(),
                    ioNs / 1e6, indexOnlyOp.getHeapPageReads(), speedup);

                assertEquals(regCount, ioCount);
                assertEquals(regSum, ioSum);
                assertTrue(speedup >= 3.0,
                    "Expected Index-Only Scan >= 3.0x faster than regular Index Scan, got " + speedup + "x");
            } finally {
                txnMgr.commit(readTx);
            }
        } finally {
            bp.close();
            dm.close();
        }
    }
}
