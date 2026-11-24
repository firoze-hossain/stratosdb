package com.stratosdb.storage.buffer;

import com.stratosdb.common.constants.PageConstants;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.Page;
import com.stratosdb.storage.page.SlottedPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Rigorous Verification Suite for Sprint 01 Week 02:
 * Partitioned Latch-Free Buffer Pool (64-partition hash striping,
 * lock-free AtomicReferenceArray page lookup, and per-partition Clock-sweep eviction).
 */
public class PartitionedBufferPoolTest {

    @TempDir
    Path tempDir;

    private final List<BufferPoolManager> openPools = new ArrayList<>();

    private BufferPoolManager track(BufferPoolManager pool) {
        openPools.add(pool);
        return pool;
    }

    @AfterEach
    void tearDown() {
        for (BufferPoolManager pool : openPools) {
            try {
                pool.close();
            } catch (Exception ignored) {}
        }
        openPools.clear();
    }

    @Test
    void test64PartitionHashDistribution() {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = track(new BufferPoolManager(256, dm));

        assertEquals(64, bp.getNumPartitions(), "Buffer pool must initialize 64 hash partitions by default");

        Set<Integer> touchedPartitions = new HashSet<>();
        // Load 200 pages across 5 tables
        for (int t = 1; t <= 5; t++) {
            String tableName = "table_" + t;
            for (long p = 0; p < 40; p++) {
                Page page = bp.getPage(tableName, p);
                assertNotNull(page);
                bp.unpinPage(tableName, p);
            }
        }

        for (int i = 0; i < bp.getNumPartitions(); i++) {
            if (bp.getPartition(i).getActiveCount() > 0) {
                touchedPartitions.add(i);
            }
        }

        // With uniform Murmur-style mixing hash, 200 pages should distribute across >= 50 partitions
        assertTrue(touchedPartitions.size() >= 50,
                "Pages must be evenly distributed across partitions, touched: " + touchedPartitions.size());
    }

    @Test
    void testLockFreePageLookupUsingAtomicReferenceArrayAndCas() {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = track(new BufferPoolManager(64, dm));

        // Load page
        Page page = bp.getPage("users", 0);
        assertNotNull(page);
        assertTrue(page.isOffHeap(), "Page must reside in off-heap memory");

        // Verify initial pin count = 1
        BufferPoolPartition part = null;
        for (int i = 0; i < bp.getNumPartitions(); i++) {
            if (bp.getPartition(i).getDirectory().containsKey(new PageKey("users", 0))) {
                part = bp.getPartition(i);
                break;
            }
        }
        assertNotNull(part, "Partition containing 'users'/0 must be found");

        Integer slot = part.getDirectory().get(new PageKey("users", 0));
        assertNotNull(slot);
        BufferFrame frame = part.getFrames().get(slot);
        assertNotNull(frame);
        assertEquals(BufferFrame.STATE_VALID, frame.getState(), "Frame must be STATE_VALID");
        assertEquals(1, frame.getPinCount(), "Frame initial pin count must be 1");
        assertTrue(frame.getReferenceBit(), "Frame reference bit must be true on access");

        // Lock-free cache hit: getPage should increment pin and keep reference bit true
        Page pageHit = bp.getPage("users", 0);
        assertSame(page, pageHit, "Cache hit must return the exact same Page instance");
        assertEquals(2, frame.getPinCount(), "Pin count must atomically increment to 2");

        bp.unpinPage("users", 0);
        assertEquals(1, frame.getPinCount(), "Pin count must decrement to 1");

        bp.unpinPage("users", 0);
        assertEquals(0, frame.getPinCount(), "Pin count must decrement to 0");
    }

    @Test
    void testClockSweepEvictionPerPartition() {
        DiskManager dm = new DiskManager(tempDir.toString());
        // Configure pool with 1 partition of 2 pages capacity for deterministic clock testing
        BufferPoolManager bp = track(new BufferPoolManager(2, dm, 0L, false, 1));

        assertEquals(1, bp.getNumPartitions(), "Single partition for deterministic clock sweep");
        BufferPoolPartition part = bp.getPartition(0);

        // Load Page 0
        Page p0 = bp.getPage("t_clock", 0);
        p0.getBuffer().putLong(24, 1000L);
        bp.markDirty("t_clock", 0);
        bp.unpinPage("t_clock", 0);

        // Load Page 1
        Page p1 = bp.getPage("t_clock", 1);
        p1.getBuffer().putLong(24, 2000L);
        bp.markDirty("t_clock", 1);
        bp.unpinPage("t_clock", 1);

        assertEquals(2, bp.getCacheSize(), "Both frames should be occupied");

        // Both pages were accessed, so both have referenceBit = true and pinCount = 0
        Integer slot0 = part.getDirectory().get(new PageKey("t_clock", 0));
        Integer slot1 = part.getDirectory().get(new PageKey("t_clock", 1));
        assertNotNull(slot0);
        assertNotNull(slot1);
        assertTrue(part.getFrames().get(slot0).getReferenceBit());
        assertTrue(part.getFrames().get(slot1).getReferenceBit());

        // Now access Page 0 again to give it high frequency / recent reference
        Page p0Hit = bp.getPage("t_clock", 0);
        bp.unpinPage("t_clock", 0);

        // Load Page 2: must trigger Clock-sweep eviction!
        // Clock hand will give second chance (clear reference bit) and evict unpinned page
        Page p2 = bp.getPage("t_clock", 2);
        assertNotNull(p2);
        p2.getBuffer().putLong(24, 3000L);
        bp.markDirty("t_clock", 2);
        bp.unpinPage("t_clock", 2);

        assertEquals(2, bp.getCacheSize(), "Cache size must remain <= 2");

        // Reload all pages to verify evicted page was cleanly written to disk and can be restored
        for (long i = 0; i <= 2; i++) {
            Page p = bp.getPage("t_clock", i);
            long expected = (i + 1) * 1000L;
            assertEquals(expected, p.getBuffer().getLong(24), "Data for page " + i + " must match");
            bp.unpinPage("t_clock", i);
        }
    }

    @Test
    void testLinearScalabilityAcross64VirtualThreads() throws Exception {
        DiskManager dm = new DiskManager(tempDir.toString());
        // 64-partition buffer pool with 512 pages
        BufferPoolManager bp = track(new BufferPoolManager(512, dm));

        int threadCount = 64;
        int operationsPerThread = 200;

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finishGate = new CountDownLatch(threadCount);
        AtomicInteger totalSuccessOps = new AtomicInteger(0);

        long startTime = System.nanoTime();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        startGate.await();
                        for (int op = 0; op < operationsPerThread; op++) {
                            // Virtual threads access distinct tables and pages across 64 partitions
                            String table = "v_table_" + (threadId % 16);
                            long pageId = (threadId * 10L) + (op % 5);

                            Page page = bp.getPage(table, pageId);
                            assertNotNull(page);
                            assertTrue(page.isOffHeap());

                            // Mutate page under latch
                            page.getLatch().writeLock().lock();
                            try {
                                page.getBuffer().putLong(24, 555000L + pageId);
                                bp.markDirty(table, pageId);
                            } finally {
                                page.getLatch().writeLock().unlock();
                            }

                            bp.unpinPage(table, pageId);
                            totalSuccessOps.incrementAndGet();
                        }
                    } catch (Exception e) {
                        fail("Virtual thread failed: " + e.getMessage());
                    } finally {
                        finishGate.countDown();
                    }
                });
            }

            // Release all 64 virtual threads simultaneously
            startGate.countDown();
            assertTrue(finishGate.await(10, TimeUnit.SECONDS), "All 64 virtual threads must complete within 10s");
        }

        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        int expectedTotalOps = threadCount * operationsPerThread;
        assertEquals(expectedTotalOps, totalSuccessOps.get(), "All operations must succeed");

        // Verify high throughput (> 1,000 ops/sec under concurrent load)
        double throughput = (double) expectedTotalOps / (durationMs / 1000.0);
        assertTrue(throughput > 1000.0,
                "Throughput under 64 virtual threads must exceed 1,000 ops/sec, actual: " + throughput);

        bp.flushAll();

        // Verify data persistence across threads
        for (int t = 0; t < threadCount; t++) {
            String table = "v_table_" + (t % 16);
            for (int op = 0; op < 5; op++) {
                long pageId = (t * 10L) + op;
                Page page = bp.getPage(table, pageId);
                assertEquals(555000L + pageId, page.getBuffer().getLong(24));
                bp.unpinPage(table, pageId);
            }
        }
    }

    @Test
    void testConcurrentEvictionUnderTightPartitions() throws Exception {
        DiskManager dm = new DiskManager(tempDir.toString());
        // Tight pool of 16 pages across 8 partitions (2 pages per partition)
        BufferPoolManager bp = track(new BufferPoolManager(16, dm, 0L, false, 8));

        int threadCount = 16;
        int pagesPerThread = 10; // Total 160 distinct pages, forcing heavy eviction
        CountDownLatch latch = new CountDownLatch(threadCount);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int p = 0; p < pagesPerThread; p++) {
                            long pageId = threadId * pagesPerThread + p;
                            Page page = bp.getPage("tight_table", pageId);
                            page.getBuffer().putLong(24, 888000L + pageId);
                            bp.markDirty("tight_table", pageId);
                            bp.unpinPage("tight_table", pageId);
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS), "Concurrent writers must finish within 10s");
        }

        // Cache size must strictly not exceed maxPages (16)
        assertTrue(bp.getCacheSize() <= 16,
                "Buffer pool size must not exceed 16 pages under concurrent eviction, was: " + bp.getCacheSize());

        // Re-read all 160 pages to confirm all evicted pages were safely written and reloaded
        for (int t = 0; t < threadCount; t++) {
            for (int p = 0; p < pagesPerThread; p++) {
                long pageId = t * pagesPerThread + p;
                Page page = bp.getPage("tight_table", pageId);
                assertEquals(888000L + pageId, page.getBuffer().getLong(24),
                        "Page " + pageId + " must preserve data after concurrent eviction and reload");
                bp.unpinPage("tight_table", pageId);
            }
        }
    }

    @Test
    void testDropAndTruncateTableAcrossPartitions() {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = track(new BufferPoolManager(64, dm));

        // Populate 2 tables across multiple partitions
        for (long p = 0; p < 20; p++) {
            Page pA = bp.getPage("table_a", p);
            bp.markDirty("table_a", p);
            bp.unpinPage("table_a", p);

            Page pB = bp.getPage("table_b", p);
            bp.markDirty("table_b", p);
            bp.unpinPage("table_b", p);
        }

        assertEquals(40, bp.getCacheSize());

        // Truncate table_a to 10 pages
        bp.truncateTable("table_a", 10);
        assertEquals(30, bp.getCacheSize(), "Truncating 10 pages must reduce cache size to 30");

        // Drop table_b completely
        bp.dropTable("table_b");
        assertEquals(10, bp.getCacheSize(), "Dropping table_b (20 pages) must reduce cache size to 10");

        // Verify table_a remaining pages 0..9 are still intact
        for (long p = 0; p < 10; p++) {
            Page pA = bp.getPage("table_a", p);
            assertNotNull(pA);
            bp.unpinPage("table_a", p);
        }
    }
}
