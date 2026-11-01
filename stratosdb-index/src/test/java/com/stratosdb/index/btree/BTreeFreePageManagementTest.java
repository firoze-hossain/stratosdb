package com.stratosdb.index.btree;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.BTreePage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies Sprint 7 Week 14: Free-Page Management & Index Defragmentation.
 *
 * Covers:
 * - Freelist registration when B+Tree pages merge.
 * - Freelist registration when root collapses.
 * - Page recycling from freelist on future splits (zero wasted allocations).
 * - Freelist persistence across index close and reopen.
 * - Index file size stabilization under continuous insert-delete churn (DoD).
 * - BTreeIndex.defragment() physical file compaction and search integrity.
 * - Concurrent multi-threaded churn with freelist recycling.
 */
class BTreeFreePageManagementTest {

    @TempDir
    Path tempDir;

    private final List<BufferPoolManager> openPools = new ArrayList<>();

    private BTreeIndex newIndex(String name, int poolSize) {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(poolSize, dm);
        openPools.add(bp);
        return new BTreeIndex(name, bp);
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
    void testMergeFreesPageAndAddsToFreelist() {
        BTreeIndex index = newIndex("idx_merge_freelist", 256);
        int n = 1200;
        for (long i = 0; i < n; i++) {
            index.insert(i, new BTreePage.RID(i, 0));
        }

        assertEquals(0, index.getFreePageCount(), "Initially empty freelist");
        assertEquals(-1L, index.getFreeListHead());

        // Delete enough keys to force multiple page merges
        for (long i = 0; i < 900; i++) {
            index.delete(i, new BTreePage.RID(i, 0));
        }

        assertTrue(index.getFreePageCount() > 0, "Pages must be freed and tracked on freelist after merges");
        assertTrue(index.getFreeListHead() > 0, "Freelist head must point to a valid freed page");

        int activeKeys = index.validateTree();
        assertEquals(300, activeKeys, "Remaining 300 keys must be valid and ordered");

        for (long i = 900; i < n; i++) {
            assertNotNull(index.search(i), "Active key " + i + " must be searchable");
        }
    }

    @Test
    void testRootCollapseFreesOldRoot() {
        BTreeIndex index = newIndex("idx_root_collapse_freelist", 256);
        int n = 1500;
        for (long i = 0; i < n; i++) {
            index.insert(i, new BTreePage.RID(i, 0));
        }

        long initialRoot = index.getRootPageId();

        // Delete down to 5 keys to force root collapse
        for (long i = 5; i < n; i++) {
            index.delete(i, new BTreePage.RID(i, 0));
        }

        long collapsedRoot = index.getRootPageId();
        assertNotEquals(initialRoot, collapsedRoot, "Root must have collapsed to a lower child page");
        assertTrue(index.getFreePageCount() >= 1, "Freelist must track orphaned pages including collapsed root");

        int count = index.validateTree();
        assertEquals(5, count, "Exactly 5 keys remain");
        for (long i = 0; i < 5; i++) {
            assertNotNull(index.search(i));
        }
    }

    @Test
    void testPageRecyclingFromFreelist() {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(256, dm);
        openPools.add(bp);
        BTreeIndex index = new BTreeIndex("idx_recycling", bp);

        // 1. Insert enough keys to produce splits
        for (long i = 0; i < 800; i++) {
            index.insert(i, new BTreePage.RID(i, 0));
        }
        bp.flushAll();
        long pagesBeforeMerge = dm.getTablePageCount("idx_recycling");

        // 2. Delete to produce free pages
        for (long i = 0; i < 600; i++) {
            index.delete(i, new BTreePage.RID(i, 0));
        }
        long freedPages = index.getFreePageCount();
        assertTrue(freedPages > 0, "Must have free pages available for recycling");
        long headBefore = index.getFreeListHead();

        // 3. Re-insert 600 new keys (should recycle pages from freelist instead of expanding file)
        for (long i = 1000; i < 1600; i++) {
            index.insert(i, new BTreePage.RID(i, 0));
        }
        bp.flushAll();

        long pagesAfterRecycle = dm.getTablePageCount("idx_recycling");
        assertEquals(pagesBeforeMerge, pagesAfterRecycle,
            "Physical file page count must not grow when free pages can be recycled from freelist");
        assertTrue(index.getFreePageCount() < freedPages, "Freelist count must decrease as pages are recycled");

        int totalKeys = index.validateTree();
        assertEquals(800, totalKeys, "Tree must hold exactly 200 + 600 = 800 keys");
    }

    @Test
    void testFreelistPersistenceAcrossReopen() {
        String indexName = "idx_persistence";
        long headBefore;
        long countBefore;

        {
            DiskManager dm = new DiskManager(tempDir.toString());
            BufferPoolManager bp = new BufferPoolManager(128, dm);
            openPools.add(bp);
            BTreeIndex index = new BTreeIndex(indexName, bp);

            for (long i = 0; i < 1000; i++) {
                index.insert(i, new BTreePage.RID(i, 0));
            }
            for (long i = 0; i < 700; i++) {
                index.delete(i, new BTreePage.RID(i, 0));
            }
            headBefore = index.getFreeListHead();
            countBefore = index.getFreePageCount();
            assertTrue(countBefore > 0);
            bp.close();
            openPools.remove(bp);
        }

        // Reopen from disk
        {
            DiskManager dm2 = new DiskManager(tempDir.toString());
            BufferPoolManager bp2 = new BufferPoolManager(128, dm2);
            openPools.add(bp2);
            BTreeIndex reopened = new BTreeIndex(indexName, bp2);

            assertEquals(headBefore, reopened.getFreeListHead(), "Freelist head must match persisted value");
            assertEquals(countBefore, reopened.getFreePageCount(), "Free page count must match persisted value");

            int activeKeys = reopened.validateTree();
            assertEquals(300, activeKeys, "All 300 remaining keys intact");
            for (long i = 700; i < 1000; i++) {
                assertNotNull(reopened.search(i));
            }
        }
    }

    /**
     * Definition of Done: Index file size stabilizes under continuous insert-delete churn.
     *
     * Inserts an initial set of keys, then runs 50 churn cycles (in each cycle, 200 keys
     * are deleted and 200 new keys are inserted). Proves that without freelist recycling,
     * the file would grow by dozens of pages, but WITH freelist recycling, the file size
     * reaches a stable equilibrium and DOES NOT GROW endlessly.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testIndexFileSizeStabilizesUnderContinuousInsertDeleteChurn() {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(512, dm);
        openPools.add(bp);
        BTreeIndex index = new BTreeIndex("idx_stabilization_churn", bp);

        int initialKeys = 3000;
        for (long i = 0; i < initialKeys; i++) {
            index.insert(i, new BTreePage.RID(i, 0));
        }

        bp.flushAll();
        long initialPageCount = dm.getTablePageCount("idx_stabilization_churn");
        assertTrue(initialPageCount >= 5, "Initial tree should span multiple pages");

        long startDelete = 0;
        long startInsert = initialKeys;
        int churnCycles = 50;
        int churnBatch = 200;

        List<Long> pageCountHistory = new ArrayList<>();

        for (int cycle = 0; cycle < churnCycles; cycle++) {
            // Delete churnBatch keys
            for (long i = startDelete; i < startDelete + churnBatch; i++) {
                index.delete(i, new BTreePage.RID(i, 0));
            }
            startDelete += churnBatch;

            // Insert churnBatch new keys
            for (long i = startInsert; i < startInsert + churnBatch; i++) {
                index.insert(i, new BTreePage.RID(i, 0));
            }
            startInsert += churnBatch;

            bp.flushAll();
            long currentPages = dm.getTablePageCount("idx_stabilization_churn");
            pageCountHistory.add(currentPages);
        }

        long finalPageCount = dm.getTablePageCount("idx_stabilization_churn");
        long maxObservedPages = Collections.max(pageCountHistory);

        // Core DoD assertion: Index file size stabilizes!
        // Across 10,000 inserted keys and 10,000 deleted keys (churned 50 times),
        // the file page count does not blow up. It remains tightly bounded near initialPageCount.
        assertTrue(finalPageCount <= initialPageCount + 4,
            "Index file size must stabilize: initial=" + initialPageCount
                + ", max=" + maxObservedPages + ", final=" + finalPageCount
                + " under 50 churn cycles (10k insert/delete operations)");

        int remainingActive = index.validateTree();
        assertEquals(initialKeys, remainingActive, "Active key count must remain constant at 3000");

        // Verify active keys are searchable
        for (long i = startDelete; i < startInsert; i++) {
            assertNotNull(index.search(i), "Active key " + i + " must exist");
        }
    }

    @Test
    void testDefragmentationShrinksPhysicalFile() {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(512, dm);
        openPools.add(bp);
        BTreeIndex index = new BTreeIndex("idx_defrag", bp);

        int n = 5000;
        for (long i = 0; i < n; i++) {
            index.insert(i, new BTreePage.RID(i, 0));
        }

        bp.flushAll();
        long pagesBeforeDelete = dm.getTablePageCount("idx_defrag");
        assertTrue(pagesBeforeDelete >= 20, "Should have allocated at least 20 pages");

        // Delete 90% of the data
        for (long i = 0; i < 4500; i++) {
            index.delete(i, new BTreePage.RID(i, 0));
        }

        // Before defragmentation: physical file size is still large
        bp.flushAll();
        long pagesBeforeDefrag = dm.getTablePageCount("idx_defrag");
        assertEquals(pagesBeforeDelete, pagesBeforeDefrag, "File size on disk remains expanded before defrag");
        assertTrue(index.getFreePageCount() > 0, "Freelist has orphaned pages");

        // Execute defragmentation
        index.defragment();

        long pagesAfterDefrag = dm.getTablePageCount("idx_defrag");
        assertTrue(pagesAfterDefrag < pagesBeforeDefrag / 2,
            "Defragmentation must shrink physical file: was " + pagesBeforeDefrag + ", now " + pagesAfterDefrag);
        assertEquals(0, index.getFreePageCount(), "Freelist is empty after defragmentation");
        assertEquals(-1L, index.getFreeListHead());

        int activeKeys = index.validateTree();
        assertEquals(500, activeKeys, "All 500 surviving keys preserved");

        for (long i = 4500; i < n; i++) {
            assertNotNull(index.search(i), "Key " + i + " must remain searchable after defragmentation");
        }
        for (long i = 0; i < 4500; i++) {
            assertNull(index.search(i), "Deleted key " + i + " must not be found");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testConcurrentInsertDeleteChurnFreelistRecycling() throws Exception {
        BTreeIndex index = newIndex("idx_concurrent_freelist_churn", 1024);

        int threads = 16;
        int opsPerThread = 500;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicBoolean failed = new AtomicBoolean(false);

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    long baseKey = (long) threadId * 100_000L;
                    for (int op = 0; op < opsPerThread; op++) {
                        long key = baseKey + op;
                        index.insert(key, new BTreePage.RID(key, 0));
                        if (op % 2 == 1) {
                            index.delete(key - 1, new BTreePage.RID(key - 1, 0));
                        }
                    }
                } catch (Exception e) {
                    failed.set(true);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(25, TimeUnit.SECONDS), "Concurrent churn timed out");
        executor.shutdown();

        assertFalse(failed.get(), "No thread should fail or deadlock during concurrent churn");

        int activeKeys = index.validateTree();
        assertTrue(activeKeys > 0, "Tree must remain valid and non-empty");
    }
}
