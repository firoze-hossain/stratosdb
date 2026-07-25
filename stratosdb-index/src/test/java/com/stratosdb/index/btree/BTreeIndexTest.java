package com.stratosdb.index.btree;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.BTreePage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class BTreeIndexTest {

    @TempDir
    Path tempDir;

    private BTreeIndex newIndex(String name, int poolSize) {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(poolSize, dm);
        return new BTreeIndex(name, bp);
    }

    @Test
    void insertAndSearch_smallSet() {
        BTreeIndex index = newIndex("idx_small", 64);
        Map<Long, BTreePage.RID> groundTruth = new HashMap<>();

        long[] keys = {50, 10, 90, 30, 70, 20, 80, 40, 60, 5, 95};
        for (long k : keys) {
            BTreePage.RID rid = new BTreePage.RID(k * 10, (int) (k % 7));
            index.insert(k, rid);
            groundTruth.put(k, rid);
        }

        for (var entry : groundTruth.entrySet()) {
            assertEquals(entry.getValue(), index.search(entry.getKey()),
                "wrong RID for key " + entry.getKey());
        }
        assertNull(index.search(999), "a key that was never inserted must not be found");
    }

    @Test
    void duplicateKeys_allReturnedByRangeAndSearchAll() {
        BTreeIndex index = newIndex("idx_dup", 64);

        index.insert(42, new BTreePage.RID(1, 0));
        index.insert(42, new BTreePage.RID(2, 0));
        index.insert(42, new BTreePage.RID(3, 0));
        index.insert(10, new BTreePage.RID(4, 0));
        index.insert(100, new BTreePage.RID(5, 0));

        List<BTreePage.RID> matches = index.searchAll(42);
        assertEquals(3, matches.size());
        assertTrue(matches.contains(new BTreePage.RID(1, 0)));
        assertTrue(matches.contains(new BTreePage.RID(2, 0)));
        assertTrue(matches.contains(new BTreePage.RID(3, 0)));
    }

    @Test
    void rangeScanReturnsAscendingOrderWithinBounds() {
        BTreeIndex index = newIndex("idx_range", 64);
        for (long k = 0; k < 100; k++) {
            index.insert(k, new BTreePage.RID(k, 0));
        }

        List<BTreePage.RID> result = index.rangeScan(20, 29);
        assertEquals(10, result.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(20 + i, result.get(i).pageId());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void survivesRealEvictionAndReloadUnderATightBufferPool() {
        // Small enough that this genuinely forces pages in and out of the
        // pool repeatedly across ~20,000/300 =~ 65+ leaf pages, without the
        // multi-minute cost a tight pool has at 250k-key scale.
        int n = 20_000;
        BTreeIndex index = newIndex("idx_evict", 20);

        for (long k = 0; k < n; k++) {
            index.insert(k, new BTreePage.RID(k, 0));
        }

        List<BTreePage.RID> all = index.rangeScan(0, n - 1);
        assertEquals(n, all.size());
        for (long k = 0; k < n; k++) {
            assertEquals(k, all.get((int) k).pageId());
        }
    }

    /**
     * The real test: enough keys to force leaf splits AND at least one
     * internal-node split (MAX_LEAF_KEYS=408, MAX_INTERNAL_KEYS=510, so this
     * needs on the order of 510+ leaves - roughly 150k+ keys - to guarantee
     * the root's own internal node overflows and splits, promoting a new
     * root). The buffer pool is sized to comfortably hold the resulting
     * ~800-page tree - a too-small pool here isn't testing the B+Tree, it's
     * testing eviction-thrashing performance, which is a separate concern
     * from correctness and belongs in a benchmark, not this test.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void manyInsertions_forcesMultiLevelSplitsAndStaysFullyCorrect() {
        int n = 250_000;
        BTreeIndex index = newIndex("idx_big", 2000);

        // Insert in shuffled order - sequential-only insertion is the easy
        // case for a B+Tree (always splitting the rightmost leaf); shuffled
        // keys exercise splitting leaves and internal nodes in the middle
        // of the tree too.
        List<Long> insertOrder = new ArrayList<>();
        for (long k = 0; k < n; k++) insertOrder.add(k);
        Collections.shuffle(insertOrder, new java.util.Random(42));

        for (long k : insertOrder) {
            index.insert(k, new BTreePage.RID(k, (int) (k % 50)));
        }

        // Comprehensive check: a full range scan must return every key,
        // in ascending order, with the correct RID - not just a sample.
        List<BTreePage.RID> all = index.rangeScan(0, n - 1);
        assertEquals(n, all.size(), "range scan must return every inserted key exactly once");
        for (long k = 0; k < n; k++) {
            BTreePage.RID rid = all.get((int) k);
            assertEquals(k, rid.pageId(), "range scan out of order or missing a key at position " + k);
            assertEquals((int) (k % 50), rid.slot());
        }

        // Spot-check point lookups across the key space too (exercises the
        // search()-specific binary search path, not just rangeScan's).
        java.util.Random sampler = new java.util.Random(7);
        for (int i = 0; i < 500; i++) {
            long k = (long) (sampler.nextDouble() * n);
            BTreePage.RID rid = index.search(k);
            assertNotNull(rid, "key " + k + " should be found");
            assertEquals(k, rid.pageId());
        }

        assertNull(index.search(-1));
        assertNull(index.search(n + 1000));
    }

    @Test
    void indexPersistsAcrossReopen() {
        String name = "idx_persist";
        DiskManager dm1 = new DiskManager(tempDir.toString());
        BufferPoolManager bp1 = new BufferPoolManager(64, dm1);
        BTreeIndex index1 = new BTreeIndex(name, bp1);

        for (long k = 0; k < 2000; k++) {
            index1.insert(k, new BTreePage.RID(k, 0));
        }
        bp1.close(); // flushes everything and closes the disk manager

        // Fresh managers, same directory - this is "restart."
        DiskManager dm2 = new DiskManager(tempDir.toString());
        BufferPoolManager bp2 = new BufferPoolManager(64, dm2);
        BTreeIndex index2 = new BTreeIndex(name, bp2);

        for (long k = 0; k < 2000; k += 137) {
            assertEquals(new BTreePage.RID(k, 0), index2.search(k),
                "key " + k + " must survive a close/reopen of the index");
        }
        assertEquals(2000, index2.rangeScan(0, 1999).size());
    }
}
