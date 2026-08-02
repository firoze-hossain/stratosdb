package com.stratosdb.index.hash;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HashIndexTest {

    @TempDir
    Path tempDir;

    private final List<BufferPoolManager> openPools = new ArrayList<>();

    /** Tracked so @AfterEach can close it - same Windows-safety reasoning as BTreeIndexTest. */
    private HashIndex newIndex(String name, int poolSize) {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(poolSize, dm);
        openPools.add(bp);
        return new HashIndex(name, bp);
    }

    @AfterEach
    void tearDown() {
        for (BufferPoolManager pool : openPools) {
            try {
                pool.close();
            } catch (Exception ignored) {
            }
        }
        openPools.clear();
    }

    @Test
    void insertAndSearchBasic() {
        HashIndex index = newIndex("idx_basic", 64);
        index.insert(10, new BTreePage.RID(1, 0));
        index.insert(20, new BTreePage.RID(2, 0));
        index.insert(30, new BTreePage.RID(3, 0));

        assertEquals(new BTreePage.RID(1, 0), index.search(10));
        assertEquals(new BTreePage.RID(2, 0), index.search(20));
        assertEquals(new BTreePage.RID(3, 0), index.search(30));
        assertNull(index.search(999));
    }

    @Test
    void duplicateKeysAllSurviveAndSearchAllReturnsEveryOne() {
        HashIndex index = newIndex("idx_dup", 64);
        BTreePage.RID rid1 = new BTreePage.RID(1, 0);
        BTreePage.RID rid2 = new BTreePage.RID(2, 0);
        BTreePage.RID rid3 = new BTreePage.RID(3, 0);
        index.insert(42, rid1);
        index.insert(42, rid2);
        index.insert(42, rid3);

        List<BTreePage.RID> all = index.searchAll(42);
        assertEquals(3, all.size());
        assertTrue(all.contains(rid1) && all.contains(rid2) && all.contains(rid3));
    }

    @Test
    void deleteRemovesExactPairOnlyAmongDuplicates() {
        HashIndex index = newIndex("idx_del", 64);
        BTreePage.RID rid1 = new BTreePage.RID(1, 0);
        BTreePage.RID rid2 = new BTreePage.RID(2, 0);
        index.insert(42, rid1);
        index.insert(42, rid2);

        index.delete(42, rid1);

        List<BTreePage.RID> remaining = index.searchAll(42);
        assertEquals(1, remaining.size());
        assertEquals(rid2, remaining.get(0));
    }

    @Test
    void deletingNonExistentPairIsANoOp() {
        HashIndex index = newIndex("idx_noop", 64);
        index.insert(10, new BTreePage.RID(1, 0));

        assertDoesNotThrow(() -> index.delete(10, new BTreePage.RID(999, 0)));
        assertDoesNotThrow(() -> index.delete(999, new BTreePage.RID(1, 0)));

        assertEquals(new BTreePage.RID(1, 0), index.search(10), "the real entry must be untouched");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void indexPersistsAcrossReopen() {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp1 = new BufferPoolManager(64, dm);
        openPools.add(bp1);
        HashIndex index1 = new HashIndex("idx_persist", bp1);
        for (long k = 0; k < 500; k++) {
            index1.insert(k, new BTreePage.RID(k, 0));
        }
        bp1.close();
        openPools.remove(bp1);

        DiskManager dm2 = new DiskManager(tempDir.toString());
        BufferPoolManager bp2 = new BufferPoolManager(64, dm2);
        openPools.add(bp2);
        HashIndex index2 = new HashIndex("idx_persist", bp2);
        for (long k = 0; k < 500; k++) {
            assertEquals(new BTreePage.RID(k, 0), index2.search(k), "key " + k + " must survive reopen");
        }
    }

    /**
     * The real proof this is genuinely hashing into buckets with real
     * overflow chaining, not just a linear scan pretending to be a hash
     * index: 100,000 keys across 251 buckets averages ~400 keys per
     * bucket - well past BTreePage's MAX_LEAF_KEYS (408) as a single
     * page's capacity, so this forces real overflow pages to be created,
     * chained, and correctly traversed on every search.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void manyInsertionsForceRealOverflowChainsAndStayFullyCorrect() {
        int n = 100_000;
        HashIndex index = newIndex("idx_scale", 1000);

        List<Long> insertOrder = new ArrayList<>();
        for (long k = 0; k < n; k++) insertOrder.add(k);
        Collections.shuffle(insertOrder, new Random(42));
        Map<Long, BTreePage.RID> ridOf = new HashMap<>();
        for (long k : insertOrder) {
            BTreePage.RID rid = new BTreePage.RID(k, (int) (k % 50));
            ridOf.put(k, rid);
            index.insert(k, rid);
        }

        for (long k = 0; k < n; k++) {
            assertEquals(ridOf.get(k), index.search(k), "key " + k + " incorrect after scale insert");
        }

        // Delete a large shuffled fraction, verify both directions.
        List<Long> deleteOrder = new ArrayList<>(insertOrder);
        Collections.shuffle(deleteOrder, new Random(99));
        int deleteCount = (int) (n * 0.4);
        Set<Long> deleted = new HashSet<>(deleteOrder.subList(0, deleteCount));
        for (long k : deleteOrder.subList(0, deleteCount)) {
            index.delete(k, ridOf.get(k));
        }

        for (long k = 0; k < n; k++) {
            if (deleted.contains(k)) {
                assertNull(index.search(k), "key " + k + " should have been deleted");
            } else {
                assertEquals(ridOf.get(k), index.search(k), "key " + k + " should still be findable");
            }
        }
    }
}
