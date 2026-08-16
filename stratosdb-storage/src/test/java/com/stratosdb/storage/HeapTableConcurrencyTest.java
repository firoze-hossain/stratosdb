package com.stratosdb.storage;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A real, serious concurrency bug found while building sequence support
 * (see PROGRESS.md for the full story of how this was diagnosed): insert()
 * read and wrote its lastPageId field with zero synchronization, and two
 * threads could both call Page.insertTuple() on the exact same page object
 * simultaneously with no locking between them - a classic read-modify-write
 * race that could silently lose one thread's insert entirely, not corrupt
 * it visibly. Not specific to sequences at all - any concurrent inserts
 * into the same table were at risk; sequences (via nextval()'s added
 * latency) just made the race window far easier to hit in practice.
 *
 * Diagnosed the hard way: by ruling out the actual new feature (a raw,
 * direct concurrency test of the Sequence class alone, with zero database
 * involved, proved it was already correct) before finding the real cause
 * one layer down. Fixed with a per-table lock around the whole insert()
 * body. This test exercises the fix directly against the raw HeapTable API
 * - no SQL layer involved - so it reproduces the original bug's exact
 * mechanism without anything else able to mask or interfere with it.
 */
class HeapTableConcurrencyTest {

    @TempDir
    Path tempDir;

    private final List<BufferPoolManager> openPools = new ArrayList<>();

    private HeapTable newTable(String name) {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(64, dm);
        openPools.add(bp);
        return new HeapTable(name, bp);
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
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentInsertsNeverLoseARow() throws Exception {
        HeapTable table = newTable("t");
        int n = 100; // matched to the exact stress level that reliably reproduced the original bug
        CountDownLatch latch = new CountDownLatch(n);
        Set<Long> insertedPageSlotPairs = ConcurrentHashMap.newKeySet();
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < n; i++) {
            final int value = i;
            new Thread(() -> {
                try {
                    byte[] data = ("row-" + value).getBytes();
                    HeapTable.InsertResult result = table.insert(data);
                    // Encode (pageId, slot) into one long to detect any collision -
                    // two different threads' inserts landing on the exact same
                    // physical slot is exactly what the original race produced.
                    long key = (result.pageId << 32) | (result.slot & 0xFFFFFFFFL);
                    if (!insertedPageSlotPairs.add(key)) {
                        errors.incrementAndGet(); // a genuine physical-slot collision
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        assertTrue(latch.await(20, TimeUnit.SECONDS), "all concurrent inserts must complete within the timeout");
        assertEquals(0, errors.get(), "no insert should throw or collide with another's physical slot");

        List<byte[]> allRows = table.scan();
        assertEquals(n, allRows.size(), () -> "expected exactly " + n
            + " rows - fewer means at least one concurrent insert was silently lost, which is exactly the original bug");

        // Every value must actually be independently readable back, not just counted -
        // guards against a scenario where the row COUNT is right but the CONTENT was
        // corrupted by two overlapping writes to the same page bytes.
        Set<String> distinctValues = new java.util.HashSet<>();
        for (byte[] row : allRows) {
            distinctValues.add(new String(row));
        }
        assertEquals(n, distinctValues.size(), "every inserted row's actual content must be distinct and intact, not overwritten by a concurrent write to the same page");
    }
}
