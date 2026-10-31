package com.stratosdb.index.btree;

import com.stratosdb.common.exceptions.UniqueConstraintViolationException;
import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.BTreePage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sprint 7 Week 13: B+Tree Multi-Page Latch Crabbing (Coupling) Test Suite.
 *
 * Verifies:
 * 1. Read Latch Crabbing: Acquire read latch on child before releasing parent.
 * 2. Range Scan Leaf Latch Crabbing: Acquire read latch on nextLeafPageId before releasing current leaf.
 * 3. Write Latch Crabbing on Split/Merge: Hand-over-hand write latches down to leaf on split,
 *    releasing ancestors as soon as safe node is encountered.
 * 4. Zero Deadlocks: Strict top-down and left-to-right acquisition order under heavy concurrency.
 * 5. Stress Harness: 64 concurrent threads running concurrent inserts, point lookups, and range scans.
 */
class BTreeLatchCrabbingTest {

    @TempDir
    Path tempDir;

    private final List<BufferPoolManager> openPools = new ArrayList<>();

    private BTreeIndex newIndex(String name, int poolSize) {
        return newIndex(name, poolSize, false);
    }

    private BTreeIndex newIndex(String name, int poolSize, boolean unique) {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(poolSize, dm);
        openPools.add(bp);
        return new BTreeIndex(name, bp, unique);
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
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testConcurrentInserts64Threads() throws Exception {
        int threadCount = 64;
        int itemsPerThread = 500; // 32,000 total insertions
        BTreeIndex index = newIndex("idx_concurrent_64", 256);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Future<?>> futures = new ArrayList<>();
        ConcurrentHashMap<Long, BTreePage.RID> groundTruth = new ConcurrentHashMap<>();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < itemsPerThread; i++) {
                        long key = (long) i * threadCount + threadId;
                        BTreePage.RID rid = new BTreePage.RID(key, threadId);
                        groundTruth.put(key, rid);
                        index.insert(key, rid);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get(45, TimeUnit.SECONDS);
        }
        executor.shutdown();

        // Validate tree structural integrity
        int totalKeys = index.validateTree();
        assertEquals(threadCount * itemsPerThread, totalKeys, "Tree key count must exactly match total inserted keys");

        // Verify point lookup for every key
        for (Map.Entry<Long, BTreePage.RID> entry : groundTruth.entrySet()) {
            assertEquals(entry.getValue(), index.search(entry.getKey()), "Key " + entry.getKey() + " returned wrong RID");
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testConcurrentInsertsAndRangeScans64Threads() throws Exception {
        int threadCount = 64;
        int writers = 32;
        int readers = 32;
        int itemsPerWriter = 400; // 12,800 total keys
        BTreeIndex index = newIndex("idx_insert_scan_64", 256);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong totalScansCompleted = new AtomicLong(0);
        AtomicLong scanKeyCountTotal = new AtomicLong(0);
        List<Future<?>> writerFutures = new ArrayList<>();
        List<Future<?>> readerFutures = new ArrayList<>();

        // Start 32 writers
        for (int w = 0; w < writers; w++) {
            final int writerId = w;
            writerFutures.add(executor.submit(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < itemsPerWriter; i++) {
                        long key = (long) i * writers + writerId;
                        index.insert(key, new BTreePage.RID(key, writerId));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // Start 32 readers concurrently running range scans and point searches
        for (int r = 0; r < readers; r++) {
            readerFutures.add(executor.submit(() -> {
                try {
                    barrier.await();
                    ThreadLocalRandom rand = ThreadLocalRandom.current();
                    while (running.get()) {
                        long from = rand.nextLong(0, writers * itemsPerWriter);
                        long to = from + rand.nextLong(10, 200);
                        List<BTreePage.RID> results = index.rangeScan(from, to);

                        // Verify results are strictly sorted ascending
                        long lastKey = -1;
                        for (BTreePage.RID rid : results) {
                            assertTrue(rid.pageId() >= from && rid.pageId() <= to,
                                "Scanned key " + rid.pageId() + " outside bound [" + from + ", " + to + "]");
                            assertTrue(rid.pageId() >= lastKey, "Range scan keys not in ascending order");
                            lastKey = rid.pageId();
                        }
                        totalScansCompleted.incrementAndGet();
                        scanKeyCountTotal.addAndGet(results.size());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // Wait for all writers to complete
        for (Future<?> f : writerFutures) {
            f.get(45, TimeUnit.SECONDS);
        }

        // Signal readers to stop
        running.set(false);
        for (Future<?> f : readerFutures) {
            f.get(15, TimeUnit.SECONDS);
        }
        executor.shutdown();

        assertTrue(totalScansCompleted.get() > 50, "At least 50 range scans should have completed during concurrent inserts");

        // Validate tree integrity
        int totalKeys = index.validateTree();
        assertEquals(writers * itemsPerWriter, totalKeys, "Tree key count must match total inserted keys");
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testConcurrentInsertsAndDeletes64Threads() throws Exception {
        int initialKeys = 10_000;
        BTreeIndex index = newIndex("idx_insert_del_64", 256);

        // Prepopulate tree
        for (long k = 0; k < initialKeys; k++) {
            index.insert(k, new BTreePage.RID(k, 0));
        }

        int threadCount = 64;
        int writers = 32;
        int deleters = 32;
        int opsPerThread = 200;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        // 32 writers insert keys >= initialKeys
        for (int w = 0; w < writers; w++) {
            final int writerId = w;
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        long key = initialKeys + (long) i * writers + writerId;
                        index.insert(key, new BTreePage.RID(key, writerId));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // 32 deleters delete keys < initialKeys
        for (int d = 0; d < deleters; d++) {
            final int deleterId = d;
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        long key = (long) i * deleters + deleterId;
                        index.delete(key, new BTreePage.RID(key, 0));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get(45, TimeUnit.SECONDS);
        }
        executor.shutdown();

        int expectedRemaining = initialKeys - (deleters * opsPerThread) + (writers * opsPerThread);
        int validatedKeys = index.validateTree();
        assertEquals(expectedRemaining, validatedKeys, "Tree key count after concurrent insert/delete mismatch");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testConcurrentUniqueKeyRace64Threads() throws Exception {
        int threadCount = 64;
        BTreeIndex index = newIndex("idx_uq_race_64", 128, true);
        assertTrue(index.isUnique());

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger violations = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        long contestedKey = 999_999L;

        for (int i = 0; i < threadCount; i++) {
            final int slot = i;
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                    index.insert(contestedKey, new BTreePage.RID(10, slot));
                    successes.incrementAndGet();
                } catch (UniqueConstraintViolationException e) {
                    violations.incrementAndGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get(20, TimeUnit.SECONDS);
        }
        executor.shutdown();

        assertEquals(1, successes.get(), "Exactly 1 concurrent thread must succeed in inserting the unique key");
        assertEquals(threadCount - 1, violations.get(), "All other 63 concurrent threads must receive UniqueConstraintViolationException");
        assertEquals(1, index.searchAll(contestedKey).size());
        assertEquals(1, index.validateTree());
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testStressHarness64ThreadsSustained() throws Exception {
        // Reads duration from system property, default 10 seconds for standard test suite
        int durationSeconds = Integer.getInteger("stratosdb.stress.duration.seconds", 10);
        int threadCount = 64;
        BTreeIndex index = newIndex("idx_stress_64", 2048);

        // Prepopulate with 5,000 keys
        int initial = 5_000;
        for (long k = 0; k < initial; k++) {
            index.insert(k, new BTreePage.RID(k, 0));
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong totalOperations = new AtomicLong();
        AtomicLong insertCount = new AtomicLong();
        AtomicLong searchCount = new AtomicLong();
        AtomicLong rangeScanCount = new AtomicLong();
        AtomicLong deleteCount = new AtomicLong();

        List<Future<?>> futures = new ArrayList<>();
        long endTime = System.currentTimeMillis() + (durationSeconds * 1000L);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                    ThreadLocalRandom rand = ThreadLocalRandom.current();
                    while (System.currentTimeMillis() < endTime && running.get()) {
                        int op = rand.nextInt(100);
                        if (op < 40) {
                            // 40% Insert
                            long key = rand.nextLong(10_000, 100_000);
                            index.insert(key, new BTreePage.RID(key, threadId));
                            insertCount.incrementAndGet();
                        } else if (op < 70) {
                            // 30% Point Search
                            long key = rand.nextLong(0, 100_000);
                            index.search(key);
                            searchCount.incrementAndGet();
                        } else if (op < 90) {
                            // 20% Range Scan
                            long from = rand.nextLong(0, 50_000);
                            long to = from + rand.nextLong(5, 50);
                            index.rangeScan(from, to);
                            rangeScanCount.incrementAndGet();
                        } else {
                            // 10% Delete
                            long key = rand.nextLong(10_000, 100_000);
                            index.delete(key, new BTreePage.RID(key, threadId));
                            deleteCount.incrementAndGet();
                        }
                        totalOperations.incrementAndGet();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get(durationSeconds + 45, TimeUnit.SECONDS);
        }
        executor.shutdown();

        System.out.println("Stress harness sustained run (" + durationSeconds + "s): totalOps=" + totalOperations.get()
            + " (inserts=" + insertCount.get() + ", searches=" + searchCount.get()
            + ", scans=" + rangeScanCount.get() + ", deletes=" + deleteCount.get() + ")");

        assertTrue(totalOperations.get() > 1_000, "Should complete substantial operations during stress test");

        // Validate tree integrity
        int validatedKeys = index.validateTree();
        assertTrue(validatedKeys > 0, "Tree must have keys and be structurally sound");
    }
}
