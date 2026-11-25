package com.stratosdb.transaction.locking;

import com.stratosdb.common.exceptions.DeadlockException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class TableIntentAndRowShareLockTest {

    private LockManager lockManager;

    @BeforeEach
    void setUp() {
        lockManager = new LockManager(64);
    }

    @Test
    @DisplayName("LockMode compatibility matrix matches standard Gray / Reuter multi-granularity rules")
    void testLockModeCompatibilityMatrix() {
        // IS compatibility
        assertTrue(LockMode.IS.isCompatible(LockMode.IS));
        assertTrue(LockMode.IS.isCompatible(LockMode.IX));
        assertTrue(LockMode.IS.isCompatible(LockMode.S));
        assertTrue(LockMode.IS.isCompatible(LockMode.SIX));
        assertFalse(LockMode.IS.isCompatible(LockMode.X));

        // IX compatibility
        assertTrue(LockMode.IX.isCompatible(LockMode.IS));
        assertTrue(LockMode.IX.isCompatible(LockMode.IX));
        assertFalse(LockMode.IX.isCompatible(LockMode.S));
        assertFalse(LockMode.IX.isCompatible(LockMode.SIX));
        assertFalse(LockMode.IX.isCompatible(LockMode.X));

        // S compatibility
        assertTrue(LockMode.S.isCompatible(LockMode.IS));
        assertFalse(LockMode.S.isCompatible(LockMode.IX));
        assertTrue(LockMode.S.isCompatible(LockMode.S));
        assertFalse(LockMode.S.isCompatible(LockMode.SIX));
        assertFalse(LockMode.S.isCompatible(LockMode.X));

        // SIX compatibility
        assertTrue(LockMode.SIX.isCompatible(LockMode.IS));
        assertFalse(LockMode.SIX.isCompatible(LockMode.IX));
        assertFalse(LockMode.SIX.isCompatible(LockMode.S));
        assertFalse(LockMode.SIX.isCompatible(LockMode.SIX));
        assertFalse(LockMode.SIX.isCompatible(LockMode.X));

        // X compatibility
        assertFalse(LockMode.X.isCompatible(LockMode.IS));
        assertFalse(LockMode.X.isCompatible(LockMode.IX));
        assertFalse(LockMode.X.isCompatible(LockMode.S));
        assertFalse(LockMode.X.isCompatible(LockMode.SIX));
        assertFalse(LockMode.X.isCompatible(LockMode.X));
    }

    @Test
    @DisplayName("Multiple transactions can hold table IS and IX locks concurrently")
    void testConcurrentTableIntentLocks() throws Exception {
        assertTrue(lockManager.acquireTableLock("accounts", LockMode.IS, 1L));
        assertTrue(lockManager.acquireTableLock("accounts", LockMode.IS, 2L));
        assertTrue(lockManager.acquireTableLock("accounts", LockMode.IX, 3L));
        assertTrue(lockManager.acquireTableLock("accounts", LockMode.IX, 4L));

        assertTrue(lockManager.isTableLocked("accounts"));
        assertEquals(LockMode.IS, lockManager.getTableLockMode("accounts", 1L));
        assertEquals(LockMode.IX, lockManager.getTableLockMode("accounts", 3L));
        assertEquals(4, lockManager.getTableHolders("accounts").size());

        lockManager.releaseAll(1L);
        lockManager.releaseAll(2L);
        lockManager.releaseAll(3L);
        lockManager.releaseAll(4L);

        assertFalse(lockManager.isTableLocked("accounts"));
        assertTrue(lockManager.getTableHolders("accounts").isEmpty());
    }

    @Test
    @DisplayName("Table Exclusive lock (DDL) is blocked by active Intent Shared reader")
    void testTableExclusiveBlockedByIntentShared() throws Exception {
        long readerXid = 100L;
        long ddlXid = 200L;

        assertTrue(lockManager.acquireTableLock("users", LockMode.IS, readerXid));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch ddlStarted = new CountDownLatch(1);
        AtomicBoolean ddlCompleted = new AtomicBoolean(false);

        Future<?> ddlFuture = executor.submit(() -> {
            try {
                ddlStarted.countDown();
                lockManager.acquireTableLock("users", LockMode.X, ddlXid);
                ddlCompleted.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        ddlStarted.await(1, TimeUnit.SECONDS);
        // Wait for DDL thread to queue up waiting on the table
        while (lockManager.getTableWaiterCount("users") == 0) {
            Thread.yield();
        }

        assertFalse(ddlCompleted.get(), "DDL (X lock) must remain blocked while reader holds IS lock");

        // Reader commits
        lockManager.releaseAll(readerXid);

        ddlFuture.get(2, TimeUnit.SECONDS);
        assertTrue(ddlCompleted.get(), "DDL must unblock and acquire X lock upon reader release");
        assertEquals(LockMode.X, lockManager.getTableLockMode("users", ddlXid));

        lockManager.releaseAll(ddlXid);
        assertFalse(lockManager.isTableLocked("users"));
        executor.shutdown();
    }

    @Test
    @DisplayName("Multiple concurrent row Shared (S) locks do not block each other")
    void testConcurrentRowSharedLocks() throws Exception {
        LockManager.RowId row = new LockManager.RowId("accounts", 1L, 0);

        lockManager.acquireShared(row, 1L);
        lockManager.acquireShared(row, 2L);
        lockManager.acquireShared(row, 3L);

        assertTrue(lockManager.isLocked(row));
        Set<Long> holders = lockManager.getHolders(row);
        assertEquals(3, holders.size());
        assertTrue(holders.containsAll(List.of(1L, 2L, 3L)));

        lockManager.release(row, 1L);
        assertTrue(lockManager.isLocked(row));
        assertEquals(2, lockManager.getHolders(row).size());

        lockManager.release(row, 2L);
        lockManager.release(row, 3L);
        assertFalse(lockManager.isLocked(row));
        assertEquals(0, lockManager.getActiveLockCount());
    }

    @Test
    @DisplayName("Row Exclusive (X) lock blocks incoming Shared (S) lock until release")
    void testRowExclusiveBlocksRowShared() throws Exception {
        LockManager.RowId row = new LockManager.RowId("orders", 5L, 2);
        long writerXid = 11L;
        long readerXid = 22L;

        lockManager.acquireExclusive(row, writerXid);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch readerStarted = new CountDownLatch(1);
        AtomicBoolean readerAcquired = new AtomicBoolean(false);

        Future<?> readerFuture = executor.submit(() -> {
            try {
                readerStarted.countDown();
                lockManager.acquireShared(row, readerXid);
                readerAcquired.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        readerStarted.await(1, TimeUnit.SECONDS);
        while (lockManager.getWaiterCount(row) == 0) {
            Thread.yield();
        }

        assertFalse(readerAcquired.get(), "Shared lock must block on held exclusive lock");

        lockManager.releaseAll(writerXid);
        readerFuture.get(2, TimeUnit.SECONDS);

        assertTrue(readerAcquired.get(), "Shared lock must be granted once exclusive lock is released");
        assertTrue(lockManager.getHolders(row).contains(readerXid));

        lockManager.releaseAll(readerXid);
        executor.shutdown();
    }

    @Test
    @DisplayName("Row Shared (S) locks block Exclusive (X) lock until all shared holders release")
    void testRowSharedBlocksRowExclusiveUntilAllRelease() throws Exception {
        LockManager.RowId row = new LockManager.RowId("orders", 10L, 1);
        long reader1 = 101L;
        long reader2 = 102L;
        long writer = 201L;

        lockManager.acquireShared(row, reader1);
        lockManager.acquireShared(row, reader2);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch writerStarted = new CountDownLatch(1);
        AtomicBoolean writerAcquired = new AtomicBoolean(false);

        Future<?> writerFuture = executor.submit(() -> {
            try {
                writerStarted.countDown();
                lockManager.acquireExclusive(row, writer);
                writerAcquired.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        writerStarted.await(1, TimeUnit.SECONDS);
        while (lockManager.getWaiterCount(row) == 0) {
            Thread.yield();
        }

        assertFalse(writerAcquired.get(), "Writer must be blocked while shared holders are active");

        // Release first reader: writer must still be blocked because reader2 still holds S!
        lockManager.release(row, reader1);
        Thread.sleep(50);
        assertFalse(writerAcquired.get(), "Writer must remain blocked while reader2 still holds S lock");

        // Release second reader: writer unblocks immediately!
        lockManager.release(row, reader2);
        writerFuture.get(2, TimeUnit.SECONDS);

        assertTrue(writerAcquired.get(), "Writer must unblock once all shared holders release");
        assertEquals(Set.of(writer), lockManager.getHolders(row));

        lockManager.releaseAll(writer);
        executor.shutdown();
    }

    @Test
    @DisplayName("Sole holder of Shared (S) lock can upgrade to Exclusive (X) lock immediately")
    void testRowShareToExclusiveUpgrade() throws Exception {
        LockManager.RowId row = new LockManager.RowId("products", 2L, 4);
        long xid = 301L;

        lockManager.acquireShared(row, xid);
        // Upgrade to exclusive
        lockManager.acquireExclusive(row, xid);

        // Verify other transactions cannot acquire shared lock
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean otherAcquired = new AtomicBoolean(false);
        executor.submit(() -> {
            try {
                boolean acquired = lockManager.acquireShared(row, 302L, 50, TimeUnit.MILLISECONDS);
                if (acquired) {
                    otherAcquired.set(true);
                }
            } catch (Exception ignored) {}
        });

        Thread.sleep(100);
        assertFalse(otherAcquired.get(), "Upgraded exclusive lock must block other shared acquisitions");

        lockManager.releaseAll(xid);
        executor.shutdown();
    }

    @Test
    @DisplayName("Cross-table intent lock cycle triggers DeadlockException")
    void testCrossTableDeadlockDetection() throws Exception {
        long xid1 = 401L;
        long xid2 = 402L;

        // Txn 1 holds table A (IS)
        lockManager.acquireTableLock("table_a", LockMode.IS, xid1);
        // Txn 2 holds table B (IS)
        lockManager.acquireTableLock("table_b", LockMode.IS, xid2);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch t1Waiting = new CountDownLatch(1);

        executor.submit(() -> {
            try {
                t1Waiting.countDown();
                // Txn 1 requests table B (X) -> conflicts with Txn 2's IS on table B
                lockManager.acquireTableLock("table_b", LockMode.X, xid1);
            } catch (Exception ignored) {}
        });

        t1Waiting.await(1, TimeUnit.SECONDS);
        while (lockManager.getTableWaiterCount("table_b") == 0) {
            Thread.yield();
        }

        // Txn 2 requests table A (X) -> conflicts with Txn 1's IS on table A, creating a cycle:
        // xid2 -> xid1 -> xid2
        assertThrows(DeadlockException.class, () -> {
            lockManager.acquireTableLock("table_a", LockMode.X, xid2);
        });

        lockManager.releaseAll(xid1);
        lockManager.releaseAll(xid2);
        executor.shutdown();
    }
}
