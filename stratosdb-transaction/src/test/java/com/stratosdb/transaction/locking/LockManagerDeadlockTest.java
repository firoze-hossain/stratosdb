package com.stratosdb.transaction.locking;

import com.stratosdb.common.exceptions.DeadlockException;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A genuine circular-wait deadlock, using two real threads and the real
 * LockManager - not a mocked wait-for graph.
 *
 * txn1 locks rowA, then tries to lock rowB (held by txn2).
 * txn2 locks rowB, then tries to lock rowA (held by txn1).
 * Exactly one side must be told "deadlock" so the other can proceed and the
 * system as a whole makes progress, rather than both threads hanging forever.
 */
class LockManagerDeadlockTest {

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void circularWaitIsDetectedAndExactlyOneSideIsAborted() throws Exception {
        LockManager lockManager = new LockManager();
        LockManager.RowId rowA = new LockManager.RowId("t", 0, 0);
        LockManager.RowId rowB = new LockManager.RowId("t", 0, 1);

        long xid1 = 1L, xid2 = 2L;

        CountDownLatch bothHoldFirstLock = new CountDownLatch(2);
        AtomicInteger deadlockCount = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicReference<Exception> unexpected = new AtomicReference<>();

        Thread t1 = new Thread(() -> {
            try {
                lockManager.acquireExclusive(rowA, xid1);
                bothHoldFirstLock.countDown();
                bothHoldFirstLock.await();
                lockManager.acquireExclusive(rowB, xid1); // will contend with t2
                successCount.incrementAndGet();
            } catch (DeadlockException e) {
                deadlockCount.incrementAndGet();
            } catch (Exception e) {
                unexpected.set(e);
            } finally {
                lockManager.releaseAll(xid1);
            }
        }, "txn1");

        Thread t2 = new Thread(() -> {
            try {
                lockManager.acquireExclusive(rowB, xid2);
                bothHoldFirstLock.countDown();
                bothHoldFirstLock.await();
                lockManager.acquireExclusive(rowA, xid2); // will contend with t1
                successCount.incrementAndGet();
            } catch (DeadlockException e) {
                deadlockCount.incrementAndGet();
            } catch (Exception e) {
                unexpected.set(e);
            } finally {
                lockManager.releaseAll(xid2);
            }
        }, "txn2");

        t1.start();
        t2.start();
        t1.join(10_000);
        t2.join(10_000);

        assertNull(unexpected.get(), "no unexpected exception should occur: " + unexpected.get());
        assertFalse(t1.isAlive(), "txn1's thread must not still be blocked");
        assertFalse(t2.isAlive(), "txn2's thread must not still be blocked");
        assertEquals(1, deadlockCount.get(),
            "exactly one side must detect the deadlock and abort");
        assertEquals(1, successCount.get(),
            "the other side must be free to proceed once the deadlocked side backs off");
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void noFalsePositive_sequentialAcquisitionOfDifferentRowsNeverDeadlocks() throws Exception {
        LockManager lockManager = new LockManager();
        LockManager.RowId rowA = new LockManager.RowId("t", 0, 0);
        LockManager.RowId rowB = new LockManager.RowId("t", 0, 1);

        // No contention at all: txn1 takes both rows, releases, then txn2 takes both.
        lockManager.acquireExclusive(rowA, 1L);
        lockManager.acquireExclusive(rowB, 1L);
        lockManager.releaseAll(1L);

        lockManager.acquireExclusive(rowA, 2L);
        lockManager.acquireExclusive(rowB, 2L);
        lockManager.releaseAll(2L);
        // Reaching here without an exception or hang is the assertion.
    }
}
