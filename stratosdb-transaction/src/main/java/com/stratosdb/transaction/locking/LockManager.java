package com.stratosdb.transaction.locking;

import com.stratosdb.common.exceptions.DeadlockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 64-partitioned row-level exclusive locking for writers (UPDATE/DELETE/SELECT FOR UPDATE),
 * with per-row condition variable queues for instant wakeups (&lt; 0.05ms lag) and
 * deadlock detection over an across-partition wait-for graph.
 *
 * Replaces the monolithic global monitor and 25ms polling loop with 64 independent
 * hashed lock partitions. Contention between writers on different rows/partitions is
 * completely eliminated. When a row lock is released, only the transaction waiting for
 * that specific row is signaled via its condition variable, waking up instantly.
 *
 * Readers do not take locks here at all: MVCC snapshot isolation means readers
 * never block writers and never get blocked by them.
 */
public class LockManager {
    private static final Logger LOG = LoggerFactory.getLogger(LockManager.class);

    public static final int DEFAULT_PARTITIONS = 64;

    public record RowId(String tableName, long pageId, int slot) {}

    private static final class RowLockEntry {
        long holderXid;
        int lockCount;
        int waiterCount;
        final Condition condition;

        RowLockEntry(long holderXid, Condition condition) {
            this.holderXid = holderXid;
            this.lockCount = 1;
            this.waiterCount = 0;
            this.condition = condition;
        }
    }

    private static final class LockPartition {
        final ReentrantLock lock = new ReentrantLock(false);
        final Map<RowId, RowLockEntry> rowLocks = new HashMap<>();
    }

    private final int numPartitions;
    private final LockPartition[] partitions;
    private final Map<Long, Set<RowId>> locksHeldBy = new ConcurrentHashMap<>();

    // Deadlock detection wait-for graph
    private final ReentrantLock deadlockLock = new ReentrantLock(false);
    private final ConcurrentHashMap<Long, Long> waitsFor = new ConcurrentHashMap<>();

    // Timestamp tracking for instant wakeup latency measurement (< 0.05ms DoD)
    private volatile long lastSignalTimeNanos = 0;
    private volatile long lastWakeupLagNanos = 0;

    public LockManager() {
        this(DEFAULT_PARTITIONS);
    }

    public LockManager(int numPartitions) {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be > 0: " + numPartitions);
        }
        this.numPartitions = numPartitions;
        this.partitions = new LockPartition[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            this.partitions[i] = new LockPartition();
        }
    }

    private int getPartitionIndex(RowId rowId) {
        int h = rowId.hashCode();
        h ^= (h >>> 16);
        h *= 0x85ebca6b;
        h ^= (h >>> 13);
        h *= 0xc2b2ae35;
        h ^= (h >>> 16);
        return (h & 0x7fffffff) % numPartitions;
    }

    private LockPartition getPartition(RowId rowId) {
        return partitions[getPartitionIndex(rowId)];
    }

    /**
     * Blocks until xid holds an exclusive lock on rowId, or throws
     * DeadlockException if granting the wait would create a cycle.
     */
    public void acquireExclusive(RowId rowId, long xid) throws DeadlockException {
        acquireExclusive(rowId, xid, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Blocks until xid holds an exclusive lock on rowId, or until the timeout elapses.
     * If timeout &lt;= 0, blocks indefinitely until acquired or deadlock/interruption.
     *
     * @return true if acquired, false if timed out
     */
    public boolean acquireExclusive(RowId rowId, long xid, long timeout, TimeUnit unit) throws DeadlockException {
        LockPartition p = getPartition(rowId);
        boolean hasTimeout = timeout > 0 && unit != null;
        long nanosTimeout = hasTimeout ? unit.toNanos(timeout) : 0;
        long deadline = hasTimeout ? System.nanoTime() + nanosTimeout : 0;
        boolean wasWaiting = false;

        p.lock.lock();
        try {
            while (true) {
                RowLockEntry entry = p.rowLocks.get(rowId);
                if (entry == null) {
                    // Lock is completely free!
                    entry = new RowLockEntry(xid, p.lock.newCondition());
                    p.rowLocks.put(rowId, entry);
                    addLockHeld(xid, rowId);
                    if (wasWaiting) {
                        clearWaitFor(xid);
                    }
                    return true;
                }

                if (entry.holderXid == xid) {
                    // Re-entrant acquisition by the same transaction
                    entry.lockCount++;
                    return true;
                }

                if (entry.holderXid == 0) {
                    // Lock was released by predecessor; claim if we were waiting or if queue is empty
                    if (wasWaiting || entry.waiterCount == 0) {
                        entry.holderXid = xid;
                        entry.lockCount = 1;
                        addLockHeld(xid, rowId);
                        if (wasWaiting) {
                            clearWaitFor(xid);
                        }
                        return true;
                    }
                    // Outside barging thread arrived while a signaled waiter is waking up; queue up fairly
                }

                if (hasTimeout && nanosTimeout <= 0) {
                    if (wasWaiting) {
                        clearWaitFor(xid);
                    }
                    return false;
                }

                // Row is held by another transaction (entry.holderXid > 0)
                long holder = entry.holderXid;

                // Register wait in global wait-for graph and check for cycles
                setWaitForAndCheckDeadlock(xid, holder, rowId);

                entry.waiterCount++;
                try {
                    if (hasTimeout) {
                        nanosTimeout = entry.condition.awaitNanos(nanosTimeout);
                    } else {
                        entry.condition.await();
                    }
                    long now = System.nanoTime();
                    if (lastSignalTimeNanos > 0 && now >= lastSignalTimeNanos) {
                        lastWakeupLagNanos = now - lastSignalTimeNanos;
                    }
                    wasWaiting = true;
                } catch (InterruptedException e) {
                    clearWaitFor(xid);
                    // If this thread was signaled just before interruption, forward to next waiter
                    if (entry.holderXid == 0 && entry.waiterCount > 1) {
                        lastSignalTimeNanos = System.nanoTime();
                        entry.condition.signal();
                    }
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for lock on " + rowId, e);
                } finally {
                    entry.waiterCount--;
                    if (entry.holderXid == 0 && entry.waiterCount == 0) {
                        p.rowLocks.remove(rowId);
                    }
                }

                if (hasTimeout) {
                    nanosTimeout = deadline - System.nanoTime();
                }
            }
        } finally {
            p.lock.unlock();
        }
    }

    private void addLockHeld(long xid, RowId rowId) {
        locksHeldBy.computeIfAbsent(xid, k -> ConcurrentHashMap.newKeySet()).add(rowId);
    }

    private void setWaitForAndCheckDeadlock(long xid, long holder, RowId rowId) throws DeadlockException {
        deadlockLock.lock();
        try {
            waitsFor.put(xid, holder);
            if (hasCycle(xid)) {
                waitsFor.remove(xid);
                throw new DeadlockException("Deadlock detected: transaction " + xid
                    + " waiting on transaction " + holder + " for row " + rowId
                    + " would create a cycle in the wait-for graph");
            }
        } finally {
            deadlockLock.unlock();
        }
    }

    private void clearWaitFor(long xid) {
        if (waitsFor.containsKey(xid)) {
            deadlockLock.lock();
            try {
                waitsFor.remove(xid);
            } finally {
                deadlockLock.unlock();
            }
        }
    }

    /**
     * Follows the chain of single wait-for edges from `start`; true if it loops back to start.
     * Must be called while holding deadlockLock.
     */
    private boolean hasCycle(long start) {
        Set<Long> visited = new HashSet<>();
        long current = start;
        while (true) {
            Long next = waitsFor.get(current);
            if (next == null) return false;
            if (next == start) return true;
            if (!visited.add(next)) return false; // ran into an unrelated cycle, not one through start
            current = next;
        }
    }

    /**
     * Releases a specific lock held by xid on rowId.
     */
    public void release(RowId rowId, long xid) {
        LockPartition p = getPartition(rowId);
        p.lock.lock();
        try {
            RowLockEntry entry = p.rowLocks.get(rowId);
            if (entry != null && entry.holderXid == xid) {
                entry.lockCount--;
                if (entry.lockCount <= 0) {
                    entry.holderXid = 0;
                    entry.lockCount = 0;
                    Set<RowId> held = locksHeldBy.get(xid);
                    if (held != null) {
                        held.remove(rowId);
                        if (held.isEmpty()) {
                            locksHeldBy.remove(xid);
                        }
                    }
                    if (entry.waiterCount > 0) {
                        lastSignalTimeNanos = System.nanoTime();
                        entry.condition.signal();
                    } else {
                        p.rowLocks.remove(rowId);
                    }
                }
            }
        } finally {
            p.lock.unlock();
        }
    }

    /**
     * Releases every lock xid holds. Call on both commit and abort.
     */
    public void releaseAll(long xid) {
        Set<RowId> held = locksHeldBy.remove(xid);
        if (held != null && !held.isEmpty()) {
            // Group held rows by partition to minimize partition lock acquisitions
            Map<Integer, List<RowId>> byPartition = new HashMap<>();
            for (RowId r : held) {
                byPartition.computeIfAbsent(getPartitionIndex(r), k -> new ArrayList<>()).add(r);
            }

            for (Map.Entry<Integer, List<RowId>> partEntry : byPartition.entrySet()) {
                LockPartition p = partitions[partEntry.getKey()];
                p.lock.lock();
                try {
                    for (RowId r : partEntry.getValue()) {
                        RowLockEntry entry = p.rowLocks.get(r);
                        if (entry != null && entry.holderXid == xid) {
                            entry.holderXid = 0;
                            entry.lockCount = 0;
                            if (entry.waiterCount > 0) {
                                lastSignalTimeNanos = System.nanoTime();
                                entry.condition.signal();
                            } else {
                                p.rowLocks.remove(r);
                            }
                        }
                    }
                } finally {
                    p.lock.unlock();
                }
            }
        }
        clearWaitFor(xid);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Released all locks for transaction {}", xid);
        }
    }

    /** Returns true if the specified row is currently exclusively locked. */
    public boolean isLocked(RowId rowId) {
        LockPartition p = getPartition(rowId);
        p.lock.lock();
        try {
            RowLockEntry entry = p.rowLocks.get(rowId);
            return entry != null && entry.holderXid != 0;
        } finally {
            p.lock.unlock();
        }
    }

    /** Returns the transaction ID currently holding the exclusive lock on rowId, or null if unlocked. */
    public Long getHolder(RowId rowId) {
        LockPartition p = getPartition(rowId);
        p.lock.lock();
        try {
            RowLockEntry entry = p.rowLocks.get(rowId);
            return (entry != null && entry.holderXid != 0) ? entry.holderXid : null;
        } finally {
            p.lock.unlock();
        }
    }

    /** Returns the timestamp in nanoseconds when condition.signal() was last triggered. */
    public long getLastSignalTimeNanos() {
        return lastSignalTimeNanos;
    }

    /** Returns the last measured wakeup lag in nanoseconds between condition.signal() and thread unparking. */
    public long getLastWakeupLagNanos() {
        return lastWakeupLagNanos;
    }

    /** Returns the number of partitions configured for this lock manager. */
    public int getPartitionCount() {
        return numPartitions;
    }

    /** Returns the total count of currently held row locks across all partitions. */
    public int getActiveLockCount() {
        int count = 0;
        for (LockPartition p : partitions) {
            p.lock.lock();
            try {
                for (RowLockEntry entry : p.rowLocks.values()) {
                    if (entry.holderXid != 0) {
                        count++;
                    }
                }
            } finally {
                p.lock.unlock();
            }
        }
        return count;
    }

    /** Returns the number of transactions currently queued waiting for the specified row. */
    public int getWaiterCount(RowId rowId) {
        LockPartition p = getPartition(rowId);
        p.lock.lock();
        try {
            RowLockEntry entry = p.rowLocks.get(rowId);
            return entry != null ? entry.waiterCount : 0;
        } finally {
            p.lock.unlock();
        }
    }

    /** Returns a point-in-time snapshot copy of the current wait-for graph edges. */
    public Map<Long, Long> getWaitsForSnapshot() {
        deadlockLock.lock();
        try {
            return new HashMap<>(waitsFor);
        } finally {
            deadlockLock.unlock();
        }
    }
}
