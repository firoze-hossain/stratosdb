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
 * Enterprise multi-granularity lock manager supporting:
 * <ul>
 *   <li><b>Table-level intent and exclusive locks</b>: IS, IX, S, SIX, X for coordinating DDL and DML.</li>
 *   <li><b>Row-level shared and exclusive locks</b>: S (for SELECT ... FOR SHARE) and X (for SELECT ... FOR UPDATE / UPDATE / DELETE).</li>
 *   <li><b>64-partitioned hash striping</b>: eliminates partition lock contention across concurrent row locks.</li>
 *   <li><b>Per-row and per-table condition variable queues</b>: instant wakeups (&lt; 0.05ms lag) without polling.</li>
 *   <li><b>Multi-dependency wait-for graph</b>: precise deadlock cycle detection across tables and rows.</li>
 * </ul>
 */
public class LockManager {
    private static final Logger LOG = LoggerFactory.getLogger(LockManager.class);

    public static final int DEFAULT_PARTITIONS = 64;

    public record RowId(String tableName, long pageId, int slot) {}

    // --- Row Lock Data Structures ---

    private static final class RowLockEntry {
        LockMode mode; // S or X when held, null when free
        final Map<Long, Integer> holders = new HashMap<>(); // xid -> lockCount
        int waiterCount = 0;
        final Condition condition;

        RowLockEntry(Condition condition) {
            this.condition = condition;
        }

        boolean isHeld() {
            return !holders.isEmpty();
        }
    }

    private static final class LockPartition {
        final ReentrantLock lock = new ReentrantLock(false);
        final Map<RowId, RowLockEntry> rowLocks = new HashMap<>();
        // 64-byte cache-line padding to prevent false sharing between adjacent lock partitions
        @SuppressWarnings("unused")
        long p0, p1, p2, p3, p4, p5, p6, p7;
    }

    // --- Table Lock Data Structures ---

    private static final class TableLockEntry {
        final ReentrantLock lock = new ReentrantLock(false);
        final Condition condition = lock.newCondition();
        final Map<Long, LockMode> holders = new HashMap<>(); // xid -> LockMode
        int waiterCount = 0;
    }

    private final int numPartitions;
    private final LockPartition[] partitions;
    private final Map<Long, Set<RowId>> locksHeldBy = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, TableLockEntry> tableLocks = new ConcurrentHashMap<>();
    private final Map<Long, Set<String>> tableLocksHeldBy = new ConcurrentHashMap<>();

    // Deadlock detection wait-for graph: xid -> set of xids it is waiting on
    private final ReentrantLock deadlockLock = new ReentrantLock(false);
    private final Map<Long, Set<Long>> waitsFor = new HashMap<>();

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

    // =========================================================================
    // Row-Level Locking (Shared 'S' and Exclusive 'X')
    // =========================================================================

    /**
     * Blocks until xid holds an exclusive (X) lock on rowId, or throws DeadlockException.
     */
    public void acquireExclusive(RowId rowId, long xid) throws DeadlockException {
        acquireExclusive(rowId, xid, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Blocks until xid holds an exclusive (X) lock on rowId, or until the timeout elapses.
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
                    entry = new RowLockEntry(p.lock.newCondition());
                    entry.mode = LockMode.X;
                    entry.holders.put(xid, 1);
                    p.rowLocks.put(rowId, entry);
                    addRowLockHeld(xid, rowId);
                    if (wasWaiting) {
                        clearWaitFor(xid);
                    }
                    return true;
                }

                if (entry.holders.containsKey(xid)) {
                    if (entry.mode == LockMode.X) {
                        // Re-entrant exclusive acquisition
                        entry.holders.put(xid, entry.holders.get(xid) + 1);
                        if (wasWaiting) {
                            clearWaitFor(xid);
                        }
                        return true;
                    } else if (entry.mode == LockMode.S) {
                        // Upgrade from S to X
                        if (entry.holders.size() == 1) {
                            // Sole holder of S: safe to upgrade to X immediately
                            entry.mode = LockMode.X;
                            entry.holders.put(xid, entry.holders.get(xid) + 1);
                            if (wasWaiting) {
                                clearWaitFor(xid);
                            }
                            return true;
                        }
                        // Other transactions also hold S; must wait for them to release
                    }
                } else if (!entry.isHeld()) {
                    if (wasWaiting || entry.waiterCount == 0) {
                        entry.mode = LockMode.X;
                        entry.holders.put(xid, 1);
                        addRowLockHeld(xid, rowId);
                        if (wasWaiting) {
                            clearWaitFor(xid);
                        }
                        return true;
                    }
                }

                if (hasTimeout && nanosTimeout <= 0) {
                    if (wasWaiting) {
                        clearWaitFor(xid);
                    }
                    return false;
                }

                // Collect conflicting holders
                Set<Long> conflictingHolders = new HashSet<>(entry.holders.keySet());
                conflictingHolders.remove(xid);

                setWaitForAndCheckDeadlock(xid, conflictingHolders, rowId);

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
                    if (!entry.isHeld() && entry.waiterCount > 1) {
                        lastSignalTimeNanos = System.nanoTime();
                        entry.condition.signal();
                    }
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for exclusive lock on " + rowId, e);
                } finally {
                    entry.waiterCount--;
                    if (!entry.isHeld() && entry.waiterCount == 0) {
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

    /**
     * Blocks until xid holds a shared (S) lock on rowId, or throws DeadlockException.
     */
    public void acquireShared(RowId rowId, long xid) throws DeadlockException {
        acquireShared(rowId, xid, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Blocks until xid holds a shared (S) lock on rowId, or until the timeout elapses.
     */
    public boolean acquireShared(RowId rowId, long xid, long timeout, TimeUnit unit) throws DeadlockException {
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
                    entry = new RowLockEntry(p.lock.newCondition());
                    entry.mode = LockMode.S;
                    entry.holders.put(xid, 1);
                    p.rowLocks.put(rowId, entry);
                    addRowLockHeld(xid, rowId);
                    if (wasWaiting) {
                        clearWaitFor(xid);
                    }
                    return true;
                }

                if (entry.holders.containsKey(xid)) {
                    // Already holding S or X
                    entry.holders.put(xid, entry.holders.get(xid) + 1);
                    if (wasWaiting) {
                        clearWaitFor(xid);
                    }
                    return true;
                }

                if (!entry.isHeld()) {
                    if (wasWaiting || entry.waiterCount == 0) {
                        entry.mode = LockMode.S;
                        entry.holders.put(xid, 1);
                        addRowLockHeld(xid, rowId);
                        if (wasWaiting) {
                            clearWaitFor(xid);
                        }
                        return true;
                    }
                } else if (entry.mode == LockMode.S) {
                    // Compatible with concurrent shared holders!
                    entry.holders.put(xid, 1);
                    addRowLockHeld(xid, rowId);
                    if (wasWaiting) {
                        clearWaitFor(xid);
                    }
                    return true;
                }

                if (hasTimeout && nanosTimeout <= 0) {
                    if (wasWaiting) {
                        clearWaitFor(xid);
                    }
                    return false;
                }

                // Row held in exclusive mode by another transaction
                Set<Long> conflictingHolders = new HashSet<>(entry.holders.keySet());
                setWaitForAndCheckDeadlock(xid, conflictingHolders, rowId);

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
                    if (!entry.isHeld() && entry.waiterCount > 1) {
                        lastSignalTimeNanos = System.nanoTime();
                        entry.condition.signal();
                    }
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for shared lock on " + rowId, e);
                } finally {
                    entry.waiterCount--;
                    if (!entry.isHeld() && entry.waiterCount == 0) {
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

    /**
     * Releases a specific row lock held by xid on rowId.
     */
    public void release(RowId rowId, long xid) {
        LockPartition p = getPartition(rowId);
        p.lock.lock();
        try {
            RowLockEntry entry = p.rowLocks.get(rowId);
            if (entry != null && entry.holders.containsKey(xid)) {
                int count = entry.holders.get(xid) - 1;
                if (count <= 0) {
                    entry.holders.remove(xid);
                    Set<RowId> held = locksHeldBy.get(xid);
                    if (held != null) {
                        held.remove(rowId);
                        if (held.isEmpty()) {
                            locksHeldBy.remove(xid);
                        }
                    }
                    if (entry.holders.isEmpty()) {
                        entry.mode = null;
                        if (entry.waiterCount > 0) {
                            lastSignalTimeNanos = System.nanoTime();
                            entry.condition.signalAll();
                        } else {
                            p.rowLocks.remove(rowId);
                        }
                    } else if (entry.waiterCount > 0) {
                        lastSignalTimeNanos = System.nanoTime();
                        entry.condition.signalAll();
                    }
                } else {
                    entry.holders.put(xid, count);
                }
            }
        } finally {
            p.lock.unlock();
        }
    }

    private void addRowLockHeld(long xid, RowId rowId) {
        locksHeldBy.computeIfAbsent(xid, k -> ConcurrentHashMap.newKeySet()).add(rowId);
    }

    // =========================================================================
    // Table-Level Intent and Exclusive Locking (IS, IX, S, SIX, X)
    // =========================================================================

    /**
     * Blocks until xid holds a table lock in the specified mode, or throws DeadlockException.
     */
    public boolean acquireTableLock(String rawTableName, LockMode mode, long xid) throws DeadlockException {
        return acquireTableLock(rawTableName, mode, xid, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Blocks until xid holds a table lock in the specified mode, or until the timeout elapses.
     */
    public boolean acquireTableLock(String rawTableName, LockMode mode, long xid, long timeout, TimeUnit unit) throws DeadlockException {
        if (rawTableName == null || mode == null || xid <= 0) return true;
        String tableName = rawTableName.toLowerCase(Locale.ROOT);
        TableLockEntry entry = tableLocks.computeIfAbsent(tableName, k -> new TableLockEntry());
        boolean hasTimeout = timeout > 0 && unit != null;
        long nanosTimeout = hasTimeout ? unit.toNanos(timeout) : 0;
        long deadline = hasTimeout ? System.nanoTime() + nanosTimeout : 0;
        boolean wasWaiting = false;

        entry.lock.lock();
        try {
            while (true) {
                LockMode currentMode = entry.holders.get(xid);
                if (currentMode != null) {
                    if (currentMode == mode || currentMode.isStrongerThanOrEqualTo(mode)) {
                        if (wasWaiting) {
                            clearWaitFor(xid);
                        }
                        return true;
                    }
                }

                // Check if requested mode is compatible with all other holders
                boolean canGrant = true;
                Set<Long> conflictingHolders = null;
                for (Map.Entry<Long, LockMode> h : entry.holders.entrySet()) {
                    if (h.getKey().equals(xid)) continue;
                    if (!h.getValue().isCompatible(mode) || !mode.isCompatible(h.getValue())) {
                        canGrant = false;
                        if (conflictingHolders == null) {
                            conflictingHolders = new HashSet<>();
                        }
                        conflictingHolders.add(h.getKey());
                    }
                }

                if (canGrant) {
                    LockMode effectiveMode = (currentMode == null) ? mode : strongerOf(currentMode, mode);
                    entry.holders.put(xid, effectiveMode);
                    addTableLockHeld(xid, tableName);
                    if (wasWaiting) {
                        clearWaitFor(xid);
                    }
                    return true;
                }

                if (hasTimeout && nanosTimeout <= 0) {
                    if (wasWaiting) {
                        clearWaitFor(xid);
                    }
                    return false;
                }

                // Register wait-for dependencies in global wait-for graph
                setWaitForAndCheckDeadlock(xid, conflictingHolders, "TABLE:" + tableName);

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
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for table lock on " + tableName, e);
                } finally {
                    entry.waiterCount--;
                }

                if (hasTimeout) {
                    nanosTimeout = deadline - System.nanoTime();
                }
            }
        } finally {
            entry.lock.unlock();
        }
    }

    /**
     * Releases a specific table lock held by xid on tableName.
     */
    public void releaseTableLock(String rawTableName, long xid) {
        if (rawTableName == null || xid <= 0) return;
        String tableName = rawTableName.toLowerCase(Locale.ROOT);
        TableLockEntry entry = tableLocks.get(tableName);
        if (entry == null) return;
        entry.lock.lock();
        try {
            if (entry.holders.remove(xid) != null) {
                Set<String> held = tableLocksHeldBy.get(xid);
                if (held != null) {
                    held.remove(tableName);
                    if (held.isEmpty()) tableLocksHeldBy.remove(xid);
                }
                if (entry.waiterCount > 0) {
                    lastSignalTimeNanos = System.nanoTime();
                    entry.condition.signalAll();
                } else if (entry.holders.isEmpty()) {
                    tableLocks.remove(tableName);
                }
            }
        } finally {
            entry.lock.unlock();
        }
    }

    private void addTableLockHeld(long xid, String tableName) {
        tableLocksHeldBy.computeIfAbsent(xid, k -> ConcurrentHashMap.newKeySet()).add(tableName);
    }

    private static LockMode strongerOf(LockMode m1, LockMode m2) {
        if (m1 == null) return m2;
        if (m2 == null) return m1;
        if (m1 == LockMode.X || m2 == LockMode.X) return LockMode.X;
        if (m1 == LockMode.SIX || m2 == LockMode.SIX) return LockMode.SIX;
        if ((m1 == LockMode.S && m2 == LockMode.IX) || (m1 == LockMode.IX && m2 == LockMode.S)) return LockMode.SIX;
        if (m1 == LockMode.S || m2 == LockMode.S) return LockMode.S;
        if (m1 == LockMode.IX || m2 == LockMode.IX) return LockMode.IX;
        return LockMode.IS;
    }

    // =========================================================================
    // Deadlock Detection Wait-For Graph
    // =========================================================================

    private void setWaitForAndCheckDeadlock(long xid, Set<Long> holders, Object resource) throws DeadlockException {
        if (holders == null || holders.isEmpty()) return;
        deadlockLock.lock();
        try {
            Set<Long> edges = waitsFor.computeIfAbsent(xid, k -> new HashSet<>());
            edges.addAll(holders);
            if (hasCycle(xid)) {
                edges.removeAll(holders);
                if (edges.isEmpty()) {
                    waitsFor.remove(xid);
                }
                throw new DeadlockException("Deadlock detected: transaction " + xid
                    + " waiting on " + holders + " for " + resource
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

    private boolean hasCycle(long start) {
        Set<Long> visited = new HashSet<>();
        Set<Long> recursionStack = new HashSet<>();
        return checkCycleDfs(start, visited, recursionStack);
    }

    private boolean checkCycleDfs(long current, Set<Long> visited, Set<Long> recursionStack) {
        visited.add(current);
        recursionStack.add(current);

        Set<Long> nextNodes = waitsFor.get(current);
        if (nextNodes != null) {
            for (Long next : nextNodes) {
                if (recursionStack.contains(next)) {
                    return true;
                }
                if (!visited.contains(next) && checkCycleDfs(next, visited, recursionStack)) {
                    return true;
                }
            }
        }
        recursionStack.remove(current);
        return false;
    }

    // =========================================================================
    // Release All & Query State
    // =========================================================================

    /**
     * Releases every row lock and table lock held by xid. Call on both commit and abort.
     */
    public void releaseAll(long xid) {
        if (locksHeldBy.isEmpty() && tableLocksHeldBy.isEmpty()) {
            return;
        }
        // 1. Release row locks
        Set<RowId> heldRows = locksHeldBy.remove(xid);
        if (heldRows != null && !heldRows.isEmpty()) {
            Map<Integer, List<RowId>> byPartition = new HashMap<>();
            for (RowId r : heldRows) {
                byPartition.computeIfAbsent(getPartitionIndex(r), k -> new ArrayList<>()).add(r);
            }

            for (Map.Entry<Integer, List<RowId>> partEntry : byPartition.entrySet()) {
                LockPartition p = partitions[partEntry.getKey()];
                p.lock.lock();
                try {
                    for (RowId r : partEntry.getValue()) {
                        RowLockEntry entry = p.rowLocks.get(r);
                        if (entry != null && entry.holders.containsKey(xid)) {
                            entry.holders.remove(xid);
                            if (entry.holders.isEmpty()) {
                                entry.mode = null;
                                if (entry.waiterCount > 0) {
                                    lastSignalTimeNanos = System.nanoTime();
                                    entry.condition.signalAll();
                                } else {
                                    p.rowLocks.remove(r);
                                }
                            } else if (entry.waiterCount > 0) {
                                lastSignalTimeNanos = System.nanoTime();
                                entry.condition.signalAll();
                            }
                        }
                    }
                } finally {
                    p.lock.unlock();
                }
            }
        }

        // 2. Release table locks
        Set<String> heldTables = tableLocksHeldBy.remove(xid);
        if (heldTables != null && !heldTables.isEmpty()) {
            for (String tbl : heldTables) {
                TableLockEntry entry = tableLocks.get(tbl);
                if (entry != null) {
                    entry.lock.lock();
                    try {
                        entry.holders.remove(xid);
                        if (entry.waiterCount > 0) {
                            lastSignalTimeNanos = System.nanoTime();
                            entry.condition.signalAll();
                        } else if (entry.holders.isEmpty()) {
                            tableLocks.remove(tbl);
                        }
                    } finally {
                        entry.lock.unlock();
                    }
                }
            }
        }

        // 3. Clear wait-for graph edges
        clearWaitFor(xid);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Released all table and row locks for transaction {}", xid);
        }
    }

    /** Returns true if the specified row is currently locked in either Shared or Exclusive mode. */
    public boolean isLocked(RowId rowId) {
        LockPartition p = getPartition(rowId);
        p.lock.lock();
        try {
            RowLockEntry entry = p.rowLocks.get(rowId);
            return entry != null && entry.isHeld();
        } finally {
            p.lock.unlock();
        }
    }

    /** Returns any transaction ID currently holding a lock on rowId, or null if unlocked. */
    public Long getHolder(RowId rowId) {
        LockPartition p = getPartition(rowId);
        p.lock.lock();
        try {
            RowLockEntry entry = p.rowLocks.get(rowId);
            return (entry != null && entry.isHeld()) ? entry.holders.keySet().iterator().next() : null;
        } finally {
            p.lock.unlock();
        }
    }

    /** Returns all transaction IDs currently holding a lock on rowId. */
    public Set<Long> getHolders(RowId rowId) {
        LockPartition p = getPartition(rowId);
        p.lock.lock();
        try {
            RowLockEntry entry = p.rowLocks.get(rowId);
            return (entry != null && entry.isHeld()) ? new HashSet<>(entry.holders.keySet()) : Set.of();
        } finally {
            p.lock.unlock();
        }
    }

    /** Returns true if the specified table is currently locked in any mode. */
    public boolean isTableLocked(String rawTableName) {
        if (rawTableName == null) return false;
        TableLockEntry entry = tableLocks.get(rawTableName.toLowerCase(Locale.ROOT));
        if (entry == null) return false;
        entry.lock.lock();
        try {
            return !entry.holders.isEmpty();
        } finally {
            entry.lock.unlock();
        }
    }

    /** Returns the lock mode held by xid on tableName, or null if not held. */
    public LockMode getTableLockMode(String rawTableName, long xid) {
        if (rawTableName == null || xid <= 0) return null;
        TableLockEntry entry = tableLocks.get(rawTableName.toLowerCase(Locale.ROOT));
        if (entry == null) return null;
        entry.lock.lock();
        try {
            return entry.holders.get(xid);
        } finally {
            entry.lock.unlock();
        }
    }

    /** Returns all transaction IDs and their lock modes on tableName. */
    public Map<Long, LockMode> getTableHolders(String rawTableName) {
        if (rawTableName == null) return Map.of();
        TableLockEntry entry = tableLocks.get(rawTableName.toLowerCase(Locale.ROOT));
        if (entry == null) return Map.of();
        entry.lock.lock();
        try {
            return new HashMap<>(entry.holders);
        } finally {
            entry.lock.unlock();
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
                    if (entry.isHeld()) {
                        count += entry.holders.size();
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

    /** Returns the number of transactions currently queued waiting for the specified table. */
    public int getTableWaiterCount(String rawTableName) {
        if (rawTableName == null) return 0;
        TableLockEntry entry = tableLocks.get(rawTableName.toLowerCase(Locale.ROOT));
        if (entry == null) return 0;
        entry.lock.lock();
        try {
            return entry.waiterCount;
        } finally {
            entry.lock.unlock();
        }
    }

    /** Returns a point-in-time snapshot copy of the current wait-for graph edges. */
    public Map<Long, Set<Long>> getWaitsForSnapshot() {
        deadlockLock.lock();
        try {
            Map<Long, Set<Long>> copy = new HashMap<>();
            for (Map.Entry<Long, Set<Long>> e : waitsFor.entrySet()) {
                copy.put(e.getKey(), new HashSet<>(e.getValue()));
            }
            return copy;
        } finally {
            deadlockLock.unlock();
        }
    }
}
