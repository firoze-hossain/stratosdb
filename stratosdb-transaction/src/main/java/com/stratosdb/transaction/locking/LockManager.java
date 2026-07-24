package com.stratosdb.transaction.locking;

import com.stratosdb.common.exceptions.DeadlockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Row-level exclusive locking for writers (UPDATE/DELETE), with deadlock
 * detection over a wait-for graph.
 *
 * Deliberately simple: one global monitor, a polling wait rather than
 * per-row condition variables, and at most one outstanding "waiting on"
 * edge per transaction (since acquireExclusive blocks the calling thread
 * until it either gets the lock or throws). That last property is what
 * makes the wait-for graph a simple functional graph - each node has
 * out-degree at most 1 - so detecting a cycle is just "follow the chain of
 * single edges and see if it leads back to where you started," no general
 * graph library needed.
 *
 * Readers do not take locks here at all: MVCC snapshot isolation means
 * readers never block writers and never get blocked by them. Locking exists
 * purely to serialize concurrent writers on the same row.
 */
public class LockManager {
    private static final Logger LOG = LoggerFactory.getLogger(LockManager.class);
    private static final long POLL_INTERVAL_MS = 25;

    public record RowId(String tableName, long pageId, int slot) {}

    private final Map<RowId, Long> lockHolder = new ConcurrentHashMap<>();
    private final Map<Long, Set<RowId>> locksHeldBy = new ConcurrentHashMap<>();
    private final Map<Long, Long> waitsFor = new ConcurrentHashMap<>();
    private final Object monitor = new Object();

    /**
     * Blocks until xid holds an exclusive lock on rowId, or throws
     * DeadlockException if granting the wait would create a cycle.
     */
    public void acquireExclusive(RowId rowId, long xid) throws DeadlockException {
        synchronized (monitor) {
            while (true) {
                Long holder = lockHolder.get(rowId);
                if (holder == null || holder.equals(xid)) {
                    lockHolder.put(rowId, xid);
                    locksHeldBy.computeIfAbsent(xid, k -> ConcurrentHashMap.newKeySet()).add(rowId);
                    waitsFor.remove(xid);
                    return;
                }

                waitsFor.put(xid, holder);
                if (hasCycle(xid)) {
                    waitsFor.remove(xid);
                    throw new DeadlockException("Deadlock detected: transaction " + xid
                        + " waiting on transaction " + holder + " for row " + rowId
                        + " would create a cycle in the wait-for graph");
                }

                try {
                    monitor.wait(POLL_INTERVAL_MS);
                } catch (InterruptedException e) {
                    waitsFor.remove(xid);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for lock on " + rowId, e);
                }
            }
        }
    }

    /** Follows the chain of single wait-for edges from `start`; true if it loops back to start. */
    private boolean hasCycle(long start) {
        Set<Long> visited = new HashSet<>();
        long current = start;
        while (true) {
            Long next = waitsFor.get(current);
            if (next == null) return false;
            if (next == start) return true;
            if (!visited.add(next)) return false; // ran into an unrelated cycle, not one through `start`
            current = next;
        }
    }

    /** Releases every lock xid holds. Call on both commit and abort. */
    public void releaseAll(long xid) {
        synchronized (monitor) {
            Set<RowId> held = locksHeldBy.remove(xid);
            if (held != null) {
                for (RowId r : held) {
                    lockHolder.remove(r, xid);
                }
            }
            waitsFor.remove(xid);
            monitor.notifyAll();
        }
        LOG.debug("Released all locks for transaction {}", xid);
    }
}
