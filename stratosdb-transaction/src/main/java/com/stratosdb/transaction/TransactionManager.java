package com.stratosdb.transaction;

import com.stratosdb.transaction.locking.LockManager;
import com.stratosdb.transaction.mvcc.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks transaction lifecycle and commit status well enough to support real
 * snapshot-isolation visibility (see mvcc.MVCCVisibility) and owns the
 * LockManager writers use for conflict detection.
 *
 * Known simplification, stated plainly: commit/abort status lives in memory
 * only, for the lifetime of this process - there is no persisted commit log
 * and no horizon/vacuum to bound committedXids' growth. Wiring transaction
 * status into the WAL so it survives a restart, and adding a vacuum-style
 * mechanism to forget transactions old enough that no live snapshot can see
 * them anymore, are the natural next steps once this is exercised for real.
 */
public class TransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

    private final AtomicLong nextXID = new AtomicLong(1);
    private final ThreadLocal<Transaction> currentTransaction = new ThreadLocal<>();

    private final Set<Long> activeXids = ConcurrentHashMap.newKeySet();
    private final Set<Long> committedXids = ConcurrentHashMap.newKeySet();
    private final Set<Long> abortedXids = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<Long, Transaction> transactionsByXid = new ConcurrentHashMap<>();

    private final LockManager lockManager = new LockManager();

    /**
     * Starts a new transaction and captures its snapshot: the set of xids
     * that are still active (not yet committed or aborted) at this exact
     * moment. That set is what lets MVCCVisibility tell "committed before I
     * started" apart from "committed after I started but before I read."
     */
    public Transaction begin() {
        long xid = nextXID.getAndIncrement();
        Set<Long> activeAtStart = new HashSet<>(activeXids); // snapshot BEFORE adding self
        activeXids.add(xid);

        Transaction tx = new Transaction(xid, new Snapshot(xid, activeAtStart));
        transactionsByXid.put(xid, tx);
        currentTransaction.set(tx);
        LOG.debug("Began transaction {} with snapshot {}", xid, tx.getSnapshot());
        return tx;
    }

    public void commit(Transaction tx) {
        if (tx == null) return;
        tx.commit();
        committedXids.add(tx.getXID());
        activeXids.remove(tx.getXID());
        transactionsByXid.remove(tx.getXID());
        lockManager.releaseAll(tx.getXID());
        if (tx.equals(currentTransaction.get())) {
            currentTransaction.remove();
        }
        LOG.debug("Committed transaction {}", tx.getXID());
    }

    public void abort(Transaction tx) {
        if (tx == null) return;
        tx.abort();
        abortedXids.add(tx.getXID());
        activeXids.remove(tx.getXID());
        transactionsByXid.remove(tx.getXID());
        lockManager.releaseAll(tx.getXID());
        if (tx.equals(currentTransaction.get())) {
            currentTransaction.remove();
        }
        LOG.debug("Aborted transaction {}", tx.getXID());
    }

    /** True iff xid committed (as opposed to still active, aborted, or unknown). */
    public boolean isCommitted(long xid) {
        return committedXids.contains(xid);
    }

    public boolean isActive(long xid) {
        return activeXids.contains(xid);
    }

    public boolean isAborted(long xid) {
        return abortedXids.contains(xid);
    }

    /**
     * The horizon vacuum needs: a tuple version superseded by a committed
     * xmax strictly less than this is guaranteed unreachable by every
     * currently active transaction (every active snapshot already started
     * after that xmax committed, so MVCCVisibility would already correctly
     * hide the old version from all of them) - safe to physically reclaim.
     *
     * Returns nextXID's current value if there are no active transactions
     * at all: with nothing active, nothing has a floor to respect, and any
     * future transaction will be assigned an xid >= that value anyway, so
     * it can't possibly need to see anything older.
     */
    public long getOldestActiveXid() {
        long min = Long.MAX_VALUE;
        for (long xid : activeXids) {
            if (xid < min) min = xid;
        }
        return min == Long.MAX_VALUE ? nextXID.get() : min;
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    // --- ThreadLocal convenience API, kept for callers that want an implicit
    // "current transaction per calling thread" model instead of passing
    // Transaction objects around explicitly. ---

    public Transaction getCurrentTransaction() {
        return currentTransaction.get();
    }

    public void commit() {
        commit(currentTransaction.get());
    }

    public void rollback() {
        abort(currentTransaction.get());
    }
}
