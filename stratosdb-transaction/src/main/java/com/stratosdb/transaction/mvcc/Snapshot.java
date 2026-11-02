package com.stratosdb.transaction.mvcc;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A point-in-time view of transaction visibility.
 *
 * Tracks:
 * - ownerXid: The xid of the transaction that owns/runs queries using this snapshot.
 * - xmax: The transaction watermark (nextXID) at the moment this snapshot was captured.
 *         Any transaction with xid >= xmax was started strictly after this snapshot and
 *         is invisible to it.
 * - activeXidsAtStart: The set of transactions that were in-flight (neither committed nor
 *                      aborted) when this snapshot was captured.
 */
public final class Snapshot {
    private final long ownerXid;
    private final long xmax;
    private final Set<Long> activeXidsAtStart;

    public Snapshot(long xid, Set<Long> activeXidsAtStart) {
        this(xid, xid + 1, activeXidsAtStart);
    }

    public Snapshot(long ownerXid, long xmax, Set<Long> activeXidsAtStart) {
        this.ownerXid = ownerXid;
        this.xmax = xmax;
        this.activeXidsAtStart = Collections.unmodifiableSet(new HashSet<>(activeXidsAtStart));
    }

    /** Returns the owner transaction ID. Maintained for backwards compatibility with getOwnerXid(). */
    public long getXid() {
        return ownerXid;
    }

    public long getOwnerXid() {
        return ownerXid;
    }

    public long getXmax() {
        return xmax;
    }

    /** True if the given transaction had not yet committed/aborted when this snapshot was taken. */
    public boolean wasActiveAtStart(long otherXid) {
        return activeXidsAtStart.contains(otherXid);
    }

    public Set<Long> getActiveXidsAtStart() {
        return activeXidsAtStart;
    }

    @Override
    public String toString() {
        return "Snapshot[ownerXid=" + ownerXid + ", xmax=" + xmax + ", activeAtStart=" + activeXidsAtStart + "]";
    }
}
