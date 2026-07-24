package com.stratosdb.transaction.mvcc;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A point-in-time view of "who else was mid-transaction when I started."
 *
 * This is the piece that makes isolation "snapshot" isolation rather than
 * "read whatever is committed right now": a transaction's visibility never
 * changes mid-flight, even if other transactions commit while it's running.
 * That requires knowing not just "is xid X committed" (which changes over
 * time) but "was X still in-flight at the moment I took my snapshot."
 */
public final class Snapshot {
    private final long xid;
    private final Set<Long> activeXidsAtStart;

    public Snapshot(long xid, Set<Long> activeXidsAtStart) {
        this.xid = xid;
        this.activeXidsAtStart = Collections.unmodifiableSet(new HashSet<>(activeXidsAtStart));
    }

    public long getXid() {
        return xid;
    }

    /** True if the given transaction had not yet committed/aborted when this snapshot was taken. */
    public boolean wasActiveAtStart(long otherXid) {
        return activeXidsAtStart.contains(otherXid);
    }

    @Override
    public String toString() {
        return "Snapshot[xid=" + xid + ", activeAtStart=" + activeXidsAtStart + "]";
    }
}
