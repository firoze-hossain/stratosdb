package com.stratosdb.transaction.mvcc;

import com.stratosdb.transaction.TransactionManager;

import java.nio.ByteBuffer;

/**
 * Encodes/decodes the 16-byte MVCC header (xmin, xmax) that the heap storage
 * layer prepends to every stored row version, and implements the standard
 * snapshot-isolation visibility rule on top of it.
 *
 * xmax == 0 (NO_XMAX) means "this version has not been superseded."
 *
 * Known simplification, stated plainly rather than hidden: this tracks
 * committed/active transaction ids in memory only, for the lifetime of the
 * process. There is no persisted commit-status log (Postgres calls this
 * pg_xact/CLOG) and no vacuum/horizon to bound the committed-set's growth.
 * Both are real production requirements and are natural follow-ups once this
 * is wired all the way through, not attempted here.
 */
public final class MVCCVisibility {
    public static final int HEADER_SIZE = 16; // xmin(8 bytes) + xmax(8 bytes)
    public static final long NO_XMAX = 0L;

    private MVCCVisibility() {}

    public static byte[] wrap(byte[] payload, long xmin, long xmax) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + payload.length);
        buf.putLong(xmin);
        buf.putLong(xmax);
        buf.put(payload);
        return buf.array();
    }

    public static long readXmin(byte[] stored) {
        return ByteBuffer.wrap(stored, 0, 8).getLong();
    }

    public static long readXmax(byte[] stored) {
        return ByteBuffer.wrap(stored, 8, 8).getLong();
    }

    public static byte[] readPayload(byte[] stored) {
        byte[] payload = new byte[stored.length - HEADER_SIZE];
        System.arraycopy(stored, HEADER_SIZE, payload, 0, payload.length);
        return payload;
    }

    /** Same payload, same xmin, new xmax - used to tombstone a version on delete/update. */
    public static byte[] withXmax(byte[] stored, long newXmax) {
        return wrap(readPayload(stored), readXmin(stored), newXmax);
    }

    /**
     * Standard snapshot-isolation visibility check (the same rule Postgres uses,
     * minus subtransactions):
     *
     * A row version is visible to `snapshot` iff:
     *   1. its creator (xmin) is this same transaction, OR its creator's xid is
     *      strictly less than snapshot.xid (started before this snapshot could
     *      possibly have known about it), was NOT still active when the
     *      snapshot was taken, and did in fact commit (as opposed to abort); AND
     *   2. it has not been superseded: either xmax is NO_XMAX, or its remover
     *      does not satisfy the same "definitely committed before my snapshot"
     *      test above (including the remover being myself, mid-transaction -
     *      in which case the row is gone even to me).
     *
     * The `xmin < snapshot.xid` / `xmax < snapshot.xid` guards matter: without
     * them, a transaction that starts *after* this snapshot was taken - and so
     * never appears in activeXidsAtStart at all, simply because it didn't exist
     * yet - would look identical to one that committed safely before the
     * snapshot. That's the difference between real snapshot isolation and
     * "read whatever happens to be committed right now."
     */
    public static boolean isVisible(byte[] stored, Snapshot snapshot, TransactionManager txnManager) {
        long xmin = readXmin(stored);
        long xmax = readXmax(stored);

        boolean creatorVisible = (xmin == snapshot.getXid()) || definitelyCommittedBefore(xmin, snapshot, txnManager);
        if (!creatorVisible) {
            return false;
        }

        if (xmax == NO_XMAX) {
            return true;
        }

        boolean removedByMe = xmax == snapshot.getXid();
        if (removedByMe) {
            return false; // I deleted/updated it myself within this same transaction - it's gone, even to me
        }

        boolean removerVisible = definitelyCommittedBefore(xmax, snapshot, txnManager);
        return !removerVisible; // still visible unless the remover's commit predates my snapshot
    }

    private static boolean definitelyCommittedBefore(long xid, Snapshot snapshot, TransactionManager txnManager) {
        return xid < snapshot.getXid()
            && !snapshot.wasActiveAtStart(xid)
            && txnManager.isCommitted(xid);
    }
}
