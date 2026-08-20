package com.stratosdb.storage.wal;

import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.SlottedPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies a live, incoming stream of WAL bytes to local storage - the
 * replica side of real physical (WAL-shipping) replication. Reuses
 * exactly the same on-disk record format {@link WALManager}'s own
 * logInsert/logUpdate/logDelete/logCommit already write and recover()
 * already parses - a replica is fed the primary's own raw WAL bytes
 * unmodified, not a separate, invented replication wire format.
 *
 * The key, real difference from WALManager.recover()'s own two-pass
 * design (see its own javadoc): recover() can afford two passes over a
 * WAL file because the file's full extent is already known and fixed
 * before either pass starts. A live stream has no such boundary - a
 * commit record for an already-streamed insert might not arrive for an
 * arbitrarily long time (or never, if that transaction is later
 * aborted - see the class-level "known, honestly-stated limitations"
 * note below). So this class buffers each transaction's own
 * insert/update/delete records in memory, keyed by xid, and only
 * applies them to real storage the moment that xid's own commit record
 * arrives - the same real "reorder buffer" idea real streaming/logical
 * replication systems use, not a shortcut.
 *
 * Not thread-safe - intended for one dedicated thread per replica
 * connection to own an instance and feed it sequentially.
 *
 * Known, honestly-stated limitations:
 *   - An aborted transaction's buffered operations are never released.
 *     This engine's own WAL never writes an explicit abort record (see
 *     WALManager's OP_ABORT, defined but never written - an aborted
 *     transaction's operations simply never get a matching commit) so
 *     this class has no signal to know a transaction was abandoned
 *     rather than merely not-yet-committed. A real, open memory-growth
 *     concern for a long-running replica behind a primary with many
 *     aborted transactions - not attempted here, since fixing it
 *     properly needs the primary to start writing a real abort record,
 *     a separate, own piece of work.
 *   - Not LSN-gated / not idempotent, the same known limitation
 *     WALManager.recover() itself already names: applying the same
 *     bytes twice (e.g., a replica reconnecting and re-streaming
 *     already-applied bytes because its own last-applied-offset
 *     bookkeeping was lost) duplicates the writes. A real replica
 *     client is responsible for tracking its own last-applied byte
 *     offset accurately and never re-requesting already-applied bytes.
 */
public class StreamingWalApplier {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingWalApplier.class);

    private final DiskManager diskManager;
    private final Map<Long, List<PendingOp>> pendingByXid = new HashMap<>();
    private byte[] unconsumed = new byte[0];
    private long totalBytesFed = 0;
    private long totalOpsApplied = 0;
    private long totalTransactionsApplied = 0;
    private final com.stratosdb.storage.buffer.BufferPoolManager bufferPool;

    private record PendingOp(int opType, String tableName, long pageId, byte[] tupleData, int slot, byte[] newData) {}

    public StreamingWalApplier(DiskManager diskManager) {
        this(diskManager, null);
    }

    /**
     * bufferPool, when supplied, is the SAME buffer pool a live replica's
     * own normal query-serving path (HeapTable/ExecutorEngine) reads
     * through - after each applied transaction's pages are written
     * directly to diskManager (bypassing the buffer pool entirely, the
     * same as WALManager.recover() already does), this applier also
     * evicts those exact pages from bufferPool's own cache, so a
     * concurrent read on the replica can never return a stale,
     * pre-replication cached copy instead of the just-applied data. Pass
     * null (see the other constructor) only when there is no live,
     * concurrently-queryable buffer pool to keep consistent - e.g. the
     * isolated, offline scenario StreamingApplierTest itself uses, where
     * a fresh BufferPoolManager is constructed only after all bytes are
     * already applied.
     */
    public StreamingWalApplier(DiskManager diskManager, com.stratosdb.storage.buffer.BufferPoolManager bufferPool) {
        this.diskManager = diskManager;
        this.bufferPool = bufferPool;
    }

    /**
     * Feeds newly-received bytes in, parsing and applying as many
     * complete records as are now available. A record split across two
     * feed() calls (a normal, expected condition for a live network
     * stream, unlike WALManager.recover()'s own "truncated trailing
     * record means the writer crashed mid-write" interpretation) is
     * simply left in the internal buffer until the rest of it arrives.
     */
    public synchronized void feed(byte[] newBytes) {
        totalBytesFed += newBytes.length;
        byte[] combined = new byte[unconsumed.length + newBytes.length];
        System.arraycopy(unconsumed, 0, combined, 0, unconsumed.length);
        System.arraycopy(newBytes, 0, combined, unconsumed.length, newBytes.length);

        int pos = 0;
        while (true) {
            int recordStart = pos;
            Integer opType = tryReadInt(combined, pos);
            if (opType == null) break;
            pos += 4;

            Integer newPos = tryParseRecordBody(combined, pos, opType);
            if (newPos == null) {
                pos = recordStart; // not enough bytes yet for this whole record - rewind, wait for more
                break;
            }
            pos = newPos;
        }

        unconsumed = java.util.Arrays.copyOfRange(combined, pos, combined.length);
    }

    /** Parses one record's body (everything after the already-consumed 4-byte opType) starting at pos, applying it immediately if it's a self-contained op, or buffering/flushing for INSERT/UPDATE/DELETE/COMMIT. Returns the position just past the record, or null if there aren't enough bytes yet. */
    private Integer tryParseRecordBody(byte[] buf, int pos, int opType) {
        switch (opType) {
            case WALManager.OP_INSERT: {
                Cursor c = new Cursor(pos);
                Long xid = tryReadLong(buf, c);
                String tableName = tryReadLengthPrefixedString(buf, c);
                Long pageId = tryReadLong(buf, c);
                Integer slot = tryReadInt(buf, c); // logged slot - unused, matching recover()'s own re-derivation approach
                Integer len = tryReadInt(buf, c);
                if (xid == null || tableName == null || pageId == null || slot == null || len == null) return null;
                byte[] tupleData = tryReadBytes(buf, c, len);
                if (tupleData == null) return null;

                pendingByXid.computeIfAbsent(xid, k -> new ArrayList<>())
                    .add(new PendingOp(opType, tableName, pageId, tupleData, -1, null));
                return c.pos;
            }
            case WALManager.OP_DELETE: {
                Cursor c = new Cursor(pos);
                Long xid = tryReadLong(buf, c);
                String tableName = tryReadLengthPrefixedString(buf, c);
                Long pageId = tryReadLong(buf, c);
                Integer slot = tryReadInt(buf, c);
                if (xid == null || tableName == null || pageId == null || slot == null) return null;

                pendingByXid.computeIfAbsent(xid, k -> new ArrayList<>())
                    .add(new PendingOp(opType, tableName, pageId, null, slot, null));
                return c.pos;
            }
            case WALManager.OP_UPDATE: {
                Cursor c = new Cursor(pos);
                Long xid = tryReadLong(buf, c);
                String tableName = tryReadLengthPrefixedString(buf, c);
                Long pageId = tryReadLong(buf, c);
                Integer slot = tryReadInt(buf, c);
                Integer oldLen = tryReadInt(buf, c);
                if (xid == null || tableName == null || pageId == null || slot == null || oldLen == null) return null;
                byte[] oldData = tryReadBytes(buf, c, oldLen); // kept for symmetry with recover(); unused by apply
                if (oldData == null) return null;
                Integer newLen = tryReadInt(buf, c);
                if (newLen == null) return null;
                byte[] newData = tryReadBytes(buf, c, newLen);
                if (newData == null) return null;

                pendingByXid.computeIfAbsent(xid, k -> new ArrayList<>())
                    .add(new PendingOp(opType, tableName, pageId, null, slot, newData));
                return c.pos;
            }
            case WALManager.OP_COMMIT: {
                Cursor c = new Cursor(pos);
                Long xid = tryReadLong(buf, c);
                if (xid == null) return null;
                applyCommittedTransaction(xid);
                return c.pos;
            }
            case WALManager.OP_CHECKPOINT: {
                Cursor c = new Cursor(pos);
                Long ts = tryReadLong(buf, c);
                if (ts == null) return null;
                return c.pos; // nothing to apply - a checkpoint is a marker, not a data change
            }
            default: {
                LOG.warn("Unknown WAL record type {} in replication stream - stopping this connection's apply loop", opType);
                return buf.length; // consume everything remaining - the caller should close this connection
            }
        }
    }

    private void applyCommittedTransaction(long xid) {
        List<PendingOp> ops = pendingByXid.remove(xid);
        if (ops == null) {
            return; // a commit for a transaction with no buffered ops (e.g. a DDL-only or read-only transaction)
        }
        Map<String, SlottedPage> dirtyPages = new HashMap<>();
        for (PendingOp op : ops) {
            SlottedPage page = loadOrGetDirtyPage(dirtyPages, op.tableName(), op.pageId());
            switch (op.opType()) {
                case WALManager.OP_INSERT -> page.insertTuple(op.tupleData());
                case WALManager.OP_DELETE -> page.deleteTuple(op.slot());
                case WALManager.OP_UPDATE -> page.updateTuple(op.slot(), op.newData());
                default -> throw new IllegalStateException("Unexpected buffered op type: " + op.opType());
            }
            totalOpsApplied++;
        }
        for (Map.Entry<String, SlottedPage> entry : dirtyPages.entrySet()) {
            String tableName = entry.getKey().substring(0, entry.getKey().lastIndexOf(':'));
            diskManager.writePage(tableName, entry.getValue());
            if (bufferPool != null) {
                bufferPool.evictPage(tableName, entry.getValue().getPageId());
            }
        }
        totalTransactionsApplied++;
    }

    private SlottedPage loadOrGetDirtyPage(Map<String, SlottedPage> dirtyPages, String tableName, long pageId) {
        String key = tableName + ":" + pageId;
        return dirtyPages.computeIfAbsent(key, k -> {
            com.stratosdb.storage.page.Page raw = diskManager.readPage(tableName, pageId);
            SlottedPage page = new SlottedPage(pageId);
            page.getBuffer().put(raw.getBytes());
            page.getBuffer().flip();
            return page;
        });
    }

    public synchronized long getTotalBytesFed() {
        return totalBytesFed;
    }

    public synchronized long getTotalOpsApplied() {
        return totalOpsApplied;
    }

    public synchronized long getTotalTransactionsApplied() {
        return totalTransactionsApplied;
    }

    public synchronized int getPendingTransactionCount() {
        return pendingByXid.size();
    }

    // --- tolerant, "not enough bytes yet" readers - the actual behavioral
    // difference from WALManager's own private readInt/readLong/etc, which
    // instead throw on a truncated read (correct for recover()'s own
    // finished-file assumption, wrong for a live, still-arriving stream).

    private static final class Cursor {
        int pos;
        Cursor(int pos) { this.pos = pos; }
    }

    private static Integer tryReadInt(byte[] buf, int pos) {
        if (pos + 4 > buf.length) return null;
        return ((buf[pos] & 0xFF) << 24) | ((buf[pos + 1] & 0xFF) << 16) | ((buf[pos + 2] & 0xFF) << 8) | (buf[pos + 3] & 0xFF);
    }

    private static Integer tryReadInt(byte[] buf, Cursor c) {
        Integer v = tryReadInt(buf, c.pos);
        if (v != null) c.pos += 4;
        return v;
    }

    private static Long tryReadLong(byte[] buf, Cursor c) {
        if (c.pos + 8 > buf.length) return null;
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (buf[c.pos + i] & 0xFF);
        }
        c.pos += 8;
        return v;
    }

    private static byte[] tryReadBytes(byte[] buf, Cursor c, int n) {
        if (n < 0 || c.pos + n > buf.length) return null;
        byte[] out = java.util.Arrays.copyOfRange(buf, c.pos, c.pos + n);
        c.pos += n;
        return out;
    }

    private static String tryReadLengthPrefixedString(byte[] buf, Cursor c) {
        Integer len = tryReadInt(buf, c);
        if (len == null) return null;
        byte[] bytes = tryReadBytes(buf, c, len);
        if (bytes == null) return null;
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
}
