package com.stratosdb.storage.wal;

import com.stratosdb.common.utils.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Write-Ahead Log for durability
 */
public class WALManager {
    private static final Logger LOG = LoggerFactory.getLogger(WALManager.class);
    
    private final String walDirectory;
    private FileChannel walChannel;
    private final AtomicLong currentLSN;
    private boolean sync = true;
    
    // Operation types
    public static final int OP_INSERT = 1;
    public static final int OP_UPDATE = 2;
    public static final int OP_DELETE = 3;
    public static final int OP_COMMIT = 4;
    public static final int OP_ABORT = 5;
    public static final int OP_CHECKPOINT = 6;
    
    public WALManager(String dataDirectory) {
        this.walDirectory = dataDirectory + "/wal";
        this.currentLSN = new AtomicLong(0);
        initialize();
    }
    
    private void initialize() {
        try {
            File dir = new File(walDirectory);
            if (!dir.exists() && !dir.mkdirs()) {
                LOG.warn("Failed to create WAL directory: {}", walDirectory);
            }
            
            File walFile = new File(walDirectory, "wal.log");
            if (!walFile.exists() && !walFile.createNewFile()) {
                LOG.warn("Failed to create WAL file");
            }
            
            RandomAccessFile raf = new RandomAccessFile(walFile, "rw");
            this.walChannel = raf.getChannel();
            this.currentLSN.set(walChannel.size());
            
            LOG.info("WAL initialized at LSN: {}", currentLSN.get());
        } catch (Exception e) {
            LOG.error("Failed to initialize WAL", e);
        }
    }
    
    /**
     * Log an insert operation. xid is the writing transaction's id - not
     * used at write time, but essential at recover() time: redo only
     * replays operations whose xid has a matching OP_COMMIT record
     * elsewhere in the log. Without this, an INSERT from a transaction
     * that crashed before committing would still get replayed on restart,
     * a real atomicity violation - previously an accepted, documented
     * limitation (see recover()'s javadoc) that became untenable once
     * transactions could span more than one statement.
     */
    public void logInsert(String tableName, long xid, long pageId, int slot, byte[] tupleData) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024 + tupleData.length);
            buffer.putInt(OP_INSERT);
            buffer.putLong(xid);
            
            byte[] tableBytes = tableName.getBytes();
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            
            buffer.putLong(pageId);
            buffer.putInt(slot);
            buffer.putInt(tupleData.length);
            buffer.put(tupleData);
            
            writeBuffer(buffer, false);
        } catch (Exception e) {
            LOG.error("Failed to log insert", e);
        }
    }
    
    /**
     * Log a delete operation. xid: see logInsert's javadoc.
     */
    public void logDelete(String tableName, long xid, long pageId, int slot) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(256);
            buffer.putInt(OP_DELETE);
            buffer.putLong(xid);
            
            byte[] tableBytes = tableName.getBytes();
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            
            buffer.putLong(pageId);
            buffer.putInt(slot);
            
            writeBuffer(buffer, false);
        } catch (Exception e) {
            LOG.error("Failed to log delete", e);
        }
    }
    
    /**
     * Log an update operation. xid: see logInsert's javadoc.
     */
    public void logUpdate(String tableName, long xid, long pageId, int slot, byte[] oldData, byte[] newData) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024 + oldData.length + newData.length);
            buffer.putInt(OP_UPDATE);
            buffer.putLong(xid);
            
            byte[] tableBytes = tableName.getBytes();
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            
            buffer.putLong(pageId);
            buffer.putInt(slot);
            
            buffer.putInt(oldData.length);
            buffer.put(oldData);
            
            buffer.putInt(newData.length);
            buffer.put(newData);
            
            writeBuffer(buffer, false);
        } catch (Exception e) {
            LOG.error("Failed to log update", e);
        }
    }
    
    /**
     * Log a commit
     */
    public void logCommit(long transactionId) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putInt(OP_COMMIT);
            buffer.putLong(transactionId);
            writeBuffer(buffer, true);
        } catch (Exception e) {
            LOG.error("Failed to log commit", e);
        }
    }
    
    /**
     * Log a checkpoint
     */
    public void checkpoint() {
        if (walChannel == null || !walChannel.isOpen()) {
            LOG.debug("Skipping checkpoint - WAL is already closed");
            return;
        }
        try {
            // putInt (4 bytes) + putLong (8 bytes) = 12 bytes.
            // This was previously allocate(8), which overflows on the putLong()
            // call and throws BufferOverflowException - caught below and silently
            // logged, so checkpoint() has never actually written a record.
            ByteBuffer buffer = ByteBuffer.allocate(12);
            buffer.putInt(OP_CHECKPOINT);
            buffer.putLong(System.currentTimeMillis());
            writeBuffer(buffer, true);
            LOG.info("Checkpoint written at LSN: {}", currentLSN.get());
        } catch (Exception e) {
            LOG.error("Failed to write checkpoint", e);
        }
    }
    
    /**
     * Write buffer to WAL
     */
    private void writeBuffer(ByteBuffer buffer, boolean force) {
        if (walChannel == null || !walChannel.isOpen()) {
            // Already known to be closed - fail fast rather than attempting the write and
            // logging a full stack trace, which would repeat on every subsequent call once
            // the WAL is in this state (a real, previously-latent issue: once the channel
            // closes for any reason mid-session - including a JVM-level ClosedByInterruptException
            // if the thread performing a slow fsync is interrupted, e.g. by a test timeout on a real,
            // slower disk - every later write silently degraded into repeated, expensive
            // full-stack-trace logging instead of failing quietly).
            LOG.warn("WAL write skipped - channel is already closed");
            return;
        }
        try {
            buffer.flip();
            long position = currentLSN.getAndAdd(buffer.limit());
            walChannel.position(position);
            walChannel.write(buffer);
            
            if (sync && force) {
                walChannel.force(false);
            }
        } catch (Exception e) {
            LOG.error("Failed to write to WAL", e);
        }
    }
    
    /**
     * Recovery from WAL - redo pass.
     *
     * Needs a DiskManager so replayed inserts/updates/deletes can actually be
     * written back to pages on disk, not just parsed and discarded.
     *
     * Two passes, standard for a WAL that logs operations before knowing
     * whether their transaction will commit:
     *   1. Scan the whole log once just to collect the set of xids with a
     *      matching OP_COMMIT record - this is what makes redo transaction-
     *      aware rather than blindly replaying everything (see below).
     *   2. Scan again and replay only INSERT/UPDATE/DELETE records whose xid
     *      is in that committed set. A transaction that crashed before
     *      COMMIT leaves its operations logged but with no matching commit
     *      record, so pass 2 correctly skips them - real atomicity, not
     *      "redo everything and hope only committed work was logged."
     *
     * This directly replaces this method's previous, explicitly documented
     * limitation ("redo cannot currently distinguish committed from never
     * committed operations... real atomicity requires threading a
     * transaction id through logInsert/logUpdate/logDelete") - that
     * threading is now in place (see logInsert/logUpdate/logDelete), which
     * is what makes this two-pass approach possible.
     *
     * Known limitation still open, stated plainly: not idempotent / not
     * LSN-gated. Redo replays every committed record unconditionally. Pages
     * are never stamped with the LSN that last modified them (pd_lsn in the
     * page header exists but is never written), so if a page was already
     * flushed before the crash, redo re-applies its writes on top and
     * duplicates them. Safe for a log that only ever describes pages that
     * were never flushed (true for a from-empty crash scenario); NOT safe
     * as a general-purpose recovery routine yet. Fixing this properly means
     * stamping each page write with its LSN and skipping WAL records whose
     * LSN is <= the page's current pd_lsn on redo - standard ARIES-style
     * redo, a separate piece of work from the transaction-awareness fixed here.
     */
    public void recover(com.stratosdb.storage.disk.DiskManager diskManager) {
        try {
            LOG.info("Starting recovery...");

            long fileSize = walChannel.size();

            java.util.Set<Long> committedXids = new java.util.HashSet<>();
            walChannel.position(0);
            while (walChannel.position() < fileSize) {
                Integer opType = readIntOrNull();
                if (opType == null) break;
                if (!skipOrCollectCommit(opType, committedXids, fileSize)) {
                    break; // unknown record type - same "stop cleanly" behavior as before
                }
            }

            java.util.Map<String, com.stratosdb.storage.page.SlottedPage> dirtyPages = new java.util.HashMap<>();
            int replayedOps = 0;

            walChannel.position(0);
            while (walChannel.position() < fileSize) {
                Integer opType = readIntOrNull();
                if (opType == null) break; // truncated/partial trailing record - stop cleanly

                switch (opType) {
                    case OP_INSERT: {
                        long xid = readLong();
                        String tableName = readLengthPrefixedString();
                        long pageId = readLong();
                        readInt(); // logged slot - redo re-derives the slot deterministically
                                   // by replaying inserts for this page in log order (see class javadoc)
                        int len = readInt();
                        byte[] tupleData = readBytes(len);

                        if (committedXids.contains(xid)) {
                            com.stratosdb.storage.page.SlottedPage page =
                                loadOrGetDirtyPage(diskManager, dirtyPages, tableName, pageId);
                            page.insertTuple(tupleData);
                            replayedOps++;
                        }
                        break;
                    }
                    case OP_DELETE: {
                        long xid = readLong();
                        String tableName = readLengthPrefixedString();
                        long pageId = readLong();
                        int slot = readInt();

                        if (committedXids.contains(xid)) {
                            com.stratosdb.storage.page.SlottedPage page =
                                loadOrGetDirtyPage(diskManager, dirtyPages, tableName, pageId);
                            page.deleteTuple(slot);
                            replayedOps++;
                        }
                        break;
                    }
                    case OP_UPDATE: {
                        long xid = readLong();
                        String tableName = readLengthPrefixedString();
                        long pageId = readLong();
                        int slot = readInt();
                        int oldLen = readInt();
                        readBytes(oldLen); // old value kept for future undo support; unused by redo
                        int newLen = readInt();
                        byte[] newData = readBytes(newLen);

                        if (committedXids.contains(xid)) {
                            com.stratosdb.storage.page.SlottedPage page =
                                loadOrGetDirtyPage(diskManager, dirtyPages, tableName, pageId);
                            page.updateTuple(slot, newData);
                            replayedOps++;
                        }
                        break;
                    }
                    case OP_COMMIT: {
                        readLong(); // transactionId - already collected in pass 1
                        break;
                    }
                    case OP_CHECKPOINT: {
                        readLong(); // timestamp
                        break;
                    }
                    default: {
                        LOG.warn("Unknown WAL record type {} at position {}; stopping recovery scan",
                            opType, walChannel.position());
                        walChannel.position(fileSize);
                    }
                }
            }

            // Flush every page touched during redo so it is durable on disk before
            // recovery is reported complete - redo that stays in memory isn't redo.
            for (java.util.Map.Entry<String, com.stratosdb.storage.page.SlottedPage> entry : dirtyPages.entrySet()) {
                String tableName = entry.getKey().substring(0, entry.getKey().lastIndexOf(':'));
                diskManager.writePage(tableName, entry.getValue());
            }

            // Now that every redone operation is durably on disk (writePage
            // above forces each one individually), the log itself is fully
            // redundant - truncate it so a LATER restart doesn't read these
            // same records again and redo them a second time. Without this,
            // recover() always replays from byte 0 with no notion of "already
            // applied past this point," so every subsequent restart -
            // whether after a clean shutdown or another crash - would
            // silently re-insert/re-update/re-delete everything this
            // recovery just did, permanently duplicating data on every
            // single restart. This is safe regardless of whether THIS
            // recovery followed a graceful shutdown or a real crash: by this
            // exact point, every redone write is independently fsynced,
            // so there's nothing left in the log that isn't already
            // reflected on disk.
            walChannel.truncate(0);
            walChannel.position(0);
            currentLSN.set(0);
            walChannel.force(true);

            LOG.info("Recovery complete: replayed {} operation(s) from {} committed transaction(s) across {} page(s)",
                replayedOps, committedXids.size(), dirtyPages.size());
        } catch (Exception e) {
            LOG.error("Recovery failed", e);
        }
    }

    /** Pass 1 of recover(): advances past a record's bytes without applying it, recording its xid into committedXids if it's an OP_COMMIT. Returns false for an unrecognized record type (caller should stop the scan). */
    private boolean skipOrCollectCommit(int opType, java.util.Set<Long> committedXids, long fileSize) throws java.io.IOException {
        switch (opType) {
            case OP_INSERT: {
                readLong(); // xid
                readLengthPrefixedString(); // tableName
                readLong(); // pageId
                readInt(); // slot
                int len = readInt();
                readBytes(len);
                return true;
            }
            case OP_DELETE: {
                readLong(); // xid
                readLengthPrefixedString(); // tableName
                readLong(); // pageId
                readInt(); // slot
                return true;
            }
            case OP_UPDATE: {
                readLong(); // xid
                readLengthPrefixedString(); // tableName
                readLong(); // pageId
                readInt(); // slot
                int oldLen = readInt();
                readBytes(oldLen);
                int newLen = readInt();
                readBytes(newLen);
                return true;
            }
            case OP_COMMIT: {
                long xid = readLong();
                committedXids.add(xid);
                return true;
            }
            case OP_CHECKPOINT: {
                readLong(); // timestamp
                return true;
            }
            default: {
                LOG.warn("Unknown WAL record type {} during pass 1 scan; stopping", opType);
                walChannel.position(fileSize);
                return false;
            }
        }
    }

    /**
     * Convenience overload for callers with no DiskManager on hand. Recovery
     * without a DiskManager cannot write anything back to disk, so this only
     * logs a warning - it exists so old call sites don't hard-fail, not because
     * it's a real recovery path. Prefer recover(DiskManager).
     */
    public void recover() {
        LOG.warn("recover() called with no DiskManager - WAL records will be parsed "
            + "but nothing can be written back to disk. Use recover(DiskManager) instead.");
    }

    private com.stratosdb.storage.page.SlottedPage loadOrGetDirtyPage(
            com.stratosdb.storage.disk.DiskManager diskManager,
            java.util.Map<String, com.stratosdb.storage.page.SlottedPage> dirtyPages,
            String tableName, long pageId) {
        String key = tableName + ":" + pageId;
        return dirtyPages.computeIfAbsent(key, k -> {
            com.stratosdb.storage.page.Page raw = diskManager.readPage(tableName, pageId);
            com.stratosdb.storage.page.SlottedPage page = new com.stratosdb.storage.page.SlottedPage(pageId);
            page.getBuffer().put(raw.getBytes());
            page.getBuffer().flip();
            return page;
        });
    }

    // --- exact-width WAL record readers ---
    // These read precisely as many bytes as each field needs, at the channel's
    // current position, so records of different lengths stay correctly aligned.
    // (The previous implementation read fixed 1024-byte chunks regardless of
    // actual record size, which desynchronized after the very first record.)

    private Integer readIntOrNull() throws java.io.IOException {
        ByteBuffer buf = ByteBuffer.allocate(4);
        int total = 0;
        while (buf.hasRemaining()) {
            int r = walChannel.read(buf);
            if (r == -1) return total == 0 ? null : throwTruncated();
            total += r;
        }
        buf.flip();
        return buf.getInt();
    }

    private int readInt() throws java.io.IOException {
        return readExact(4).getInt();
    }

    private long readLong() throws java.io.IOException {
        return readExact(8).getLong();
    }

    private byte[] readBytes(int n) throws java.io.IOException {
        if (n == 0) return new byte[0];
        ByteBuffer buf = readExact(n);
        byte[] out = new byte[n];
        buf.get(out);
        return out;
    }

    private String readLengthPrefixedString() throws java.io.IOException {
        int len = readInt();
        return new String(readBytes(len));
    }

    private ByteBuffer readExact(int n) throws java.io.IOException {
        ByteBuffer buf = ByteBuffer.allocate(n);
        while (buf.hasRemaining()) {
            int r = walChannel.read(buf);
            if (r == -1) throwTruncated();
        }
        buf.flip();
        return buf;
    }

    private <T> T throwTruncated() throws java.io.IOException {
        throw new java.io.IOException("WAL file ends mid-record (truncated write before crash)");
    }
    
    public void setSync(boolean sync) {
        this.sync = sync;
    }
    
    public long getCurrentLSN() {
        return currentLSN.get();
    }
    
    public void close() {
        if (walChannel == null || !walChannel.isOpen()) {
            return; // already closed - makes close() safe to call more than once
        }
        try {
            checkpoint(); // writes to walChannel, so this MUST run before the isOpen() check above would be false
            walChannel.force(true);
            // Truncate now that everything is durably flushed (bufferPool.close()
            // already ran before this - see StratosDB.shutdown()'s ordering) -
            // the same reasoning as recover()'s own truncation, just for the
            // graceful-shutdown path instead of the crash-recovery path. Without
            // this, even the FIRST restart after a perfectly clean shutdown would
            // redo everything in the log once, creating a one-time duplicate
            // before recover()'s own truncation ever gets a chance to prevent
            // anything further - found via testing three sequential restarts
            // with data added in the middle one, not by inspection.
            walChannel.truncate(0);
            currentLSN.set(0);
            walChannel.force(true);
            walChannel.close();
        } catch (Exception e) {
            LOG.error("Failed to close WAL", e);
        }
    }
}