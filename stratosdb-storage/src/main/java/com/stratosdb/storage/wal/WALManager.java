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
     * Log an insert operation
     */
    public void logInsert(String tableName, long pageId, int slot, byte[] tupleData) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024 + tupleData.length);
            buffer.putInt(OP_INSERT);
            
            byte[] tableBytes = tableName.getBytes();
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            
            buffer.putLong(pageId);
            buffer.putInt(slot);
            buffer.putInt(tupleData.length);
            buffer.put(tupleData);
            
            writeBuffer(buffer);
        } catch (Exception e) {
            LOG.error("Failed to log insert", e);
        }
    }
    
    /**
     * Log a delete operation
     */
    public void logDelete(String tableName, long pageId, int slot) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(256);
            buffer.putInt(OP_DELETE);
            
            byte[] tableBytes = tableName.getBytes();
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            
            buffer.putLong(pageId);
            buffer.putInt(slot);
            
            writeBuffer(buffer);
        } catch (Exception e) {
            LOG.error("Failed to log delete", e);
        }
    }
    
    /**
     * Log an update operation
     */
    public void logUpdate(String tableName, long pageId, int slot, byte[] oldData, byte[] newData) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024 + oldData.length + newData.length);
            buffer.putInt(OP_UPDATE);
            
            byte[] tableBytes = tableName.getBytes();
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            
            buffer.putLong(pageId);
            buffer.putInt(slot);
            
            buffer.putInt(oldData.length);
            buffer.put(oldData);
            
            buffer.putInt(newData.length);
            buffer.put(newData);
            
            writeBuffer(buffer);
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
            writeBuffer(buffer);
        } catch (Exception e) {
            LOG.error("Failed to log commit", e);
        }
    }
    
    /**
     * Log a checkpoint
     */
    public void checkpoint() {
        try {
            // putInt (4 bytes) + putLong (8 bytes) = 12 bytes.
            // This was previously allocate(8), which overflows on the putLong()
            // call and throws BufferOverflowException - caught below and silently
            // logged, so checkpoint() has never actually written a record.
            ByteBuffer buffer = ByteBuffer.allocate(12);
            buffer.putInt(OP_CHECKPOINT);
            buffer.putLong(System.currentTimeMillis());
            writeBuffer(buffer);
            LOG.info("Checkpoint written at LSN: {}", currentLSN.get());
        } catch (Exception e) {
            LOG.error("Failed to write checkpoint", e);
        }
    }
    
    /**
     * Write buffer to WAL
     */
    private void writeBuffer(ByteBuffer buffer) {
        try {
            buffer.flip();
            long position = currentLSN.getAndAdd(buffer.limit());
            walChannel.position(position);
            walChannel.write(buffer);
            
            if (sync) {
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
     * Known limitations (call these out rather than pretend they don't exist):
     *  - Not idempotent / not LSN-gated. Redo replays every record in the log
     *    unconditionally. Pages are never stamped with the LSN that last modified
     *    them (pd_lsn in the page header exists but is never written), so if a page
     *    was already flushed before the crash, redo re-applies its inserts on top
     *    and duplicates them. Safe for a log that only ever describes pages that
     *    were never flushed (true for a from-empty crash scenario); NOT safe as a
     *    general-purpose recovery routine yet. Fixing this properly means stamping
     *    each page write with its LSN and skipping WAL records whose LSN is <= the
     *    page's current pd_lsn on redo - standard ARIES-style redo.
     *  - Insert/update/delete records are not associated with a transaction id, so
     *    redo cannot currently distinguish "committed" from "never committed"
     *    operations - it replays everything regardless of whether a matching
     *    OP_COMMIT exists. Real atomicity requires threading a transaction id
     *    through logInsert/logUpdate/logDelete, which belongs with the
     *    transaction manager / MVCC work.
     */
    public void recover(com.stratosdb.storage.disk.DiskManager diskManager) {
        try {
            LOG.info("Starting recovery...");

            walChannel.position(0);
            long fileSize = walChannel.size();

            // Pages touched during redo, keyed by "table:pageId". Kept in memory for
            // the whole pass (mirroring what a buffer pool would hold) and flushed
            // once at the end, after every record has been replayed.
            java.util.Map<String, com.stratosdb.storage.page.SlottedPage> dirtyPages = new java.util.HashMap<>();
            int replayedOps = 0;

            while (walChannel.position() < fileSize) {
                Integer opType = readIntOrNull();
                if (opType == null) break; // truncated/partial trailing record - stop cleanly

                switch (opType) {
                    case OP_INSERT: {
                        String tableName = readLengthPrefixedString();
                        long pageId = readLong();
                        readInt(); // logged slot - redo re-derives the slot deterministically
                                   // by replaying inserts for this page in log order (see class javadoc)
                        int len = readInt();
                        byte[] tupleData = readBytes(len);

                        com.stratosdb.storage.page.SlottedPage page =
                            loadOrGetDirtyPage(diskManager, dirtyPages, tableName, pageId);
                        page.insertTuple(tupleData);
                        replayedOps++;
                        break;
                    }
                    case OP_DELETE: {
                        String tableName = readLengthPrefixedString();
                        long pageId = readLong();
                        int slot = readInt();

                        com.stratosdb.storage.page.SlottedPage page =
                            loadOrGetDirtyPage(diskManager, dirtyPages, tableName, pageId);
                        page.deleteTuple(slot);
                        replayedOps++;
                        break;
                    }
                    case OP_UPDATE: {
                        String tableName = readLengthPrefixedString();
                        long pageId = readLong();
                        int slot = readInt();
                        int oldLen = readInt();
                        readBytes(oldLen); // old value kept for future undo support; unused by redo
                        int newLen = readInt();
                        byte[] newData = readBytes(newLen);

                        com.stratosdb.storage.page.SlottedPage page =
                            loadOrGetDirtyPage(diskManager, dirtyPages, tableName, pageId);
                        page.updateTuple(slot, newData);
                        replayedOps++;
                        break;
                    }
                    case OP_COMMIT: {
                        readLong(); // transactionId - see javadoc limitation above
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

            LOG.info("Recovery complete: replayed {} operation(s) across {} page(s)",
                replayedOps, dirtyPages.size());
        } catch (Exception e) {
            LOG.error("Recovery failed", e);
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
        try {
            checkpoint();
            if (walChannel != null && walChannel.isOpen()) {
                walChannel.force(true);
                walChannel.close();
            }
        } catch (Exception e) {
            LOG.error("Failed to close WAL", e);
        }
    }
}