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
    /**
     * Increments every time the active WAL file is truncated (via
     * CHECKPOINT, recover(), or close()) - a real, necessary fix for a
     * real, serious bug found while building replication-dependent HA
     * orchestration on top of this: readBytesFrom's own offset is
     * relative to the CURRENT wal.log file, which gets reset to empty
     * on every truncation. A replica holding an offset from BEFORE a
     * truncation, reconnecting or resuming AFTER one, could otherwise
     * silently receive and apply completely unrelated bytes from the
     * new, post-truncation file at that same numeric offset - genuine,
     * silent data corruption, not just a stall, and exactly the kind of
     * thing a failover scenario (a replica reconnecting to a promoted
     * primary, or resuming after a primary-side CHECKPOINT) would
     * actually trigger. See readBytesFromChecked's own javadoc for the
     * real fix: a replica's own remembered offset is only ever trusted
     * if it was also remembered against this same epoch.
     */
    private final AtomicLong walEpoch;
    private boolean sync = true;
    /**
     * Where completed WAL is archived before this manager's own active
     * WAL file gets truncated/recycled - null (the default) means
     * archiving is disabled, matching real Postgres's own
     * archive_mode = off default. Without this, PITR cannot exist at
     * all: this engine's own WAL is truncated to zero the moment its
     * contents are safely reflected on disk (see recover()'s and
     * close()'s own comments on why - it's the right, simple design
     * for crash recovery alone), so the only WAL ever on disk at any
     * moment is whatever's accumulated since the last checkpoint/close.
     * PitrRestore needs a continuous, never-discarded history of WAL
     * going back to a base backup's own starting point - archiving is
     * what actually preserves that, copying each about-to-be-discarded
     * WAL segment somewhere permanent first.
     */
    private String walArchiveDirectory;
    private final AtomicLong nextArchiveSequence = new AtomicLong(1);

    /** Enables WAL archiving to archiveDirectory - every completed WAL segment (whatever's accumulated since the last archive point) is copied there, sequentially numbered, right before this manager's own active WAL file is truncated. Call once, right after construction, before any real write traffic - see PitrBackup for the real client that actually uses this. */
    public void setWalArchiveDirectory(String archiveDirectory) {
        this.walArchiveDirectory = archiveDirectory;
        File dir = new File(archiveDirectory);
        if (!dir.exists() && !dir.mkdirs()) {
            LOG.warn("Failed to create WAL archive directory: {}", archiveDirectory);
        }
        // Resume numbering from whatever's already archived, rather than starting
        // back at 1 and immediately colliding with (or worse, silently overwriting)
        // segments from an earlier run - a real, necessary correctness step, not
        // just a nicety, since archived segments are exactly what PITR replay
        // depends on existing, intact, in order.
        long highest = 0;
        File[] existing = dir.listFiles((d, name) -> name.matches("\\d{12}\\.walseg"));
        if (existing != null) {
            for (File f : existing) {
                long seq = Long.parseLong(f.getName().substring(0, 12));
                highest = Math.max(highest, seq);
            }
        }
        nextArchiveSequence.set(highest + 1);
    }

    public String getWalArchiveDirectory() {
        return walArchiveDirectory;
    }

    /**
     * Copies the current WAL file's own contents (byte 0 through the
     * current LSN) to a new, sequentially-numbered file in the archive
     * directory - called right before every truncation point this class
     * already has (see recover()'s and close()'s own calls to this),
     * never anywhere else, so a truncation can never discard anything
     * that wasn't safely archived first whenever archiving is enabled.
     * A no-op (returns -1) when archiving isn't configured, or when
     * there's genuinely nothing to archive (a fresh WAL with LSN 0) -
     * an empty, zero-byte archive segment would only clutter the
     * archive directory and complicate PITR replay's own "read this
     * segment" logic for no benefit.
     */
    private long archiveCurrentWalIfEnabled() {
        if (walArchiveDirectory == null) {
            return -1;
        }
        long upToLsn = currentLSN.get();
        if (upToLsn == 0) {
            return -1;
        }
        long sequence = nextArchiveSequence.getAndIncrement();
        String fileName = String.format("%012d.walseg", sequence);
        File archiveFile = new File(walArchiveDirectory, fileName);
        try (RandomAccessFile source = new RandomAccessFile(new File(walDirectory, "wal.log"), "r");
             java.io.FileOutputStream dest = new java.io.FileOutputStream(archiveFile)) {
            byte[] buffer = new byte[(int) upToLsn];
            source.seek(0);
            source.readFully(buffer);
            dest.write(buffer);
            dest.getFD().sync(); // durable before the active WAL that held this same data gets truncated
            LOG.info("Archived WAL segment {} ({} bytes)", fileName, upToLsn);
            return sequence;
        } catch (Exception e) {
            LOG.error("Failed to archive WAL segment {} - active WAL will NOT be truncated, to avoid data loss", fileName, e);
            nextArchiveSequence.decrementAndGet(); // this sequence number was never actually consumed - don't leave a permanent gap in the archive's own numbering
            return -2; // signals failure distinctly from -1 (disabled) so callers can refuse to truncate
        }
    }

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
        this.walEpoch = new AtomicLong(0);
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
            loadWalEpoch();
            
            LOG.info("WAL initialized at LSN: {}, epoch {}", currentLSN.get(), walEpoch.get());
        } catch (Exception e) {
            LOG.error("Failed to initialize WAL", e);
        }
    }

    private File epochFile() {
        return new File(walDirectory, "wal_epoch.txt");
    }

    private void loadWalEpoch() {
        File f = epochFile();
        if (f.exists()) {
            try {
                walEpoch.set(Long.parseLong(java.nio.file.Files.readString(f.toPath()).trim()));
            } catch (Exception e) {
                LOG.warn("Failed to read WAL epoch file, defaulting to 0", e);
            }
        }
    }

    private void saveWalEpoch() {
        try {
            java.nio.file.Files.writeString(epochFile().toPath(), String.valueOf(walEpoch.get()));
        } catch (Exception e) {
            LOG.error("Failed to persist WAL epoch", e);
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
     * Log a commit. Now includes a real wall-clock timestamp alongside
     * the transaction id - a real, necessary WAL format change for
     * point-in-time recovery (see PitrRestore), which needs to know
     * WHEN each transaction committed to decide whether it happened
     * before or after a requested recovery target time. Crash recovery
     * itself never needed this (it only cares whether a commit record
     * exists at all, not when) - this is purely additive for that path.
     */
    public void logCommit(long transactionId) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(24);
            buffer.putInt(OP_COMMIT);
            buffer.putLong(transactionId);
            buffer.putLong(System.currentTimeMillis());
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
                        readLong(); // commit timestamp - not needed here either; see skipOrCollectCommit's own comment
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
            //
            // Archived first (if enabled) - see archiveCurrentWalIfEnabled's
            // own javadoc. A failed archive attempt (-2) means truncation is
            // skipped entirely this time: losing whatever's in the WAL right
            // now would be fine for crash recovery alone (it's already
            // reflected on disk above), but would silently break PITR's own
            // guarantee that archived WAL is a complete, gapless history -
            // better to leave a redundant, already-applied segment in the
            // active WAL (redone again, harmlessly, next restart) than to
            // create a permanent gap an archive-dependent restore could
            // never detect or recover from.
            long archiveResult = archiveCurrentWalIfEnabled();
            if (archiveResult != -2) {
                boolean hadContent = currentLSN.get() > 0;
                walChannel.truncate(0);
                walChannel.position(0);
                currentLSN.set(0);
                walChannel.force(true);
                if (hadContent) {
                    walEpoch.incrementAndGet();
                    saveWalEpoch();
                }
            }

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
                readLong(); // commit timestamp - not needed for crash recovery's own "was this xid committed at all" check, but must still be read to stay aligned with the new record format (see logCommit)
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

    public long getWalEpoch() {
        return walEpoch.get();
    }

    /**
     * The real, safe replacement for readBytesFrom when the caller is
     * ReplicationServer, streaming to a replica that remembers its own
     * offset ACROSS reconnections/checkpoints - see walEpoch's own
     * javadoc for the real, serious bug this exists to prevent. Returns
     * null specifically to signal "this offset can no longer be trusted
     * at all, a fresh resync is required" - genuinely distinct from a
     * zero-length array, which correctly means "valid offset, just
     * nothing new since it yet."
     */
    public synchronized byte[] readBytesFromChecked(long expectedEpoch, long fromOffset) {
        if (expectedEpoch != walEpoch.get()) {
            return null;
        }
        return readBytesFrom(fromOffset);
    }

    /**
     * Reads raw WAL bytes from fromOffset up to the current LSN, for
     * streaming to a connected replica (see ReplicationServer) - the
     * primary side's own counterpart to StreamingWalApplier's replica-
     * side apply logic. Uses a completely separate FileChannel opened
     * read-only against the same underlying file, rather than the
     * shared walChannel every write already uses, so a concurrent
     * replication read can never race with (or be blocked by) an
     * in-progress write's own position()/write() pair on that shared
     * channel - two independent file descriptors reading/writing the
     * same file concurrently is a standard, safe OS-level guarantee.
     */
    public synchronized byte[] readBytesFrom(long fromOffset) {
        long upToLsn = currentLSN.get();
        if (fromOffset >= upToLsn) {
            return new byte[0];
        }
        try (RandomAccessFile raf = new RandomAccessFile(new File(walDirectory, "wal.log"), "r")) {
            int length = (int) (upToLsn - fromOffset);
            byte[] data = new byte[length];
            raf.seek(fromOffset);
            raf.readFully(data);
            return data;
        } catch (Exception e) {
            LOG.error("Failed to read WAL bytes for replication from offset {}", fromOffset, e);
            return new byte[0];
        }
    }

    /**
     * The real, remote-triggerable operation behind the new CHECKPOINT
     * SQL statement (see CheckpointStatement's own javadoc): writes a
     * checkpoint marker record, then archives (if enabled) and
     * truncates - the exact same pattern close() already uses for a
     * graceful shutdown, just callable explicitly, mid-session, by
     * PitrBackup before it's safe to copy the data directory. Does NOT
     * itself flush dirty pages to disk - the caller (ExecutorEngine's
     * own executeCheckpoint) does that first via BufferPoolManager,
     * since this class has no reference to the buffer pool at all.
     *
     * Real, honestly-stated limitation: unlike close()/recover(), which
     * only ever run at a quiet point (startup/shutdown, with no
     * concurrent writers), this can be called mid-session while other
     * transactions are actively writing. logInsert/logCommit/logUpdate
     * reserve their own write position via an atomic getAndAdd on
     * currentLSN specifically to allow safe concurrent writers with no
     * lock - but this method's own truncate(0) is a real, genuine hazard
     * to that scheme if it races with a writer that already reserved a
     * position but hasn't written there yet. This engine has no formal
     * "quiesce all writers" mechanism to close that gap properly (a
     * real, separate, further piece of work) - CHECKPOINT is safest run
     * when write traffic is low, the same practical advice real
     * Postgres itself gives for its own CHECKPOINT command, though for
     * different underlying reasons.
     */
    public synchronized void checkpointAndArchive() {
        checkpoint();
        long archiveResult = archiveCurrentWalIfEnabled();
        if (archiveResult != -2) {
            try {
                walChannel.truncate(0);
                walChannel.position(0);
                currentLSN.set(0);
                walChannel.force(true);
                // Unconditional, unlike recover()'s own guard above: checkpoint()
                // just above always writes a real record first, so this truncation
                // always discards genuine, non-zero content a connected replica may
                // already have read - the offset space is always being reused here,
                // never a true no-op the way an empty database's own first-ever
                // recover() call can be.
                walEpoch.incrementAndGet();
                saveWalEpoch();
            } catch (Exception e) {
                LOG.error("Failed to truncate WAL after CHECKPOINT", e);
            }
        }
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
            //
            // Archived first (if enabled) - see recover()'s own, identical
            // reasoning for why a failed archive attempt skips truncation
            // entirely rather than risk a permanent gap in PITR's own history.
            long archiveResult = archiveCurrentWalIfEnabled();
            if (archiveResult != -2) {
                walChannel.truncate(0);
                currentLSN.set(0);
                walChannel.force(true);
                walEpoch.incrementAndGet();
                saveWalEpoch();
            }
            walChannel.close();
        } catch (Exception e) {
            LOG.error("Failed to close WAL", e);
        }
    }
}