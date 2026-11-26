package com.stratosdb.storage.wal;

import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.BTreePage;
import com.stratosdb.storage.page.Page;
import com.stratosdb.storage.page.SlottedPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Segmented 16MB Write-Ahead Log for Durability.
 *
 * Implements:
 *   - Segmented WAL naming convention: wal_00000001.log, wal_00000002.log, etc.
 *   - Continuous WAL segment rotation at 16MB boundary.
 *   - Seamless multi-segment writes that span across 16MB files without truncation.
 *   - Background asynchronous segment pre-allocation.
 *   - Configurable wal_keep_segments retention policy preserving old segments for archiving and PITR.
 *   - Full backward compatibility with legacy unsegmented wal.log files.
 */
public class WALManager {
    private static final Logger LOG = LoggerFactory.getLogger(WALManager.class);

    public static final long DEFAULT_SEGMENT_SIZE = 16L * 1024 * 1024; // 16MB
    public static final int DEFAULT_WAL_KEEP_SEGMENTS = 16;
    public static final String SEGMENT_PREFIX = "wal_";
    public static final String SEGMENT_SUFFIX = ".log";

    private final String walDirectory;
    private final long segmentSize;
    private int walKeepSegments;
    private boolean preallocateSegments = true;

    private final AtomicLong currentLSN;
    private final AtomicLong walEpoch;
    private final AtomicLong lastCheckpointLSN;
    private final AtomicLong syncdLSN = new AtomicLong(0);
    private final Object appendLock = new Object();
    private final Object syncLock = new Object();
    private boolean sync = true;
    private volatile boolean closed = false;

    private long currentActiveSegNum = -1L;
    private RandomAccessFile activeRaf;
    private FileChannel activeChannel;

    private final Set<Long> preallocatedSegments = ConcurrentHashMap.newKeySet();
    private final ExecutorService preallocator;

    private static final int DIRECT_WAL_BUFFER_CAPACITY = 64 * 1024;
    private static final ThreadLocal<ByteBuffer> DIRECT_RECORD_BUFFER = ThreadLocal.withInitial(() ->
        ByteBuffer.allocateDirect(DIRECT_WAL_BUFFER_CAPACITY)
    );

    private String walArchiveDirectory;
    private final AtomicLong nextArchiveSequence = new AtomicLong(1);
    private final AtomicLong lastArchivedLSN = new AtomicLong(0);

    // Group Commit Coalescing Engine
    private boolean groupCommitEnabled = true;
    private int groupCommitMaxBatchSize = GroupCommitRingBuffer.DEFAULT_MAX_BATCH_SIZE;
    private long groupCommitDelayMicros = GroupCommitRingBuffer.DEFAULT_DELAY_MICROS;
    private GroupCommitRingBuffer groupCommitCoordinator;

    // Operation types
    public static final int OP_INSERT = 1;
    public static final int OP_UPDATE = 2;
    public static final int OP_DELETE = 3;
    public static final int OP_COMMIT = 4;
    public static final int OP_ABORT = 5;
    public static final int OP_CHECKPOINT = 6;
    public static final int OP_INDEX_INSERT = 7;
    public static final int OP_INDEX_SPLIT = 8;
    public static final int OP_INDEX_DELETE = 9;
    public static final int OP_INDEX_MERGE = 10;

    public WALManager(String dataDirectory) {
        this(dataDirectory, DEFAULT_SEGMENT_SIZE, DEFAULT_WAL_KEEP_SEGMENTS, true);
    }

    public WALManager(String dataDirectory, long segmentSize, int walKeepSegments) {
        this(dataDirectory, segmentSize, walKeepSegments, true);
    }

    public WALManager(String dataDirectory, long segmentSize, int walKeepSegments, boolean preallocateSegments) {
        this(dataDirectory, segmentSize, walKeepSegments, preallocateSegments, true,
            GroupCommitRingBuffer.DEFAULT_MAX_BATCH_SIZE, GroupCommitRingBuffer.DEFAULT_DELAY_MICROS);
    }

    public WALManager(String dataDirectory, long segmentSize, int walKeepSegments, boolean preallocateSegments,
                      boolean groupCommitEnabled, int groupCommitMaxBatchSize, long groupCommitDelayMicros) {
        this.walDirectory = dataDirectory + "/wal";
        this.segmentSize = segmentSize > 0 ? segmentSize : DEFAULT_SEGMENT_SIZE;
        this.walKeepSegments = walKeepSegments >= 0 ? walKeepSegments : DEFAULT_WAL_KEEP_SEGMENTS;
        this.preallocateSegments = preallocateSegments;
        this.groupCommitEnabled = groupCommitEnabled;
        this.groupCommitMaxBatchSize = groupCommitMaxBatchSize > 0 ? groupCommitMaxBatchSize : GroupCommitRingBuffer.DEFAULT_MAX_BATCH_SIZE;
        this.groupCommitDelayMicros = groupCommitDelayMicros >= 0 ? groupCommitDelayMicros : GroupCommitRingBuffer.DEFAULT_DELAY_MICROS;
        this.currentLSN = new AtomicLong(0);
        this.walEpoch = new AtomicLong(0);
        this.lastCheckpointLSN = new AtomicLong(0);

        this.preallocator = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "wal-preallocator");
            t.setDaemon(true);
            return t;
        });

        initialize();
    }

    public static String formatSegmentFileName(long segmentNumber) {
        return String.format("%s%08d%s", SEGMENT_PREFIX, segmentNumber, SEGMENT_SUFFIX);
    }

    public static boolean isSegmentFileName(String name) {
        return name != null && name.matches("^wal_\\d{8}\\.log$");
    }

    public static long parseSegmentNumber(String name) {
        if (isSegmentFileName(name)) {
            String numStr = name.substring(SEGMENT_PREFIX.length(), name.length() - SEGMENT_SUFFIX.length());
            try {
                return Long.parseLong(numStr);
            } catch (NumberFormatException ignored) {}
        }
        return -1L;
    }

    public long getSegmentNumberForLsn(long lsn) {
        return (lsn / segmentSize) + 1;
    }

    public long getSegmentOffsetForLsn(long lsn) {
        return lsn % segmentSize;
    }

    public File getSegmentFile(long segmentNumber) {
        return new File(walDirectory, formatSegmentFileName(segmentNumber));
    }

    private void initialize() {
        try {
            File dir = new File(walDirectory);
            if (!dir.exists() && !dir.mkdirs()) {
                LOG.warn("Failed to create WAL directory: {}", walDirectory);
            }

            // Check for legacy unsegmented wal.log and migrate if needed
            File legacyWal = new File(walDirectory, "wal.log");
            File firstSegment = getSegmentFile(1);
            if (legacyWal.exists() && !firstSegment.exists()) {
                if (legacyWal.renameTo(firstSegment)) {
                    LOG.info("Migrated legacy wal.log to {}", firstSegment.getName());
                }
            }

            // Load control file metadata if present
            loadWalControl();

            // Detect existing segments
            List<Long> existingSegs = getExistingSegmentNumbers();
            if (existingSegs.isEmpty()) {
                // Fresh WAL initialization starting at segment 1
                ensureActiveSegment(1);
                currentLSN.set(0);
            } else {
                long highestSeg = existingSegs.get(existingSegs.size() - 1);
                File highestFile = getSegmentFile(highestSeg);
                long fileLen = highestFile.exists() ? highestFile.length() : 0;

                long calculatedLsn = (highestSeg - 1) * segmentSize + fileLen;
                if (!preallocateSegments) {
                    currentLSN.set(Math.max(currentLSN.get(), calculatedLsn));
                } else if (currentLSN.get() == 0 && calculatedLsn > 0) {
                    currentLSN.set(calculatedLsn);
                }

                long activeSeg = getSegmentNumberForLsn(currentLSN.get());
                ensureActiveSegment(activeSeg);
            }

            // Pre-allocate next segment in background
            schedulePreallocation(getSegmentNumberForLsn(currentLSN.get()) + 1);

            if (groupCommitEnabled && groupCommitCoordinator == null) {
                this.groupCommitCoordinator = new GroupCommitRingBuffer(this,
                    GroupCommitRingBuffer.DEFAULT_RING_CAPACITY,
                    this.groupCommitMaxBatchSize,
                    this.groupCommitDelayMicros);
            }

            LOG.info("Segmented WAL initialized: active segment {}, LSN {}, epoch {}, segmentSize {}MB, wal_keep_segments {}",
                formatSegmentFileName(getSegmentNumberForLsn(currentLSN.get())),
                currentLSN.get(), walEpoch.get(), segmentSize / (1024 * 1024), walKeepSegments);
        } catch (Exception e) {
            LOG.error("Failed to initialize segmented WAL", e);
        }
    }

    public List<Long> getExistingSegmentNumbers() {
        File dir = new File(walDirectory);
        File[] files = dir.listFiles((d, name) -> isSegmentFileName(name));
        List<Long> segs = new ArrayList<>();
        if (files != null) {
            for (File f : files) {
                long num = parseSegmentNumber(f.getName());
                if (num > 0) {
                    segs.add(num);
                }
            }
        }
        Collections.sort(segs);
        return segs;
    }

    private synchronized void ensureActiveSegment(long segmentNumber) throws IOException {
        if (currentActiveSegNum == segmentNumber && activeChannel != null && activeChannel.isOpen()) {
            return;
        }

        if (activeChannel != null && activeChannel.isOpen()) {
            activeChannel.force(true);
            activeChannel.close();
        }
        if (activeRaf != null) {
            activeRaf.close();
        }

        File segFile = getSegmentFile(segmentNumber);
        if (!segFile.exists()) {
            File prealloc = new File(walDirectory, formatSegmentFileName(segmentNumber) + ".prealloc");
            if (prealloc.exists()) {
                prealloc.renameTo(segFile);
            } else {
                segFile.createNewFile();
            }
        }

        this.activeRaf = new RandomAccessFile(segFile, "rw");
        this.activeChannel = activeRaf.getChannel();
        this.currentActiveSegNum = segmentNumber;
        preallocatedSegments.remove(segmentNumber);

        // Keep legacy wal.log synchronized or linked for segment 1 if needed
        syncLegacyWalFileIfNeeded();
    }

    private void syncLegacyWalFileIfNeeded() {
        if (currentActiveSegNum == 1) {
            try {
                File legacy = new File(walDirectory, "wal.log");
                File seg1 = getSegmentFile(1);
                if (seg1.exists()) {
                    if (legacy.exists()) {
                        legacy.delete();
                    }
                    try {
                        Files.createLink(legacy.toPath(), seg1.toPath());
                    } catch (Exception e) {
                        // On platforms where hardlinks are restricted, copy on flush/close
                    }
                }
            } catch (Exception ignored) {}
        }
    }

    private void schedulePreallocation(long segmentNumber) {
        if (!preallocateSegments || segmentNumber <= 0 || closed) {
            return;
        }
        preallocator.submit(() -> {
            try {
                preallocateSegment(segmentNumber);
            } catch (Exception e) {
                LOG.debug("Background pre-allocation of segment {} failed", segmentNumber, e);
            }
        });
    }

    public synchronized void preallocateSegment(long segmentNumber) throws IOException {
        File segFile = getSegmentFile(segmentNumber);
        if (segFile.exists() && segFile.length() >= segmentSize) {
            preallocatedSegments.add(segmentNumber);
            return;
        }
        File preallocFile = new File(walDirectory, formatSegmentFileName(segmentNumber) + ".prealloc");
        try (RandomAccessFile raf = new RandomAccessFile(preallocFile, "rw")) {
            raf.setLength(segmentSize);
        }
        if (!preallocFile.renameTo(segFile)) {
            if (!segFile.exists()) {
                try (RandomAccessFile raf = new RandomAccessFile(segFile, "rw")) {
                    raf.setLength(segmentSize);
                }
            }
            preallocFile.delete();
        }
        preallocatedSegments.add(segmentNumber);
        LOG.debug("Pre-allocated WAL segment {} ({} bytes)", segFile.getName(), segmentSize);
    }

    public boolean isSegmentPreallocated(long segmentNumber) {
        File segFile = getSegmentFile(segmentNumber);
        return (segFile.exists() && segFile.length() >= segmentSize) || preallocatedSegments.contains(segmentNumber);
    }

    private File controlFile() {
        return new File(walDirectory, "wal_control.txt");
    }

    private void loadWalControl() {
        File f = controlFile();
        if (f.exists()) {
            try {
                List<String> lines = Files.readAllLines(f.toPath());
                for (String line : lines) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        String k = parts[0].trim();
                        String v = parts[1].trim();
                        switch (k) {
                            case "current_lsn" -> currentLSN.set(Long.parseLong(v));
                            case "checkpoint_lsn" -> lastCheckpointLSN.set(Long.parseLong(v));
                            case "wal_epoch" -> walEpoch.set(Long.parseLong(v));
                            case "wal_keep_segments" -> this.walKeepSegments = Integer.parseInt(v);
                            case "last_archived_lsn" -> lastArchivedLSN.set(Long.parseLong(v));
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to read WAL control file", e);
            }
        }
    }

    private void saveWalControl() {
        try {
            String content = "current_lsn=" + currentLSN.get() + "\n"
                + "checkpoint_lsn=" + lastCheckpointLSN.get() + "\n"
                + "wal_epoch=" + walEpoch.get() + "\n"
                + "segment_size=" + segmentSize + "\n"
                + "wal_keep_segments=" + walKeepSegments + "\n"
                + "last_archived_lsn=" + lastArchivedLSN.get() + "\n";
            Files.writeString(controlFile().toPath(), content);
        } catch (Exception e) {
            LOG.debug("Failed to persist WAL control file", e);
        }
    }

    private ByteBuffer getDirectRecordBuffer(int requiredCapacity) {
        if (requiredCapacity <= DIRECT_WAL_BUFFER_CAPACITY) {
            ByteBuffer buf = DIRECT_RECORD_BUFFER.get();
            buf.clear();
            return buf;
        }
        return ByteBuffer.allocateDirect(requiredCapacity);
    }

    public void setWalArchiveDirectory(String archiveDirectory) {
        this.walArchiveDirectory = archiveDirectory;
        File dir = new File(archiveDirectory);
        if (!dir.exists() && !dir.mkdirs()) {
            LOG.warn("Failed to create WAL archive directory: {}", archiveDirectory);
        }
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

    public synchronized long archiveSegmentIfEnabled(long segmentNumber) {
        if (walArchiveDirectory == null) {
            return -1;
        }
        return archiveCurrentWalIfEnabled();
    }

    private synchronized long archiveCurrentWalIfEnabled() {
        if (walArchiveDirectory == null) {
            return -1;
        }
        long upToLsn = currentLSN.get();
        long fromLsn = lastArchivedLSN.get();
        if (upToLsn <= fromLsn) {
            return -1;
        }

        byte[] newWalBytes = readBytesFrom(fromLsn);
        if (newWalBytes == null || newWalBytes.length == 0) {
            return -1;
        }

        long sequence = nextArchiveSequence.getAndIncrement();
        String fileName = String.format("%012d.walseg", sequence);
        File archiveFile = new File(walArchiveDirectory, fileName);
        try (java.io.FileOutputStream dest = new java.io.FileOutputStream(archiveFile)) {
            dest.write(newWalBytes);
            dest.getFD().sync();
            lastArchivedLSN.set(upToLsn);
            saveWalControl();
            LOG.info("Archived active WAL bytes as {} ({} bytes, LSN {} to {})", fileName, newWalBytes.length, fromLsn, upToLsn);
            return sequence;
        } catch (Exception e) {
            LOG.error("Failed to archive active WAL segment {}", fileName, e);
            nextArchiveSequence.decrementAndGet();
            return -2;
        }
    }

    // --- Write Methods ---

    public long logInsert(String tableName, long xid, long pageId, int slot, byte[] tupleData) {
        try {
            ByteBuffer buffer = getDirectRecordBuffer(1024 + tupleData.length);
            buffer.putInt(OP_INSERT);
            buffer.putLong(xid);
            byte[] tableBytes = tableName.getBytes();
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            buffer.putLong(pageId);
            buffer.putInt(slot);
            buffer.putInt(tupleData.length);
            buffer.put(tupleData);
            return writeBuffer(buffer, false);
        } catch (Exception e) {
            LOG.error("Failed to log insert", e);
            return -1L;
        }
    }

    public long logDelete(String tableName, long xid, long pageId, int slot) {
        try {
            ByteBuffer buffer = getDirectRecordBuffer(256);
            buffer.putInt(OP_DELETE);
            buffer.putLong(xid);
            byte[] tableBytes = tableName.getBytes();
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            buffer.putLong(pageId);
            buffer.putInt(slot);
            return writeBuffer(buffer, false);
        } catch (Exception e) {
            LOG.error("Failed to log delete", e);
            return -1L;
        }
    }

    public long logUpdate(String tableName, long xid, long pageId, int slot, byte[] oldData, byte[] newData) {
        try {
            ByteBuffer buffer = getDirectRecordBuffer(1024 + oldData.length + newData.length);
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
            return writeBuffer(buffer, false);
        } catch (Exception e) {
            LOG.error("Failed to log update", e);
            return -1L;
        }
    }

    public long logCommit(long transactionId) {
        long commitLsn;
        if (!sync || !groupCommitEnabled || groupCommitCoordinator == null) {
            try {
                ByteBuffer buffer = getDirectRecordBuffer(24);
                buffer.putInt(OP_COMMIT);
                buffer.putLong(transactionId);
                buffer.putLong(System.currentTimeMillis());
                commitLsn = writeBuffer(buffer, true);
            } catch (Exception e) {
                LOG.error("Failed to log commit", e);
                return -1L;
            }
        } else {
            commitLsn = groupCommitCoordinator.commit(transactionId);
        }
        SyncReplicationWaiter waiter = this.syncReplicationWaiter;
        if (commitLsn > 0 && waiter != null && synchronousCommitMode.requiresRemoteAck()) {
            waiter.awaitSyncAck(commitLsn);
        }
        return commitLsn;
    }

    /**
     * Writes a batch of group-committed records to the WAL.
     * The buffer contains multiple consecutive commit records (each 20 bytes: OP_COMMIT + xid + timestamp).
     * Automatically handles segment boundaries, forces to disk (fsync), and returns the new LSN.
     *
     * @param buffer Direct ByteBuffer containing prepared commit records
     * @return the end LSN of the written batch
     */
    public long writeGroupCommitBatch(ByteBuffer buffer) {
        return writeBuffer(buffer, true);
    }

    public void checkpoint() {
        if (closed) {
            LOG.debug("Skipping checkpoint - WAL is already closed");
            return;
        }
        try {
            ByteBuffer buffer = getDirectRecordBuffer(12);
            buffer.putInt(OP_CHECKPOINT);
            buffer.putLong(System.currentTimeMillis());
            long cpLsn = writeBuffer(buffer, true);
            lastCheckpointLSN.set(cpLsn);
            saveWalControl();
            pruneOldSegments();
            LOG.info("Checkpoint written at LSN: {}", cpLsn);
        } catch (Exception e) {
            LOG.error("Failed to write checkpoint", e);
        }
    }

    public long logIndexInsert(String indexName, long xid, long pageId, long key, long ridPageId, int ridSlot) {
        try {
            byte[] tableBytes = indexName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ByteBuffer buffer = getDirectRecordBuffer(64 + tableBytes.length);
            buffer.putInt(OP_INDEX_INSERT);
            buffer.putLong(xid);
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            buffer.putLong(pageId);
            buffer.putLong(key);
            buffer.putLong(ridPageId);
            buffer.putInt(ridSlot);
            return writeBuffer(buffer, xid == 0L);
        } catch (Exception e) {
            LOG.error("Failed to log index insert", e);
            return -1L;
        }
    }

    public long logIndexDelete(String indexName, long xid, long pageId, long key, long ridPageId, int ridSlot) {
        try {
            byte[] tableBytes = indexName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ByteBuffer buffer = getDirectRecordBuffer(64 + tableBytes.length);
            buffer.putInt(OP_INDEX_DELETE);
            buffer.putLong(xid);
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            buffer.putLong(pageId);
            buffer.putLong(key);
            buffer.putLong(ridPageId);
            buffer.putInt(ridSlot);
            return writeBuffer(buffer, xid == 0L);
        } catch (Exception e) {
            LOG.error("Failed to log index delete", e);
            return -1L;
        }
    }

    public long logIndexSplit(String indexName, long xid, boolean isLeaf,
                              long pageId, long newPageId, long splitKey,
                              long nextLeafPageId, long parentPageId, long newRootId,
                              List<Long> rightKeys, List<BTreePage.RID> rightValues, List<Long> rightChildren,
                              List<Long> leftKeys, List<BTreePage.RID> leftValues, List<Long> leftChildren) {
        try {
            byte[] tableBytes = indexName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            int rCount = rightKeys != null ? rightKeys.size() : 0;
            int rightSize = isLeaf ? (rCount * 20) : (rCount * 8 + (rightChildren != null ? rightChildren.size() * 8 : 0));
            int lCount = leftKeys != null ? leftKeys.size() : 0;
            int leftSize = isLeaf ? (lCount * 20) : (lCount * 8 + (leftChildren != null ? leftChildren.size() * 8 : 0));

            ByteBuffer buffer = getDirectRecordBuffer(256 + tableBytes.length + rightSize + leftSize);
            buffer.putInt(OP_INDEX_SPLIT);
            buffer.putLong(xid);
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            buffer.putInt(isLeaf ? 1 : 0);
            buffer.putLong(pageId);
            buffer.putLong(newPageId);
            buffer.putLong(splitKey);
            buffer.putLong(nextLeafPageId);
            buffer.putLong(parentPageId);
            buffer.putLong(newRootId);

            buffer.putInt(rCount);
            if (isLeaf) {
                for (int i = 0; i < rCount; i++) {
                    buffer.putLong(rightKeys.get(i));
                    buffer.putLong(rightValues.get(i).pageId());
                    buffer.putInt(rightValues.get(i).slot());
                }
            } else {
                for (int i = 0; i < rCount; i++) buffer.putLong(rightKeys.get(i));
                if (rightChildren != null) {
                    for (long c : rightChildren) buffer.putLong(c);
                }
            }

            buffer.putInt(lCount);
            if (isLeaf) {
                for (int i = 0; i < lCount; i++) {
                    buffer.putLong(leftKeys.get(i));
                    buffer.putLong(leftValues.get(i).pageId());
                    buffer.putInt(leftValues.get(i).slot());
                }
            } else {
                for (int i = 0; i < lCount; i++) buffer.putLong(leftKeys.get(i));
                if (leftChildren != null) {
                    for (long c : leftChildren) buffer.putLong(c);
                }
            }

            return writeBuffer(buffer, true);
        } catch (Exception e) {
            LOG.error("Failed to log index split", e);
            return -1L;
        }
    }

    public long logIndexMerge(String indexName, long xid, boolean isLeaf,
                              long parentPageId, long leftPageId, long rightPageId,
                              long separatorKey, long rightNextLeafPageId,
                              boolean collapsedRoot, long newRootId,
                              List<Long> mergedKeys, List<BTreePage.RID> mergedValues, List<Long> mergedChildren,
                              List<Long> parentKeys, List<Long> parentChildren) {
        try {
            byte[] tableBytes = indexName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            int mCount = mergedKeys != null ? mergedKeys.size() : 0;
            int leftSize = isLeaf ? (mCount * 20) : (mCount * 8 + (mergedChildren != null ? mergedChildren.size() * 8 : 0));
            int pKeyCount = parentKeys != null ? parentKeys.size() : 0;
            int pChildCount = parentChildren != null ? parentChildren.size() : 0;
            int parentSize = pKeyCount * 8 + pChildCount * 8;

            ByteBuffer buffer = getDirectRecordBuffer(256 + tableBytes.length + leftSize + parentSize);
            buffer.putInt(OP_INDEX_MERGE);
            buffer.putLong(xid);
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            buffer.putInt(isLeaf ? 1 : 0);
            buffer.putLong(parentPageId);
            buffer.putLong(leftPageId);
            buffer.putLong(rightPageId);
            buffer.putLong(separatorKey);
            buffer.putLong(rightNextLeafPageId);
            buffer.putInt(collapsedRoot ? 1 : 0);
            buffer.putLong(newRootId);

            buffer.putInt(mCount);
            if (isLeaf) {
                for (int i = 0; i < mCount; i++) {
                    buffer.putLong(mergedKeys.get(i));
                    buffer.putLong(mergedValues.get(i).pageId());
                    buffer.putInt(mergedValues.get(i).slot());
                }
            } else {
                for (int i = 0; i < mCount; i++) buffer.putLong(mergedKeys.get(i));
                if (mergedChildren != null) {
                    for (long c : mergedChildren) buffer.putLong(c);
                }
            }

            buffer.putInt(pKeyCount);
            if (parentKeys != null) {
                for (long k : parentKeys) buffer.putLong(k);
            }
            if (parentChildren != null) {
                for (long c : parentChildren) buffer.putLong(c);
            }

            return writeBuffer(buffer, true);
        } catch (Exception e) {
            LOG.error("Failed to log index merge", e);
            return -1L;
        }
    }

    /**
     * Appends buffer contents to the segmented WAL across segment boundaries.
     * Takes ~1µs in OS page cache. Does NOT fsync.
     */
    public long appendBuffer(ByteBuffer buffer) {
        if (closed) {
            LOG.warn("WAL write skipped - channel is already closed");
            return -1L;
        }
        synchronized (appendLock) {
            try {
                buffer.flip();
                if (tdeCipher != null) {
                    byte[] plain = new byte[buffer.remaining()];
                    buffer.get(plain);
                    byte[] enc = tdeCipher.encrypt(plain);
                    int frameLen = 4 + enc.length;
                    ByteBuffer frame = ByteBuffer.allocate(frameLen);
                    frame.putInt(enc.length);
                    frame.put(enc);
                    frame.flip();

                    long lsn = currentLSN.get();
                    long segNum = getSegmentNumberForLsn(lsn);
                    long segOffset = getSegmentOffsetForLsn(lsn);
                    if (segOffset + frameLen > segmentSize) {
                        rotateToNextSegment(segNum);
                        lsn = currentLSN.get();
                        segNum = getSegmentNumberForLsn(lsn);
                        segOffset = getSegmentOffsetForLsn(lsn);
                    }
                    ensureActiveSegment(segNum);
                    activeChannel.position(segOffset);
                    while (frame.hasRemaining()) {
                        activeChannel.write(frame);
                    }
                    currentLSN.addAndGet(frameLen);
                    if (segOffset + frameLen >= segmentSize) {
                        rotateToNextSegment(segNum);
                    }
                    return lsn + frameLen;
                }
                int totalBytes = buffer.remaining();
                long recordStartLsn = currentLSN.get();

                while (buffer.hasRemaining()) {
                    long lsn = currentLSN.get();
                    long segNum = getSegmentNumberForLsn(lsn);
                    long segOffset = getSegmentOffsetForLsn(lsn);
                    long bytesRemainingInSeg = segmentSize - segOffset;

                    ensureActiveSegment(segNum);

                    int bytesToWrite = (int) Math.min(buffer.remaining(), bytesRemainingInSeg);
                    int origLimit = buffer.limit();
                    buffer.limit(buffer.position() + bytesToWrite);

                    activeChannel.position(segOffset);
                    while (buffer.hasRemaining()) {
                        activeChannel.write(buffer);
                    }
                    buffer.limit(origLimit);

                    currentLSN.addAndGet(bytesToWrite);

                    // Continuous rotation when segment boundary reached
                    if (segOffset + bytesToWrite >= segmentSize) {
                        rotateToNextSegment(segNum);
                    }
                }
                return recordStartLsn + totalBytes;
            } catch (Exception e) {
                LOG.error("Failed to append to WAL", e);
                return -1L;
            }
        }
    }

    /**
     * Ensures all WAL records up to at least targetLsn are durable on disk.
     * Multiple concurrent sync requests coalesce into a single fsync.
     */
    public void syncWal(long targetLsn) {
        if (!sync || closed) {
            return;
        }
        if (syncdLSN.get() >= targetLsn) {
            return;
        }
        synchronized (syncLock) {
            if (syncdLSN.get() >= targetLsn) {
                return;
            }
            try {
                FileChannel ch = activeChannel;
                if (ch != null && ch.isOpen()) {
                    ch.force(false);
                }
                long flushedUpTo = currentLSN.get();
                syncdLSN.accumulateAndGet(flushedUpTo, Math::max);
            } catch (Exception e) {
                LOG.error("Failed to fsync WAL", e);
            }
        }
    }

    /**
     * Seamless multi-segment write. Spans records across 16MB segment boundaries without truncation.
     */
    private long writeBuffer(ByteBuffer buffer, boolean force) {
        long endLsn = appendBuffer(buffer);
        if (force && endLsn > 0) {
            syncWal(endLsn);
        }
        if (endLsn > 0) {
            synchronized (walPushMonitor) {
                walPushMonitor.notifyAll();
            }
        }
        return endLsn;
    }

    /** Callback interface for synchronous streaming replication quorum ACK wait at commit. */
    public interface SyncReplicationWaiter {
        void awaitSyncAck(long commitLsn);
    }

    private final Object walPushMonitor = new Object();
    private volatile SynchronousCommitMode synchronousCommitMode = SynchronousCommitMode.LOCAL;
    private volatile SyncReplicationWaiter syncReplicationWaiter;
    private volatile com.stratosdb.storage.crypto.TdeCipher tdeCipher;

    public void setTdeCipher(com.stratosdb.storage.crypto.TdeCipher tdeCipher) {
        this.tdeCipher = tdeCipher;
    }

    public com.stratosdb.storage.crypto.TdeCipher getTdeCipher() {
        return tdeCipher;
    }

    public void setSynchronousCommitMode(SynchronousCommitMode mode) {
        this.synchronousCommitMode = mode != null ? mode : SynchronousCommitMode.LOCAL;
    }

    public SynchronousCommitMode getSynchronousCommitMode() {
        return synchronousCommitMode;
    }

    public void setSyncReplicationWaiter(SyncReplicationWaiter waiter) {
        this.syncReplicationWaiter = waiter;
    }

    /** Blocks until WAL advances past {@code knownLsn} or {@code timeoutMs} elapses. */
    public void awaitWalAdvance(long knownLsn, long timeoutMs) throws InterruptedException {
        if (currentLSN.get() > knownLsn) {
            return;
        }
        synchronized (walPushMonitor) {
            if (currentLSN.get() <= knownLsn) {
                walPushMonitor.wait(Math.max(1L, timeoutMs));
            }
        }
    }

    /**
     * Rotates from completed segment to next segment, archiving and pruning according to retention policy.
     */
    private synchronized void rotateToNextSegment(long completedSegmentNumber) {
        try {
            if (activeChannel != null && activeChannel.isOpen()) {
                activeChannel.force(true);
            }

            // Archive completed segment continuously
            archiveSegmentIfEnabled(completedSegmentNumber);

            // Prune segments beyond wal_keep_segments
            pruneOldSegments();

            // Open next segment
            long nextSegNum = completedSegmentNumber + 1;
            ensureActiveSegment(nextSegNum);

            // Schedule background pre-allocation for next + 1
            schedulePreallocation(nextSegNum + 1);

            saveWalControl();

            LOG.info("Continuous WAL rotation: completed segment {}, active segment is now {}",
                formatSegmentFileName(completedSegmentNumber),
                formatSegmentFileName(nextSegNum));
        } catch (Exception e) {
            LOG.error("Error during WAL rotation for segment {}", completedSegmentNumber, e);
        }
    }

    /**
     * Explicit segment rotation trigger (equivalent to PostgreSQL pg_switch_wal()).
     */
    public synchronized void rotateSegment() {
        long curLsn = currentLSN.get();
        long segNum = getSegmentNumberForLsn(curLsn);
        long nextLsn = segNum * segmentSize;
        currentLSN.set(nextLsn);
        rotateToNextSegment(segNum);
    }

    /**
     * Prunes segments older than wal_keep_segments and prior to the last checkpoint.
     */
    public synchronized void pruneOldSegments() {
        if (walKeepSegments <= 0) {
            return;
        }
        long activeSeg = getSegmentNumberForLsn(currentLSN.get());
        long checkpointSeg = getSegmentNumberForLsn(lastCheckpointLSN.get());

        long minSegToKeep = Math.min(checkpointSeg, Math.max(1L, activeSeg - walKeepSegments + 1));

        File dir = new File(walDirectory);
        File[] segmentFiles = dir.listFiles((d, name) -> isSegmentFileName(name));
        if (segmentFiles != null) {
            for (File f : segmentFiles) {
                long segNum = parseSegmentNumber(f.getName());
                if (segNum > 0 && segNum < minSegToKeep) {
                    boolean deleted = f.delete();
                    if (deleted) {
                        preallocatedSegments.remove(segNum);
                        LOG.info("Pruned old WAL segment {} (wal_keep_segments retention threshold: {})", f.getName(), minSegToKeep);
                    }
                }
            }
        }
    }

    // --- Recovery ---

    public void recover(DiskManager diskManager) {
        try {
            LOG.info("Starting segmented WAL recovery...");

            List<Long> segNumbers = getExistingSegmentNumbers();
            if (segNumbers.isEmpty()) {
                LOG.info("No WAL segments found to recover.");
                return;
            }

            long highestSeg = segNumbers.get(segNumbers.size() - 1);
            File highestFile = getSegmentFile(highestSeg);
            long fileLen = highestFile.exists() ? highestFile.length() : 0;
            long maxPossibleLsn = (highestSeg - 1) * segmentSize + fileLen;

            long startLsn = 0;
            long endLsn = Math.max(currentLSN.get(), maxPossibleLsn);

            Set<Long> committedXids = new HashSet<>();
            long[] maxCheckpointLsn = new long[]{lastCheckpointLSN.get()};
            try (SegmentedWalReader pass1Reader = new SegmentedWalReader(new File(walDirectory), segmentSize, segNumbers, startLsn, endLsn, tdeCipher)) {
                while (pass1Reader.hasRemaining()) {
                    Integer opType;
                    try {
                        opType = pass1Reader.readIntOrNull();
                    } catch (IOException e) {
                        LOG.debug("WAL ended mid-record in pass 1: {}", e.getMessage());
                        break;
                    }
                    if (opType == null) break;
                    try {
                        if (opType == OP_CHECKPOINT) {
                            pass1Reader.readLong(); // ts
                            maxCheckpointLsn[0] = Math.max(maxCheckpointLsn[0], pass1Reader.position());
                            continue;
                        }
                        if (!skipOrCollectCommit(pass1Reader, opType, committedXids)) {
                            break;
                        }
                    } catch (IOException e) {
                        LOG.debug("WAL record truncated in pass 1: {}", e.getMessage());
                        break;
                    }
                }
            }

            long checkpointBoundary = maxCheckpointLsn[0];
            Map<String, Page> dirtyPages = new HashMap<>();
            Set<String> recoveredIndexes = new HashSet<>();
            int replayedOps = 0;
            long recoveredLsn = startLsn;

            try (SegmentedWalReader pass2Reader = new SegmentedWalReader(new File(walDirectory), segmentSize, segNumbers, startLsn, endLsn, tdeCipher)) {
                while (pass2Reader.hasRemaining()) {
                    Integer opType;
                    try {
                        opType = pass2Reader.readIntOrNull();
                    } catch (IOException e) {
                        LOG.debug("WAL ended mid-record in pass 2: {}", e.getMessage());
                        break;
                    }
                    if (opType == null) break;

                    try {
                        switch (opType) {
                            case OP_INSERT -> {
                                long xid = pass2Reader.readLong();
                                String tableName = pass2Reader.readLengthPrefixedString();
                                long pageId = pass2Reader.readLong();
                                pass2Reader.readInt(); // slot
                                int len = pass2Reader.readInt();
                                byte[] tupleData = pass2Reader.readBytes(len);
                                long recordLSN = pass2Reader.position();
                                recoveredLsn = Math.max(recoveredLsn, recordLSN);

                                if (committedXids.contains(xid) && recordLSN > checkpointBoundary) {
                                    SlottedPage page = loadOrGetDirtySlottedPage(diskManager, dirtyPages, tableName, pageId);
                                    if (page.getPageLSN() < recordLSN) {
                                        page.insertTuple(tupleData);
                                        page.setPageLSN(recordLSN);
                                        replayedOps++;
                                    }
                                }
                            }
                            case OP_DELETE -> {
                                long xid = pass2Reader.readLong();
                                String tableName = pass2Reader.readLengthPrefixedString();
                                long pageId = pass2Reader.readLong();
                                int slot = pass2Reader.readInt();
                                long recordLSN = pass2Reader.position();
                                recoveredLsn = Math.max(recoveredLsn, recordLSN);

                                if (committedXids.contains(xid) && recordLSN > checkpointBoundary) {
                                    SlottedPage page = loadOrGetDirtySlottedPage(diskManager, dirtyPages, tableName, pageId);
                                    if (page.getPageLSN() < recordLSN) {
                                        page.deleteTuple(slot);
                                        page.setPageLSN(recordLSN);
                                        replayedOps++;
                                    }
                                }
                            }
                            case OP_UPDATE -> {
                                long xid = pass2Reader.readLong();
                                String tableName = pass2Reader.readLengthPrefixedString();
                                long pageId = pass2Reader.readLong();
                                int slot = pass2Reader.readInt();
                                int oldLen = pass2Reader.readInt();
                                pass2Reader.readBytes(oldLen);
                                int newLen = pass2Reader.readInt();
                                byte[] newData = pass2Reader.readBytes(newLen);
                                long recordLSN = pass2Reader.position();
                                recoveredLsn = Math.max(recoveredLsn, recordLSN);

                                if (committedXids.contains(xid) && recordLSN > checkpointBoundary) {
                                    SlottedPage page = loadOrGetDirtySlottedPage(diskManager, dirtyPages, tableName, pageId);
                                    if (page.getPageLSN() < recordLSN) {
                                        page.updateTuple(slot, newData);
                                        page.setPageLSN(recordLSN);
                                        replayedOps++;
                                    }
                                }
                            }
                            case OP_INDEX_INSERT -> {
                                long xid = pass2Reader.readLong();
                                String indexName = pass2Reader.readLengthPrefixedString();
                                recoveredIndexes.add(indexName);
                                long pageId = pass2Reader.readLong();
                                long key = pass2Reader.readLong();
                                long ridPageId = pass2Reader.readLong();
                                int ridSlot = pass2Reader.readInt();
                                long recordLSN = pass2Reader.position();
                                recoveredLsn = Math.max(recoveredLsn, recordLSN);

                                if ((xid == 0L || committedXids.contains(xid)) && recordLSN > checkpointBoundary) {
                                    BTreePage page = loadOrGetDirtyBTreePage(diskManager, dirtyPages, indexName, pageId);
                                    if (page.getPageLSN() < recordLSN) {
                                        List<Long> keys = new ArrayList<>(page.getKeys());
                                        List<BTreePage.RID> values = new ArrayList<>(page.getLeafValues());
                                        int idx = Collections.binarySearch(keys, key);
                                        int insertPos = idx >= 0 ? idx : -(idx + 1);
                                        boolean alreadyPresent = false;
                                        if (idx >= 0) {
                                            for (int i = idx; i < keys.size() && keys.get(i) == key; i++) {
                                                if (values.get(i).pageId() == ridPageId && values.get(i).slot() == ridSlot) {
                                                    alreadyPresent = true;
                                                    break;
                                                }
                                            }
                                        }
                                        if (!alreadyPresent) {
                                            keys.add(insertPos, key);
                                            values.add(insertPos, new BTreePage.RID(ridPageId, ridSlot));
                                            page.setLeafContents(keys, values);
                                        }
                                        page.setPageLSN(recordLSN);
                                        replayedOps++;
                                    }
                                }
                            }
                            case OP_INDEX_DELETE -> {
                                long xid = pass2Reader.readLong();
                                String indexName = pass2Reader.readLengthPrefixedString();
                                recoveredIndexes.add(indexName);
                                long pageId = pass2Reader.readLong();
                                long key = pass2Reader.readLong();
                                long ridPageId = pass2Reader.readLong();
                                int ridSlot = pass2Reader.readInt();
                                long recordLSN = pass2Reader.position();
                                recoveredLsn = Math.max(recoveredLsn, recordLSN);

                                if ((xid == 0L || committedXids.contains(xid)) && recordLSN > checkpointBoundary) {
                                    BTreePage page = loadOrGetDirtyBTreePage(diskManager, dirtyPages, indexName, pageId);
                                    if (page.getPageLSN() < recordLSN) {
                                        List<Long> keys = new ArrayList<>(page.getKeys());
                                        List<BTreePage.RID> values = new ArrayList<>(page.getLeafValues());
                                        for (int i = 0; i < keys.size(); i++) {
                                            if (keys.get(i) == key && values.get(i).pageId() == ridPageId && values.get(i).slot() == ridSlot) {
                                                keys.remove(i);
                                                values.remove(i);
                                                page.setLeafContents(keys, values);
                                                break;
                                            }
                                        }
                                        page.setPageLSN(recordLSN);
                                        replayedOps++;
                                    }
                                }
                            }
                            case OP_INDEX_SPLIT -> {
                                long xid = pass2Reader.readLong();
                                String indexName = pass2Reader.readLengthPrefixedString();
                                recoveredIndexes.add(indexName);
                                boolean isLeaf = (pass2Reader.readInt() == 1);
                                long pageId = pass2Reader.readLong();
                                long newPageId = pass2Reader.readLong();
                                long splitKey = pass2Reader.readLong();
                                long nextLeafPageId = pass2Reader.readLong();
                                long parentPageId = pass2Reader.readLong();
                                long newRootId = pass2Reader.readLong();

                                int rightCount = pass2Reader.readInt();
                                List<Long> rightKeys = new ArrayList<>(rightCount);
                                List<BTreePage.RID> rightValues = isLeaf ? new ArrayList<>(rightCount) : null;
                                List<Long> rightChildren = !isLeaf ? new ArrayList<>(rightCount + 1) : null;
                                if (isLeaf) {
                                    for (int i = 0; i < rightCount; i++) {
                                        rightKeys.add(pass2Reader.readLong());
                                        rightValues.add(new BTreePage.RID(pass2Reader.readLong(), pass2Reader.readInt()));
                                    }
                                } else {
                                    for (int i = 0; i < rightCount; i++) rightKeys.add(pass2Reader.readLong());
                                    for (int i = 0; i <= rightCount; i++) rightChildren.add(pass2Reader.readLong());
                                }

                                int leftCount = pass2Reader.readInt();
                                List<Long> leftKeys = new ArrayList<>(leftCount);
                                List<BTreePage.RID> leftValues = isLeaf ? new ArrayList<>(leftCount) : null;
                                List<Long> leftChildren = !isLeaf ? new ArrayList<>(leftCount + 1) : null;
                                if (isLeaf) {
                                    for (int i = 0; i < leftCount; i++) {
                                        leftKeys.add(pass2Reader.readLong());
                                        leftValues.add(new BTreePage.RID(pass2Reader.readLong(), pass2Reader.readInt()));
                                    }
                                } else {
                                    for (int i = 0; i < leftCount; i++) leftKeys.add(pass2Reader.readLong());
                                    for (int i = 0; i <= leftCount; i++) leftChildren.add(pass2Reader.readLong());
                                }
                                long recordLSN = pass2Reader.position();
                                recoveredLsn = Math.max(recoveredLsn, recordLSN);

                                BTreePage originalPage = loadOrGetDirtyBTreePage(diskManager, dirtyPages, indexName, pageId);
                                BTreePage newPage = loadOrGetDirtyBTreePage(diskManager, dirtyPages, indexName, newPageId);

                                boolean needsSplitReplay = newPage.getPageLSN() < recordLSN
                                    || originalPage.getPageLSN() < recordLSN
                                    || (newRootId != -1 && loadOrGetDirtyBTreePage(diskManager, dirtyPages, indexName, newRootId).getPageLSN() < recordLSN)
                                    || (parentPageId != -1 && loadOrGetDirtyBTreePage(diskManager, dirtyPages, indexName, parentPageId).getPageLSN() < recordLSN);

                                if (needsSplitReplay && recordLSN > checkpointBoundary) {
                                    if (isLeaf) {
                                        newPage.setLeafContents(rightKeys, rightValues);
                                        newPage.setNextLeafPageId(nextLeafPageId);
                                        originalPage.setLeafContents(leftKeys, leftValues);
                                        originalPage.setNextLeafPageId(newPageId);
                                    } else {
                                        newPage.setInternalContents(rightKeys, rightChildren);
                                        originalPage.setInternalContents(leftKeys, leftChildren);
                                    }
                                    newPage.setPageLSN(recordLSN);
                                    originalPage.setPageLSN(recordLSN);

                                    if (newRootId != -1) {
                                        BTreePage newRoot = loadOrGetDirtyBTreePage(diskManager, dirtyPages, indexName, newRootId);
                                        newRoot.setInternalContents(List.of(splitKey), List.of(pageId, newPageId));
                                        newRoot.setPageLSN(recordLSN);

                                        Page meta = loadOrGetDirtyPage(diskManager, dirtyPages, indexName, 0);
                                        meta.getBuffer().putInt(24, 0x53545242);
                                        meta.getBuffer().putLong(28, newRootId);
                                        meta.setPageLSN(recordLSN);
                                    } else if (parentPageId != -1) {
                                        BTreePage parent = loadOrGetDirtyBTreePage(diskManager, dirtyPages, indexName, parentPageId);
                                        List<Long> pKeys = new ArrayList<>(parent.getKeys());
                                        List<Long> pChildren = new ArrayList<>(parent.getChildren());
                                        if (!pChildren.contains(newPageId)) {
                                            int insertIdx = 0;
                                            while (insertIdx < pKeys.size() && splitKey >= pKeys.get(insertIdx)) {
                                                insertIdx++;
                                            }
                                            pKeys.add(insertIdx, splitKey);
                                            pChildren.add(insertIdx + 1, newPageId);
                                            parent.setInternalContents(pKeys, pChildren);
                                            parent.setPageLSN(recordLSN);
                                        }
                                    }
                                    replayedOps++;
                                }
                            }
                            case OP_INDEX_MERGE -> {
                                long xid = pass2Reader.readLong();
                                String indexName = pass2Reader.readLengthPrefixedString();
                                recoveredIndexes.add(indexName);
                                boolean isLeaf = (pass2Reader.readInt() == 1);
                                long parentPageId = pass2Reader.readLong();
                                long leftPageId = pass2Reader.readLong();
                                long rightPageId = pass2Reader.readLong();
                                long separatorKey = pass2Reader.readLong();
                                long rightNextLeafPageId = pass2Reader.readLong();
                                boolean collapsedRoot = (pass2Reader.readInt() == 1);
                                long newRootId = pass2Reader.readLong();

                                int leftCount = pass2Reader.readInt();
                                List<Long> mergedKeys = new ArrayList<>(leftCount);
                                List<BTreePage.RID> mergedValues = isLeaf ? new ArrayList<>(leftCount) : null;
                                List<Long> mergedChildren = !isLeaf ? new ArrayList<>(leftCount + 1) : null;
                                if (isLeaf) {
                                    for (int i = 0; i < leftCount; i++) {
                                        mergedKeys.add(pass2Reader.readLong());
                                        mergedValues.add(new BTreePage.RID(pass2Reader.readLong(), pass2Reader.readInt()));
                                    }
                                } else {
                                    for (int i = 0; i < leftCount; i++) mergedKeys.add(pass2Reader.readLong());
                                    for (int i = 0; i <= leftCount; i++) mergedChildren.add(pass2Reader.readLong());
                                }

                                int parentKeyCount = pass2Reader.readInt();
                                List<Long> parentKeys = new ArrayList<>(parentKeyCount);
                                List<Long> parentChildren = new ArrayList<>(parentKeyCount + 1);
                                for (int i = 0; i < parentKeyCount; i++) parentKeys.add(pass2Reader.readLong());
                                for (int i = 0; i <= parentKeyCount; i++) parentChildren.add(pass2Reader.readLong());
                                long recordLSN = pass2Reader.position();
                                recoveredLsn = Math.max(recoveredLsn, recordLSN);

                                BTreePage leftPage = loadOrGetDirtyBTreePage(diskManager, dirtyPages, indexName, leftPageId);
                                if (leftPage.getPageLSN() < recordLSN && recordLSN > checkpointBoundary) {
                                    if (isLeaf) {
                                        leftPage.setLeafContents(mergedKeys, mergedValues);
                                        leftPage.setNextLeafPageId(rightNextLeafPageId);
                                    } else {
                                        leftPage.setInternalContents(mergedKeys, mergedChildren);
                                    }
                                    leftPage.setPageLSN(recordLSN);

                                    if (!collapsedRoot) {
                                        BTreePage parent = loadOrGetDirtyBTreePage(diskManager, dirtyPages, indexName, parentPageId);
                                        parent.setInternalContents(parentKeys, parentChildren);
                                        parent.setPageLSN(recordLSN);
                                    } else {
                                        Page meta = loadOrGetDirtyPage(diskManager, dirtyPages, indexName, 0);
                                        meta.getBuffer().putInt(24, 0x53545242);
                                        meta.getBuffer().putLong(28, newRootId);
                                        meta.setPageLSN(recordLSN);
                                    }

                                    BTreePage rightPage = loadOrGetDirtyBTreePage(diskManager, dirtyPages, indexName, rightPageId);
                                    rightPage.setLeaf(isLeaf);
                                    if (isLeaf) {
                                        rightPage.setLeafContents(List.of(), List.of());
                                        rightPage.setNextLeafPageId(-1);
                                    } else {
                                        rightPage.setInternalContents(List.of(), List.of(0L));
                                    }
                                    rightPage.setPageLSN(recordLSN);
                                    replayedOps++;
                                }
                            }
                            case OP_COMMIT -> {
                                pass2Reader.readLong(); // xid
                                pass2Reader.readLong(); // ts
                                recoveredLsn = Math.max(recoveredLsn, pass2Reader.position());
                            }
                            case OP_CHECKPOINT -> {
                                pass2Reader.readLong(); // ts
                                recoveredLsn = Math.max(recoveredLsn, pass2Reader.position());
                            }
                            default -> {
                                LOG.warn("Unknown WAL record type {} at position {}; stopping pass 2 scan", opType, pass2Reader.position());
                                break;
                            }
                        }
                    } catch (IOException e) {
                        LOG.debug("WAL record truncated in pass 2: {}", e.getMessage());
                        break;
                    }
                }
            }

            // Durably persist recovered metadata pages
            for (String idx : recoveredIndexes) {
                Page meta = loadOrGetDirtyPage(diskManager, dirtyPages, idx, 0);
                int magic = meta.getBuffer().getInt(24);
                if (magic != 0x53545242) {
                    meta.getBuffer().putInt(24, 0x53545242);
                    long curRoot = meta.getBuffer().getLong(28);
                    if (curRoot <= 0) {
                        meta.getBuffer().putLong(28, 1L);
                    }
                    meta.setDirty(true);
                }
            }

            // Flush dirty pages to disk
            for (Map.Entry<String, Page> entry : dirtyPages.entrySet()) {
                String tableName = entry.getKey().substring(0, entry.getKey().lastIndexOf(':'));
                diskManager.writePage(tableName, entry.getValue());
            }

            // Archive active WAL if archiving is enabled
            archiveCurrentWalIfEnabled();

            // Set currentLSN and lastCheckpointLSN without truncating the segment files
            if (recoveredLsn > 0) {
                currentLSN.set(recoveredLsn);
                lastCheckpointLSN.set(recoveredLsn);
            }

            saveWalControl();
            ensureActiveSegment(getSegmentNumberForLsn(currentLSN.get()));

            LOG.info("Recovery complete: replayed {} operation(s) from {} committed transaction(s) across {} page(s). WAL preserved without truncation at LSN {}",
                replayedOps, committedXids.size(), dirtyPages.size(), currentLSN.get());
        } catch (Exception e) {
            LOG.error("Segmented WAL recovery failed", e);
        }
    }

    private boolean skipOrCollectCommit(SegmentedWalReader reader, int opType, Set<Long> committedXids) throws IOException {
        switch (opType) {
            case OP_INSERT -> {
                reader.readLong();
                reader.readLengthPrefixedString();
                reader.readLong();
                reader.readInt();
                int len = reader.readInt();
                reader.readBytes(len);
                return true;
            }
            case OP_DELETE -> {
                reader.readLong();
                reader.readLengthPrefixedString();
                reader.readLong();
                reader.readInt();
                return true;
            }
            case OP_UPDATE -> {
                reader.readLong();
                reader.readLengthPrefixedString();
                reader.readLong();
                reader.readInt();
                int oldLen = reader.readInt();
                reader.readBytes(oldLen);
                int newLen = reader.readInt();
                reader.readBytes(newLen);
                return true;
            }
            case OP_COMMIT -> {
                long xid = reader.readLong();
                reader.readLong(); // commit timestamp
                committedXids.add(xid);
                return true;
            }
            case OP_CHECKPOINT -> {
                reader.readLong();
                return true;
            }
            case OP_INDEX_INSERT -> {
                reader.readLong();
                reader.readLengthPrefixedString();
                reader.readLong();
                reader.readLong();
                reader.readLong();
                reader.readInt();
                return true;
            }
            case OP_INDEX_DELETE -> {
                reader.readLong();
                reader.readLengthPrefixedString();
                reader.readLong();
                reader.readLong();
                reader.readLong();
                reader.readInt();
                return true;
            }
            case OP_INDEX_SPLIT -> {
                reader.readLong();
                reader.readLengthPrefixedString();
                boolean isLeaf = (reader.readInt() == 1);
                reader.readLong();
                reader.readLong();
                reader.readLong();
                reader.readLong();
                reader.readLong();
                reader.readLong();
                int rightCount = reader.readInt();
                int rightBytes = isLeaf ? (rightCount * 20) : (rightCount * 8 + (rightCount + 1) * 8);
                reader.readBytes(rightBytes);
                int leftCount = reader.readInt();
                int leftBytes = isLeaf ? (leftCount * 20) : (leftCount * 8 + (leftCount + 1) * 8);
                reader.readBytes(leftBytes);
                return true;
            }
            case OP_INDEX_MERGE -> {
                reader.readLong();
                reader.readLengthPrefixedString();
                boolean isLeaf = (reader.readInt() == 1);
                reader.readLong();
                reader.readLong();
                reader.readLong();
                reader.readLong();
                reader.readLong();
                reader.readInt();
                reader.readLong();
                int leftCount = reader.readInt();
                int leftBytes = isLeaf ? (leftCount * 20) : (leftCount * 8 + (leftCount + 1) * 8);
                reader.readBytes(leftBytes);
                int parentKeyCount = reader.readInt();
                int parentBytes = (parentKeyCount * 8) + ((parentKeyCount + 1) * 8);
                reader.readBytes(parentBytes);
                return true;
            }
            default -> {
                LOG.warn("Unknown WAL record type {} during pass 1 scan; stopping", opType);
                return false;
            }
        }
    }

    public void recover() {
        LOG.warn("recover() called with no DiskManager - WAL records will be parsed but nothing written to disk.");
    }

    private Page loadOrGetDirtyPage(DiskManager diskManager, Map<String, Page> dirtyPages, String tableName, long pageId) {
        String key = tableName + ":" + pageId;
        return dirtyPages.computeIfAbsent(key, k -> diskManager.readPage(tableName, pageId));
    }

    private SlottedPage loadOrGetDirtySlottedPage(DiskManager diskManager, Map<String, Page> dirtyPages, String tableName, long pageId) {
        String key = tableName + ":" + pageId;
        Page page = dirtyPages.get(key);
        if (page instanceof SlottedPage sp) return sp;
        if (page != null) {
            SlottedPage sp = new SlottedPage(pageId, page.getBuffer());
            dirtyPages.put(key, sp);
            return sp;
        }
        Page raw = diskManager.readPage(tableName, pageId);
        SlottedPage sp = new SlottedPage(pageId, raw.getBuffer());
        dirtyPages.put(key, sp);
        return sp;
    }

    private BTreePage loadOrGetDirtyBTreePage(DiskManager diskManager, Map<String, Page> dirtyPages, String indexName, long pageId) {
        String key = indexName + ":" + pageId;
        Page page = dirtyPages.get(key);
        if (page instanceof BTreePage btp) return btp;
        if (page != null) {
            BTreePage btp = new BTreePage(pageId, page.getBuffer());
            dirtyPages.put(key, btp);
            return btp;
        }
        if (pageId >= diskManager.getTablePageCount(indexName)) {
            BTreePage btp = new BTreePage(pageId);
            dirtyPages.put(key, btp);
            return btp;
        }
        Page raw = diskManager.readPage(indexName, pageId);
        BTreePage btp = new BTreePage(pageId, raw.getBuffer());
        dirtyPages.put(key, btp);
        return btp;
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

    public long getLastCheckpointLSN() {
        return lastCheckpointLSN.get();
    }

    public long getSegmentSize() {
        return segmentSize;
    }

    public int getWalKeepSegments() {
        return walKeepSegments;
    }

    public void setWalKeepSegments(int walKeepSegments) {
        this.walKeepSegments = walKeepSegments;
        saveWalControl();
    }

    public boolean isPreallocateSegments() {
        return preallocateSegments;
    }

    public void setPreallocateSegments(boolean preallocateSegments) {
        this.preallocateSegments = preallocateSegments;
    }

    public long getCurrentSegmentNumber() {
        return getSegmentNumberForLsn(currentLSN.get());
    }

    public int getSegmentCount() {
        return getExistingSegmentNumbers().size();
    }

    public List<File> getSegmentFiles() {
        List<Long> segNums = getExistingSegmentNumbers();
        List<File> list = new ArrayList<>();
        for (long s : segNums) {
            list.add(getSegmentFile(s));
        }
        return list;
    }

    public synchronized byte[] readBytesFromChecked(long expectedEpoch, long fromOffset) {
        if (expectedEpoch != walEpoch.get()) {
            return null;
        }
        return readBytesFrom(fromOffset);
    }

    /**
     * Reads raw WAL bytes from fromOffset up to current LSN, seamlessly reading across multiple 16MB segments.
     */
    public synchronized byte[] readBytesFrom(long fromOffset) {
        long upToLsn = currentLSN.get();
        if (fromOffset >= upToLsn) {
            return new byte[0];
        }
        int totalLength = (int) (upToLsn - fromOffset);
        byte[] result = new byte[totalLength];
        int destPos = 0;

        long curLsn = fromOffset;
        while (curLsn < upToLsn) {
            long segNum = getSegmentNumberForLsn(curLsn);
            long segOffset = getSegmentOffsetForLsn(curLsn);
            long bytesLeftInSeg = segmentSize - segOffset;
            int bytesToRead = (int) Math.min(upToLsn - curLsn, bytesLeftInSeg);

            File segFile = getSegmentFile(segNum);
            if (!segFile.exists()) {
                File legacy = new File(walDirectory, "wal.log");
                if (legacy.exists() && segNum == 1) {
                    segFile = legacy;
                } else {
                    LOG.error("WAL segment {} missing when reading from LSN {}", segFile.getName(), curLsn);
                    break;
                }
            }

            try (RandomAccessFile raf = new RandomAccessFile(segFile, "r")) {
                raf.seek(segOffset);
                raf.readFully(result, destPos, bytesToRead);
            } catch (Exception e) {
                LOG.error("Failed to read from WAL segment {} at offset {}", segFile.getName(), segOffset, e);
                break;
            }

            destPos += bytesToRead;
            curLsn += bytesToRead;
        }

        return result;
    }

    /**
     * Checkpoint and archiving without file truncation. Preserves continuous WAL segments.
     */
    public synchronized void checkpointAndArchive() {
        checkpoint();
        long archiveResult = archiveCurrentWalIfEnabled();
        if (archiveResult != -2) {
            try {
                if (activeChannel != null && activeChannel.isOpen()) {
                    activeChannel.force(true);
                }
                pruneOldSegments();
                walEpoch.incrementAndGet();
                saveWalControl();
            } catch (Exception e) {
                LOG.error("Failed to complete checkpoint and archive", e);
            }
        }
    }

    public boolean isGroupCommitEnabled() {
        return groupCommitEnabled;
    }

    public synchronized void setGroupCommitEnabled(boolean groupCommitEnabled) {
        this.groupCommitEnabled = groupCommitEnabled;
        if (groupCommitEnabled && groupCommitCoordinator == null && !closed) {
            this.groupCommitCoordinator = new GroupCommitRingBuffer(this,
                GroupCommitRingBuffer.DEFAULT_RING_CAPACITY,
                this.groupCommitMaxBatchSize,
                this.groupCommitDelayMicros);
        } else if (!groupCommitEnabled && groupCommitCoordinator != null) {
            groupCommitCoordinator.close();
            groupCommitCoordinator = null;
        }
    }

    public int getGroupCommitMaxBatchSize() {
        return groupCommitMaxBatchSize;
    }

    public synchronized void setGroupCommitMaxBatchSize(int maxBatchSize) {
        this.groupCommitMaxBatchSize = maxBatchSize;
        if (groupCommitCoordinator != null) {
            groupCommitCoordinator.close();
            this.groupCommitCoordinator = new GroupCommitRingBuffer(this,
                GroupCommitRingBuffer.DEFAULT_RING_CAPACITY,
                this.groupCommitMaxBatchSize,
                this.groupCommitDelayMicros);
        }
    }

    public long getGroupCommitDelayMicros() {
        return groupCommitDelayMicros;
    }

    public synchronized void setGroupCommitDelayMicros(long delayMicros) {
        this.groupCommitDelayMicros = delayMicros;
        if (groupCommitCoordinator != null) {
            groupCommitCoordinator.close();
            this.groupCommitCoordinator = new GroupCommitRingBuffer(this,
                GroupCommitRingBuffer.DEFAULT_RING_CAPACITY,
                this.groupCommitMaxBatchSize,
                this.groupCommitDelayMicros);
        }
    }

    public GroupCommitRingBuffer getGroupCommitCoordinator() {
        return groupCommitCoordinator;
    }

    /**
     * Simulates an abrupt server crash by immediately closing file handles
     * without flushing a final checkpoint or performing graceful shutdown.
     */
    public synchronized void simulateCrash() {
        closed = true;
        try {
            preallocator.shutdownNow();
            if (groupCommitCoordinator != null) {
                groupCommitCoordinator.close();
            }
            if (activeChannel != null && activeChannel.isOpen()) {
                activeChannel.close();
            }
            if (activeRaf != null) {
                activeRaf.close();
            }
        } catch (Exception e) {
            LOG.error("Failed to simulate crash", e);
        }
    }

    public synchronized void close() {
        if (closed) {
            return;
        }
        try {
            if (groupCommitCoordinator != null) {
                groupCommitCoordinator.close();
            }
            checkpoint();
            closed = true;
            preallocator.shutdownNow();

            if (activeChannel != null && activeChannel.isOpen()) {
                activeChannel.force(true);
            }

            archiveCurrentWalIfEnabled();
            saveWalControl();
            pruneOldSegments();

            if (activeChannel != null && activeChannel.isOpen()) {
                activeChannel.close();
            }
            if (activeRaf != null) {
                activeRaf.close();
            }
        } catch (Exception e) {
            LOG.error("Failed to close segmented WAL", e);
        } finally {
            closed = true;
        }
    }
}
