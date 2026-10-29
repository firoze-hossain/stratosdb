package com.stratosdb.storage.heap;

import com.stratosdb.common.constants.PageConstants;
import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.SlottedPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 2-level Free Space Map (FSM) for O(1) amortized page discovery during insertions.
 *
 * Modeled after PostgreSQL and enterprise relational storage engine FSM architectures:
 *
 * Level 1 (Leaf level):
 *   Tracks available free bytes on each individual page of a table. Pages are partitioned
 *   into fixed-size Chunks of 64 pages (CHUNK_SIZE = 64).
 *
 * Level 2 (Summary / Root level):
 *   Maintains a compact summary array where summary[chunkId] = max_{page in chunk}(leaf[pageId]).
 *   Each summary entry summarizes 64 pages (~512KB of heap space), allowing the insertion
 *   engine to evaluate space availability across large sections of the table with a single
 *   integer comparison.
 *
 * O(1) Amortized Search (findPageForInsert):
 *   1. Starts at the active hint chunk (currentChunk).
 *   2. If summary[currentChunk] >= requiredBytes: scans at most 64 leaf entries in that chunk
 *      to return the candidate pageId (constant <= 64 integer checks).
 *   3. If the active chunk has insufficient space: scans the Level 2 summary array. Full chunks
 *      are skipped in a single comparison. When a chunk with space is found, currentChunk is updated
 *      and its leaf slots are checked.
 *   4. If no chunk has sufficient space: returns -1 immediately, directing HeapTable to allocate
 *      a new page without reading any full pages into memory.
 *
 * Persistence:
 *   - Automatically persisted to `<tableName>.fsm` sidecar file in the data directory.
 *   - On startup, loads existing state from `<tableName>.fsm` if available; otherwise dynamically
 *     reconstructs the map from page headers and writes the file.
 */
public class FreeSpaceMap {
    private static final Logger LOG = LoggerFactory.getLogger(FreeSpaceMap.class);

    public static final int CHUNK_SIZE = 64;
    private static final int FSM_MAGIC = 0x46534D31; // "FSM1"

    private final String tableName;
    private final BufferPool bufferPool;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private long totalPages;
    private final List<int[]> leafChunks = new ArrayList<>();
    private int[] summary = new int[0];
    private int currentChunk = 0;
    private boolean dirty = false;

    public FreeSpaceMap(String tableName, BufferPool bufferPool, long existingPages) {
        this.tableName = tableName;
        this.bufferPool = bufferPool;
        this.totalPages = existingPages;

        if (existingPages > 0) {
            if (!loadFromDisk()) {
                rebuildFromPages();
                saveToDisk();
            }
        } else {
            // New table: register initial page 0 with maximum free space
            this.totalPages = 1;
            int initialSpace = PageConstants.PAGE_SIZE - PageConstants.HEADER_SIZE;
            ensureCapacity(0);
            leafChunks.get(0)[0] = initialSpace;
            summary[0] = initialSpace;
            currentChunk = 0;
            this.dirty = true;
            saveToDisk();
        }
    }

    /**
     * Finds a candidate pageId with at least requiredBytes free space in O(1) amortized time.
     * Returns -1 if no existing page has sufficient space.
     */
    public long findPageForInsert(int requiredBytes) {
        lock.readLock().lock();
        try {
            int numChunks = leafChunks.size();
            if (numChunks == 0) return -1;

            // 1. Fast path: check active hint chunk
            int hint = currentChunk;
            if (hint < numChunks && summary[hint] >= requiredBytes) {
                int[] leaf = leafChunks.get(hint);
                long basePageId = (long) hint * CHUNK_SIZE;
                for (int i = 0; i < CHUNK_SIZE; i++) {
                    long pid = basePageId + i;
                    if (pid >= totalPages) break;
                    if (leaf[i] >= requiredBytes) {
                        return pid;
                    }
                }
            }

            // 2. Scan Level 2 summary array (each entry skips 64 pages)
            for (int c = 0; c < numChunks; c++) {
                if (c == hint) continue;
                if (summary[c] >= requiredBytes) {
                    int[] leaf = leafChunks.get(c);
                    long basePageId = (long) c * CHUNK_SIZE;
                    for (int i = 0; i < CHUNK_SIZE; i++) {
                        long pid = basePageId + i;
                        if (pid >= totalPages) break;
                        if (leaf[i] >= requiredBytes) {
                            currentChunk = c; // advance hint
                            return pid;
                        }
                    }
                }
            }

            return -1;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Updates the free space for a specific page.
     */
    public void update(long pageId, int freeSpace) {
        lock.writeLock().lock();
        try {
            ensureCapacity(pageId);
            if (pageId >= totalPages) {
                totalPages = pageId + 1;
            }

            int chunkIdx = (int) (pageId / CHUNK_SIZE);
            int slot = (int) (pageId % CHUNK_SIZE);
            int[] leaf = leafChunks.get(chunkIdx);
            leaf[slot] = freeSpace;

            // Recompute chunk maximum
            int max = 0;
            int limit = (int) Math.min(CHUNK_SIZE, totalPages - ((long) chunkIdx * CHUNK_SIZE));
            for (int i = 0; i < limit; i++) {
                if (leaf[i] > max) max = leaf[i];
            }
            summary[chunkIdx] = max;

            if (max >= PageConstants.FREE_SPACE_THRESHOLD) {
                currentChunk = chunkIdx;
            }
            dirty = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Registers a newly allocated page.
     */
    public void registerNewPage(long pageId, int freeSpace) {
        lock.writeLock().lock();
        try {
            ensureCapacity(pageId);
            if (pageId >= totalPages) {
                totalPages = pageId + 1;
            }

            int chunkIdx = (int) (pageId / CHUNK_SIZE);
            int slot = (int) (pageId % CHUNK_SIZE);
            int[] leaf = leafChunks.get(chunkIdx);
            leaf[slot] = freeSpace;

            if (freeSpace > summary[chunkIdx]) {
                summary[chunkIdx] = freeSpace;
            }
            currentChunk = chunkIdx;
            dirty = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns tracked free space for a pageId.
     */
    public int getFreeSpace(long pageId) {
        lock.readLock().lock();
        try {
            int chunkIdx = (int) (pageId / CHUNK_SIZE);
            int slot = (int) (pageId % CHUNK_SIZE);
            if (chunkIdx >= leafChunks.size() || pageId >= totalPages) {
                return 0;
            }
            return leafChunks.get(chunkIdx)[slot];
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Returns Level 2 summary array copy for testing / inspection. */
    public int[] getSummaryArray() {
        lock.readLock().lock();
        try {
            return Arrays.copyOf(summary, summary.length);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Returns total tracked pages. */
    public long getTotalPages() {
        lock.readLock().lock();
        try {
            return totalPages;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void ensureCapacity(long maxPageId) {
        int requiredChunks = (int) (maxPageId / CHUNK_SIZE) + 1;
        while (leafChunks.size() < requiredChunks) {
            int[] newChunk = new int[CHUNK_SIZE];
            leafChunks.add(newChunk);
        }
        if (summary.length < requiredChunks) {
            summary = Arrays.copyOf(summary, requiredChunks);
        }
    }

    private void rebuildFromPages() {
        lock.writeLock().lock();
        try {
            for (long pid = 0; pid < totalPages; pid++) {
                SlottedPage page = (SlottedPage) bufferPool.getPage(tableName, pid);
                page.getLatch().readLock().lock();
                try {
                    int free = page.getFreeSpace();
                    ensureCapacity(pid);
                    int chunkIdx = (int) (pid / CHUNK_SIZE);
                    int slot = (int) (pid % CHUNK_SIZE);
                    leafChunks.get(chunkIdx)[slot] = free;
                } finally {
                    page.getLatch().readLock().unlock();
                    bufferPool.unpinPage(tableName, pid);
                }
            }

            // Recompute all summary nodes
            for (int c = 0; c < leafChunks.size(); c++) {
                int[] leaf = leafChunks.get(c);
                int max = 0;
                int limit = (int) Math.min(CHUNK_SIZE, totalPages - ((long) c * CHUNK_SIZE));
                for (int i = 0; i < limit; i++) {
                    if (leaf[i] > max) max = leaf[i];
                }
                summary[c] = max;
            }
            dirty = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public synchronized boolean saveToDisk() {
        DiskManager dm = bufferPool.getDiskManager();
        if (dm == null || dm.getDataDirectory() == null) return false;

        lock.readLock().lock();
        try {
            File fsmFile = new File(dm.getDataDirectory(), tableName + ".fsm");
            File tmpFile = new File(dm.getDataDirectory(), tableName + ".fsm.tmp");

            int numChunks = leafChunks.size();
            ByteBuffer buf = ByteBuffer.allocate(20 + (numChunks * 4) + (numChunks * CHUNK_SIZE * 4));
            buf.putInt(FSM_MAGIC);
            buf.putLong(totalPages);
            buf.putInt(CHUNK_SIZE);
            buf.putInt(numChunks);

            for (int i = 0; i < numChunks; i++) {
                buf.putInt(summary[i]);
            }
            for (int[] chunk : leafChunks) {
                for (int i = 0; i < CHUNK_SIZE; i++) {
                    buf.putInt(chunk[i]);
                }
            }

            buf.flip();
            try (FileOutputStream fos = new FileOutputStream(tmpFile);
                 FileChannel ch = fos.getChannel()) {
                ch.write(buf);
                ch.force(true);
            }
            if (!tmpFile.renameTo(fsmFile)) {
                // Windows fallback if renameTo fails when target exists
                fsmFile.delete();
                tmpFile.renameTo(fsmFile);
            }
            dirty = false;
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to save FSM for table {}: {}", tableName, e.getMessage());
            return false;
        } finally {
            lock.readLock().unlock();
        }
    }

    public synchronized boolean loadFromDisk() {
        DiskManager dm = bufferPool.getDiskManager();
        if (dm == null || dm.getDataDirectory() == null) return false;

        File fsmFile = new File(dm.getDataDirectory(), tableName + ".fsm");
        if (!fsmFile.exists() || fsmFile.length() < 20) return false;

        lock.writeLock().lock();
        try (FileInputStream fis = new FileInputStream(fsmFile);
             FileChannel ch = fis.getChannel()) {
            ByteBuffer buf = ByteBuffer.allocate((int) fsmFile.length());
            ch.read(buf);
            buf.flip();

            int magic = buf.getInt();
            if (magic != FSM_MAGIC) return false;

            long savedPages = buf.getLong();
            int savedChunkSize = buf.getInt();
            int numChunks = buf.getInt();

            if (savedChunkSize != CHUNK_SIZE || numChunks <= 0) return false;

            summary = new int[numChunks];
            for (int i = 0; i < numChunks; i++) {
                summary[i] = buf.getInt();
            }

            leafChunks.clear();
            for (int c = 0; c < numChunks; c++) {
                int[] chunk = new int[CHUNK_SIZE];
                for (int i = 0; i < CHUNK_SIZE; i++) {
                    chunk[i] = buf.getInt();
                }
                leafChunks.add(chunk);
            }

            this.totalPages = savedPages;
            this.dirty = false;
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to load FSM for table {}, will rebuild from pages: {}", tableName, e.getMessage());
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void close() {
        if (dirty) {
            saveToDisk();
        }
    }
}
