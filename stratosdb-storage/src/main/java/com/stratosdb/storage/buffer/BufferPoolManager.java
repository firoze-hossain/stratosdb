package com.stratosdb.storage.buffer;

import com.stratosdb.common.constants.PageConstants;
import com.stratosdb.common.exceptions.StorageException;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.Page;
import com.stratosdb.storage.page.PageFactory;
import com.stratosdb.storage.page.SlottedPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 64-Partitioned Latch-Free Buffer Pool Manager.
 *
 * Replaces the monolithic globalLock with 64-partitioned hash striping,
 * lock-free page frame lookup using AtomicReferenceArray and CAS,
 * and per-partition Clock-sweep (Second-Chance) eviction.
 * Page frames live entirely in native off-heap memory managed via Java 25 Arena.
 */
public class BufferPoolManager implements BufferPool {
    private static final Logger LOG = LoggerFactory.getLogger(BufferPoolManager.class);

    public static final int DEFAULT_PARTITION_COUNT = 64;

    private final int maxPages;
    private final int numPartitions;
    private final BufferPoolPartition[] partitions;
    private final DiskManager diskManager;

    private final Arena poolArena;
    private final Queue<MemorySegment> memorySegmentPool = new ConcurrentLinkedQueue<>();
    private final Queue<ByteBuffer> directFramePool = new ConcurrentLinkedQueue<>();
    private final AtomicLong totalFramesRecycled = new AtomicLong(0);

    private final Thread backgroundFlusherThread;
    private final AtomicBoolean flusherRunning = new AtomicBoolean(false);
    private final Object flusherLock = new Object();
    private final long flushIntervalMs;
    private final AtomicLong totalBackgroundFlushes = new AtomicLong(0);
    private final AtomicLong totalDirtyPagesFlushed = new AtomicLong(0);

    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();

    public BufferPoolManager(int maxPages, DiskManager diskManager) {
        this(maxPages, diskManager, 0L, false, DEFAULT_PARTITION_COUNT);
    }

    public BufferPoolManager(int maxPages, DiskManager diskManager, long flushIntervalMs, boolean startFlusher) {
        this(maxPages, diskManager, flushIntervalMs, startFlusher, DEFAULT_PARTITION_COUNT);
    }

    public BufferPoolManager(int maxPages, DiskManager diskManager, long flushIntervalMs, boolean startFlusher, int requestedPartitions) {
        this.maxPages = maxPages;
        this.diskManager = diskManager;
        this.flushIntervalMs = flushIntervalMs;
        this.poolArena = Arena.ofShared();

        this.numPartitions = Math.min(requestedPartitions, Math.max(1, maxPages));
        this.partitions = new BufferPoolPartition[this.numPartitions];

        int partitionCap = Math.max(4, (int) Math.ceil((double) maxPages / this.numPartitions) * 2);
        for (int i = 0; i < this.numPartitions; i++) {
            this.partitions[i] = new BufferPoolPartition(i, partitionCap);
        }

        if (startFlusher && flushIntervalMs > 0) {
            this.flusherRunning.set(true);
            this.backgroundFlusherThread = new Thread(this::runBackgroundFlusher, "stratos-bgflusher-" + System.identityHashCode(this));
            this.backgroundFlusherThread.setDaemon(true);
            this.backgroundFlusherThread.start();
            LOG.debug("Dirty page background flusher started with interval {}ms across {} partitions", flushIntervalMs, numPartitions);
        } else {
            this.flusherRunning.set(false);
            this.backgroundFlusherThread = null;
        }
    }

    private int getPartitionIndex(String tableName, long pageId) {
        int h = tableName.hashCode() * 31 + (int)(pageId ^ (pageId >>> 32));
        h ^= (h >>> 16);
        h *= 0x85ebca6b;
        h ^= (h >>> 13);
        h *= 0xc2b2ae35;
        h ^= (h >>> 16);
        return (h & 0x7FFFFFFF) % numPartitions;
    }

    @Override
    public int getNumPartitions() {
        return numPartitions;
    }

    public BufferPoolPartition getPartition(int index) {
        return partitions[index];
    }

    private void runBackgroundFlusher() {
        while (flusherRunning.get()) {
            try {
                synchronized (flusherLock) {
                    flusherLock.wait(flushIntervalMs);
                }
                if (!flusherRunning.get()) {
                    break;
                }
                int flushed = flushDirtyPagesInternal(false, 32);
                if (flushed > 0) {
                    totalBackgroundFlushes.incrementAndGet();
                    totalDirtyPagesFlushed.addAndGet(flushed);
                    LOG.debug("Background flusher wrote {} dirty pages to disk", flushed);
                }
            } catch (InterruptedException e) {
                break;
            } catch (Exception e) {
                LOG.warn("Error during background dirty page flush", e);
            }
        }
    }

    private static final PageFactory<SlottedPage> SLOTTED_PAGE_FACTORY =
        new PageFactory<>() {
            @Override
            public SlottedPage createEmpty(long pageId) {
                return new SlottedPage(pageId);
            }

            @Override
            public SlottedPage wrap(long pageId, byte[] existingBytes) {
                return new SlottedPage(pageId, existingBytes);
            }

            @Override
            public SlottedPage wrap(long pageId, ByteBuffer directBuffer) {
                return new SlottedPage(pageId, directBuffer);
            }

            @Override
            public SlottedPage wrap(long pageId, MemorySegment segment) {
                return new SlottedPage(pageId, segment);
            }
        };

    @Override
    public Page getPage(String tableName, long pageId) {
        return getPage(tableName, pageId, SLOTTED_PAGE_FACTORY);
    }

    @Override
    public <T extends Page> T getPage(String tableName, long pageId, PageFactory<T> factory) {
        int partIdx = getPartitionIndex(tableName, pageId);
        BufferPoolPartition targetPartition = partitions[partIdx];

        // 1. Lock-free cache hit attempt
        T cached = targetPartition.getPageIfPresent(tableName, pageId);
        if (cached != null) {
            hits.increment();
            return cached;
        }

        // 2. Cache miss: ensure global capacity constraint before loading
        while (getCacheSize() >= maxPages) {
            // Evict an unpinned frame using per-partition Clock sweep
            BufferFrame evicted = targetPartition.clockSweepEvict(diskManager, this::recycleMemorySegment);
            if (evicted == null) {
                // Try neighboring partitions for an unpinned frame
                for (int i = 1; i < numPartitions; i++) {
                    int otherIdx = (partIdx + i) % numPartitions;
                    evicted = partitions[otherIdx].clockSweepEvict(diskManager, this::recycleMemorySegment);
                    if (evicted != null) {
                        break;
                    }
                }
            }
            if (evicted == null) {
                break;
            }
        }

        // 3. Load page into target partition under partition-local lock
        T page = targetPartition.loadPage(
            tableName, pageId, factory, diskManager,
            this::acquireMemorySegment, this::recycleMemorySegment,
            null
        );

        if (page == null) {
            // Partition temporarily exhausted of unpinned frames under concurrent load; retry
            for (int retry = 0; retry < 5 && page == null; retry++) {
                for (int i = 0; i < numPartitions; i++) {
                    partitions[i].clockSweepEvict(diskManager, this::recycleMemorySegment);
                }
                try {
                    Thread.sleep(5);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                page = targetPartition.loadPage(
                    tableName, pageId, factory, diskManager,
                    this::acquireMemorySegment, this::recycleMemorySegment,
                    null
                );
            }
            if (page == null) {
                throw new StorageException("Buffer pool exhausted: all " + maxPages + " pages are pinned under concurrent load");
            }
        }

        misses.increment();
        return page;
    }

    @Override
    public void markDirty(String tableName, long pageId) {
        partitions[getPartitionIndex(tableName, pageId)].markDirty(tableName, pageId);
    }

    @Override
    public void unpinPage(String tableName, long pageId) {
        partitions[getPartitionIndex(tableName, pageId)].unpinPage(tableName, pageId);
    }

    @Override
    public void flushAll() {
        for (BufferPoolPartition part : partitions) {
            part.flushAll(diskManager);
        }
    }

    @Override
    public void flushPage(String tableName, long pageId) {
        partitions[getPartitionIndex(tableName, pageId)].flushPage(tableName, pageId, diskManager);
    }

    @Override
    public void evictPage(String tableName, long pageId) {
        partitions[getPartitionIndex(tableName, pageId)].evictPage(tableName, pageId, diskManager, this::recycleMemorySegment);
    }

    @Override
    public Arena getArena() {
        return poolArena;
    }

    @Override
    public MemorySegment acquireMemorySegment() {
        MemorySegment seg = memorySegmentPool.poll();
        if (seg != null && seg.scope().isAlive()) {
            directFramePool.poll();
            return seg;
        }
        return poolArena.allocate(PageConstants.PAGE_SIZE, 8);
    }

    public ByteBuffer acquireDirectFrame() {
        ByteBuffer buf = directFramePool.poll();
        if (buf != null) {
            memorySegmentPool.poll();
            buf.clear();
            return buf;
        }
        return acquireMemorySegment().asByteBuffer();
    }

    @Override
    public void recycleMemorySegment(MemorySegment seg) {
        if (seg != null && seg.scope().isAlive()) {
            totalFramesRecycled.incrementAndGet();
            if (memorySegmentPool.size() < maxPages) {
                memorySegmentPool.offer(seg);
            }
            if (directFramePool.size() < maxPages) {
                try {
                    ByteBuffer buf = seg.asByteBuffer();
                    buf.clear();
                    directFramePool.offer(buf);
                } catch (Exception ignored) {
                }
            }
        }
    }

    public void recycleDirectFrame(ByteBuffer buf) {
        if (buf != null && buf.isDirect()) {
            totalFramesRecycled.incrementAndGet();
            if (directFramePool.size() < maxPages) {
                buf.clear();
                directFramePool.offer(buf);
            }
        }
    }

    @Override
    public int getDirectFramePoolSize() {
        return directFramePool.size();
    }

    @Override
    public long getTotalFramesRecycled() {
        return totalFramesRecycled.get();
    }

    @Override
    public double getCacheHitRatio() {
        long h = hits.sum();
        long m = misses.sum();
        long total = h + m;
        return total > 0 ? (double) h / total : 0.0;
    }

    @Override
    public long getTablePageCount(String tableName) {
        return diskManager.getTablePageCount(tableName);
    }

    @Override
    public void truncateTable(String tableName, long newPageCount) {
        for (BufferPoolPartition part : partitions) {
            part.truncateTable(tableName, newPageCount, this::recycleMemorySegment);
        }
        diskManager.truncateTable(tableName, newPageCount);
    }

    @Override
    public void dropTable(String tableName) {
        for (BufferPoolPartition part : partitions) {
            part.dropTable(tableName, this::recycleMemorySegment);
        }
        diskManager.deleteTable(tableName);
    }

    @Override
    public DiskManager getDiskManager() {
        return diskManager;
    }

    @Override
    public int getCacheSize() {
        int total = 0;
        for (BufferPoolPartition part : partitions) {
            total += part.getActiveCount();
        }
        return total;
    }

    @Override
    public boolean isOffHeap() {
        return true;
    }

    @Override
    public long getOffHeapMemoryUsed() {
        return (long) getCacheSize() * PageConstants.PAGE_SIZE;
    }

    @Override
    public int getDirtyPageCount() {
        int count = 0;
        for (BufferPoolPartition part : partitions) {
            count += part.getDirtyCount();
        }
        return count;
    }

    @Override
    public int flushDirtyPages(int maxPagesToFlush) {
        return flushDirtyPagesInternal(false, maxPagesToFlush);
    }

    private int flushDirtyPagesInternal(boolean forceAll, int maxPagesToFlush) {
        int flushed = 0;
        for (BufferPoolPartition part : partitions) {
            if (!forceAll && maxPagesToFlush > 0 && flushed >= maxPagesToFlush) {
                break;
            }
            flushed += part.flushDirtyPages(maxPagesToFlush - flushed, forceAll, diskManager);
        }
        return flushed;
    }

    @Override
    public boolean isBackgroundFlusherRunning() {
        return flusherRunning.get() && backgroundFlusherThread != null && backgroundFlusherThread.isAlive();
    }

    @Override
    public void stopBackgroundFlusher() {
        if (flusherRunning.compareAndSet(true, false)) {
            synchronized (flusherLock) {
                flusherLock.notifyAll();
            }
            if (backgroundFlusherThread != null) {
                try {
                    backgroundFlusherThread.join(2000);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public long getTotalBackgroundFlushes() {
        return totalBackgroundFlushes.get();
    }

    public long getTotalDirtyPagesFlushed() {
        return totalDirtyPagesFlushed.get();
    }

    @Override
    public void close() {
        stopBackgroundFlusher();
        flushAll();
        for (BufferPoolPartition part : partitions) {
            part.clear(this::recycleMemorySegment);
        }
        directFramePool.clear();
        memorySegmentPool.clear();
        diskManager.close();
        if (poolArena != null && poolArena.scope().isAlive()) {
            try {
                poolArena.close();
            } catch (Exception e) {
                LOG.debug("Exception closing buffer pool arena", e);
            }
        }
    }
}
