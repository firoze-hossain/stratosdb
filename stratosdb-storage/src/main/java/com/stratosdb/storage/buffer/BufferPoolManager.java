package com.stratosdb.storage.buffer;

import com.stratosdb.common.exceptions.StorageException;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.Page;
import com.stratosdb.storage.page.SlottedPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BufferPoolManager implements BufferPool {
    private static final Logger LOG = LoggerFactory.getLogger(BufferPoolManager.class);

    private final int maxPages;
    private final DiskManager diskManager;
    private final Map<String, Map<Long, Page>> pageCache;
    private final Map<String, Map<Long, Integer>> pinCounts;
    private final ReentrantReadWriteLock globalLock;

    private final Thread backgroundFlusherThread;
    private final java.util.concurrent.atomic.AtomicBoolean flusherRunning = new java.util.concurrent.atomic.AtomicBoolean(false);
    private final Object flusherLock = new Object();
    private final long flushIntervalMs;
    private final java.util.concurrent.atomic.AtomicLong totalBackgroundFlushes = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong totalDirtyPagesFlushed = new java.util.concurrent.atomic.AtomicLong(0);

    private long hits = 0;
    private long misses = 0;

    public BufferPoolManager(int maxPages, DiskManager diskManager) {
        this(maxPages, diskManager, 100L, true);
    }

    public BufferPoolManager(int maxPages, DiskManager diskManager, long flushIntervalMs, boolean startFlusher) {
        this.maxPages = maxPages;
        this.diskManager = diskManager;
        this.flushIntervalMs = flushIntervalMs;
        this.pageCache = new ConcurrentHashMap<>();
        this.pinCounts = new ConcurrentHashMap<>();
        this.globalLock = new ReentrantReadWriteLock();

        if (startFlusher && flushIntervalMs > 0) {
            this.flusherRunning.set(true);
            this.backgroundFlusherThread = new Thread(this::runBackgroundFlusher, "stratos-bgflusher-" + System.identityHashCode(this));
            this.backgroundFlusherThread.setDaemon(true);
            this.backgroundFlusherThread.start();
            LOG.debug("Dirty page background flusher started with interval {}ms", flushIntervalMs);
        } else {
            this.flusherRunning.set(false);
            this.backgroundFlusherThread = null;
        }
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

    private static final com.stratosdb.storage.page.PageFactory<SlottedPage> SLOTTED_PAGE_FACTORY =
        new com.stratosdb.storage.page.PageFactory<>() {
            @Override
            public SlottedPage createEmpty(long pageId) {
                return new SlottedPage(pageId);
            }

            @Override
            public SlottedPage wrap(long pageId, byte[] existingBytes) {
                return new SlottedPage(pageId, existingBytes);
            }

            @Override
            public SlottedPage wrap(long pageId, java.nio.ByteBuffer directBuffer) {
                return new SlottedPage(pageId, directBuffer);
            }
        };

    @Override
    public Page getPage(String tableName, long pageId) {
        // Unchanged behavior from before this class supported other page types:
        // every page is wrapped as a SlottedPage, whether it's brand new or
        // freshly read from disk.
        return getPage(tableName, pageId, SLOTTED_PAGE_FACTORY);
    }

    @Override
    public <T extends Page> T getPage(String tableName, long pageId, com.stratosdb.storage.page.PageFactory<T> factory) {
        // Check if in cache
        Map<Long, Page> tablePages = pageCache.get(tableName);
        if (tablePages != null && tablePages.containsKey(pageId)) {
            @SuppressWarnings("unchecked")
            T page = (T) tablePages.get(pageId);
            pinPage(tableName, pageId);
            hits++;
            LOG.debug("Cache hit: {}/{}", tableName, pageId);
            return page;
        }

        // Cache miss - load from disk
        globalLock.writeLock().lock();
        try {
            // Double-check after acquiring write lock
            tablePages = pageCache.get(tableName);
            if (tablePages != null && tablePages.containsKey(pageId)) {
                @SuppressWarnings("unchecked")
                T page = (T) tablePages.get(pageId);
                pinPage(tableName, pageId);
                hits++;
                return page;
            }

            // Evict if needed
            evictIfNeeded();

            // Load from disk if page already exists, otherwise create a properly initialized empty page.
            T page;
            if (pageId >= diskManager.getTablePageCount(tableName)) {
                page = factory.createEmpty(pageId);
            } else {
                Page raw = diskManager.readPage(tableName, pageId);
                page = factory.wrap(pageId, raw.getBuffer());
            }

            // Add to cache
            tablePages = pageCache.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>());
            tablePages.put(pageId, page);

            pinPage(tableName, pageId);
            misses++;

            LOG.debug("Cache miss: {}/{}", tableName, pageId);
            return page;
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    @Override
    public void markDirty(String tableName, long pageId) {
        Map<Long, Page> tablePages = pageCache.get(tableName);
        if (tablePages != null && tablePages.containsKey(pageId)) {
            tablePages.get(pageId).setDirty(true);
        }
    }

    @Override
    public void unpinPage(String tableName, long pageId) {
        Map<Long, Integer> pins = pinCounts.get(tableName);
        if (pins != null) {
            pins.computeIfPresent(pageId, (k, v) -> v > 0 ? v - 1 : 0);
        }
    }

    @Override
    public void flushAll() {
        globalLock.writeLock().lock();
        try {
            for (Map.Entry<String, Map<Long, Page>> entry : pageCache.entrySet()) {
                String tableName = entry.getKey();
                for (Page page : entry.getValue().values()) {
                    if (page.isDirty()) {
                        diskManager.writePage(tableName, page);
                    }
                }
            }
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    @Override
    public void flushPage(String tableName, long pageId) {
        Map<Long, Page> tablePages = pageCache.get(tableName);
        if (tablePages != null) {
            Page page = tablePages.get(pageId);
            if (page != null && page.isDirty()) {
                diskManager.writePage(tableName, page);
            }
        }
    }

    @Override
    public void evictPage(String tableName, long pageId) {
        globalLock.writeLock().lock();
        try {
            Map<Long, Page> tablePages = pageCache.get(tableName);
            if (tablePages != null) {
                Page page = tablePages.remove(pageId);
                if (page != null && page.isDirty()) {
                    diskManager.writePage(tableName, page);
                }
                Map<Long, Integer> pins = pinCounts.get(tableName);
                if (pins != null) {
                    pins.remove(pageId);
                }
                LOG.debug("Evicted: {}/{}", tableName, pageId);
            }
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    private void pinPage(String tableName, long pageId) {
        Map<Long, Integer> pins = pinCounts.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>());
        pins.compute(pageId, (k, v) -> (v == null) ? 1 : v + 1);
    }

    private void evictIfNeeded() {
        int totalPages = pageCache.values().stream()
                .mapToInt(Map::size)
                .sum();

        if (totalPages < maxPages) {
            return;
        }

        // Find unpinned page to evict
        for (Map.Entry<String, Map<Long, Page>> entry : pageCache.entrySet()) {
            String tableName = entry.getKey();
            Map<Long, Page> pages = entry.getValue();
            Map<Long, Integer> pins = pinCounts.getOrDefault(tableName, new ConcurrentHashMap<>());

            for (Map.Entry<Long, Page> pageEntry : pages.entrySet()) {
                Long pageId = pageEntry.getKey();
                if (pins.getOrDefault(pageId, 0) == 0) {
                    Page page = pageEntry.getValue();
                    if (page.isDirty()) {
                        diskManager.writePage(tableName, page);
                    }
                    pages.remove(pageId);
                    pins.remove(pageId);
                    LOG.debug("Evicted: {}/{}", tableName, pageId);
                    return;
                }
            }
        }
    }

    @Override
    public double getCacheHitRatio() {
        long total = hits + misses;
        return total > 0 ? (double) hits / total : 0.0;
    }

    @Override
    public long getTablePageCount(String tableName) {
        return diskManager.getTablePageCount(tableName);
    }

    @Override
    public DiskManager getDiskManager() {
        return diskManager;
    }

    @Override
    public int getCacheSize() {
        return pageCache.values().stream().mapToInt(Map::size).sum();
    }

    @Override
    public boolean isOffHeap() {
        return true;
    }

    @Override
    public long getOffHeapMemoryUsed() {
        return (long) getCacheSize() * com.stratosdb.common.constants.PageConstants.PAGE_SIZE;
    }

    @Override
    public int getDirtyPageCount() {
        int count = 0;
        for (Map<Long, Page> pages : pageCache.values()) {
            for (Page page : pages.values()) {
                if (page.isDirty()) {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    public int flushDirtyPages(int maxPagesToFlush) {
        return flushDirtyPagesInternal(false, maxPagesToFlush);
    }

    private int flushDirtyPagesInternal(boolean forceAll, int maxPagesToFlush) {
        int flushed = 0;
        for (Map.Entry<String, Map<Long, Page>> entry : pageCache.entrySet()) {
            String tableName = entry.getKey();
            Map<Long, Page> pages = entry.getValue();
            Map<Long, Integer> pins = pinCounts.getOrDefault(tableName, Map.of());

            for (Map.Entry<Long, Page> pageEntry : pages.entrySet()) {
                if (!forceAll && maxPagesToFlush > 0 && flushed >= maxPagesToFlush) {
                    return flushed;
                }
                Long pageId = pageEntry.getKey();
                Page page = pageEntry.getValue();

                if (page.isDirty()) {
                    int pin = pins.getOrDefault(pageId, 0);
                    if (pin == 0 || forceAll) {
                        page.getLatch().readLock().lock();
                        try {
                            if (page.isDirty()) {
                                diskManager.writePage(tableName, page);
                                flushed++;
                            }
                        } finally {
                            page.getLatch().readLock().unlock();
                        }
                    }
                }
            }
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

    /**
     * Real, previously-latent bug found via a real DROP DATABASE + CREATE
     * DATABASE (same name) reproduction: flushing dirty pages and closing
     * the underlying file handles is not enough to make this buffer pool
     * stop serving reads. Any already-open connection that had touched
     * this table before close() was called would keep finding its own
     * already-cached pages sitting right here in pageCache, and would go
     * on serving them - correctly reflecting exactly what this table
     * looked like before shutdown, but silently wrong the moment the
     * underlying data directory has since been deleted and a genuinely
     * new, empty table created at the very same path (StratosCluster's
     * own real DROP DATABASE/CREATE DATABASE support does precisely
     * this). Clearing the cache here, after the final flush, means any
     * further read attempt genuinely misses and falls through to
     * diskManager - which, already closed by this same call, throws a
     * real, honest I/O error instead of silently returning stale data.
     */
    @Override
    public void close() {
        stopBackgroundFlusher();
        flushAll();
        globalLock.writeLock().lock();
        try {
            pageCache.clear();
        } finally {
            globalLock.writeLock().unlock();
        }
        diskManager.close();
    }
}