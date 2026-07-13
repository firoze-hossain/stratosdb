package com.stratosdb.storage.buffer;

import com.stratosdb.common.exceptions.StorageException;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe buffer pool with LRU eviction policy
 */
public class BufferPoolManager implements BufferPool {
    private static final Logger LOG = LoggerFactory.getLogger(BufferPoolManager.class);
    
    private final int maxPages;
    private final DiskManager diskManager;
    private final Map<String, Map<Long, Page>> pageCache;
    private final Map<String, Map<Long, Integer>> pinCounts;
    private final ReentrantReadWriteLock globalLock;
    
    private long hits = 0;
    private long misses = 0;
    
    public BufferPoolManager(int maxPages, DiskManager diskManager) {
        this.maxPages = maxPages;
        this.diskManager = diskManager;
        this.pageCache = new ConcurrentHashMap<>();
        this.pinCounts = new ConcurrentHashMap<>();
        this.globalLock = new ReentrantReadWriteLock();
    }
    
    @Override
    public Page getPage(String tableName, long pageId) {
        // Check if in cache
        Map<Long, Page> tablePages = pageCache.get(tableName);
        if (tablePages != null && tablePages.containsKey(pageId)) {
            Page page = tablePages.get(pageId);
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
                Page page = tablePages.get(pageId);
                pinPage(tableName, pageId);
                hits++;
                return page;
            }
            
            // Evict if needed
            evictIfNeeded();
            
            // Load from disk
            Page page = diskManager.readPage(tableName, pageId);
            
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
        
        // Find unpinned page to evict (simple approach - first found)
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
    public int getCacheSize() {
        return pageCache.values().stream().mapToInt(Map::size).sum();
    }
    
    @Override
    public void close() {
        flushAll();
        diskManager.close();
    }
}

