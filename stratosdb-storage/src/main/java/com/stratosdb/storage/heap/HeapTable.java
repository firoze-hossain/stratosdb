package com.stratosdb.storage.heap;

import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.page.SlottedPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Heap table implementation
 */
public class HeapTable {
    private static final Logger LOG = LoggerFactory.getLogger(HeapTable.class);
    
    private final String name;
    private final BufferPool bufferPool;
    private long lastPageId;
    
    public HeapTable(String name, BufferPool bufferPool) {
        this.name = name;
        this.bufferPool = bufferPool;
        // Discover how many pages this table actually has on disk. Previously this
        // was hardcoded to 0, meaning every HeapTable object - including one freshly
        // constructed after a restart - only ever looked at page 0, no matter how
        // many pages the table really had. scan()/insert() silently ignored every
        // page beyond the first for any table that survived a restart.
        long existingPages = bufferPool.getTablePageCount(name);
        this.lastPageId = existingPages > 0 ? existingPages - 1 : 0;
    }
    
    /**
     * Insert a tuple
     */
    public InsertResult insert(byte[] tupleData) {
        // Try existing pages first
        for (long pageId = 0; pageId <= lastPageId; pageId++) {
            SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
            
            // Check if we have space for this tuple
            if (page.hasSpace(tupleData)) {
                int slot = page.insertTuple(tupleData);
                if (slot != -1) {
                    bufferPool.markDirty(name, pageId);
                    bufferPool.unpinPage(name, pageId);
                    LOG.debug("Inserted at {}/{}", pageId, slot);
                    return new InsertResult(pageId, slot);
                }
            }
            bufferPool.unpinPage(name, pageId);
        }
        
        // Need new page. This MUST go through the buffer pool (getPage), the same
        // as every existing page above - otherwise the page only exists in this
        // local variable, is never registered in the pool's cache, and is silently
        // dropped on the floor: flushAll()/eviction can only persist pages they know
        // about. (Previously this called `new SlottedPage(newPageId)` directly,
        // which is exactly why data on any page past the first was lost.)
        long newPageId = lastPageId + 1;
        SlottedPage newPage = (SlottedPage) bufferPool.getPage(name, newPageId);
        int slot = newPage.insertTuple(tupleData);
        
        bufferPool.markDirty(name, newPageId);
        bufferPool.unpinPage(name, newPageId);
        
        lastPageId = newPageId;
        LOG.debug("Created new page {} for insertion", newPageId);
        
        return new InsertResult(newPageId, slot);
    }
    
    /**
     * Scan all tuples
     */
    public List<byte[]> scan() {
        List<byte[]> results = new ArrayList<>();
        
        for (long pageId = 0; pageId <= lastPageId; pageId++) {
            SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
            
            List<Integer> slots = page.getValidSlots();
            for (int slot : slots) {
                byte[] tuple = page.readTuple(slot);
                if (tuple != null) {
                    results.add(tuple);
                }
            }
            
            bufferPool.unpinPage(name, pageId);
        }
        
        LOG.debug("Scanned {} tuples from table {}", results.size(), name);
        return results;
    }
    
    /**
     * Scan with limit
     */
    public List<byte[]> scan(int limit) {
        List<byte[]> results = new ArrayList<>();
        int count = 0;
        
        for (long pageId = 0; pageId <= lastPageId && count < limit; pageId++) {
            SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
            
            List<Integer> slots = page.getValidSlots();
            for (int slot : slots) {
                if (count >= limit) break;
                byte[] tuple = page.readTuple(slot);
                if (tuple != null) {
                    results.add(tuple);
                    count++;
                }
            }
            
            bufferPool.unpinPage(name, pageId);
        }
        
        return results;
    }
    
    /**
     * Delete by page and slot
     */
    public boolean delete(long pageId, int slot) {
        SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
        page.deleteTuple(slot);
        bufferPool.markDirty(name, pageId);
        bufferPool.unpinPage(name, pageId);
        LOG.debug("Deleted {}/{}", pageId, slot);
        return true;
    }
    
    /**
     * Update by page and slot
     */
    public boolean update(long pageId, int slot, byte[] newData) {
        SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
        boolean result = page.updateTuple(slot, newData);
        if (result) {
            bufferPool.markDirty(name, pageId);
        }
        bufferPool.unpinPage(name, pageId);
        return result;
    }
    
    /**
     * Get tuple by page and slot
     */
    public byte[] readTuple(long pageId, int slot) {
        SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
        byte[] data = page.readTuple(slot);
        bufferPool.unpinPage(name, pageId);
        return data;
    }
    
    public String getName() { return name; }
    public long getLastPageId() { return lastPageId; }
    
    public static class InsertResult {
        public final long pageId;
        public final int slot;
        
        public InsertResult(long pageId, int slot) {
            this.pageId = pageId;
            this.slot = slot;
        }
    }
}