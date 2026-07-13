package com.stratosdb.storage.page;

import com.stratosdb.common.constants.PageConstants;
import com.stratosdb.common.utils.ByteUtil;
import com.stratosdb.common.utils.ChecksumUtil;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 8KB Page with PostgreSQL-style layout
 * 
 * Layout:
 * +------------------+ 0
 * | PageHeader       | 24 bytes
 * +------------------+ 24
 * | ItemIdArray      | 6 bytes per item
 * | (grows down)     |
 * +------------------+ 
 * | Free Space       |
 * +------------------+
 * | Tuple Data       |
 * | (grows up)       |
 * +------------------+ PAGE_SIZE
 */
public class Page {
    private final ByteBuffer buffer;
    private long pageId;
    private boolean dirty = false;
    private int pinCount = 0;
    
    public Page(long pageId) {
        this.pageId = pageId;
        this.buffer = ByteBuffer.allocateDirect(PageConstants.PAGE_SIZE);
        initializeHeader();
    }
    
    public Page(long pageId, byte[] data) {
        this.pageId = pageId;
        this.buffer = ByteBuffer.allocateDirect(PageConstants.PAGE_SIZE);
        this.buffer.put(data, 0, Math.min(data.length, PageConstants.PAGE_SIZE));
        this.buffer.flip();
    }
    
    private void initializeHeader() {
        // pd_lsn (8 bytes)
        buffer.putLong(0, 0L);
        // pd_checksum (2 bytes)
        buffer.putShort(8, (short) 0);
        // pd_flags (2 bytes)
        buffer.putShort(10, (short) 0);
        // pd_lower - offset to start of item pointers (starts at 24)
        buffer.putShort(12, (short) PageConstants.HEADER_SIZE);
        // pd_upper - offset to start of free space (starts at PAGE_SIZE)
        buffer.putShort(14, (short) PageConstants.PAGE_SIZE);
        // pd_special (2 bytes)
        buffer.putShort(16, (short) 0);
        // pd_pagesize_version (2 bytes)
        buffer.putShort(18, (short) PageConstants.PAGE_SIZE);
    }
    
    // Getters
    public ByteBuffer getBuffer() { return buffer; }
    public long getPageId() { return pageId; }
    public boolean isDirty() { return dirty; }
    public void setDirty(boolean dirty) { this.dirty = dirty; }
    public int getPinCount() { return pinCount; }
    public void pin() { pinCount++; }
    public void unpin() { if (pinCount > 0) pinCount--; }
    
    public int getFreeSpace() {
        int lower = buffer.getShort(12);
        int upper = buffer.getShort(14);
        return upper - lower;
    }
    
    public int getTupleCount() {
        int lower = buffer.getShort(12);
        return (lower - PageConstants.HEADER_SIZE) / 6;
    }
    
    public byte[] getBytes() {
        byte[] data = new byte[PageConstants.PAGE_SIZE];
        buffer.position(0);
        buffer.get(data);
        return data;
    }
    
    public long getChecksum() {
        return ChecksumUtil.calculateCRC32(getBytes());
    }
    
    public boolean verifyChecksum() {
        // Skip checksum verification for now
        return true;
    }
    
    @Override
    public String toString() {
        return String.format("Page[id=%d, freeSpace=%d, tuples=%d, dirty=%s]",
                pageId, getFreeSpace(), getTupleCount(), dirty);
    }
}

