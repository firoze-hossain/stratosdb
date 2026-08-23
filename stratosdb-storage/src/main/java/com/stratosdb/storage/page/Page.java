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

    /**
     * The real, fine-grained physical latch protecting THIS page's own
     * bytes from concurrent corruption - a genuinely different, lower-
     * level concern from MVCC's own row-level locks (LockManager), which
     * only protect against LOGICAL write-write conflicts between
     * transactions on the SAME row, held for a transaction's whole
     * lifetime. Two transactions modifying two DIFFERENT rows on the
     * SAME page each acquire their own, distinct row lock and can both
     * proceed concurrently as far as MVCC is concerned - but they are
     * both about to mutate this exact page's own slot directory and free-
     * space pointer, which is a real, physical data race without a latch
     * protecting the page itself, regardless of how correct the row-level
     * locking above it is. A latch is held only for the actual duration
     * of the physical page operation (a single insert/update/delete/scan
     * call), never for a transaction's whole lifetime, and carries none
     * of MVCC's own deadlock-detection machinery - by design, latches are
     * meant to be held briefly enough that real deadlock risk between two
     * latches doesn't arise the way it can between two long-held row
     * locks.
     */
    private final java.util.concurrent.locks.ReentrantReadWriteLock latch = new java.util.concurrent.locks.ReentrantReadWriteLock();

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

    /** This page's own physical latch - see the field's own javadoc for why this is a real, separate concern from MVCC's row-level locks. */
    public java.util.concurrent.locks.ReentrantReadWriteLock getLatch() { return latch; }
    
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

