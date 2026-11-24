package com.stratosdb.storage.buffer;

import com.stratosdb.storage.page.Page;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Descriptor for a single page frame in a buffer pool partition.
 * Supports lock-free inspection and atomic state transitions:
 * STATE_EMPTY (0) -> STATE_LOADING (1) -> STATE_VALID (2) -> STATE_EVICTING (3).
 */
public class BufferFrame {
    public static final int STATE_EMPTY = 0;
    public static final int STATE_LOADING = 1;
    public static final int STATE_VALID = 2;
    public static final int STATE_EVICTING = 3;

    private final int slotIndex;
    private final AtomicInteger state = new AtomicInteger(STATE_EMPTY);
    private final AtomicInteger pinCount = new AtomicInteger(0);
    private final AtomicBoolean referenceBit = new AtomicBoolean(true);
    private final AtomicBoolean dirty = new AtomicBoolean(false);

    private volatile String tableName;
    private volatile long pageId = -1;
    private volatile Page page;
    private volatile MemorySegment segment;

    public BufferFrame(int slotIndex) {
        this.slotIndex = slotIndex;
    }

    public int getSlotIndex() {
        return slotIndex;
    }

    public int getState() {
        return state.get();
    }

    public AtomicInteger getStateAtomic() {
        return state;
    }

    public int getPinCount() {
        return pinCount.get();
    }

    public AtomicInteger getPinCountAtomic() {
        return pinCount;
    }

    public boolean getReferenceBit() {
        return referenceBit.get();
    }

    public AtomicBoolean getReferenceBitAtomic() {
        return referenceBit;
    }

    public boolean isDirty() {
        return dirty.get() || (page != null && page.isDirty());
    }

    public void setDirty(boolean d) {
        dirty.set(d);
        if (page != null) {
            page.setDirty(d);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public long getPageId() {
        return pageId;
    }

    public Page getPage() {
        return page;
    }

    public MemorySegment getSegment() {
        return segment;
    }

    public void setSegment(MemorySegment segment) {
        this.segment = segment;
    }

    public void init(String tableName, long pageId, Page page, MemorySegment segment) {
        this.tableName = tableName;
        this.pageId = pageId;
        this.page = page;
        this.segment = segment;
        this.pinCount.set(1);
        this.referenceBit.set(true);
        this.dirty.set(false);
        if (page != null) {
            page.pin();
        }
    }

    public void pin() {
        pinCount.incrementAndGet();
        referenceBit.set(true);
        if (page != null) {
            page.pin();
        }
    }

    public void unpin() {
        pinCount.updateAndGet(v -> v > 0 ? v - 1 : 0);
        if (page != null) {
            page.unpin();
        }
    }

    public boolean matches(String table, long pid) {
        return pid == this.pageId && table != null && table.equals(this.tableName);
    }

    public void reset() {
        this.state.set(STATE_EMPTY);
        this.pinCount.set(0);
        this.referenceBit.set(false);
        this.dirty.set(false);
        this.tableName = null;
        this.pageId = -1;
        this.page = null;
    }
}
