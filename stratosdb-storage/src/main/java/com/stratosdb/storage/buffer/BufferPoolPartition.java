package com.stratosdb.storage.buffer;

import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.Page;
import com.stratosdb.storage.page.PageFactory;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A striped, independent buffer pool partition managing a subset of page frames.
 * Provides lock-free page frame lookup on cache hits, fine-grained partition latching
 * on cache misses, and per-partition Clock-sweep (Second-Chance) eviction.
 */
public class BufferPoolPartition {

    private final int partitionId;
    private final int capacity;
    private final AtomicReferenceArray<BufferFrame> frames;
    private final ConcurrentHashMap<PageKey, Integer> directory;
    private final AtomicInteger clockHand = new AtomicInteger(0);
    private final AtomicInteger activeCount = new AtomicInteger(0);
    private final ReentrantLock partitionLock = new ReentrantLock();

    public BufferPoolPartition(int partitionId, int capacity) {
        this.partitionId = partitionId;
        this.capacity = capacity;
        this.frames = new AtomicReferenceArray<>(capacity);
        this.directory = new ConcurrentHashMap<>();
        for (int i = 0; i < capacity; i++) {
            this.frames.set(i, new BufferFrame(i));
        }
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getActiveCount() {
        return activeCount.get();
    }

    public AtomicReferenceArray<BufferFrame> getFrames() {
        return frames;
    }

    public Map<PageKey, Integer> getDirectory() {
        return directory;
    }

    private BufferFrame findFrame(String tableName, long pageId) {
        if (capacity <= 16) {
            for (int i = 0; i < capacity; i++) {
                BufferFrame frame = frames.get(i);
                if (frame != null && frame.getState() == BufferFrame.STATE_VALID && frame.matches(tableName, pageId)) {
                    return frame;
                }
            }
            return null;
        }
        PageKey key = new PageKey(tableName, pageId);
        Integer slot = directory.get(key);
        return slot != null ? frames.get(slot) : null;
    }

    /**
     * Lock-free page lookup on cache hit.
     * Uses AtomicReferenceArray and atomic pin/reference bit updates without acquiring any lock.
     */
    @SuppressWarnings("unchecked")
    public <T extends Page> T getPageIfPresent(String tableName, long pageId) {
        BufferFrame frame = findFrame(tableName, pageId);
        if (frame != null && frame.getState() == BufferFrame.STATE_VALID && frame.matches(tableName, pageId)) {
            frame.pin();
            // Re-verify after pin to prevent race with concurrent eviction
            if (frame.getState() == BufferFrame.STATE_VALID && frame.matches(tableName, pageId)) {
                return (T) frame.getPage();
            } else {
                frame.unpin();
            }
        }
        return null;
    }

    /**
     * Clock-sweep (Second-Chance) eviction within this partition.
     * Advances the clock hand across frames, clearing reference bits,
     * and evicting the first unpinned page whose reference bit is false.
     */
    public BufferFrame clockSweepEvict(DiskManager diskManager, Consumer<MemorySegment> segmentRecycler) {
        partitionLock.lock();
        try {
            int maxSweeps = capacity * 3;
            for (int i = 0; i < maxSweeps; i++) {
                int slot = Math.abs(clockHand.getAndIncrement() % capacity);
                BufferFrame candidate = frames.get(slot);
                if (candidate == null) {
                    continue;
                }

                if (candidate.getState() == BufferFrame.STATE_EMPTY) {
                    continue;
                }

                if (candidate.getState() != BufferFrame.STATE_VALID) {
                    continue;
                }

                if (candidate.getPinCount() > 0) {
                    continue; // Pinned pages cannot be evicted
                }

                // Check reference bit for second chance
                if (candidate.getReferenceBitAtomic().compareAndSet(true, false)) {
                    continue; // Second chance granted
                }

                // Candidate is unpinned and reference bit was false
                if (candidate.getStateAtomic().compareAndSet(BufferFrame.STATE_VALID, BufferFrame.STATE_EVICTING)) {
                    if (candidate.getPinCount() > 0) {
                        // Raced with pin, revert to valid
                        candidate.getStateAtomic().set(BufferFrame.STATE_VALID);
                        continue;
                    }

                    // Successfully claimed for eviction!
                    String oldTable = candidate.getTableName();
                    long oldPid = candidate.getPageId();
                    Page oldPage = candidate.getPage();

                    // Flush dirty page to disk
                    if (candidate.isDirty() || (oldPage != null && oldPage.isDirty())) {
                        if (oldPage != null) {
                            oldPage.getLatch().readLock().lock();
                            try {
                                diskManager.writePage(oldTable, oldPage);
                                oldPage.setDirty(false);
                            } finally {
                                oldPage.getLatch().readLock().unlock();
                            }
                        }
                        candidate.setDirty(false);
                    }

                    // Remove from directory
                    if (oldTable != null) {
                        directory.remove(new PageKey(oldTable, oldPid));
                    }

                    if (candidate.getSegment() != null) {
                        if (segmentRecycler != null) {
                            segmentRecycler.accept(candidate.getSegment());
                        }
                        candidate.setSegment(null);
                    }

                    candidate.reset();
                    activeCount.decrementAndGet();
                    return candidate;
                }
            }
            return null;
        } finally {
            partitionLock.unlock();
        }
    }

    /**
     * Loads a page from disk into this partition under partition-local lock.
     */
    public <T extends Page> T loadPage(String tableName, long pageId, PageFactory<T> factory,
                                       DiskManager diskManager, Supplier<MemorySegment> segmentSupplier,
                                       Consumer<MemorySegment> segmentRecycler, Runnable onEviction) {
        partitionLock.lock();
        try {
            // Double-check if loaded by another thread while waiting for lock
            T existing = getPageIfPresent(tableName, pageId);
            if (existing != null) {
                return existing;
            }

            PageKey key = new PageKey(tableName, pageId);

            // Find an empty frame or run clock sweep
            BufferFrame targetFrame = null;
            for (int i = 0; i < capacity; i++) {
                BufferFrame f = frames.get(i);
                if (f != null && f.getState() == BufferFrame.STATE_EMPTY) {
                    targetFrame = f;
                    break;
                }
            }

            if (targetFrame == null) {
                targetFrame = clockSweepEvict(diskManager, segmentRecycler);
                if (targetFrame != null && onEviction != null) {
                    onEviction.run();
                }
            }

            if (targetFrame == null) {
                return null; // Partition exhausted of unpinned frames
            }

            targetFrame.getStateAtomic().set(BufferFrame.STATE_LOADING);

            MemorySegment seg = targetFrame.getSegment();
            if (seg == null || !seg.scope().isAlive()) {
                seg = segmentSupplier.get();
                targetFrame.setSegment(seg);
            }

            ByteBuffer buf = seg.asByteBuffer();
            T page;
            long tablePageCount = diskManager.getTablePageCount(tableName);
            if (pageId >= tablePageCount) {
                buf.clear();
                page = factory.wrap(pageId, seg);
                page.initializeHeader();
            } else {
                boolean read = diskManager.readPageIntoBuffer(tableName, pageId, buf);
                if (read) {
                    page = factory.wrap(pageId, seg);
                } else {
                    buf.clear();
                    page = factory.wrap(pageId, seg);
                    page.initializeHeader();
                }
            }

            targetFrame.init(tableName, pageId, page, seg);
            targetFrame.getStateAtomic().set(BufferFrame.STATE_VALID);
            directory.put(key, targetFrame.getSlotIndex());
            activeCount.incrementAndGet();

            return page;
        } finally {
            partitionLock.unlock();
        }
    }

    public void unpinPage(String tableName, long pageId) {
        BufferFrame frame = findFrame(tableName, pageId);
        if (frame != null && frame.matches(tableName, pageId)) {
            frame.unpin();
        }
    }

    public void markDirty(String tableName, long pageId) {
        BufferFrame frame = findFrame(tableName, pageId);
        if (frame != null && frame.matches(tableName, pageId)) {
            frame.setDirty(true);
        }
    }

    public void flushPage(String tableName, long pageId, DiskManager diskManager) {
        PageKey key = new PageKey(tableName, pageId);
        Integer slot = directory.get(key);
        if (slot != null) {
            BufferFrame frame = frames.get(slot);
            if (frame != null && frame.matches(tableName, pageId) && frame.isDirty()) {
                Page page = frame.getPage();
                if (page != null) {
                    page.getLatch().readLock().lock();
                    try {
                        if (frame.isDirty() || page.isDirty()) {
                            diskManager.writePage(tableName, page);
                            frame.setDirty(false);
                            page.setDirty(false);
                        }
                    } finally {
                        page.getLatch().readLock().unlock();
                    }
                }
            }
        }
    }

    public void evictPage(String tableName, long pageId, DiskManager diskManager, Consumer<MemorySegment> segmentRecycler) {
        partitionLock.lock();
        try {
            PageKey key = new PageKey(tableName, pageId);
            Integer slot = directory.remove(key);
            if (slot != null) {
                BufferFrame frame = frames.get(slot);
                if (frame != null) {
                    if (frame.isDirty()) {
                        Page page = frame.getPage();
                        if (page != null) {
                            diskManager.writePage(tableName, page);
                        }
                    }
                    if (frame.getSegment() != null) {
                        segmentRecycler.accept(frame.getSegment());
                        frame.setSegment(null);
                    }
                    frame.reset();
                    activeCount.decrementAndGet();
                }
            }
        } finally {
            partitionLock.unlock();
        }
    }

    public void flushAll(DiskManager diskManager) {
        for (int i = 0; i < capacity; i++) {
            BufferFrame frame = frames.get(i);
            if (frame != null && frame.getState() == BufferFrame.STATE_VALID && frame.isDirty()) {
                Page page = frame.getPage();
                if (page != null) {
                    page.getLatch().readLock().lock();
                    try {
                        if (frame.isDirty() || page.isDirty()) {
                            diskManager.writePage(frame.getTableName(), page);
                            frame.setDirty(false);
                            page.setDirty(false);
                        }
                    } finally {
                        page.getLatch().readLock().unlock();
                    }
                }
            }
        }
    }

    public int flushDirtyPages(int maxPagesToFlush, boolean forceAll, DiskManager diskManager) {
        int flushed = 0;
        for (int i = 0; i < capacity; i++) {
            if (!forceAll && maxPagesToFlush > 0 && flushed >= maxPagesToFlush) {
                break;
            }
            BufferFrame frame = frames.get(i);
            if (frame != null && frame.getState() == BufferFrame.STATE_VALID && frame.isDirty()) {
                if (frame.getPinCount() == 0 || forceAll) {
                    Page page = frame.getPage();
                    if (page != null) {
                        page.getLatch().readLock().lock();
                        try {
                            if (frame.isDirty() || page.isDirty()) {
                                diskManager.writePage(frame.getTableName(), page);
                                frame.setDirty(false);
                                page.setDirty(false);
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

    public void truncateTable(String tableName, long newPageCount, Consumer<MemorySegment> segmentRecycler) {
        partitionLock.lock();
        try {
            for (int i = 0; i < capacity; i++) {
                BufferFrame frame = frames.get(i);
                if (frame != null && frame.getState() == BufferFrame.STATE_VALID) {
                    if (tableName.equals(frame.getTableName()) && frame.getPageId() >= newPageCount) {
                        directory.remove(new PageKey(tableName, frame.getPageId()));
                        if (frame.getSegment() != null) {
                            segmentRecycler.accept(frame.getSegment());
                            frame.setSegment(null);
                        }
                        frame.reset();
                        activeCount.decrementAndGet();
                    }
                }
            }
        } finally {
            partitionLock.unlock();
        }
    }

    public void dropTable(String tableName, Consumer<MemorySegment> segmentRecycler) {
        partitionLock.lock();
        try {
            for (int i = 0; i < capacity; i++) {
                BufferFrame frame = frames.get(i);
                if (frame != null && frame.getState() != BufferFrame.STATE_EMPTY) {
                    if (tableName.equals(frame.getTableName())) {
                        directory.remove(new PageKey(tableName, frame.getPageId()));
                        if (frame.getSegment() != null) {
                            segmentRecycler.accept(frame.getSegment());
                            frame.setSegment(null);
                        }
                        frame.reset();
                        activeCount.decrementAndGet();
                    }
                }
            }
        } finally {
            partitionLock.unlock();
        }
    }

    public void clear(Consumer<MemorySegment> segmentRecycler) {
        partitionLock.lock();
        try {
            directory.clear();
            for (int i = 0; i < capacity; i++) {
                BufferFrame frame = frames.get(i);
                if (frame != null) {
                    if (frame.getSegment() != null) {
                        segmentRecycler.accept(frame.getSegment());
                        frame.setSegment(null);
                    }
                    frame.reset();
                }
            }
            activeCount.set(0);
        } finally {
            partitionLock.unlock();
        }
    }

    public int getDirtyCount() {
        int count = 0;
        for (int i = 0; i < capacity; i++) {
            BufferFrame frame = frames.get(i);
            if (frame != null && frame.getState() == BufferFrame.STATE_VALID && frame.isDirty()) {
                count++;
            }
        }
        return count;
    }
}
