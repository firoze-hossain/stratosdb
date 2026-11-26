package com.stratosdb.storage.heap;

import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.disk.DiskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Persistent 2-bit-per-page Visibility Map (modeled after PostgreSQL's pg_visibility / _vm fork).
 *
 * Each heap page occupies 2 bits (4 pages packed per byte):
 * - Bit 0 (0x01): ALL_VISIBLE — every tuple on the page has a committed xmin and no xmax,
 *   allowing Index-Only Scans to bypass heap tuple reads completely.
 * - Bit 1 (0x02): ALL_FROZEN — every tuple on the page is frozen below the vacuum horizon
 *   and requires no future anti-wraparound vacuum work.
 *
 * Persisted to {@code <tableName>.vm} in the data directory so visibility state survives
 * database restarts without requiring a full re-VACUUM.
 */
public final class VisibilityMap {
    private static final Logger LOG = LoggerFactory.getLogger(VisibilityMap.class);

    public static final int ALL_VISIBLE_BIT = 0x01;
    public static final int ALL_FROZEN_BIT = 0x02;
    private static final int VM_MAGIC = 0x564D3031; // "VM01"

    private final String tableName;
    private final BufferPool bufferPool;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private byte[] bitmap;
    private long totalPages;
    private boolean dirty = false;

    public VisibilityMap(String tableName, BufferPool bufferPool, long existingPages) {
        this.tableName = tableName;
        this.bufferPool = bufferPool;
        this.totalPages = Math.max(1L, existingPages);
        int initialBytes = (int) Math.max(16L, (this.totalPages + 3) / 4);
        this.bitmap = new byte[initialBytes];

        if (existingPages > 0) {
            loadFromDisk();
        }
    }

    public boolean isAllVisible(long pageId) {
        if (pageId < 0) return false;
        lock.readLock().lock();
        try {
            if (pageId >= totalPages) return false;
            int byteIdx = (int) (pageId >>> 2);
            if (byteIdx >= bitmap.length) return false;
            int shift = (int) ((pageId & 3L) << 1);
            return ((bitmap[byteIdx] >>> shift) & ALL_VISIBLE_BIT) != 0;
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isAllFrozen(long pageId) {
        if (pageId < 0) return false;
        lock.readLock().lock();
        try {
            if (pageId >= totalPages) return false;
            int byteIdx = (int) (pageId >>> 2);
            if (byteIdx >= bitmap.length) return false;
            int shift = (int) ((pageId & 3L) << 1);
            return ((bitmap[byteIdx] >>> shift) & ALL_FROZEN_BIT) != 0;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void setVisibility(long pageId, boolean allVisible, boolean allFrozen) {
        if (pageId < 0) return;
        lock.writeLock().lock();
        try {
            ensureCapacity(pageId);
            if (pageId >= totalPages) {
                totalPages = pageId + 1;
            }
            int byteIdx = (int) (pageId >>> 2);
            int shift = (int) ((pageId & 3L) << 1);
            int bits = (allVisible ? ALL_VISIBLE_BIT : 0) | ((allVisible && allFrozen) ? ALL_FROZEN_BIT : 0);
            int clearMask = ~(0x03 << shift);
            byte updated = (byte) ((bitmap[byteIdx] & clearMask) | (bits << shift));
            if (bitmap[byteIdx] != updated) {
                bitmap[byteIdx] = updated;
                dirty = true;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clear(long pageId) {
        if (pageId < 0) return;
        lock.writeLock().lock();
        try {
            int byteIdx = (int) (pageId >>> 2);
            if (byteIdx >= bitmap.length) return;
            int shift = (int) ((pageId & 3L) << 1);
            int clearMask = ~(0x03 << shift);
            byte updated = (byte) (bitmap[byteIdx] & clearMask);
            if (bitmap[byteIdx] != updated) {
                bitmap[byteIdx] = updated;
                dirty = true;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long countAllVisible() {
        lock.readLock().lock();
        try {
            long count = 0;
            for (long p = 0; p < totalPages; p++) {
                int byteIdx = (int) (p >>> 2);
                int shift = (int) ((p & 3L) << 1);
                if (((bitmap[byteIdx] >>> shift) & ALL_VISIBLE_BIT) != 0) {
                    count++;
                }
            }
            return count;
        } finally {
            lock.readLock().unlock();
        }
    }

    public long countAllFrozen() {
        lock.readLock().lock();
        try {
            long count = 0;
            for (long p = 0; p < totalPages; p++) {
                int byteIdx = (int) (p >>> 2);
                int shift = (int) ((p & 3L) << 1);
                if (((bitmap[byteIdx] >>> shift) & ALL_FROZEN_BIT) != 0) {
                    count++;
                }
            }
            return count;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void ensureCapacity(long pageId) {
        int neededBytes = (int) ((pageId >>> 2) + 1);
        if (neededBytes > bitmap.length) {
            int newLen = Math.max(neededBytes, bitmap.length * 2);
            bitmap = Arrays.copyOf(bitmap, newLen);
        }
    }

    public synchronized boolean saveToDisk() {
        DiskManager dm = bufferPool.getDiskManager();
        if (dm == null || dm.getDataDirectory() == null) return false;

        lock.readLock().lock();
        try {
            int activeBytes = (int) Math.max(1L, (totalPages + 3) / 4);
            ByteBuffer buf = ByteBuffer.allocate(16 + activeBytes);
            buf.putInt(VM_MAGIC);
            buf.putLong(totalPages);
            buf.putInt(activeBytes);
            buf.put(bitmap, 0, Math.min(activeBytes, bitmap.length));
            buf.flip();

            File vmFile = new File(dm.getDataDirectory(), tableName + ".vm");
            try (FileOutputStream fos = new FileOutputStream(vmFile);
                 FileChannel ch = fos.getChannel()) {
                while (buf.hasRemaining()) {
                    ch.write(buf);
                }
                ch.force(true);
            }
            this.dirty = false;
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to save VisibilityMap for table {}: {}", tableName, e.getMessage());
            return false;
        } finally {
            lock.readLock().unlock();
        }
    }

    public synchronized boolean loadFromDisk() {
        DiskManager dm = bufferPool.getDiskManager();
        if (dm == null || dm.getDataDirectory() == null) return false;

        File vmFile = new File(dm.getDataDirectory(), tableName + ".vm");
        if (!vmFile.exists() || vmFile.length() < 16) return false;

        lock.writeLock().lock();
        try (FileInputStream fis = new FileInputStream(vmFile);
             FileChannel ch = fis.getChannel()) {
            ByteBuffer buf = ByteBuffer.allocate((int) vmFile.length());
            ch.read(buf);
            buf.flip();

            int magic = buf.getInt();
            if (magic != VM_MAGIC) return false;

            long savedPages = buf.getLong();
            int byteCount = buf.getInt();
            if (byteCount <= 0 || buf.remaining() < byteCount) return false;

            byte[] loaded = new byte[Math.max(16, byteCount)];
            buf.get(loaded, 0, byteCount);
            this.bitmap = loaded;
            this.totalPages = savedPages;
            this.dirty = false;
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to load VisibilityMap for table {}: {}", tableName, e.getMessage());
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean isDirty() {
        return dirty;
    }
}
