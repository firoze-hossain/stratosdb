package com.stratosdb.storage.buffer;

import com.stratosdb.common.constants.PageConstants;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Page;
import com.stratosdb.storage.page.SlottedPage;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class OffHeapBufferPoolTest {

    @TempDir
    Path tempDir;

    private final List<BufferPoolManager> openPools = new ArrayList<>();

    private BufferPoolManager track(BufferPoolManager pool) {
        openPools.add(pool);
        return pool;
    }

    @AfterEach
    void tearDown() {
        for (BufferPoolManager pool : openPools) {
            try {
                pool.close();
            } catch (Exception ignored) {}
        }
        openPools.clear();
    }

    @Test
    void testBufferPoolPagesStoredOffHeap() {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = track(new BufferPoolManager(16, dm));

        assertTrue(bp.isOffHeap(), "Buffer pool must report off-heap storage enabled");

        // Retrieve a newly created page
        Page page0 = bp.getPage("users", 0);
        assertNotNull(page0);
        assertTrue(page0.isOffHeap(), "Page must report off-heap storage");
        assertTrue(page0.getBuffer().isDirect(), "Page ByteBuffer must be direct (off-heap)");
        assertNotNull(page0.getMemorySegment(), "Page must be backed by a Foreign MemorySegment");
        assertTrue(page0.getMemorySegment().isNative(), "MemorySegment must be native off-heap memory");
        assertEquals(PageConstants.PAGE_SIZE, page0.getBuffer().capacity());

        // Off-heap memory used tracking
        long offHeapUsed = bp.getOffHeapMemoryUsed();
        assertTrue(offHeapUsed >= PageConstants.PAGE_SIZE,
                "Buffer pool must report at least 8KB of off-heap memory in use, got: " + offHeapUsed);

        bp.unpinPage("users", 0);
    }

    @Test
    void testDirtyPageBackgroundFlushThread() throws Exception {
        DiskManager dm = new DiskManager(tempDir.toString());
        // Configure with fast 40ms interval flusher
        BufferPoolManager bp = track(new BufferPoolManager(32, dm, 40L, true));

        assertTrue(bp.isBackgroundFlusherRunning(), "Background flusher thread must be running");

        // Modify 5 pages and mark them dirty
        for (long i = 0; i < 5; i++) {
            SlottedPage p = (SlottedPage) bp.getPage("orders", i);
            p.getBuffer().putInt(24, (int) (1000 + i));
            p.setDirty(true);
            bp.markDirty("orders", i);
            bp.unpinPage("orders", i);
        }

        assertEquals(5, bp.getDirtyPageCount(), "Initially 5 pages must be dirty in cache");

        // Wait for background flusher thread to flush dirty unpinned pages to disk
        long deadline = System.currentTimeMillis() + 3000;
        while (bp.getDirtyPageCount() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }

        assertEquals(0, bp.getDirtyPageCount(),
                "Background flusher thread must flush all dirty unpinned pages to disk without flushAll()");
        assertTrue(bp.getTotalBackgroundFlushes() > 0, "Flusher counter must record background flush cycles");
        assertTrue(bp.getTotalDirtyPagesFlushed() >= 5, "Flusher must record at least 5 pages flushed");

        // Verify disk persistence by reopening a separate DiskManager
        DiskManager dm2 = new DiskManager(tempDir.toString());
        for (long i = 0; i < 5; i++) {
            Page onDisk = dm2.readPage("orders", i);
            assertEquals((int) (1000 + i), onDisk.getBuffer().getInt(24),
                    "Data on disk must match values flushed by background thread for page " + i);
        }
        dm2.close();
    }

    @Test
    void testSequentialScanNearZeroGcHeapAllocation() {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = track(new BufferPoolManager(128, dm, 0L, false));
        HeapTable table = new HeapTable("test_scan", bp);

        // Insert tuples spanning across 30 pages
        byte[] samplePayload = new byte[120];
        for (int i = 0; i < samplePayload.length; i++) samplePayload[i] = (byte) (i & 0x7F);

        int totalInserted = 0;
        for (int i = 0; i < 2000; i++) {
            HeapTable.InsertResult res = table.insert(samplePayload);
            assertNotNull(res);
            totalInserted++;
        }
        assertTrue(table.getLastPageId() >= 25,
                "Must span across at least 25 pages, actual lastPageId: " + table.getLastPageId());

        bp.flushAll();

        // Warm up / ensure pages reside in the off-heap buffer pool
        final int[] rowCount = {0};
        table.scanPages(page -> {
            rowCount[0] += page.getTupleCount();
        });
        assertEquals(totalInserted, rowCount[0]);

        // Measure JVM GC heap memory allocation during sequential scan
        ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        long threadId = Thread.currentThread().threadId();

        // Perform multiple scan iterations to ensure JIT compilation
        for (int iter = 0; iter < 3; iter++) {
            rowCount[0] = 0;
            table.scanPages(page -> {
                rowCount[0] += page.getTupleCount();
            });
        }

        // Assert page off-heap properties
        SlottedPage samplePage = (SlottedPage) bp.getPage("test_scan", 0);
        assertTrue(samplePage.isOffHeap(), "Sample page must be off-heap");
        assertTrue(samplePage.getBuffer().isDirect(), "Sample page buffer must be direct");
        assertNotNull(samplePage.getMemorySegment(), "Sample page must have native MemorySegment");
        bp.unpinPage("test_scan", 0);

        // Measure exact heap bytes allocated
        long startBytes = threadMXBean.getThreadAllocatedBytes(threadId);

        final int[] verifiedCount = {0};
        table.scanPages(page -> {
            verifiedCount[0] += page.getTupleCount();
        });

        long endBytes = threadMXBean.getThreadAllocatedBytes(threadId);
        long heapAllocated = endBytes - startBytes;

        assertEquals(totalInserted, verifiedCount[0]);

        // Over 30 pages of 8192 bytes = 245,760+ bytes of page data.
        // With off-heap storage, ZERO bytes of page data are allocated on the Java heap.
        // Total allocations during the scan must be a fraction of the 245KB+ page payload (< 35KB).
        assertTrue(heapAllocated < 40000,
                "Sequential scan must have near-zero GC heap allocation! Expected < 40000 bytes, was: " + heapAllocated);
    }

    @Test
    void testBufferPoolEvictionWithOffHeapPages() {
        DiskManager dm = new DiskManager(tempDir.toString());
        // Tight pool of only 4 pages
        BufferPoolManager bp = track(new BufferPoolManager(4, dm, 0L, false));

        // Write to 12 different pages, triggering eviction
        for (long i = 0; i < 12; i++) {
            Page p = bp.getPage("t_evict", i);
            p.getBuffer().putLong(24, 999000L + i);
            bp.markDirty("t_evict", i);
            bp.unpinPage("t_evict", i);
        }

        assertTrue(bp.getCacheSize() <= 4, "Cache size must not exceed maxPages (4)");

        // Reload previously evicted pages and verify content was safely preserved on disk
        for (long i = 0; i < 12; i++) {
            Page p = bp.getPage("t_evict", i);
            assertEquals(999000L + i, p.getBuffer().getLong(24),
                    "Evicted and reloaded page " + i + " must retain data");
            assertTrue(p.isOffHeap(), "Reloaded page must be stored off-heap");
            bp.unpinPage("t_evict", i);
        }
    }

    @Test
    void testBackgroundFlusherCleanShutdown() {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(16, dm, 50L, true);

        assertTrue(bp.isBackgroundFlusherRunning(), "Background flusher must be alive");
        bp.close();
        assertFalse(bp.isBackgroundFlusherRunning(), "Background flusher must stop cleanly on close()");
    }
}
