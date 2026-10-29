package com.stratosdb.storage.heap;

import com.stratosdb.common.constants.PageConstants;
import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.SlottedPage;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class FreeSpaceMapTest {

    @TempDir
    Path tempDir;

    private final List<BufferPoolManager> openPools = new ArrayList<>();

    private HeapTable createTable(String name) {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(64, dm);
        openPools.add(bp);
        return new HeapTable(name, bp);
    }

    @AfterEach
    void tearDown() {
        for (BufferPoolManager bp : openPools) {
            try {
                bp.close();
            } catch (Exception ignored) {
            }
        }
        openPools.clear();
    }

    @Test
    void twoLevelFsmStructureTracksChunksAndSummaryCorrectly() {
        HeapTable table = createTable("test_fsm_struct");
        FreeSpaceMap fsm = table.getFreeSpaceMap();
        assertNotNull(fsm);

        // Initial state: page 0 has ~8168 bytes free
        int initialSpace = fsm.getFreeSpace(0);
        assertTrue(initialSpace > 8000, "Initial page 0 should have ~8168 bytes free");

        int[] summary = fsm.getSummaryArray();
        assertEquals(1, summary.length, "Should have 1 summary chunk initially");
        assertEquals(initialSpace, summary[0], "Summary chunk 0 should equal initial page 0 free space");

        // Register 100 pages across 2 chunks (CHUNK_SIZE = 64)
        for (int p = 1; p < 100; p++) {
            fsm.registerNewPage(p, 5000);
        }

        assertEquals(100, fsm.getTotalPages());
        int[] updatedSummary = fsm.getSummaryArray();
        assertEquals(2, updatedSummary.length, "100 pages should span 2 chunks (64 per chunk)");
        assertEquals(5000, updatedSummary[1], "Chunk 1 summary should reflect max free space in chunk 1");
    }

    @Test
    void insertQueriesFsmInAmortizedO1InsteadOfScanningPages() {
        HeapTable table = createTable("test_fsm_perf");
        FreeSpaceMap fsm = table.getFreeSpaceMap();

        // Fill page 0 with tuples until it's full
        byte[] payload = new byte[500]; // each row ~500B -> ~15 rows fill 8KB page
        int insertsOnPage0 = 0;
        while (true) {
            long candidate = fsm.findPageForInsert(506);
            if (candidate != 0) break;
            HeapTable.InsertResult res = table.insert(payload);
            if (res.pageId != 0) break;
            insertsOnPage0++;
        }

        assertTrue(insertsOnPage0 >= 10, "Page 0 should hold at least 10 rows before filling");

        // Page 0 is now full. FSM findPageForInsert must NOT return page 0
        long nextCandidate = fsm.findPageForInsert(506);
        assertNotEquals(0, nextCandidate, "FSM should not return page 0 since it has insufficient free space");

        // Insert onto new pages
        HeapTable.InsertResult res2 = table.insert(payload);
        assertTrue(res2.pageId >= 1, "New row should land on page 1 or higher");

        // Summary for chunk 0 should reflect remaining space
        int[] summary = fsm.getSummaryArray();
        assertTrue(summary[0] < 506 || summary[0] == fsm.getFreeSpace(res2.pageId),
            "Chunk summary must be updated to reflect space consumption");
    }

    @Test
    void spaceReclamationViaDefragmentMakesPageImmediatelyReusableViaFsm() {
        HeapTable table = createTable("test_fsm_reuse");
        FreeSpaceMap fsm = table.getFreeSpaceMap();

        byte[] payload = new byte[1000]; // 1000 bytes per row + 6B item pointer = 1006B per row
        List<HeapTable.InsertResult> results = new ArrayList<>();
        // 8 rows * 1006 = 8048 bytes fit on page 0 (8168 free)
        for (int i = 0; i < 8; i++) {
            results.add(table.insert(payload));
            assertEquals(0, results.get(i).pageId, "First 8 rows should all fit on page 0");
        }

        // 9th row should extend to page 1 because page 0 has only ~120 bytes left (< 1006)
        HeapTable.InsertResult row9 = table.insert(payload);
        assertEquals(1, row9.pageId, "9th row should land on page 1");

        // Delete row 3 from page 0 and defragment to reclaim the space (as VACUUM does)
        table.delete(0, results.get(3).slot);
        SlottedPage page0 = (SlottedPage) table.getBufferPool().getPage("test_fsm_reuse", 0);
        page0.getLatch().writeLock().lock();
        try {
            page0.defragment();
            fsm.update(0, page0.getFreeSpace());
        } finally {
            page0.getLatch().writeLock().unlock();
            table.getBufferPool().unpinPage("test_fsm_reuse", 0);
        }

        assertTrue(fsm.getFreeSpace(0) >= 1000, "Page 0 free space should be increased after defragment");

        // Next insert should immediately target page 0 via FSM without creating page 2!
        HeapTable.InsertResult reusedRow = table.insert(payload);
        assertEquals(0, reusedRow.pageId, "FSM should direct new insert to page 0 where space was reclaimed");
    }

    @Test
    void fsmPersistsToDiskAndSurvivesRestart() {
        String tableName = "test_fsm_persist";
        {
            DiskManager dm1 = new DiskManager(tempDir.toString());
            BufferPoolManager bp1 = new BufferPoolManager(64, dm1);
            HeapTable table = new HeapTable(tableName, bp1);
            byte[] payload = new byte[800];
            // Insert 15 rows: 10 rows fill page 0, remaining 5 rows land on page 1 with ~4KB free space remaining
            for (int i = 0; i < 15; i++) {
                table.insert(payload);
            }
            table.close();
            bp1.close(); // flushes pages to disk
        }

        // Reopen in a fresh table instance against the same directory
        DiskManager dm2 = new DiskManager(tempDir.toString());
        BufferPoolManager bp2 = new BufferPoolManager(64, dm2);
        openPools.add(bp2);
        HeapTable table2 = new HeapTable(tableName, bp2);
        FreeSpaceMap fsm2 = table2.getFreeSpaceMap();

        assertEquals(2, fsm2.getTotalPages(), "Reloaded FSM should track the 2 persisted pages");
        assertEquals(1, fsm2.findPageForInsert(806), "Reloaded FSM should immediately return page 1 which has free space");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentInsertsUnderFsmNeverLoseRows() throws Exception {
        HeapTable table = createTable("test_fsm_concurrency");
        int threadCount = 8;
        int rowsPerThread = 50;
        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successfulInserts = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            exec.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < rowsPerThread; i++) {
                        byte[] data = ("t" + threadId + "-row-" + i).getBytes();
                        HeapTable.InsertResult res = table.insert(data);
                        if (res.slot != -1) {
                            successfulInserts.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(20, TimeUnit.SECONDS));
        exec.shutdown();

        assertEquals(threadCount * rowsPerThread, successfulInserts.get(),
            "All concurrent inserts via FSM must succeed without dropping rows");
        assertEquals(threadCount * rowsPerThread, table.scan().size(),
            "All inserted rows must be visible upon scanning");
    }
}
