package com.stratosdb.storage.wal;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.SlottedPage;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PageLsnRecoveryTest {

    @TempDir
    Path tempDir;

    private final List<BufferPoolManager> openPools = new ArrayList<>();

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
    void slottedPageStoresAndRetrievesPageLsnHeader() {
        SlottedPage page = new SlottedPage(1);
        assertEquals(0L, page.getPageLSN(), "Initial page LSN should be 0");

        page.setPageLSN(123456789L);
        assertEquals(123456789L, page.getPageLSN(), "Stored page LSN should match");
        assertTrue(page.isDirty(), "Setting page LSN must mark page dirty");

        // Verify across raw bytes round-trip
        byte[] bytes = page.getBytes();
        SlottedPage reloaded = new SlottedPage(1, bytes);
        assertEquals(123456789L, reloaded.getPageLSN(), "Reloaded slotted page must retain exact 8-byte PageLSN");
    }

    @Test
    void redoSkipsAlreadyFlushedPageWhenPageLsnGreaterOrEqualRecordLsn() {
        String tableName = "t_lsn_skip";
        DiskManager dm = new DiskManager(tempDir.toString());
        WALManager wal = new WALManager(tempDir.toString());

        // Prepare a tuple
        Tuple tuple = new Tuple();
        tuple.addValue("id", 1);
        tuple.addValue("val", "hello");
        byte[] data = tuple.serialize();

        // 1. Log an insert into page 0 slot 0 at xid 1
        long lsn = wal.logInsert(tableName, 1L, 0L, 0, data);
        wal.logCommit(1L);
        assertTrue(lsn > 0, "WAL logInsert must return positive LSN");

        // 2. Prepare a page on disk that ALREADY has this insert and is stamped with pageLSN == lsn
        SlottedPage diskPage = new SlottedPage(0);
        diskPage.insertTuple(data);
        diskPage.setPageLSN(lsn); // already flushed to disk at or after this LSN
        dm.writePage(tableName, diskPage);

        // 3. Run WAL recovery
        wal.recover(dm);

        // 4. Verify that page 0 still has exactly 1 row (redo skipped re-inserting)
        BufferPoolManager bp = new BufferPoolManager(64, dm);
        openPools.add(bp);
        HeapTable table = new HeapTable(tableName, bp);
        List<byte[]> rows = table.scan();
        assertEquals(1, rows.size(), "Redo must skip already-flushed page, avoiding duplicate row insertion");
    }

    @Test
    void redoAppliesUnflushedRecordWhenPageLsnLessThanRecordLsn() {
        String tableName = "t_lsn_apply";
        DiskManager dm = new DiskManager(tempDir.toString());
        WALManager wal = new WALManager(tempDir.toString());

        Tuple tuple = new Tuple();
        tuple.addValue("id", 42);
        tuple.addValue("name", "unflushed_data");
        byte[] data = tuple.serialize();

        // 1. Log insert and commit
        long recordLSN = wal.logInsert(tableName, 10L, 0L, 0, data);
        wal.logCommit(10L);
        assertTrue(recordLSN > 0);

        // 2. Prepare page on disk with PageLSN = 0 (never flushed the write before crash)
        SlottedPage emptyPage = new SlottedPage(0);
        emptyPage.setPageLSN(0L);
        dm.writePage(tableName, emptyPage);

        // 3. Run WAL recovery
        wal.recover(dm);

        // 4. Verify that redo applied the record and updated pageLSN on disk
        SlottedPage recoveredPage = new SlottedPage(0, dm.readPage(tableName, 0).getBytes());
        assertEquals(1, recoveredPage.getTupleCount(), "Redo must apply unflushed record");
        assertEquals(recordLSN, recoveredPage.getPageLSN(), "Redo must stamp page with record LSN");

        BufferPoolManager bp = new BufferPoolManager(64, dm);
        openPools.add(bp);
        HeapTable table = new HeapTable(tableName, bp);
        assertEquals(1, table.scan().size());
    }

    @Test
    void multiRecordRedoOnlyAppliesUnappliedTailRecords() {
        String tableName = "t_lsn_tail";
        DiskManager dm = new DiskManager(tempDir.toString());
        WALManager wal = new WALManager(tempDir.toString());

        Tuple t1 = new Tuple();
        t1.addValue("id", 101);
        byte[] d1 = t1.serialize();

        Tuple t2 = new Tuple();
        t2.addValue("id", 102);
        byte[] d2 = t2.serialize();

        // Record 1
        long lsn1 = wal.logInsert(tableName, 1L, 0L, 0, d1);
        wal.logCommit(1L);

        // Record 2
        long lsn2 = wal.logInsert(tableName, 2L, 0L, 1, d2);
        wal.logCommit(2L);
        assertTrue(lsn2 > lsn1, "Subsequent WAL record must have higher LSN");

        // Simulate page was flushed after Record 1 (contains d1, stamped with lsn1),
        // but crashed before Record 2 was flushed
        SlottedPage pageOnDisk = new SlottedPage(0);
        pageOnDisk.insertTuple(d1);
        pageOnDisk.setPageLSN(lsn1);
        dm.writePage(tableName, pageOnDisk);

        // Run recovery
        wal.recover(dm);

        // Verify: Record 1 was NOT re-applied (would have resulted in 3 rows if re-applied),
        // Record 2 was applied, resulting in exactly 2 rows on disk with pageLSN == lsn2!
        SlottedPage finalPage = new SlottedPage(0, dm.readPage(tableName, 0).getBytes());
        assertEquals(2, finalPage.getTupleCount(), "Final page must have exactly 2 rows (d1 intact, d2 applied once)");
        assertEquals(lsn2, finalPage.getPageLSN(), "Final pageLSN must be stamped with latest record LSN");
    }
}
