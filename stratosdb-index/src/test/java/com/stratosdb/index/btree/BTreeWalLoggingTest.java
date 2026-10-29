package com.stratosdb.index.btree;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.BTreePage;
import com.stratosdb.storage.wal.WALManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests verifying B+Tree WAL logging and recovery:
 * OP_INDEX_INSERT, OP_INDEX_SPLIT, OP_INDEX_DELETE, OP_INDEX_MERGE,
 * PageLSN idempotency, and full structural validation across ungraceful crashes.
 */
class BTreeWalLoggingTest {

    @TempDir
    Path tempDir;

    private final List<BufferPoolManager> openPools = new ArrayList<>();
    private final List<WALManager> openWals = new ArrayList<>();

    private BufferPoolManager track(BufferPoolManager pool) {
        openPools.add(pool);
        return pool;
    }

    private WALManager track(WALManager wal) {
        openWals.add(wal);
        return wal;
    }

    @AfterEach
    void tearDown() {
        for (BufferPoolManager pool : openPools) {
            try {
                pool.close();
            } catch (Exception ignored) {}
        }
        openPools.clear();
        for (WALManager wal : openWals) {
            try {
                wal.close();
            } catch (Exception ignored) {}
        }
        openWals.clear();
    }

    @Test
    void testWalLoggingOnInsertAndSearchAfterRecovery() {
        String indexName = "idx_wal_insert";
        String dir = tempDir.toString();

        // Phase 1: Insert entries with WAL logging, but abort process WITHOUT flushing buffer pool
        {
            DiskManager dm = new DiskManager(dir);
            BufferPoolManager bp = track(new BufferPoolManager(64, dm));
            WALManager wal = track(new WALManager(dir));
            BTreeIndex index = new BTreeIndex(indexName, bp, wal);

            for (long i = 1; i <= 50; i++) {
                index.insert(i, new BTreePage.RID(i * 10, (int) (i % 5)));
            }

            assertTrue(wal.getCurrentLSN() > 0, "WAL records must be generated for index inserts");
            // Simulate crash: discard buffer pool without bp.flushAll() or bp.close()
        }

        // Phase 2: Fresh start, run recovery from WAL
        {
            DiskManager dm2 = new DiskManager(dir);
            BufferPoolManager bp2 = track(new BufferPoolManager(64, dm2));
            WALManager wal2 = track(new WALManager(dir));
            wal2.recover(dm2);

            BTreeIndex index2 = new BTreeIndex(indexName, bp2, wal2);
            int validatedCount = index2.validateTree();
            assertEquals(50, validatedCount, "validateTree must find all 50 replayed keys");

            for (long i = 1; i <= 50; i++) {
                BTreePage.RID rid = index2.search(i);
                assertNotNull(rid, "Key " + i + " must be searchable after WAL recovery");
                assertEquals(i * 10, rid.pageId());
                assertEquals((int) (i % 5), rid.slot());
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testWalLoggingOnLeafSplitAndRecovery() {
        String indexName = "idx_wal_split";
        String dir = tempDir.toString();
        int totalKeys = 600; // Exceeds MAX_LEAF_KEYS (408), forcing at least one leaf split and new root

        // Phase 1: Insert enough keys to split leaf into multiple pages
        {
            DiskManager dm = new DiskManager(dir);
            BufferPoolManager bp = track(new BufferPoolManager(64, dm));
            WALManager wal = track(new WALManager(dir));
            BTreeIndex index = new BTreeIndex(indexName, bp, wal);

            for (long i = 0; i < totalKeys; i++) {
                index.insert(i, new BTreePage.RID(i, 0));
            }
            assertTrue(index.getRootPageId() > 1, "Root should have split and allocated new root");
            // Simulate sudden crash: do not flush buffer pool
        }

        // Phase 2: Recover from WAL and verify structural invariants
        {
            DiskManager dm2 = new DiskManager(dir);
            BufferPoolManager bp2 = track(new BufferPoolManager(64, dm2));
            WALManager wal2 = track(new WALManager(dir));
            wal2.recover(dm2);

            BTreeIndex index2 = new BTreeIndex(indexName, bp2, wal2);
            int total = index2.validateTree();
            assertEquals(totalKeys, total, "All " + totalKeys + " keys must be present and structurally sound");

            List<BTreePage.RID> scan = index2.rangeScan(0, totalKeys - 1);
            assertEquals(totalKeys, scan.size(), "Range scan must traverse sibling chain without missing keys");
            for (int i = 0; i < totalKeys; i++) {
                assertEquals(i, scan.get(i).pageId());
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testWalLoggingOnDeleteAndRecovery() {
        String indexName = "idx_wal_delete";
        String dir = tempDir.toString();
        int n = 500;

        // Phase 1: Insert keys, then delete a subset
        {
            DiskManager dm = new DiskManager(dir);
            BufferPoolManager bp = track(new BufferPoolManager(64, dm));
            WALManager wal = track(new WALManager(dir));
            BTreeIndex index = new BTreeIndex(indexName, bp, wal);

            for (long i = 0; i < n; i++) {
                index.insert(i, new BTreePage.RID(i, 0));
            }
            for (long i = 0; i < 150; i++) {
                index.delete(i, new BTreePage.RID(i, 0));
            }
            // Crash without graceful flush
        }

        // Phase 2: Recover from WAL
        {
            DiskManager dm2 = new DiskManager(dir);
            BufferPoolManager bp2 = track(new BufferPoolManager(64, dm2));
            WALManager wal2 = track(new WALManager(dir));
            wal2.recover(dm2);

            BTreeIndex index2 = new BTreeIndex(indexName, bp2, wal2);
            int validatedCount = index2.validateTree();
            assertEquals(n - 150, validatedCount);

            for (long i = 0; i < 150; i++) {
                assertNull(index2.search(i), "Key " + i + " was deleted and must not be found");
            }
            for (long i = 150; i < n; i++) {
                assertNotNull(index2.search(i), "Key " + i + " must remain intact");
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testWalLoggingOnMergeAndRootCollapse() {
        String indexName = "idx_wal_merge";
        String dir = tempDir.toString();
        int n = 1500;

        // Phase 1: Insert keys to build multi-level tree, then delete almost everything to force merge and collapse
        {
            DiskManager dm = new DiskManager(dir);
            BufferPoolManager bp = track(new BufferPoolManager(128, dm));
            WALManager wal = track(new WALManager(dir));
            BTreeIndex index = new BTreeIndex(indexName, bp, wal);

            for (long i = 0; i < n; i++) {
                index.insert(i, new BTreePage.RID(i, 0));
            }
            // Delete almost everything to collapse tree
            for (long i = 10; i < n; i++) {
                index.delete(i, new BTreePage.RID(i, 0));
            }
            // Crash without flush
        }

        // Phase 2: Recover and verify tree collapsed safely with 0 broken links
        {
            DiskManager dm2 = new DiskManager(dir);
            BufferPoolManager bp2 = track(new BufferPoolManager(128, dm2));
            WALManager wal2 = track(new WALManager(dir));
            wal2.recover(dm2);

            BTreeIndex index2 = new BTreeIndex(indexName, bp2, wal2);
            int count = index2.validateTree();
            assertEquals(10, count, "Exactly 10 keys must survive merge and collapse");

            for (long i = 0; i < 10; i++) {
                assertNotNull(index2.search(i));
            }
        }
    }

    @Test
    void testPageLsnPreventsDuplicateRedo() {
        String indexName = "idx_wal_lsn_idempotency";
        String dir = tempDir.toString();

        DiskManager dm = new DiskManager(dir);
        BufferPoolManager bp = track(new BufferPoolManager(64, dm));
        WALManager wal = track(new WALManager(dir));
        BTreeIndex index = new BTreeIndex(indexName, bp, wal);

        for (long i = 1; i <= 20; i++) {
            index.insert(i, new BTreePage.RID(i, 0));
        }

        // Flush all pages to disk and record current file size
        bp.flushAll();

        // Now run recovery against already-flushed pages:
        // PageLSN check will see page.getPageLSN() >= recordLSN and skip redo
        assertDoesNotThrow(() -> wal.recover(dm));

        BTreeIndex index2 = new BTreeIndex(indexName, bp, wal);
        assertEquals(20, index2.validateTree());
        for (long i = 1; i <= 20; i++) {
            assertEquals(new BTreePage.RID(i, 0), index2.search(i));
        }
    }
}
