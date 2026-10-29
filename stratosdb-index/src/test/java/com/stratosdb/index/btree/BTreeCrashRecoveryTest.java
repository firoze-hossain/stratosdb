package com.stratosdb.index.btree;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.BTreePage;
import com.stratosdb.storage.wal.WALManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real SIGKILL crash recovery test for B+Tree structural integrity.
 *
 * Definition of Done (DoD):
 * "SIGKILL during B+tree leaf split recovers with zero orphan pointers or broken links."
 *
 * Verifies that an ungraceful hard termination (destroyForcibly / SIGKILL) mid-split
 * leaves no dangling parent pointers, no broken nextLeafPageId sibling links,
 * and that WALManager.recover() restores complete structural consistency.
 */
class BTreeCrashRecoveryTest {

    @TempDir
    Path tempDir;

    private String dataDir;
    private final List<BufferPoolManager> openPools = new ArrayList<>();
    private final List<WALManager> openWals = new ArrayList<>();

    @BeforeEach
    void setUp() {
        dataDir = tempDir.toString();
    }

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

    private int readMarker(String path) {
        File f = new File(path);
        if (!f.exists()) return -1;
        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            byte[] bytes = new byte[(int) raf.length()];
            raf.readFully(bytes);
            return Integer.parseInt(new String(bytes, StandardCharsets.UTF_8).trim());
        } catch (Exception e) {
            return -1;
        }
    }

    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void sigkillDuringBTreeLeafSplitRecoversWithZeroOrphanPointersOrBrokenLinks() throws Exception {
        int totalKeys = 15_000;

        String classpath = System.getProperty("java.class.path");
        ProcessBuilder pb = new ProcessBuilder(
            System.getProperty("java.home") + "/bin/java",
            "-cp", classpath,
            "com.stratosdb.index.btree.BTreeCrashHarnessMain",
            dataDir,
            String.valueOf(totalKeys)
        );
        pb.redirectErrorStream(true);
        pb.redirectOutput(new File(dataDir, "btree_harness.out"));
        Process process = pb.start();

        // Wait until child has started and inserted past MAX_LEAF_KEYS (408) to ensure
        // leaf splits have been actively occurring
        File marker = new File(dataDir, "committed.marker");
        long startWait = System.currentTimeMillis();
        int lastCommitted = -1;
        while (System.currentTimeMillis() - startWait < 15_000) {
            lastCommitted = readMarker(marker.getAbsolutePath());
            if (lastCommitted >= 450) {
                break;
            }
            Thread.sleep(20);
        }

        assertTrue(lastCommitted >= 450,
            "Harness did not reach split threshold (>= 450 keys) before timeout; lastCommitted=" + lastCommitted);

        assertTrue(process.isAlive(), "Harness must still be actively executing when killed");

        // Hard SIGKILL mid-flight while leaf splits are occurring
        process.destroyForcibly();
        boolean exited = process.waitFor(10, TimeUnit.SECONDS);
        assertTrue(exited, "Killed harness process did not terminate within timeout");

        // Reopen database with fresh managers and perform WAL recovery
        DiskManager diskManager = new DiskManager(dataDir);
        BufferPoolManager bufferPool = track(new BufferPoolManager(64, diskManager));
        WALManager walManager = track(new WALManager(dataDir));

        // Replay WAL index records
        walManager.recover(diskManager);

        // Open BTree index over the recovered state
        BTreeIndex index = new BTreeIndex("btree_crash_idx", bufferPool, walManager);

        // DoD verification: validateTree() checks zero orphan pointers and zero broken links
        int totalValidatedKeys = assertDoesNotThrow(
            index::validateTree,
            "validateTree() must succeed with zero orphan pointers and zero broken links after crash recovery"
        );

        assertTrue(totalValidatedKeys > 0, "Recovered tree must contain validated keys");
        assertTrue(totalValidatedKeys >= lastCommitted,
            "Tree must contain at least all keys reported committed before SIGKILL (" + lastCommitted + " vs " + totalValidatedKeys + ")");

        // Spot-check every committed key to ensure point search finds exact RID
        for (long k = 0; k <= lastCommitted; k++) {
            BTreePage.RID rid = index.search(k);
            assertNotNull(rid, "Key " + k + " must be searchable after recovery from crash mid-split");
            assertEquals(k, rid.pageId(), "Key " + k + " must point to valid row page");
            assertEquals((int) (k % 50), rid.slot());
        }

        // Full range scan must return sorted keys with intact sibling chain
        List<BTreePage.RID> scan = index.rangeScan(0, lastCommitted);
        assertEquals(lastCommitted + 1, scan.size(),
            "Sibling chain traversal via rangeScan must return every committed key in ascending order");

        for (int i = 0; i <= lastCommitted; i++) {
            assertEquals((long) i, scan.get(i).pageId(),
                "Range scan out of order or corrupted at position " + i);
        }
    }
}
