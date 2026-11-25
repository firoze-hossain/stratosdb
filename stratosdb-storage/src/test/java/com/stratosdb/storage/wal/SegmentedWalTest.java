package com.stratosdb.storage.wal;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sprint 02 Week 03: Comprehensive test suite for 16MB Segmented WAL Files,
 * continuous segment rotation, background pre-allocation, retention policy,
 * and zero-truncation guarantees.
 */
public class SegmentedWalTest {

    @TempDir
    Path tempDir;

    private final List<Runnable> closeActions = new ArrayList<>();

    @BeforeEach
    void setUp() {
        closeActions.clear();
    }

    @AfterEach
    void tearDown() {
        for (Runnable r : closeActions) {
            try {
                r.run();
            } catch (Exception ignored) {}
        }
        closeActions.clear();
    }

    private WALManager track(WALManager wal) {
        closeActions.add(wal::close);
        return wal;
    }

    private BufferPoolManager track(BufferPoolManager bp) {
        closeActions.add(bp::close);
        return bp;
    }

    @Test
    void testSegmentedWalNamingConvention() {
        assertEquals("wal_00000001.log", WALManager.formatSegmentFileName(1));
        assertEquals("wal_00000042.log", WALManager.formatSegmentFileName(42));
        assertEquals("wal_00001000.log", WALManager.formatSegmentFileName(1000));

        assertTrue(WALManager.isSegmentFileName("wal_00000001.log"));
        assertTrue(WALManager.isSegmentFileName("wal_12345678.log"));
        assertFalse(WALManager.isSegmentFileName("wal.log"));
        assertFalse(WALManager.isSegmentFileName("wal_01.log"));
        assertFalse(WALManager.isSegmentFileName("wal_00000001.log.prealloc"));

        assertEquals(1L, WALManager.parseSegmentNumber("wal_00000001.log"));
        assertEquals(42L, WALManager.parseSegmentNumber("wal_00000042.log"));
        assertEquals(99999999L, WALManager.parseSegmentNumber("wal_99999999.log"));
        assertEquals(-1L, WALManager.parseSegmentNumber("invalid"));
    }

    @Test
    void testContinuousSegmentRotationAtBoundary() throws Exception {
        // Use small 32KB segment size to trigger rotation quickly
        long segmentSize = 32 * 1024;
        int keepSegments = 16;
        WALManager wal = track(new WALManager(tempDir.toString(), segmentSize, keepSegments, false));

        File walDir = new File(tempDir.toFile(), "wal");
        File seg1 = new File(walDir, "wal_00000001.log");
        assertTrue(seg1.exists(), "Segment 1 must exist upon initialization");

        // Write enough data to cross 32KB boundary
        byte[] payload = new byte[2048];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i % 127);

        // 32KB / ~2KB = ~16 writes per segment. Writing 40 times forces rotation to segment 2 and 3
        for (int i = 1; i <= 40; i++) {
            wal.logInsert("tbl", i, 0L, i, payload);
            wal.logCommit(i);
        }

        File seg2 = new File(walDir, "wal_00000002.log");
        File seg3 = new File(walDir, "wal_00000003.log");

        assertTrue(seg1.exists(), "Segment 1 must be preserved on disk");
        assertTrue(seg2.exists(), "Segment 2 must be created after segment 1 filled up");
        assertTrue(seg3.exists(), "Segment 3 must be created after segment 2 filled up");

        assertEquals(segmentSize, seg1.length(), "Segment 1 must be exactly full segment size (32KB)");
        assertEquals(segmentSize, seg2.length(), "Segment 2 must be exactly full segment size (32KB)");
        assertTrue(seg3.length() > 0, "Segment 3 must have active records");
    }

    @Test
    void testWalWritesSeamlesslySpanAcrossSegmentBoundaries() throws Exception {
        long segmentSize = 16 * 1024; // 16KB segment size
        WALManager wal = track(new WALManager(tempDir.toString(), segmentSize, 16, false));

        // Write until 500 bytes remain in segment 1
        // Insert a record of 1200 bytes, which spans across segment 1 and 2
        byte[] filler = new byte[15000];
        wal.logInsert("t", 1L, 0L, 0, filler);
        wal.logCommit(1L);

        // Spanning record: 2000 bytes
        byte[] spanningPayload = new byte[2000];
        for (int i = 0; i < spanningPayload.length; i++) spanningPayload[i] = (byte) ('A' + (i % 26));

        long spanningLsn = wal.logInsert("spanning_tbl", 2L, 1L, 5, spanningPayload);
        wal.logCommit(2L);
        assertTrue(spanningLsn > segmentSize, "Spanning record LSN must cross segment boundary");

        // Recover and verify spanning record is cleanly parsed
        DiskManager dm = new DiskManager(tempDir.toString());
        WALManager recoveryWal = track(new WALManager(tempDir.toString(), segmentSize, 16, false));
        recoveryWal.recover(dm);

        // Verify data was applied to page 1 without truncation
        BufferPoolManager bp = track(new BufferPoolManager(64, dm));
        HeapTable table = new HeapTable("spanning_tbl", bp);
        List<byte[]> rows = table.scan();
        assertEquals(1, rows.size(), "Spanning record must be fully recovered across segment boundary");
        assertArrayEquals(spanningPayload, rows.get(0), "Spanning tuple content must match byte-for-byte");
    }

    @Test
    void testBackgroundSegmentPreallocation() throws Exception {
        long segmentSize = 64 * 1024; // 64KB
        WALManager wal = track(new WALManager(tempDir.toString(), segmentSize, 8, true));

        // Background pre-allocator runs asynchronously; allow it to pre-allocate segment 2
        File seg2 = new File(new File(tempDir.toFile(), "wal"), "wal_00000002.log");
        File seg2Pre = new File(new File(tempDir.toFile(), "wal"), "wal_00000002.log.prealloc");

        // Wait up to 3 seconds for background pre-allocation
        long deadline = System.currentTimeMillis() + 3000;
        boolean preallocated = false;
        while (System.currentTimeMillis() < deadline) {
            if (seg2.exists() && seg2.length() == segmentSize) {
                preallocated = true;
                break;
            }
            if (seg2Pre.exists() && seg2Pre.length() == segmentSize) {
                preallocated = true;
                break;
            }
            if (wal.isSegmentPreallocated(2)) {
                preallocated = true;
                break;
            }
            Thread.sleep(50);
        }

        assertTrue(preallocated, "Segment 2 must be pre-allocated in background to segment size");
    }

    @Test
    void testWalKeepSegmentsRetentionPolicy() throws Exception {
        long segmentSize = 16 * 1024; // 16KB
        int walKeepSegments = 3; // Keep only 3 segments
        WALManager wal = track(new WALManager(tempDir.toString(), segmentSize, walKeepSegments, false));

        byte[] payload = new byte[4096];
        // Generate enough data to fill 6 segments (6 * 16KB = 96KB)
        for (int i = 1; i <= 30; i++) {
            wal.logInsert("retention_tbl", i, 0L, i, payload);
            wal.logCommit(i);
        }

        File walDir = new File(tempDir.toFile(), "wal");
        assertTrue(wal.getCurrentSegmentNumber() >= 6, "Must have advanced to at least segment 6");

        // Force a checkpoint to allow pruning segments prior to checkpoint
        wal.checkpoint();
        wal.pruneOldSegments();

        // Segments 1 and 2 should have been pruned (active is ~6, keep 3 means segments 4, 5, 6 kept)
        File seg1 = new File(walDir, "wal_00000001.log");
        File seg2 = new File(walDir, "wal_00000002.log");
        File segActive = new File(walDir, WALManager.formatSegmentFileName(wal.getCurrentSegmentNumber()));

        assertFalse(seg1.exists(), "Segment 1 must be pruned per wal_keep_segments retention threshold");
        assertFalse(seg2.exists(), "Segment 2 must be pruned per wal_keep_segments retention threshold");
        assertTrue(segActive.exists(), "Active segment must be retained");
    }

    @Test
    void testCrashRecoveryAcrossMultipleSegmentsWithoutTruncation() throws Exception {
        long segmentSize = 16 * 1024;
        WALManager wal1 = track(new WALManager(tempDir.toString(), segmentSize, 10, false));

        int totalInserts = 25;
        for (int i = 0; i < totalInserts; i++) {
            Tuple t = new Tuple();
            t.addValue("id", i);
            t.addValue("payload", "x".repeat(1500));
            byte[] data = t.serialize();
            long pageId = i / 4;
            int slot = i % 4;
            wal1.logInsert("multi_seg_tbl", i, pageId, slot, data);
            wal1.logCommit(i);
        }

        // Segments were rotated across multiple files
        List<Long> segsBefore = wal1.getExistingSegmentNumbers();
        assertTrue(segsBefore.size() > 1, "Data must span across multiple WAL segment files");

        // Crash simulation: close WAL channel directly without checkpoint
        wal1.simulateCrash();

        // Reopen and recover
        DiskManager dm = new DiskManager(tempDir.toString());
        WALManager walRecovered = track(new WALManager(tempDir.toString(), segmentSize, 10, false));
        walRecovered.recover(dm);

        // Verify WAL files were preserved WITHOUT truncation
        for (long segNum : segsBefore) {
            File sf = walRecovered.getSegmentFile(segNum);
            assertTrue(sf.exists(), "Segment " + sf.getName() + " must be preserved without truncation");
            assertTrue(sf.length() > 0, "Segment " + sf.getName() + " must not be truncated to 0");
        }

        // Verify all 25 rows recovered
        BufferPoolManager bp = track(new BufferPoolManager(64, dm));
        HeapTable table = new HeapTable("multi_seg_tbl", bp);
        List<byte[]> rows = table.scan(100);
        assertEquals(totalInserts, rows.size(), "All rows spanning multiple segments must survive crash recovery");
    }

    @Test
    void testSeamlessReplicationStreamingAcrossSegments() throws Exception {
        long segmentSize = 16 * 1024; // 16KB
        WALManager wal = track(new WALManager(tempDir.toString(), segmentSize, 10, false));

        byte[] payload = new byte[1024];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i & 0xFF);

        // Write 30KB of data across segment 1 and 2
        for (int i = 1; i <= 25; i++) {
            wal.logInsert("rep_tbl", i, 0L, i, payload);
            wal.logCommit(i);
        }

        long currentLsn = wal.getCurrentLSN();
        assertTrue(currentLsn > segmentSize, "Current LSN must cross into segment 2");

        // Read the entire stream from LSN 0 across segments
        byte[] allBytes = wal.readBytesFrom(0);
        assertEquals(currentLsn, allBytes.length, "readBytesFrom(0) must return complete byte stream across segments");

        // Read tail slice starting at 10KB (midway in segment 1) up to end
        long fromOffset = 10 * 1024;
        byte[] tailBytes = wal.readBytesFrom(fromOffset);
        assertEquals(currentLsn - fromOffset, tailBytes.length, "readBytesFrom(fromOffset) must read seamless tail across segment boundary");
    }

    @Test
    void testManualWalSwitchRotation() throws Exception {
        long segmentSize = 32 * 1024;
        WALManager wal = track(new WALManager(tempDir.toString(), segmentSize, 8, false));

        assertEquals(1L, wal.getCurrentSegmentNumber(), "Should begin on segment 1");
        wal.logInsert("tbl", 1L, 0L, 0, new byte[100]);
        wal.logCommit(1L);

        // Manual rotation (equivalent to pg_switch_wal())
        wal.rotateSegment();

        assertEquals(2L, wal.getCurrentSegmentNumber(), "Should advance to segment 2 after rotateSegment()");
        File seg1 = new File(new File(tempDir.toFile(), "wal"), "wal_00000001.log");
        File seg2 = new File(new File(tempDir.toFile(), "wal"), "wal_00000002.log");

        assertTrue(seg1.exists(), "Previous segment 1 must be preserved on disk");
        assertTrue(seg2.exists(), "New active segment 2 must exist on disk");
    }
}
