package com.stratosdb.storage.wal;

import com.stratosdb.storage.disk.DiskManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Focused unit coverage for WAL archiving itself - the real foundation
 * point-in-time recovery is built on (see PitrBackup/PitrRestore, and
 * PitrEndToEndTest for the full, real round trip). This tests
 * WALManager's own archiving mechanics in isolation: disabled by
 * default (every pre-existing test and deployment must be
 * unaffected), sequential numbering, and correct resumption of that
 * numbering across a real restart.
 */
class WalArchivingTest {

    @Test
    void archivingIsDisabledByDefault(@TempDir Path tempDir) {
        WALManager wal = new WALManager(tempDir.toString());
        assertNull(wal.getWalArchiveDirectory(), "WAL archiving must be off by default - every pre-existing deployment must be unaffected");
        wal.close();
    }

    @Test
    void enablingArchivingArchivesEachSegmentBeforeTruncating(@TempDir Path tempDir) throws Exception {
        Path dataDir = tempDir.resolve("data");
        Path archiveDir = tempDir.resolve("archive");
        WALManager wal = new WALManager(dataDir.toString());
        wal.setWalArchiveDirectory(archiveDir.toString());
        DiskManager dm = new DiskManager(dataDir.toString());

        wal.logInsert("t", 1L, 0L, 0, "hello".getBytes());
        wal.logCommit(1L);
        long lsnBeforeCheckpoint = wal.getCurrentLSN();
        assertTrue(lsnBeforeCheckpoint > 0);

        wal.recover(dm);

        File[] archived = new File(archiveDir.toString()).listFiles((d, name) -> name.matches("\\d{12}\\.walseg"));
        assertNotNull(archived);
        assertEquals(1, archived.length, "the first checkpoint-triggered truncation must archive exactly one segment");
        assertEquals("000000000001.walseg", archived[0].getName());
        assertEquals(lsnBeforeCheckpoint, archived[0].length(), "the archived segment's own byte length must exactly match what was in the WAL right before truncation");

        wal.close();
    }

    @Test
    void archiveNumberingResumesAcrossARealRestartRatherThanCollidingOrOverwriting(@TempDir Path tempDir) throws Exception {
        Path dataDir = tempDir.resolve("data");
        Path archiveDir = tempDir.resolve("archive");

        WALManager wal1 = new WALManager(dataDir.toString());
        wal1.setWalArchiveDirectory(archiveDir.toString());
        DiskManager dm = new DiskManager(dataDir.toString());
        wal1.logInsert("t", 1L, 0L, 0, "a".getBytes());
        wal1.logCommit(1L);
        wal1.recover(dm);
        wal1.close(); // close() itself also archives its own final checkpoint - see its own javadoc

        File[] afterFirstSession = new File(archiveDir.toString()).listFiles((d, name) -> name.matches("\\d{12}\\.walseg"));
        assertNotNull(afterFirstSession);
        int countAfterFirstSession = afterFirstSession.length;
        assertTrue(countAfterFirstSession >= 1);

        // A fresh WALManager, simulating a real restart - must resume archive
        // numbering from where the previous session left off, never restarting at 1
        // and colliding with (or silently overwriting) an already-archived segment.
        WALManager wal2 = new WALManager(dataDir.toString());
        wal2.setWalArchiveDirectory(archiveDir.toString());
        wal2.logInsert("t", 2L, 0L, 1, "b".getBytes());
        wal2.logCommit(2L);
        wal2.recover(dm);

        File[] afterSecondSession = new File(archiveDir.toString()).listFiles((d, name) -> name.matches("\\d{12}\\.walseg"));
        assertNotNull(afterSecondSession);
        assertTrue(afterSecondSession.length > countAfterFirstSession, "a fresh WALManager must add new archive segments, not overwrite the previous session's own");

        java.util.Set<String> names = new java.util.HashSet<>();
        for (File f : afterSecondSession) {
            assertTrue(names.add(f.getName()), "no archived segment filename may ever repeat - a collision here would mean one session's data silently overwrote another's");
        }

        wal2.close();
    }
}
