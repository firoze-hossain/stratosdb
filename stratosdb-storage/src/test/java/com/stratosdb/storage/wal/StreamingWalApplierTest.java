package com.stratosdb.storage.wal;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.transaction.mvcc.MVCCVisibility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The replica-side apply logic for real physical (WAL-shipping)
 * replication. These tests generate real WAL bytes via a real
 * WALManager + HeapTable pair (a real "primary"), then feed those exact
 * bytes into a StreamingWalApplier pointed at a completely separate
 * DiskManager (a real, physically separate "replica"), verifying the
 * resulting data matches - the same real, no-mocking standard
 * CrashRecoveryTest already holds this engine to.
 */
public class StreamingWalApplierTest {

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void basicInsertsReplicateCorrectlyInOneChunk(@TempDir Path tempDir) throws Exception {
        File primaryDir = new File(tempDir.toFile(), "primary");
        File replicaDir = new File(tempDir.toFile(), "replica");

        DiskManager primaryDisk = new DiskManager(primaryDir.getPath());
        BufferPoolManager primaryPool = new BufferPoolManager(64, primaryDisk);
        WALManager wal = new WALManager(primaryDir.getPath());
        HeapTable primaryTable = new HeapTable("users", primaryPool);

        long xid = 100;
        for (int i = 0; i < 5; i++) {
            Tuple t = new Tuple();
            t.addValue("id", i);
            t.addValue("name", "user" + i);
            byte[] payload = t.serialize();
            byte[] stored = MVCCVisibility.wrap(payload, xid, MVCCVisibility.NO_XMAX);
            HeapTable.InsertResult result = primaryTable.insertMvcc(stored, xid);
            wal.logInsert("users", xid, result.pageId, result.slot, stored);
        }
        wal.logCommit(xid);
        primaryPool.flushAll();

        byte[] walBytes = readWalFile(primaryDir);
        assertTrue(walBytes.length > 0, "primary should have produced non-empty WAL bytes");

        DiskManager replicaDisk = new DiskManager(replicaDir.getPath());
        StreamingWalApplier applier = new StreamingWalApplier(replicaDisk);
        applier.feed(walBytes);

        assertEquals(1, applier.getTotalTransactionsApplied());
        assertEquals(5, applier.getTotalOpsApplied());
        assertEquals(0, applier.getPendingTransactionCount());

        BufferPoolManager replicaPool = new BufferPoolManager(64, replicaDisk);
        HeapTable replicaTable = new HeapTable("users", replicaPool);
        List<byte[]> rows = replicaTable.scan(100);
        assertEquals(5, rows.size());

        wal.close();
        primaryPool.close();
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void insertsReplicateCorrectlyEvenWhenFedOneByteAtATime(@TempDir Path tempDir) throws Exception {
        // The harshest possible test of a record split across feed() calls -
        // worst-case network fragmentation, one byte per TCP read.
        File primaryDir = new File(tempDir.toFile(), "primary");
        File replicaDir = new File(tempDir.toFile(), "replica");

        DiskManager primaryDisk = new DiskManager(primaryDir.getPath());
        BufferPoolManager primaryPool = new BufferPoolManager(64, primaryDisk);
        WALManager wal = new WALManager(primaryDir.getPath());
        HeapTable primaryTable = new HeapTable("orders", primaryPool);

        long xid = 200;
        for (int i = 0; i < 10; i++) {
            Tuple t = new Tuple();
            t.addValue("id", i);
            t.addValue("amount", i * 10);
            byte[] payload = t.serialize();
            byte[] stored = MVCCVisibility.wrap(payload, xid, MVCCVisibility.NO_XMAX);
            HeapTable.InsertResult result = primaryTable.insertMvcc(stored, xid);
            wal.logInsert("orders", xid, result.pageId, result.slot, stored);
        }
        wal.logCommit(xid);
        primaryPool.flushAll();

        byte[] walBytes = readWalFile(primaryDir);

        DiskManager replicaDisk = new DiskManager(replicaDir.getPath());
        StreamingWalApplier applier = new StreamingWalApplier(replicaDisk);
        for (byte b : walBytes) {
            applier.feed(new byte[]{b});
        }

        assertEquals(1, applier.getTotalTransactionsApplied());
        assertEquals(10, applier.getTotalOpsApplied());

        BufferPoolManager replicaPool = new BufferPoolManager(64, replicaDisk);
        HeapTable replicaTable = new HeapTable("orders", replicaPool);
        assertEquals(10, replicaTable.scan(100).size());

        wal.close();
        primaryPool.close();
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void uncommittedTransactionStaysBufferedAndNeverApplies(@TempDir Path tempDir) throws Exception {
        File primaryDir = new File(tempDir.toFile(), "primary");
        File replicaDir = new File(tempDir.toFile(), "replica");

        DiskManager primaryDisk = new DiskManager(primaryDir.getPath());
        BufferPoolManager primaryPool = new BufferPoolManager(64, primaryDisk);
        WALManager wal = new WALManager(primaryDir.getPath());
        HeapTable primaryTable = new HeapTable("t", primaryPool);

        long xid = 300;
        Tuple t = new Tuple();
        t.addValue("id", 1);
        byte[] stored = MVCCVisibility.wrap(t.serialize(), xid, MVCCVisibility.NO_XMAX);
        HeapTable.InsertResult result = primaryTable.insertMvcc(stored, xid);
        wal.logInsert("t", xid, result.pageId, result.slot, stored);
        // Deliberately no commit - simulates a transaction still in flight, or one that aborted
        // (this engine's WAL never writes an explicit abort record - see the class-level
        // javadoc on StreamingWalApplier's own known, honestly-stated limitations).

        byte[] walBytes = readWalFile(primaryDir);

        DiskManager replicaDisk = new DiskManager(replicaDir.getPath());
        StreamingWalApplier applier = new StreamingWalApplier(replicaDisk);
        applier.feed(walBytes);

        assertEquals(0, applier.getTotalTransactionsApplied());
        assertEquals(1, applier.getPendingTransactionCount());

        BufferPoolManager replicaPool = new BufferPoolManager(64, replicaDisk);
        HeapTable replicaTable = new HeapTable("t", replicaPool);
        assertEquals(0, replicaTable.scan(100).size(), "an uncommitted insert must never be visible on the replica");

        wal.close();
        primaryPool.close();
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void interleavedTransactionsEachApplyIndependentlyWhenTheirOwnCommitArrives(@TempDir Path tempDir) throws Exception {
        File primaryDir = new File(tempDir.toFile(), "primary");
        File replicaDir = new File(tempDir.toFile(), "replica");

        DiskManager primaryDisk = new DiskManager(primaryDir.getPath());
        BufferPoolManager primaryPool = new BufferPoolManager(64, primaryDisk);
        WALManager wal = new WALManager(primaryDir.getPath());
        HeapTable primaryTable = new HeapTable("t", primaryPool);

        // Two transactions, interleaved in the log: xid1 starts, xid2 starts, xid2 commits
        // first, xid1 inserts again, then xid1 commits.
        long xid1 = 401, xid2 = 402;
        Tuple ta = new Tuple();
        ta.addValue("id", 1);
        byte[] storedA = MVCCVisibility.wrap(ta.serialize(), xid1, MVCCVisibility.NO_XMAX);
        HeapTable.InsertResult ra = primaryTable.insertMvcc(storedA, xid1);
        wal.logInsert("t", xid1, ra.pageId, ra.slot, storedA);

        Tuple tb = new Tuple();
        tb.addValue("id", 2);
        byte[] storedB = MVCCVisibility.wrap(tb.serialize(), xid2, MVCCVisibility.NO_XMAX);
        HeapTable.InsertResult rb = primaryTable.insertMvcc(storedB, xid2);
        wal.logInsert("t", xid2, rb.pageId, rb.slot, storedB);

        wal.logCommit(xid2);

        Tuple tc = new Tuple();
        tc.addValue("id", 3);
        byte[] storedC = MVCCVisibility.wrap(tc.serialize(), xid1, MVCCVisibility.NO_XMAX);
        HeapTable.InsertResult rc = primaryTable.insertMvcc(storedC, xid1);
        wal.logInsert("t", xid1, rc.pageId, rc.slot, storedC);

        wal.logCommit(xid1);
        primaryPool.flushAll();

        byte[] walBytes = readWalFile(primaryDir);

        DiskManager replicaDisk = new DiskManager(replicaDir.getPath());
        StreamingWalApplier applier = new StreamingWalApplier(replicaDisk);
        applier.feed(walBytes);

        assertEquals(2, applier.getTotalTransactionsApplied());
        assertEquals(3, applier.getTotalOpsApplied());

        BufferPoolManager replicaPool = new BufferPoolManager(64, replicaDisk);
        HeapTable replicaTable = new HeapTable("t", replicaPool);
        assertEquals(3, replicaTable.scan(100).size());

        wal.close();
        primaryPool.close();
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void updatesAndDeletesReplicateCorrectly(@TempDir Path tempDir) throws Exception {
        File primaryDir = new File(tempDir.toFile(), "primary");
        File replicaDir = new File(tempDir.toFile(), "replica");

        DiskManager primaryDisk = new DiskManager(primaryDir.getPath());
        BufferPoolManager primaryPool = new BufferPoolManager(64, primaryDisk);
        WALManager wal = new WALManager(primaryDir.getPath());
        HeapTable primaryTable = new HeapTable("t", primaryPool);

        long xid1 = 500;
        Tuple t1 = new Tuple();
        t1.addValue("id", 1);
        t1.addValue("status", "active");
        byte[] stored1 = MVCCVisibility.wrap(t1.serialize(), xid1, MVCCVisibility.NO_XMAX);
        HeapTable.InsertResult r1 = primaryTable.insertMvcc(stored1, xid1);
        wal.logInsert("t", xid1, r1.pageId, r1.slot, stored1);

        Tuple t2 = new Tuple();
        t2.addValue("id", 2);
        t2.addValue("status", "active");
        byte[] stored2 = MVCCVisibility.wrap(t2.serialize(), xid1, MVCCVisibility.NO_XMAX);
        HeapTable.InsertResult r2 = primaryTable.insertMvcc(stored2, xid1);
        wal.logInsert("t", xid1, r2.pageId, r2.slot, stored2);
        wal.logCommit(xid1);
        primaryPool.flushAll();

        long xid2 = 501;
        Tuple t1New = new Tuple();
        t1New.addValue("id", 1);
        t1New.addValue("status", "inactive");
        byte[] newStored1 = MVCCVisibility.wrap(t1New.serialize(), xid1, MVCCVisibility.NO_XMAX);
        wal.logUpdate("t", xid2, r1.pageId, r1.slot, stored1, newStored1);
        wal.logDelete("t", xid2, r2.pageId, r2.slot);
        wal.logCommit(xid2);
        primaryPool.flushAll();

        byte[] walBytes = readWalFile(primaryDir);

        DiskManager replicaDisk = new DiskManager(replicaDir.getPath());
        StreamingWalApplier applier = new StreamingWalApplier(replicaDisk);
        applier.feed(walBytes);

        assertEquals(2, applier.getTotalTransactionsApplied());

        BufferPoolManager replicaPool = new BufferPoolManager(64, replicaDisk);
        HeapTable replicaTable = new HeapTable("t", replicaPool);
        List<byte[]> rawRows = replicaTable.scan(100);
        int nonDeleted = 0;
        for (byte[] raw : rawRows) {
            if (MVCCVisibility.readXmax(raw) == MVCCVisibility.NO_XMAX) nonDeleted++;
        }
        assertEquals(1, nonDeleted, "exactly one non-deleted row should remain on the replica after the delete");

        wal.close();
        primaryPool.close();
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void evictsAppliedPagesFromASuppliedBufferPoolSoConcurrentReadsSeeFreshData(@TempDir Path tempDir) throws Exception {
        // A real, previously-latent bug found and fixed while building this feature: applying
        // WAL records straight to DiskManager, bypassing a LIVE replica's own buffer pool
        // entirely, could leave a stale, pre-replication cached page behind for a concurrent
        // read - see StreamingWalApplier's own constructor javadoc.
        File primaryDir = new File(tempDir.toFile(), "primary");
        File replicaDir = new File(tempDir.toFile(), "replica");

        DiskManager primaryDisk = new DiskManager(primaryDir.getPath());
        BufferPoolManager primaryPool = new BufferPoolManager(64, primaryDisk);
        WALManager wal = new WALManager(primaryDir.getPath());
        HeapTable primaryTable = new HeapTable("t", primaryPool);

        long xid = 600;
        Tuple t = new Tuple();
        t.addValue("id", 1);
        byte[] stored = MVCCVisibility.wrap(t.serialize(), xid, MVCCVisibility.NO_XMAX);
        HeapTable.InsertResult result = primaryTable.insertMvcc(stored, xid);
        wal.logInsert("t", xid, result.pageId, result.slot, stored);
        wal.logCommit(xid);
        primaryPool.flushAll();

        DiskManager replicaDisk = new DiskManager(replicaDir.getPath());
        BufferPoolManager replicaPool = new BufferPoolManager(64, replicaDisk);
        HeapTable replicaTable = new HeapTable("t", replicaPool);

        // Prime the replica's own buffer pool cache BEFORE any data has been replicated -
        // this caches an empty page 0, the exact staleness scenario this test targets.
        assertEquals(0, replicaTable.scan(100).size());

        StreamingWalApplier applier = new StreamingWalApplier(replicaDisk, replicaPool);
        applier.feed(readWalFile(primaryDir));

        // A fresh HeapTable would trivially see the new data by re-deriving lastPageId from
        // disk; re-using the SAME, already-primed instance is the real test - it would only
        // see the new row if the stale page was actually evicted from the buffer pool's cache.
        assertEquals(1, replicaTable.scan(100).size(),
            "the already-cached HeapTable instance must see the replicated row, proving the stale page was evicted, not left cached");

        wal.close();
        primaryPool.close();
    }

    private byte[] readWalFile(File dataDir) throws Exception {
        File walFile = new File(dataDir, "wal/wal.log");
        try (RandomAccessFile raf = new RandomAccessFile(walFile, "r")) {
            byte[] data = new byte[(int) raf.length()];
            raf.readFully(data);
            return data;
        }
    }
}
