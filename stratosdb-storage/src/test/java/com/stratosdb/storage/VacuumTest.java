package com.stratosdb.storage;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.transaction.Transaction;
import com.stratosdb.transaction.TransactionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real vacuum behavior against the actual HeapTable/TransactionManager
 * classes - no mocking of the MVCC machinery. The property that matters
 * most here isn't "does vacuum reclaim space" (the easy direction to get
 * right) - it's "does vacuum ever reclaim a version some other active
 * transaction's snapshot still needs" (the direction that's a real
 * correctness bug, not just a missed optimization, if it's ever wrong).
 */
class VacuumTest {

    @TempDir
    Path tempDir;

    private final List<BufferPoolManager> openPools = new ArrayList<>();

    private HeapTable newTable(String name) {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(64, dm);
        openPools.add(bp);
        return new HeapTable(name, bp);
    }

    @AfterEach
    void tearDown() {
        for (BufferPoolManager pool : openPools) {
            try {
                pool.close();
            } catch (Exception e) {
                // Best-effort cleanup; shouldn't mask the test's own result.
            }
        }
        openPools.clear();
    }

    @Test
    void vacuumReclaimsCommittedDeadVersions() throws Exception {
        HeapTable table = newTable("t");
        TransactionManager txnManager = new TransactionManager();

        Transaction inserter = txnManager.begin();
        HeapTable.InsertResult row = table.insertMvcc("v1".getBytes(StandardCharsets.UTF_8), inserter.getXID());
        txnManager.commit(inserter);

        // Update the same row several times - each one tombstones the
        // previous version. All of these transactions commit, so all the
        // resulting dead versions should be reclaimable once nothing active
        // predates them.
        long pageId = row.pageId;
        int slot = row.slot;
        for (int i = 2; i <= 5; i++) {
            Transaction updater = txnManager.begin();
            HeapTable.InsertResult updated = table.updateMvcc(pageId, slot, ("v" + i).getBytes(StandardCharsets.UTF_8),
                updater.getXID(), updater.getSnapshot(), txnManager, txnManager.getLockManager());
            txnManager.commit(updater);
            pageId = updated.pageId;
            slot = updated.slot;
        }

        long horizon = txnManager.getOldestActiveXid();
        HeapTable.VacuumResult result = table.vacuum(horizon, txnManager);
        assertEquals(4, result.reclaimedVersions(), "4 tombstoned versions (v1 through v4) should be reclaimed");
        assertEquals(1, result.pagesCompacted());

        // The current value must be completely unaffected.
        Transaction reader = txnManager.begin();
        List<byte[]> visible = table.scanMvcc(reader.getSnapshot(), txnManager);
        assertEquals(1, visible.size());
        assertEquals("v5", new String(visible.get(0), StandardCharsets.UTF_8));
    }

    @Test
    void vacuumNeverReclaimsAVersionAnOlderActiveTransactionStillNeeds() throws Exception {
        HeapTable table = newTable("t");
        TransactionManager txnManager = new TransactionManager();

        Transaction inserter = txnManager.begin();
        HeapTable.InsertResult inserted = table.insertMvcc("v1".getBytes(StandardCharsets.UTF_8), inserter.getXID());
        txnManager.commit(inserter);

        // This reader's snapshot is taken BEFORE the update below - by
        // snapshot-isolation rules, it must see "v1" for its entire
        // lifetime, no matter what commits after it or how long it stays open.
        Transaction reader = txnManager.begin();

        Transaction updater = txnManager.begin();
        table.updateMvcc(inserted.pageId, inserted.slot, "v2".getBytes(StandardCharsets.UTF_8),
            updater.getXID(), updater.getSnapshot(), txnManager, txnManager.getLockManager());
        txnManager.commit(updater);

        // reader is still open. It must be the oldest active transaction now.
        long horizon = txnManager.getOldestActiveXid();
        assertEquals(reader.getXID(), horizon, "the still-open reader must be the vacuum horizon");

        HeapTable.VacuumResult result = table.vacuum(horizon, txnManager);
        assertEquals(0, result.reclaimedVersions(),
            "the old version must NOT be reclaimed while an active transaction's snapshot still needs it");

        // Prove it's not just a count that stayed at zero - the actual bytes must still be there and correct.
        List<byte[]> visibleToReader = table.scanMvcc(reader.getSnapshot(), txnManager);
        assertEquals(1, visibleToReader.size());
        assertEquals("v1", new String(visibleToReader.get(0), StandardCharsets.UTF_8),
            "the reader's snapshot must still see the pre-update value after a vacuum ran");

        txnManager.commit(reader);

        // Now that the only thing protecting the old version is gone, vacuum must be able to reclaim it.
        long horizonAfter = txnManager.getOldestActiveXid();
        HeapTable.VacuumResult result2 = table.vacuum(horizonAfter, txnManager);
        assertEquals(1, result2.reclaimedVersions(), "once the reader committed, the old version becomes reclaimable");
    }

    @Test
    void vacuumIsIdempotentWhenNothingNewIsDead() throws Exception {
        HeapTable table = newTable("t");
        TransactionManager txnManager = new TransactionManager();

        Transaction inserter = txnManager.begin();
        table.insertMvcc("v1".getBytes(StandardCharsets.UTF_8), inserter.getXID());
        txnManager.commit(inserter);

        long horizon = txnManager.getOldestActiveXid();
        HeapTable.VacuumResult first = table.vacuum(horizon, txnManager);
        assertEquals(0, first.reclaimedVersions(), "nothing is dead yet");

        HeapTable.VacuumResult second = table.vacuum(horizon, txnManager);
        assertEquals(0, second.reclaimedVersions(), "running vacuum again with nothing new dead must report zero, not re-count anything");
    }

    @Test
    void vacuumSkipsUncommittedTombstones() throws Exception {
        // An aborted (or still-in-flight) delete/update must not be treated
        // as "dead" just because its xmax field is set - only a COMMITTED
        // xmax means the old version was genuinely superseded.
        HeapTable table = newTable("t");
        TransactionManager txnManager = new TransactionManager();

        Transaction inserter = txnManager.begin();
        HeapTable.InsertResult inserted = table.insertMvcc("v1".getBytes(StandardCharsets.UTF_8), inserter.getXID());
        txnManager.commit(inserter);

        Transaction deleter = txnManager.begin();
        table.deleteMvcc(inserted.pageId, inserted.slot, deleter.getXID(),
            deleter.getSnapshot(), txnManager, txnManager.getLockManager());
        txnManager.abort(deleter); // the delete never actually committed

        long horizon = txnManager.getOldestActiveXid();
        HeapTable.VacuumResult result = table.vacuum(horizon, txnManager);
        assertEquals(0, result.reclaimedVersions(), "an aborted delete's xmax must not be treated as a committed removal");
    }
}
