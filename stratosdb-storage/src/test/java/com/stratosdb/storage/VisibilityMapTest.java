package com.stratosdb.storage;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.transaction.Transaction;
import com.stratosdb.transaction.TransactionManager;
import com.stratosdb.transaction.locking.LockManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The visibility map: a real prerequisite for index-only scans, built
 * this round specifically to unblock them (see PROGRESS.md). Tested
 * directly against the raw HeapTable/TransactionManager API, at the
 * layer where the actual visibility logic lives, rather than only
 * indirectly through SQL - the same approach HeapTableConcurrencyTest
 * already established for testing a real storage-layer bug precisely.
 */
class VisibilityMapTest {

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
            } catch (Exception ignored) {
            }
        }
        openPools.clear();
    }

    @Test
    void freshInsertIsNotAllVisibleUntilVacuumConfirmsIt() {
        HeapTable table = newTable("t");
        TransactionManager txnMgr = new TransactionManager();

        Transaction tx = txnMgr.begin();
        HeapTable.InsertResult result = table.insertMvcc("row".getBytes(), tx.getXID());
        txnMgr.commit(tx);

        assertFalse(table.isAllVisible(result.pageId), "a freshly inserted, uncheck page must not be all-visible before vacuum confirms it - even though the insert itself already committed");

        long horizon = txnMgr.getOldestActiveXid();
        table.vacuum(horizon, txnMgr);
        assertTrue(table.isAllVisible(result.pageId), "after vacuum, a page containing only committed, never-deleted tuples must become all-visible");
    }

    @Test
    void newInsertClearsAllVisibleOnItsOwnPageImmediately() {
        HeapTable table = newTable("t");
        TransactionManager txnMgr = new TransactionManager();

        Transaction tx1 = txnMgr.begin();
        HeapTable.InsertResult r1 = table.insertMvcc("row1".getBytes(), tx1.getXID());
        txnMgr.commit(tx1);
        table.vacuum(txnMgr.getOldestActiveXid(), txnMgr);
        assertTrue(table.isAllVisible(r1.pageId));

        Transaction tx2 = txnMgr.begin();
        HeapTable.InsertResult r2 = table.insertMvcc("row2".getBytes(), tx2.getXID());
        txnMgr.commit(tx2);

        assertFalse(table.isAllVisible(r2.pageId), "a brand new insert must immediately clear all-visible on the page it lands on, before any vacuum runs again");
    }

    @Test
    void deleteImmediatelyClearsAllVisibleBeforeVacuumEvenRuns() throws Exception {
        HeapTable table = newTable("t");
        TransactionManager txnMgr = new TransactionManager();
        LockManager lockMgr = new LockManager();

        Transaction tx1 = txnMgr.begin();
        HeapTable.InsertResult result = table.insertMvcc("row".getBytes(), tx1.getXID());
        txnMgr.commit(tx1);
        table.vacuum(txnMgr.getOldestActiveXid(), txnMgr);
        assertTrue(table.isAllVisible(result.pageId));

        Transaction tx2 = txnMgr.begin();
        boolean deleted = table.deleteMvcc(result.pageId, result.slot, tx2.getXID(), tx2.getSnapshot(), txnMgr, lockMgr);
        assertTrue(deleted);
        txnMgr.commit(tx2);

        assertFalse(table.isAllVisible(result.pageId), "a delete (setting xmax) must immediately clear all-visible on that page, even before vacuum ever runs again - the tuple's visibility is now snapshot-dependent");
    }

    @Test
    void isAllVisibleNeverThrowsForAnUnknownPage() {
        HeapTable table = newTable("t");
        assertFalse(table.isAllVisible(999), "querying a page that's never been touched must return false safely, not throw");
    }
}
