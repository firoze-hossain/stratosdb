package com.stratosdb.storage;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.transaction.Transaction;
import com.stratosdb.transaction.TransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real snapshot-isolation behavior against the actual HeapTable/TransactionManager
 * classes - no mocking of the MVCC machinery itself.
 */
class MvccIsolationTest {

    @TempDir
    Path tempDir;

    private HeapTable newTable(String name) {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(64, dm);
        return new HeapTable(name, bp);
    }

    @Test
    void snapshotDoesNotSeeCommitsThatHappenAfterItWasTaken() {
        HeapTable table = newTable("t1");
        TransactionManager txnManager = new TransactionManager();

        // txn1 begins first, before anything else exists.
        Transaction txn1 = txnManager.begin();
        assertTrue(table.scanMvcc(txn1.getSnapshot(), txnManager).isEmpty());

        // txn2 begins after txn1, inserts a row, and commits.
        Transaction txn2 = txnManager.begin();
        table.insertMvcc("row-from-txn2".getBytes(), txn2.getXID());
        txnManager.commit(txn2);

        // txn1's snapshot was taken before txn2 committed - it must not see it,
        // even though txn2 is now committed and a brand-new transaction would see it.
        List<byte[]> txn1View = table.scanMvcc(txn1.getSnapshot(), txnManager);
        assertEquals(0, txn1View.size(),
            "txn1 started before txn2 committed and must not observe txn2's insert");

        // A transaction that begins AFTER txn2 committed must see it.
        Transaction txn3 = txnManager.begin();
        List<byte[]> txn3View = table.scanMvcc(txn3.getSnapshot(), txnManager);
        assertEquals(1, txn3View.size());
        assertEquals("row-from-txn2", new String(txn3View.get(0)));

        txnManager.commit(txn1);
        txnManager.commit(txn3);
    }

    @Test
    void uncommittedInsertsAreInvisibleToOtherTransactions() {
        HeapTable table = newTable("t2");
        TransactionManager txnManager = new TransactionManager();

        Transaction writer = txnManager.begin();
        table.insertMvcc("not-committed-yet".getBytes(), writer.getXID());
        // Deliberately not committing yet.

        Transaction reader = txnManager.begin();
        List<byte[]> readerView = table.scanMvcc(reader.getSnapshot(), txnManager);
        assertEquals(0, readerView.size(), "an uncommitted insert must not be visible to another transaction");

        // The writer itself must see its own uncommitted insert.
        List<byte[]> writerOwnView = table.scanMvcc(writer.getSnapshot(), txnManager);
        assertEquals(1, writerOwnView.size(), "a transaction must see its own uncommitted writes");

        txnManager.abort(writer);
        Transaction afterAbort = txnManager.begin();
        List<byte[]> afterAbortView = table.scanMvcc(afterAbort.getSnapshot(), txnManager);
        assertEquals(0, afterAbortView.size(), "an aborted insert must never become visible to anyone");

        txnManager.commit(reader);
        txnManager.commit(afterAbort);
    }

    @Test
    void deleteIsInvisibleToSnapshotsTakenBeforeTheDeleteCommitted() throws Exception {
        HeapTable table = newTable("t3");
        TransactionManager txnManager = new TransactionManager();

        Transaction inserter = txnManager.begin();
        HeapTable.InsertResult inserted = table.insertMvcc("to-be-deleted".getBytes(), inserter.getXID());
        txnManager.commit(inserter);

        Transaction beforeDelete = txnManager.begin(); // snapshot taken before the delete
        assertEquals(1, table.scanMvcc(beforeDelete.getSnapshot(), txnManager).size());

        Transaction deleter = txnManager.begin();
        boolean removed = table.deleteMvcc(inserted.pageId, inserted.slot, deleter.getXID(),
            deleter.getSnapshot(), txnManager, txnManager.getLockManager());
        assertTrue(removed);
        txnManager.commit(deleter);

        // Old snapshot must still see the row - that's what "snapshot" means.
        assertEquals(1, table.scanMvcc(beforeDelete.getSnapshot(), txnManager).size(),
            "a snapshot taken before the delete committed must still see the row as present");

        // A new snapshot, taken after the delete committed, must not see it.
        Transaction afterDelete = txnManager.begin();
        assertEquals(0, table.scanMvcc(afterDelete.getSnapshot(), txnManager).size());

        txnManager.commit(beforeDelete);
        txnManager.commit(afterDelete);
    }
}
