package com.stratosdb.storage.heap;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.transaction.Transaction;
import com.stratosdb.transaction.TransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for persistent 2-bit-per-page VisibilityMap (all-visible, all-frozen).
 */
public class VisibilityMapTest {

    @Test
    void testTwoBitPackingAndPersistenceAcrossRestart(@TempDir Path tempDir) {
        DiskManager dm1 = new DiskManager(tempDir.toString());
        BufferPoolManager bp1 = new BufferPoolManager(32, dm1);
        try {
            VisibilityMap vm = new VisibilityMap("orders", bp1, 0);
            vm.setVisibility(0, true, false);
            vm.setVisibility(1, true, true);
            vm.setVisibility(2, false, false);
            vm.setVisibility(3, true, true);
            vm.setVisibility(7, true, false);

            assertTrue(vm.isAllVisible(0));
            assertFalse(vm.isAllFrozen(0));
            assertTrue(vm.isAllVisible(1));
            assertTrue(vm.isAllFrozen(1));
            assertFalse(vm.isAllVisible(2));
            assertFalse(vm.isAllFrozen(2));
            assertTrue(vm.isAllVisible(3));
            assertTrue(vm.isAllFrozen(3));
            assertTrue(vm.isAllVisible(7));
            assertFalse(vm.isAllFrozen(7));
            assertEquals(4, vm.countAllVisible());
            assertEquals(2, vm.countAllFrozen());

            assertTrue(vm.saveToDisk());
        } finally {
            bp1.close();
            dm1.close();
        }

        // Reload in fresh DiskManager / BufferPoolManager
        DiskManager dm2 = new DiskManager(tempDir.toString());
        BufferPoolManager bp2 = new BufferPoolManager(32, dm2);
        try {
            VisibilityMap reloaded = new VisibilityMap("orders", bp2, 8);
            assertTrue(reloaded.isAllVisible(0));
            assertFalse(reloaded.isAllFrozen(0));
            assertTrue(reloaded.isAllVisible(1));
            assertTrue(reloaded.isAllFrozen(1));
            assertFalse(reloaded.isAllVisible(2));
            assertTrue(reloaded.isAllVisible(3));
            assertTrue(reloaded.isAllFrozen(3));
            assertTrue(reloaded.isAllVisible(7));
            assertFalse(reloaded.isAllFrozen(7));

            reloaded.clear(1);
            assertFalse(reloaded.isAllVisible(1));
            assertFalse(reloaded.isAllFrozen(1));
        } finally {
            bp2.close();
            dm2.close();
        }
    }

    @Test
    void testHeapTableVacuumUpdatesAndPersistsVisibilityMap(@TempDir Path tempDir) {
        DiskManager dm1 = new DiskManager(tempDir.toString());
        BufferPoolManager bp1 = new BufferPoolManager(32, dm1);
        TransactionManager txnMgr = new TransactionManager();
        try {
            HeapTable table = new HeapTable("items", bp1);
            Transaction tx = txnMgr.begin();
            for (int i = 0; i < 100; i++) {
                table.insertMvcc(new Tuple(List.of(i, "item_" + i), List.of("id", "name")).serialize(), tx.getXID());
            }
            txnMgr.commit(tx);

            // Before vacuum: page 0 is not all-visible
            assertFalse(table.isAllVisible(0));
            assertFalse(table.isAllFrozen(0));

            // Vacuum with horizon > tx.getXID() marks page 0 all-visible & all-frozen
            Transaction nextTx = txnMgr.begin();
            table.vacuum(nextTx.getXID(), txnMgr);
            txnMgr.commit(nextTx);

            assertTrue(table.isAllVisible(0));
            assertTrue(table.isAllFrozen(0));
        } finally {
            bp1.close();
            dm1.close();
        }

        // Reopen table and verify visibility map persisted to items.vm
        DiskManager dm2 = new DiskManager(tempDir.toString());
        BufferPoolManager bp2 = new BufferPoolManager(32, dm2);
        try {
            HeapTable reopened = new HeapTable("items", bp2);
            assertTrue(reopened.isAllVisible(0));
            assertTrue(reopened.isAllFrozen(0));
        } finally {
            bp2.close();
            dm2.close();
        }
    }
}
