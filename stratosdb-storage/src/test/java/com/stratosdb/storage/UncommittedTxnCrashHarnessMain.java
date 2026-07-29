package com.stratosdb.storage;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.storage.wal.WALManager;

import java.io.RandomAccessFile;

/**
 * Standalone process used by CrashRecoveryTest's uncommitted-transaction
 * test. Unlike CrashHarnessMain (where every row is its own immediately-
 * committed transaction), this writes a batch of rows under ONE shared
 * transaction id and, depending on args, either commits it or doesn't
 * before being SIGKILLed - simulating a multi-statement transaction that
 * crashes before COMMIT ever happens.
 *
 * Args: <dataDir> <totalRows> <xid> <commit: true|false>
 */
public class UncommittedTxnCrashHarnessMain {
    public static void main(String[] args) throws Exception {
        String dataDir = args[0];
        int totalRows = Integer.parseInt(args[1]);
        long xid = Long.parseLong(args[2]);
        boolean commit = Boolean.parseBoolean(args[3]);

        DiskManager diskManager = new DiskManager(dataDir);
        BufferPoolManager bufferPool = new BufferPoolManager(64, diskManager);
        WALManager walManager = new WALManager(dataDir);
        HeapTable table = new HeapTable("txn_crash_test", bufferPool);

        for (int i = 0; i < totalRows; i++) {
            Tuple tuple = new Tuple();
            tuple.addValue("id", i);
            tuple.addValue("xid", xid);
            byte[] data = tuple.serialize();

            HeapTable.InsertResult result = table.insert(data);
            walManager.logInsert("txn_crash_test", xid, result.pageId, result.slot, data);
        }

        if (commit) {
            walManager.logCommit(xid);
        }

        RandomAccessFile marker = new RandomAccessFile(dataDir + "/harness.marker", "rw");
        marker.writeBytes("HARNESS_WROTE_ALL_ROWS");
        marker.getFD().sync();
        marker.close();

        // Deliberately no bufferPool.close()/flushAll(), no walManager.close():
        // measuring what survives without a graceful shutdown, exactly like CrashHarnessMain.
        System.out.println("HARNESS_DONE");
    }
}
