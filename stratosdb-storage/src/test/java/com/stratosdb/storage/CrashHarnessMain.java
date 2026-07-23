package com.stratosdb.storage;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.storage.wal.WALManager;

import java.io.FileOutputStream;
import java.io.FileDescriptor;
import java.io.RandomAccessFile;

/**
 * Standalone process used by CrashRecoveryTest. It is launched as a *separate JVM*
 * (not a thread) so the test can send it a real SIGKILL and reproduce an actual crash,
 * rather than a graceful shutdown.
 *
 * Contract with the test:
 *  - For every row i this process writes to the table AND logs an insert+commit to the WAL,
 *    it then appends the row id to committed.marker and forces it to disk before moving on.
 *  - committed.marker is therefore ground truth for "the engine told the world this row is
 *    durable" - independent of whatever the buffer pool/WAL actually managed to persist.
 *  - This process never calls bufferPool.flushAll(), walManager.checkpoint(), or any other
 *    graceful-shutdown path. It is meant to be killed with SIGKILL by the parent test.
 *
 * Args: <dataDir> <totalRows>
 */
public class CrashHarnessMain {
    public static void main(String[] args) throws Exception {
        String dataDir = args[0];
        int totalRows = Integer.parseInt(args[1]);

        DiskManager diskManager = new DiskManager(dataDir);
        BufferPoolManager bufferPool = new BufferPoolManager(64, diskManager);
        WALManager walManager = new WALManager(dataDir);
        HeapTable table = new HeapTable("crash_test", bufferPool);

        RandomAccessFile marker = new RandomAccessFile(dataDir + "/committed.marker", "rw");

        for (int i = 0; i < totalRows; i++) {
            Tuple tuple = new Tuple();
            tuple.addValue("id", i);
            tuple.addValue("payload", "row-" + i + "-payload-padding-to-look-like-a-real-column");
            byte[] data = tuple.serialize();

            HeapTable.InsertResult result = table.insert(data);
            walManager.logInsert("crash_test", result.pageId, result.slot, data);
            walManager.logCommit(i);

            // Ground truth: this row has been "told to the world" as committed.
            // Force it to disk so a SIGKILL right after this line cannot erase the marker itself.
            marker.seek(0);
            marker.writeInt(i);
            marker.getFD().sync();
        }

        marker.close();
        // Deliberately NOT calling bufferPool.close()/flushAll() or walManager.close():
        // we want to measure what survives without a graceful shutdown.
        System.out.println("HARNESS_DONE " + (totalRows - 1));
    }
}
