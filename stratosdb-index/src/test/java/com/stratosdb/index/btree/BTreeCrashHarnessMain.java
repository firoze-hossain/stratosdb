package com.stratosdb.index.btree;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.BTreePage;
import com.stratosdb.storage.wal.WALManager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

/**
 * Child process main for B+Tree crash testing.
 *
 * Concurrently inserts keys into a B+Tree with active WAL logging, repeatedly
 * triggering leaf splits (every ~200-400 keys). Continues until killed by SIGKILL
 * from the parent test process.
 */
public class BTreeCrashHarnessMain {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: BTreeCrashHarnessMain <dataDir> <totalKeys>");
            System.exit(1);
        }

        String dataDir = args[0];
        int totalKeys = Integer.parseInt(args[1]);

        try {
            DiskManager diskManager = new DiskManager(dataDir);
            BufferPoolManager bufferPool = new BufferPoolManager(64, diskManager);
            WALManager walManager = new WALManager(dataDir);
            BTreeIndex index = new BTreeIndex("btree_crash_idx", bufferPool, walManager);

            // Signal start to parent test
            File startMarker = new File(dataDir, "started.marker");
            try (FileOutputStream fos = new FileOutputStream(startMarker)) {
                fos.write("ready\n".getBytes(StandardCharsets.UTF_8));
                fos.getFD().sync();
            }

            File markerFile = new File(dataDir, "committed.marker");

            for (long i = 0; i < totalKeys; i++) {
                index.insert(i, new BTreePage.RID(i, (int) (i % 50)));

                if (i % 20 == 0) {
                    try (RandomAccessFile raf = new RandomAccessFile(markerFile, "rwd")) {
                        raf.seek(0);
                        raf.write(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
                        raf.setLength(raf.getFilePointer());
                        raf.getFD().sync();
                    }
                }
            }

            // Keep process alive if it finished fast, so parent can kill it mid-flight
            Thread.sleep(60_000);

        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(2);
        }
    }
}
