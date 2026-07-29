package com.stratosdb.storage;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.storage.wal.WALManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Week 1 durability contract for the storage engine.
 *
 * These tests do not mock anything below HeapTable: real DiskManager, real
 * BufferPoolManager, real WALManager, writing real files to a real temp directory.
 * Test 2 forks a second JVM and sends it SIGKILL to reproduce an actual crash,
 * rather than simulating one with a thread interrupt or an exception.
 *
 * If either test fails, the failure message tells you exactly which durability
 * guarantee is broken and where - that's the point of this class.
 */
class CrashRecoveryTest {

    @TempDir
    Path tempDir;

    private String dataDir;
    private final List<Runnable> closeActions = new java.util.ArrayList<>();

    @BeforeEach
    void setUp() {
        dataDir = tempDir.toString();
    }

    /**
     * Every DiskManager/BufferPoolManager AND every WALManager a test
     * creates holds a real open file handle - DiskManager keeps a
     * FileChannel per table file open in a cache (crash_test.dat),
     * WALManager keeps its own separate one open for wal/wal.log - both
     * only released by their own close(). On Linux, an open file can still
     * be deleted (the directory entry goes away immediately, space is
     * reclaimed once the last handle closes), so @TempDir's post-test
     * cleanup never noticed anything was still open. Windows refuses to
     * delete a file that's still open by any process, so the exact same
     * test - correct in every assertion - fails during teardown with "The
     * process cannot access the file because it is being used by another
     * process." Tracking and closing both kinds of resource here fixes it.
     * Closing happens strictly after each test's own assertions have
     * already run, so it changes nothing about what's actually being
     * tested - including the tests that deliberately simulate "no graceful
     * shutdown" during the test body itself.
     */
    private BufferPoolManager track(BufferPoolManager pool) {
        closeActions.add(pool::close);
        return pool;
    }

    private WALManager track(WALManager wal) {
        closeActions.add(wal::close);
        return wal;
    }

    @AfterEach
    void tearDown() {
        for (Runnable close : closeActions) {
            try {
                close.run();
            } catch (Exception e) {
                // Best-effort cleanup; a close failure here shouldn't mask the test's own result.
            }
        }
        closeActions.clear();
    }

    /**
     * Baseline durability: no crash at all. Insert enough rows to force multiple
     * heap pages, exit the process cleanly (no explicit flush/checkpoint - just like
     * a JVM that gets stopped without running its shutdown hooks), then open a brand
     * new set of managers against the same directory and scan.
     *
     * This intentionally does NOT go through WAL recovery timing tricks. It exists to
     * isolate one question: does the storage engine persist rows that span more than
     * one heap page at all, independent of crash-recovery logic?
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void committedMultiPageDataSurvivesRestartWithoutExplicitFlush() {
        int totalRows = 500; // large enough to force several pages at 8KB/page
        Set<Integer> insertedIds = new HashSet<>();

        {
            DiskManager diskManager = new DiskManager(dataDir);
            BufferPoolManager bufferPool = track(new BufferPoolManager(64, diskManager));
            WALManager walManager = track(new WALManager(dataDir));
            HeapTable table = new HeapTable("crash_test", bufferPool);

            for (int i = 0; i < totalRows; i++) {
                Tuple tuple = new Tuple();
                tuple.addValue("id", i);
                tuple.addValue("payload", "row-" + i + "-payload-padding-to-look-like-a-real-column");
                byte[] data = tuple.serialize();

                HeapTable.InsertResult result = table.insert(data);
                walManager.logInsert("crash_test", i, result.pageId, result.slot, data);
                walManager.logCommit(i);
                insertedIds.add(i);
            }
            // Deliberately no bufferPool.flushAll(), no walManager.checkpoint()/close():
            // this is what "the process ended" looks like without a graceful shutdown path.
            // (The pool is still tracked for teardown, once this test's own assertions are
            // done - see track()'s javadoc.)
        }

        // Fresh managers, same directory - this is what "restart" means.
        DiskManager diskManager2 = new DiskManager(dataDir);
        BufferPoolManager bufferPool2 = track(new BufferPoolManager(64, diskManager2));
        WALManager walManager2 = track(new WALManager(dataDir));
        walManager2.recover(diskManager2);
        HeapTable table2 = new HeapTable("crash_test", bufferPool2);

        Set<Integer> recoveredIds = new HashSet<>();
        List<byte[]> rows = table2.scan(totalRows * 2);
        for (byte[] raw : rows) {
            Tuple t = Tuple.deserialize(raw);
            recoveredIds.add((Integer) t.getValue("id"));
        }

        Set<Integer> missing = new HashSet<>(insertedIds);
        missing.removeAll(recoveredIds);

        assertEquals(
            insertedIds.size(),
            recoveredIds.size(),
            () -> "Expected all " + insertedIds.size() + " rows to survive a restart, "
                + "but only " + recoveredIds.size() + " were recovered. "
                + "Missing " + missing.size() + " row ids, e.g. " + sample(missing, 5) + ". "
                + "HeapTable.insert()'s 'need new page' branch builds a SlottedPage directly "
                + "instead of obtaining it via bufferPool.getPage(), so it is never registered "
                + "in the buffer pool cache and is never written to disk by flushAll()/eviction."
        );
    }

    /**
     * Real crash: a second JVM inserts rows and WAL-commits them one at a time, marking
     * each row as committed (fsynced) only after the WAL commit call returns. The test
     * sends SIGKILL partway through, then verifies that every row the child claimed as
     * committed is actually recoverable after WALManager.recover() runs against the
     * surviving files.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void committedRowsSurviveAHardCrashMidBatch() throws Exception {
        int totalRows = 5000;

        String classpath = System.getProperty("java.class.path");
        ProcessBuilder pb = new ProcessBuilder(
            System.getProperty("java.home") + "/bin/java",
            "-cp", classpath,
            "com.stratosdb.storage.CrashHarnessMain",
            dataDir,
            String.valueOf(totalRows)
        );
        pb.redirectErrorStream(true);
        pb.redirectOutput(new File(dataDir, "harness.out"));
        Process process = pb.start();

        // Let it run for a short, fixed window - long enough to commit many rows,
        // short enough that it cannot possibly finish all totalRows first.
        Thread.sleep(400);

        assertTrue(process.isAlive(), "Harness exited before the test could kill it; "
            + "increase totalRows or decrease the sleep to keep this a true mid-batch crash.");
        process.destroyForcibly();
        boolean exited = process.waitFor(10, TimeUnit.SECONDS);
        assertTrue(exited, "Killed process did not terminate in time");

        int lastCommittedId = readMarker(dataDir + "/committed.marker");
        assertTrue(lastCommittedId >= 0,
            "Harness never got far enough to commit a single row before being killed; "
            + "increase the sleep window.");

        // Fresh managers over the crashed data directory - this is "restart after crash".
        DiskManager diskManager = new DiskManager(dataDir);
        BufferPoolManager bufferPool = track(new BufferPoolManager(64, diskManager));
        WALManager walManager = track(new WALManager(dataDir));
        walManager.recover(diskManager);
        HeapTable table = new HeapTable("crash_test", bufferPool);

        Set<Integer> recoveredIds = new HashSet<>();
        for (byte[] raw : table.scan(totalRows * 2)) {
            recoveredIds.add((Integer) Tuple.deserialize(raw).getValue("id"));
        }

        Set<Integer> expectedCommitted = new HashSet<>();
        for (int i = 0; i <= lastCommittedId; i++) expectedCommitted.add(i);

        Set<Integer> lost = new HashSet<>(expectedCommitted);
        lost.removeAll(recoveredIds);

        assertTrue(lost.isEmpty(),
            "Harness committed rows 0.." + lastCommittedId + " (fsynced marker file) before "
            + "being killed, but " + lost.size() + " of them were not recoverable after "
            + "WALManager.recover() - e.g. missing ids " + sample(lost, 5) + ". "
            + "WALManager.recover()'s switch statement has empty case bodies (no actual redo "
            + "logic), so committed WAL records are never replayed into the heap on restart.");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void uncommittedMultiRowTransactionDoesNotSurviveAHardCrash() throws Exception {
        // The property that matters most once transactions can span more than
        // one statement: every row here is logged under the SAME xid, and that
        // xid is deliberately never committed before the hard kill. Real
        // atomicity means NONE of these rows should be recoverable - not "the
        // first few" or "however many made it to disk," none at all, because
        // the transaction that wrote them never told the WAL it was done.
        int totalRows = 200;
        long xid = 9001;

        Process process = runHarness(totalRows, xid, false);
        waitForMarkerThenKill(process);

        DiskManager diskManager = new DiskManager(dataDir);
        BufferPoolManager bufferPool = track(new BufferPoolManager(64, diskManager));
        WALManager walManager = track(new WALManager(dataDir));
        walManager.recover(diskManager);
        HeapTable table = new HeapTable("txn_crash_test", bufferPool);

        List<byte[]> recovered = table.scan(totalRows * 2);
        assertEquals(0, recovered.size(),
            "an uncommitted transaction's writes must not survive a crash - found "
            + recovered.size() + " row(s) from a transaction that never committed. "
            + "This means recover() replayed operations with no matching OP_COMMIT record.");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void committedMultiRowTransactionSurvivesAHardCrash() throws Exception {
        // The positive counterpart, using the same harness and the same
        // multi-row-one-transaction shape - proves the atomicity fix didn't
        // just make recovery overly conservative (reject everything).
        int totalRows = 200;
        long xid = 9002;

        Process process = runHarness(totalRows, xid, true);
        waitForMarkerThenKill(process);

        DiskManager diskManager = new DiskManager(dataDir);
        BufferPoolManager bufferPool = track(new BufferPoolManager(64, diskManager));
        WALManager walManager = track(new WALManager(dataDir));
        walManager.recover(diskManager);
        HeapTable table = new HeapTable("txn_crash_test", bufferPool);

        Set<Integer> recoveredIds = new HashSet<>();
        for (byte[] raw : table.scan(totalRows * 2)) {
            recoveredIds.add((Integer) Tuple.deserialize(raw).getValue("id"));
        }

        Set<Integer> expected = new HashSet<>();
        for (int i = 0; i < totalRows; i++) expected.add(i);

        assertEquals(expected, recoveredIds,
            "a committed multi-row transaction must fully survive a crash - all " + totalRows
            + " rows should be recoverable since their shared transaction DID commit before the kill");
    }

    private Process runHarness(int totalRows, long xid, boolean commit) throws Exception {
        String classpath = System.getProperty("java.class.path");
        ProcessBuilder pb = new ProcessBuilder(
            System.getProperty("java.home") + "/bin/java",
            "-cp", classpath,
            "com.stratosdb.storage.UncommittedTxnCrashHarnessMain",
            dataDir,
            String.valueOf(totalRows),
            String.valueOf(xid),
            String.valueOf(commit)
        );
        pb.redirectErrorStream(true);
        pb.redirectOutput(new File(dataDir, "harness.out"));
        return pb.start();
    }

    /** Waits until the harness has finished writing (and optionally committing) all its rows, then kills it - isolating the commit-record property from mid-write timing. */
    private void waitForMarkerThenKill(Process process) throws Exception {
        File marker = new File(dataDir, "harness.marker");
        long deadline = System.currentTimeMillis() + 15_000;
        while (!marker.exists() && System.currentTimeMillis() < deadline) {
            assertTrue(process.isAlive(), "Harness exited before writing its completion marker - check harness.out");
            Thread.sleep(20);
        }
        assertTrue(marker.exists(), "Harness never wrote its completion marker within the timeout");

        process.destroyForcibly();
        boolean exited = process.waitFor(10, TimeUnit.SECONDS);
        assertTrue(exited, "Killed process did not terminate in time");
    }

    private static int readMarker(String path) throws Exception {
        File f = new File(path);
        if (!f.exists() || f.length() < 4) return -1;
        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            raf.seek(0);
            return raf.readInt();
        }
    }

    private static String sample(Set<Integer> ids, int n) {
        return ids.stream().sorted().limit(n).toList().toString();
    }
}
