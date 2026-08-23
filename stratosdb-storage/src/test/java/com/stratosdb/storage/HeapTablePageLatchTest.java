package com.stratosdb.storage;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Page;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, fine-grained per-page latching (Page.getLatch()) - a genuinely
 * different, lower-level concern from MVCC's own row-level locks
 * (LockManager), which HeapTableConcurrencyTest already covers. Row
 * locks correctly prevent two transactions from both writing the SAME
 * row - but two transactions writing two DIFFERENT rows that happen to
 * live on the SAME physical page each acquire their own, distinct row
 * lock and are both free to proceed concurrently as far as MVCC is
 * concerned, while both about to mutate that one page's own slot
 * directory and free-space pointer with zero physical protection
 * between them, unless something latches the page itself. These tests
 * go around the MVCC/row-lock layer entirely (calling HeapTable's raw
 * update/delete/scan API directly, the same way HeapTableConcurrencyTest
 * already does for insert) specifically so nothing but the page latch
 * itself could be responsible for a passing result.
 */
class HeapTablePageLatchTest {

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
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentUpdatesToDifferentRowsOnTheSamePageNeverCorruptEachOther() throws Exception {
        HeapTable table = newTable("t");
        int rowCount = 50; // small rows, small count - deliberately all land on page 0 together

        List<HeapTable.InsertResult> positions = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            positions.add(table.insert(String.format("row-%03d-orig ", i).getBytes())); // 14 bytes, fixed width
        }
        // Confirm the setup actually exercises the scenario this test exists for -
        // if this ever failed, every row wouldn't really be sharing one page anymore
        // and the rest of this test wouldn't be testing what it claims to.
        long distinctPages = positions.stream().map(r -> r.pageId).distinct().count();
        assertEquals(1, distinctPages, "test setup assumption violated: all rows must land on the same physical page");

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(rowCount);
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < rowCount; i++) {
            HeapTable.InsertResult pos = positions.get(i);
            String newValue = String.format("row-%03d-updt ", i); // same 14 bytes - see this method's own javadoc above
            new Thread(() -> {
                try {
                    startLatch.await();
                    boolean ok = table.update(pos.pageId, pos.slot, newValue.getBytes());
                    if (!ok) errors.incrementAndGet();
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown(); // release all threads at once - maximize actual concurrent contention on the one shared page
        assertTrue(doneLatch.await(20, TimeUnit.SECONDS), "all concurrent updates must complete within the timeout");
        assertEquals(0, errors.get(), "no concurrent update should fail or throw");

        // Verify every single row's new value survived intact - not corrupted, not
        // silently overwritten by a neighboring slot's own concurrent update.
        for (int i = 0; i < rowCount; i++) {
            HeapTable.InsertResult pos = positions.get(i);
            byte[] actual = table.readTuple(pos.pageId, pos.slot);
            final int rowIndex = i;
            assertNotNull(actual, () -> "row " + rowIndex + " (page " + pos.pageId + "/slot " + pos.slot + ") must still be readable after the concurrent update");
            assertEquals(String.format("row-%03d-updt ", i), new String(actual),
                () -> "row " + rowIndex + " (page " + pos.pageId + "/slot " + pos.slot
                    + ") must contain exactly its own updated value, not corrupted or overwritten by a concurrent neighbor's update to the same page");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentDeletesToDifferentRowsOnTheSamePageEachRemoveExactlyOneRow() throws Exception {
        HeapTable table = newTable("t");
        int rowCount = 40;

        List<HeapTable.InsertResult> positions = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            positions.add(table.insert(("row-" + i).getBytes()));
        }
        assertEquals(1, positions.stream().map(r -> r.pageId).distinct().count());

        // Delete every odd-indexed row concurrently, keep every even-indexed one.
        List<HeapTable.InsertResult> toDelete = new ArrayList<>();
        List<HeapTable.InsertResult> toKeep = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            (i % 2 == 0 ? toKeep : toDelete).add(positions.get(i));
        }

        CountDownLatch doneLatch = new CountDownLatch(toDelete.size());
        AtomicInteger errors = new AtomicInteger(0);
        for (HeapTable.InsertResult pos : toDelete) {
            new Thread(() -> {
                try {
                    table.delete(pos.pageId, pos.slot);
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        assertTrue(doneLatch.await(20, TimeUnit.SECONDS));
        assertEquals(0, errors.get());

        // Every kept row's content must be exactly, individually intact.
        for (HeapTable.InsertResult pos : toKeep) {
            byte[] actual = table.readTuple(pos.pageId, pos.slot);
            assertNotNull(actual, "a kept row must still be readable - concurrent deletes elsewhere on the same page must not have disturbed it");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentScanNeverObservesATornOrCorruptedTupleWhileInsertsAreHappening() throws Exception {
        HeapTable table = newTable("t");
        int insertCount = 200;
        AtomicInteger scanErrors = new AtomicInteger(0);
        AtomicInteger insertErrors = new AtomicInteger(0);
        java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean(false);

        Thread scanner = new Thread(() -> {
            while (!stop.get()) {
                try {
                    for (byte[] row : table.scan()) {
                        String s = new String(row);
                        // A torn/corrupted read would produce something that doesn't match
                        // this table's own, exact, known row format - not a graceful "empty
                        // string" or similar, but garbage bytes from a half-written slot.
                        if (!s.startsWith("row-")) {
                            scanErrors.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    scanErrors.incrementAndGet();
                }
            }
        });
        scanner.start();

        CountDownLatch insertsDone = new CountDownLatch(insertCount);
        for (int i = 0; i < insertCount; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    table.insert(("row-" + idx).getBytes());
                } catch (Exception e) {
                    insertErrors.incrementAndGet();
                } finally {
                    insertsDone.countDown();
                }
            }).start();
        }

        assertTrue(insertsDone.await(20, TimeUnit.SECONDS), "all concurrent inserts must complete");
        stop.set(true);
        scanner.join(5000);

        assertEquals(0, insertErrors.get(), "no insert should fail while concurrent scans are happening");
        assertEquals(0, scanErrors.get(), "no concurrent scan should ever observe a torn or corrupted tuple");

        List<byte[]> finalRows = table.scan();
        assertEquals(insertCount, finalRows.size(), "every concurrently-inserted row must be present in a final scan");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aWriteLatchHeldOnOnePageNeverBlocksAnOperationOnAGenuinelyDifferentPage() throws Exception {
        // Proves the actual "fine-grained" property, not just correctness: an
        // operation on page B must not wait for a latch held on page A.
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bp = new BufferPoolManager(64, dm);
        openPools.add(bp);
        HeapTable table = new HeapTable("t2", bp);

        HeapTable.InsertResult onFirstPage = table.insert("first-page-row".getBytes());

        // Force a second, distinct page to exist by inserting small tuples until
        // one naturally lands on a new page - more robust than guessing the exact
        // byte arithmetic needed for one huge tuple to not fit alongside the first.
        HeapTable.InsertResult onSecondPage = onFirstPage;
        for (int i = 0; i < 2000 && onSecondPage.pageId == onFirstPage.pageId; i++) {
            onSecondPage = table.insert(("filler-" + i).getBytes());
        }
        assertNotEquals(onFirstPage.pageId, onSecondPage.pageId, "test setup assumption violated: need two genuinely different pages");

        Page firstPage = bp.getPage("t2", onFirstPage.pageId);
        firstPage.getLatch().writeLock().lock();
        try {
            // While page A's write latch is held (simulating a slow, in-progress
            // operation on it), an update targeting the DIFFERENT page B must still
            // complete quickly - not block waiting on page A's own latch at all.
            long start = System.nanoTime();
            boolean ok = table.update(onSecondPage.pageId, onSecondPage.slot, "updated-second-page".getBytes());
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            assertTrue(ok);
            assertTrue(elapsedMillis < 2000,
                () -> "an update to a genuinely different page took " + elapsedMillis
                    + "ms while another page's write latch was held - fine-grained latching must not serialize unrelated pages");
        } finally {
            firstPage.getLatch().writeLock().unlock();
            bp.unpinPage("t2", onFirstPage.pageId);
        }
    }
}
