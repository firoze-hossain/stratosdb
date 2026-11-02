package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sprint 10 Week 19: Transaction Isolation Levels & SELECT FOR UPDATE Test Suite.
 *
 * Verifies:
 *  1. SET TRANSACTION ISOLATION LEVEL / SET SESSION CHARACTERISTICS / SHOW TRANSACTION ISOLATION LEVEL.
 *  2. Non-repeatable reads observed under Read Committed (fresh MVCC snapshot per statement).
 *  3. Non-repeatable reads prevented under Repeatable Read (snapshot preserved across transaction).
 *  4. Phantom reads prevented under Repeatable Read.
 *  5. SELECT ... FOR UPDATE acquires row-level exclusive locks and blocks concurrent UPDATE.
 *  6. Concurrent SELECT ... FOR UPDATE blocks until the holder commits or aborts.
 *  7. Plain SELECT (MVCC readers) is never blocked by SELECT ... FOR UPDATE.
 *  8. Circular wait deadlock detection on concurrent SELECT ... FOR UPDATE.
 *  9. SELECT ... FOR UPDATE is rejected in read-only mode.
 */
public class TransactionIsolationLevelsTest {

    @Test
    void testSetAndShowTransactionIsolationLevel(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            // Default transaction isolation is Read Committed
            QueryResult res = db.execute("SHOW TRANSACTION ISOLATION LEVEL");
            assertTrue(res.isSuccess());
            assertEquals("read committed", res.getRows().get(0).getValue("transaction_isolation"));

            res = db.execute("SHOW default_transaction_isolation");
            assertTrue(res.isSuccess());
            assertEquals("read committed", res.getRows().get(0).getValue("default_transaction_isolation"));

            res = db.execute("SHOW transaction_isolation");
            assertTrue(res.isSuccess());
            assertEquals("read committed", res.getRows().get(0).getValue("transaction_isolation"));

            // BEGIN ISOLATION LEVEL REPEATABLE READ
            res = db.execute("BEGIN ISOLATION LEVEL REPEATABLE READ");
            assertTrue(res.isSuccess());
            res = db.execute("SHOW TRANSACTION ISOLATION LEVEL");
            assertTrue(res.isSuccess());
            assertEquals("repeatable read", res.getRows().get(0).getValue("transaction_isolation"));
            db.execute("COMMIT");

            // After commit, returns to default session isolation level
            res = db.execute("SHOW TRANSACTION ISOLATION LEVEL");
            assertTrue(res.isSuccess());
            assertEquals("read committed", res.getRows().get(0).getValue("transaction_isolation"));

            // BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ
            db.execute("BEGIN");
            res = db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            assertTrue(res.isSuccess());
            res = db.execute("SHOW TRANSACTION ISOLATION LEVEL");
            assertTrue(res.isSuccess());
            assertEquals("repeatable read", res.getRows().get(0).getValue("transaction_isolation"));
            db.execute("COMMIT");

            // SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ
            res = db.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            assertTrue(res.isSuccess());
            res = db.execute("SHOW default_transaction_isolation");
            assertTrue(res.isSuccess());
            assertEquals("repeatable read", res.getRows().get(0).getValue("default_transaction_isolation"));

            db.execute("BEGIN");
            res = db.execute("SHOW TRANSACTION ISOLATION LEVEL");
            assertTrue(res.isSuccess());
            assertEquals("repeatable read", res.getRows().get(0).getValue("transaction_isolation"));
            db.execute("COMMIT");

            // SET default_transaction_isolation = 'read committed'
            res = db.execute("SET default_transaction_isolation = 'read committed'");
            assertTrue(res.isSuccess());
            res = db.execute("SHOW default_transaction_isolation");
            assertTrue(res.isSuccess());
            assertEquals("read committed", res.getRows().get(0).getValue("default_transaction_isolation"));
        } finally {
            db.shutdown();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testNonRepeatableReadObservedUnderReadCommitted(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE accounts (id INT, balance INT)");
            db.execute("INSERT INTO accounts VALUES (1, 100)");

            CountDownLatch t1Started = new CountDownLatch(1);
            CountDownLatch t2Updated = new CountDownLatch(1);
            CountDownLatch t1Finished = new CountDownLatch(1);
            AtomicInteger val1 = new AtomicInteger();
            AtomicInteger val2 = new AtomicInteger();
            AtomicReference<String> errorRef = new AtomicReference<>();

            // Thread 1: READ COMMITTED transaction
            Thread t1 = new Thread(() -> {
                try {
                    QueryResult b = db.execute("BEGIN ISOLATION LEVEL READ COMMITTED");
                    if (!b.isSuccess()) errorRef.set(b.getError());

                    QueryResult r1 = db.execute("SELECT balance FROM accounts WHERE id = 1");
                    if (!r1.isSuccess()) errorRef.set(r1.getError());
                    val1.set(((Number) r1.getRows().get(0).getValue("balance")).intValue());

                    t1Started.countDown();
                    assertTrue(t2Updated.await(5, TimeUnit.SECONDS));

                    // Statement 2: in READ COMMITTED, a fresh MVCC snapshot is acquired per statement
                    QueryResult r2 = db.execute("SELECT balance FROM accounts WHERE id = 1");
                    if (!r2.isSuccess()) errorRef.set(r2.getError());
                    val2.set(((Number) r2.getRows().get(0).getValue("balance")).intValue());

                    db.execute("COMMIT");
                } catch (Exception e) {
                    errorRef.set(e.getMessage());
                } finally {
                    t1Finished.countDown();
                }
            }, "T1-ReadCommitted");

            // Thread 2: Writer
            Thread t2 = new Thread(() -> {
                try {
                    assertTrue(t1Started.await(5, TimeUnit.SECONDS));
                    QueryResult u = db.execute("UPDATE accounts SET balance = 250 WHERE id = 1");
                    if (!u.isSuccess()) errorRef.set(u.getError());
                    t2Updated.countDown();
                } catch (Exception e) {
                    errorRef.set(e.getMessage());
                }
            }, "T2-Writer");

            t1.start();
            t2.start();
            assertTrue(t1Finished.await(10, TimeUnit.SECONDS));
            t1.join();
            t2.join();

            assertNull(errorRef.get(), "No errors expected during execution: " + errorRef.get());
            assertEquals(100, val1.get(), "First read must observe balance = 100");
            assertEquals(250, val2.get(), "Under READ COMMITTED, second read observes committed update (250)");
        } finally {
            db.shutdown();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testNonRepeatableReadPreventedUnderRepeatableRead(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE accounts (id INT, balance INT)");
            db.execute("INSERT INTO accounts VALUES (1, 100)");

            CountDownLatch t1Started = new CountDownLatch(1);
            CountDownLatch t2Updated = new CountDownLatch(1);
            CountDownLatch t1Finished = new CountDownLatch(1);
            AtomicInteger val1 = new AtomicInteger();
            AtomicInteger val2 = new AtomicInteger();
            AtomicReference<String> errorRef = new AtomicReference<>();

            // Thread 1: REPEATABLE READ transaction
            Thread t1 = new Thread(() -> {
                try {
                    QueryResult b = db.execute("BEGIN ISOLATION LEVEL REPEATABLE READ");
                    if (!b.isSuccess()) errorRef.set(b.getError());

                    QueryResult r1 = db.execute("SELECT balance FROM accounts WHERE id = 1");
                    if (!r1.isSuccess()) errorRef.set(r1.getError());
                    val1.set(((Number) r1.getRows().get(0).getValue("balance")).intValue());

                    t1Started.countDown();
                    assertTrue(t2Updated.await(5, TimeUnit.SECONDS));

                    // Statement 2: in REPEATABLE READ, the snapshot created at transaction begin is preserved
                    QueryResult r2 = db.execute("SELECT balance FROM accounts WHERE id = 1");
                    if (!r2.isSuccess()) errorRef.set(r2.getError());
                    val2.set(((Number) r2.getRows().get(0).getValue("balance")).intValue());

                    db.execute("COMMIT");
                } catch (Exception e) {
                    errorRef.set(e.getMessage());
                } finally {
                    t1Finished.countDown();
                }
            }, "T1-RepeatableRead");

            // Thread 2: Writer
            Thread t2 = new Thread(() -> {
                try {
                    assertTrue(t1Started.await(5, TimeUnit.SECONDS));
                    QueryResult u = db.execute("UPDATE accounts SET balance = 500 WHERE id = 1");
                    if (!u.isSuccess()) errorRef.set(u.getError());
                    t2Updated.countDown();
                } catch (Exception e) {
                    errorRef.set(e.getMessage());
                }
            }, "T2-Writer");

            t1.start();
            t2.start();
            assertTrue(t1Finished.await(10, TimeUnit.SECONDS));
            t1.join();
            t2.join();

            assertNull(errorRef.get(), "No errors expected during execution: " + errorRef.get());
            assertEquals(100, val1.get(), "First read must observe balance = 100");
            assertEquals(100, val2.get(), "Under REPEATABLE READ, non-repeatable read is PREVENTED; second read must still be 100");

            // A new statement outside T1 now observes the committed value
            QueryResult rAfter = db.execute("SELECT balance FROM accounts WHERE id = 1");
            assertTrue(rAfter.isSuccess());
            assertEquals(500, ((Number) rAfter.getRows().get(0).getValue("balance")).intValue());
        } finally {
            db.shutdown();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testPhantomReadPreventedUnderRepeatableRead(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE products (id INT, price INT)");
            db.execute("INSERT INTO products VALUES (1, 10)");
            db.execute("INSERT INTO products VALUES (2, 20)");

            CountDownLatch t1Started = new CountDownLatch(1);
            CountDownLatch t2Inserted = new CountDownLatch(1);
            CountDownLatch t1Finished = new CountDownLatch(1);
            AtomicInteger count1 = new AtomicInteger();
            AtomicInteger count2 = new AtomicInteger();
            AtomicReference<String> errorRef = new AtomicReference<>();

            Thread t1 = new Thread(() -> {
                try {
                    QueryResult b = db.execute("BEGIN ISOLATION LEVEL REPEATABLE READ");
                    if (!b.isSuccess()) errorRef.set(b.getError());

                    QueryResult r1 = db.execute("SELECT id FROM products");
                    if (!r1.isSuccess()) errorRef.set(r1.getError());
                    count1.set(r1.getRows().size());

                    t1Started.countDown();
                    assertTrue(t2Inserted.await(5, TimeUnit.SECONDS));

                    // Statement 2: Under REPEATABLE READ, phantom rows are not visible
                    QueryResult r2 = db.execute("SELECT id FROM products");
                    if (!r2.isSuccess()) errorRef.set(r2.getError());
                    count2.set(r2.getRows().size());

                    db.execute("COMMIT");
                } catch (Exception e) {
                    errorRef.set(e.getMessage());
                } finally {
                    t1Finished.countDown();
                }
            }, "T1-RepeatableRead-Phantom");

            Thread t2 = new Thread(() -> {
                try {
                    assertTrue(t1Started.await(5, TimeUnit.SECONDS));
                    QueryResult ins = db.execute("INSERT INTO products VALUES (3, 30)");
                    if (!ins.isSuccess()) errorRef.set(ins.getError());
                    t2Inserted.countDown();
                } catch (Exception e) {
                    errorRef.set(e.getMessage());
                }
            }, "T2-Inserter");

            t1.start();
            t2.start();
            assertTrue(t1Finished.await(10, TimeUnit.SECONDS));
            t1.join();
            t2.join();

            assertNull(errorRef.get(), "No errors expected: " + errorRef.get());
            assertEquals(2, count1.get(), "Initial row count must be 2");
            assertEquals(2, count2.get(), "Under REPEATABLE READ, phantom insert is invisible; row count remains 2");

            QueryResult rAfter = db.execute("SELECT id FROM products");
            assertTrue(rAfter.isSuccess());
            assertEquals(3, rAfter.getRows().size(), "After T1 commits, table has 3 rows");
        } finally {
            db.shutdown();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testSelectForUpdateLocksRowsAndBlocksConcurrentUpdate(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE inventory (id INT, qty INT)");
            db.execute("INSERT INTO inventory VALUES (1, 10)");

            CountDownLatch t1Locked = new CountDownLatch(1);
            CountDownLatch t2Started = new CountDownLatch(1);
            CountDownLatch t2Done = new CountDownLatch(1);
            AtomicBoolean t2FinishedBeforeT1Commit = new AtomicBoolean(false);
            AtomicReference<String> errorRef = new AtomicReference<>();

            Thread t1 = new Thread(() -> {
                try {
                    db.execute("BEGIN");
                    QueryResult sel = db.execute("SELECT * FROM inventory WHERE id = 1 FOR UPDATE");
                    if (!sel.isSuccess()) errorRef.set(sel.getError());
                    t1Locked.countDown();

                    // Wait until T2 has initiated its update and is waiting on the lock
                    assertTrue(t2Started.await(5, TimeUnit.SECONDS));
                    Thread.sleep(500); // Give T2 time to block

                    if (t2Done.getCount() == 0) {
                        t2FinishedBeforeT1Commit.set(true);
                    }

                    db.execute("COMMIT");
                } catch (Exception e) {
                    errorRef.set(e.getMessage());
                }
            }, "T1-LockHolder");

            Thread t2 = new Thread(() -> {
                try {
                    assertTrue(t1Locked.await(5, TimeUnit.SECONDS));
                    t2Started.countDown();
                    // This update must block until T1 commits
                    QueryResult upd = db.execute("UPDATE inventory SET qty = 20 WHERE id = 1");
                    if (!upd.isSuccess()) errorRef.set(upd.getError());
                    t2Done.countDown();
                } catch (Exception e) {
                    errorRef.set(e.getMessage());
                }
            }, "T2-Contender");

            t1.start();
            t2.start();

            t1.join();
            t2.join();

            assertNull(errorRef.get(), "No unexpected errors: " + errorRef.get());
            assertFalse(t2FinishedBeforeT1Commit.get(), "T2 UPDATE should have been blocked while T1 held FOR UPDATE lock");

            QueryResult check = db.execute("SELECT qty FROM inventory WHERE id = 1");
            assertTrue(check.isSuccess());
            assertEquals(20, ((Number) check.getRows().get(0).getValue("qty")).intValue());
        } finally {
            db.shutdown();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testSelectForUpdateBlocksConcurrentForUpdate(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE tickets (id INT, status VARCHAR)");
            db.execute("INSERT INTO tickets VALUES (1, 'open')");

            CountDownLatch t1Locked = new CountDownLatch(1);
            CountDownLatch t2Started = new CountDownLatch(1);
            CountDownLatch t2Done = new CountDownLatch(1);
            AtomicBoolean t2FinishedBeforeT1Commit = new AtomicBoolean(false);
            AtomicReference<String> errorRef = new AtomicReference<>();

            Thread t1 = new Thread(() -> {
                try {
                    db.execute("BEGIN");
                    QueryResult sel = db.execute("SELECT * FROM tickets WHERE id = 1 FOR UPDATE");
                    if (!sel.isSuccess()) errorRef.set(sel.getError());
                    t1Locked.countDown();

                    assertTrue(t2Started.await(5, TimeUnit.SECONDS));
                    Thread.sleep(500);

                    if (t2Done.getCount() == 0) {
                        t2FinishedBeforeT1Commit.set(true);
                    }

                    db.execute("COMMIT");
                } catch (Exception e) {
                    errorRef.set(e.getMessage());
                }
            }, "T1-LockHolder");

            Thread t2 = new Thread(() -> {
                try {
                    assertTrue(t1Locked.await(5, TimeUnit.SECONDS));
                    t2Started.countDown();
                    // Concurrent SELECT ... FOR UPDATE should block
                    db.execute("BEGIN");
                    QueryResult sel = db.execute("SELECT * FROM tickets WHERE id = 1 FOR UPDATE");
                    if (!sel.isSuccess()) errorRef.set(sel.getError());
                    t2Done.countDown();
                    db.execute("COMMIT");
                } catch (Exception e) {
                    errorRef.set(e.getMessage());
                }
            }, "T2-ForUpdateContender");

            t1.start();
            t2.start();

            t1.join();
            t2.join();

            assertNull(errorRef.get(), "No unexpected errors: " + errorRef.get());
            assertFalse(t2FinishedBeforeT1Commit.get(), "T2 SELECT FOR UPDATE should have been blocked while T1 held lock");
        } finally {
            db.shutdown();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testPlainSelectDoesNotBlockOnForUpdate(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE logs (id INT, msg VARCHAR)");
            db.execute("INSERT INTO logs VALUES (1, 'hello')");

            CountDownLatch t1Locked = new CountDownLatch(1);
            AtomicReference<String> readMsg = new AtomicReference<>();
            AtomicReference<String> errorRef = new AtomicReference<>();

            Thread t1 = new Thread(() -> {
                try {
                    db.execute("BEGIN");
                    QueryResult sel = db.execute("SELECT * FROM logs WHERE id = 1 FOR UPDATE");
                    if (!sel.isSuccess()) errorRef.set(sel.getError());
                    t1Locked.countDown();

                    // Hold the lock for 1 second
                    Thread.sleep(1000);
                    db.execute("COMMIT");
                } catch (Exception e) {
                    errorRef.set(e.getMessage());
                }
            }, "T1-LockHolder");

            Thread t2 = new Thread(() -> {
                try {
                    assertTrue(t1Locked.await(5, TimeUnit.SECONDS));
                    long start = System.currentTimeMillis();
                    // MVCC reader does not block on row lock
                    QueryResult sel = db.execute("SELECT msg FROM logs WHERE id = 1");
                    long elapsed = System.currentTimeMillis() - start;
                    if (!sel.isSuccess()) {
                        errorRef.set(sel.getError());
                    } else {
                        readMsg.set((String) sel.getRows().get(0).getValue("msg"));
                    }
                    assertTrue(elapsed < 800, "Plain SELECT should complete immediately via MVCC without waiting for FOR UPDATE lock");
                } catch (Exception e) {
                    errorRef.set(e.getMessage());
                }
            }, "T2-PlainReader");

            t1.start();
            t2.start();

            t1.join();
            t2.join();

            assertNull(errorRef.get(), "No unexpected errors: " + errorRef.get());
            assertEquals("hello", readMsg.get());
        } finally {
            db.shutdown();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testDeadlockDetectionOnForUpdate(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE pairs (id INT, val VARCHAR)");
            db.execute("INSERT INTO pairs VALUES (1, 'A')");
            db.execute("INSERT INTO pairs VALUES (2, 'B')");

            CountDownLatch bothFirstLocked = new CountDownLatch(2);
            AtomicInteger deadlockErrors = new AtomicInteger(0);
            AtomicInteger successCount = new AtomicInteger(0);

            Thread t1 = new Thread(() -> {
                try {
                    db.execute("BEGIN");
                    QueryResult r1 = db.execute("SELECT * FROM pairs WHERE id = 1 FOR UPDATE");
                    assertTrue(r1.isSuccess());
                    bothFirstLocked.countDown();
                    assertTrue(bothFirstLocked.await(5, TimeUnit.SECONDS));

                    // Now try to lock row 2 (held by T2)
                    QueryResult r2 = db.execute("SELECT * FROM pairs WHERE id = 2 FOR UPDATE");
                    if (r2.isSuccess()) {
                        successCount.incrementAndGet();
                        db.execute("COMMIT");
                    } else if (r2.getError().toLowerCase().contains("deadlock")) {
                        deadlockErrors.incrementAndGet();
                        db.execute("ROLLBACK");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, "T1-Deadlock");

            Thread t2 = new Thread(() -> {
                try {
                    db.execute("BEGIN");
                    QueryResult r1 = db.execute("SELECT * FROM pairs WHERE id = 2 FOR UPDATE");
                    assertTrue(r1.isSuccess());
                    bothFirstLocked.countDown();
                    assertTrue(bothFirstLocked.await(5, TimeUnit.SECONDS));

                    // Now try to lock row 1 (held by T1)
                    QueryResult r2 = db.execute("SELECT * FROM pairs WHERE id = 1 FOR UPDATE");
                    if (r2.isSuccess()) {
                        successCount.incrementAndGet();
                        db.execute("COMMIT");
                    } else if (r2.getError().toLowerCase().contains("deadlock")) {
                        deadlockErrors.incrementAndGet();
                        db.execute("ROLLBACK");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, "T2-Deadlock");

            t1.start();
            t2.start();
            t1.join();
            t2.join();

            assertEquals(1, deadlockErrors.get(), "Exactly one transaction must be aborted due to deadlock");
            assertEquals(1, successCount.get(), "The surviving transaction must proceed to lock and commit");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testForUpdateInReadOnlyModeFails(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE ro_tbl (id INT, val VARCHAR)");
            db.execute("INSERT INTO ro_tbl VALUES (1, 'test')");

            db.setReadOnly(true);

            // Plain SELECT succeeds in read-only mode
            QueryResult plain = db.execute("SELECT * FROM ro_tbl");
            assertTrue(plain.isSuccess());
            assertEquals(1, plain.getRows().size());

            // SELECT FOR UPDATE is rejected in read-only mode
            QueryResult forUpdate = db.execute("SELECT * FROM ro_tbl FOR UPDATE");
            assertFalse(forUpdate.isSuccess());
            assertTrue(forUpdate.getError().contains("cannot execute SELECT FOR UPDATE in a read-only replica"));

            db.setReadOnly(false);
        } finally {
            db.shutdown();
        }
    }
}
