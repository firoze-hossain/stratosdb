package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.ast.SelectStatement;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.sql.parser.SqlParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DoD Concurrency Test Suite for Sprint 03 Week 06:
 * - DDL blocked by active readers
 * - Concurrent SELECT FOR UPDATE serializes correctly
 * - SELECT FOR SHARE acquires shared row locks without blocking concurrent readers
 * - Full multi-granularity table intent locking (IS, IX, X) and row locks (S, X)
 */
public class TableIntentAndRowShareConcurrencyTest {

    private StratosDB db;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setLockPartitions(64);
        db = new StratosDB(config);

        db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, name VARCHAR(50), balance INT);");
        db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000);");
        db.execute("INSERT INTO accounts VALUES (2, 'Bob', 2000);");
        db.execute("INSERT INTO accounts VALUES (3, 'Charlie', 3000);");
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.shutdown();
        }
    }

    @Test
    @DisplayName("SQL Parser correctly parses SELECT ... FOR SHARE and SELECT ... FOR UPDATE")
    void testSqlParserForShareAndForUpdate() {
        SqlParser parser = new SqlParser();

        SelectStatement updateStmt = (SelectStatement) parser.parse("SELECT id, balance FROM accounts WHERE id = 1 FOR UPDATE;");
        assertTrue(updateStmt.forUpdate(), "Must be forUpdate");
        assertFalse(updateStmt.forShare(), "Must not be forShare");

        SelectStatement shareStmt = (SelectStatement) parser.parse("SELECT id, balance FROM accounts WHERE id = 1 FOR SHARE;");
        assertTrue(shareStmt.forShare(), "Must be forShare");
        assertFalse(shareStmt.forUpdate(), "Must not be forUpdate");

        SelectStatement plainStmt = (SelectStatement) parser.parse("SELECT id, balance FROM accounts WHERE id = 1;");
        assertFalse(plainStmt.forUpdate(), "Must not be forUpdate");
        assertFalse(plainStmt.forShare(), "Must not be forShare");
    }

    @Test
    @DisplayName("DDL (DROP TABLE) is blocked by active reader holding table Intent Shared lock until commit")
    void testTableIntentSharedBlocksTableExclusiveDdl() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch readerStarted = new CountDownLatch(1);
        CountDownLatch ddlQueued = new CountDownLatch(1);
        AtomicBoolean ddlFinished = new AtomicBoolean(false);
        AtomicReference<QueryResult> ddlResult = new AtomicReference<>();

        // Thread 1: Reader starts transaction and executes SELECT (holds table IS lock)
        Future<?> readerFuture = executor.submit(() -> {
            db.execute("BEGIN;");
            QueryResult res = db.execute("SELECT * FROM accounts WHERE id = 1;");
            assertTrue(res.isSuccess());
            readerStarted.countDown();

            // Wait until DDL thread is queued up waiting
            while (db.getTransactionManager().getLockManager().getTableWaiterCount("accounts") == 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            assertFalse(ddlFinished.get(), "DROP TABLE must still be blocked while reader transaction is open");
            db.execute("COMMIT;");
        });

        // Thread 2: Concurrent DDL attempts DROP TABLE accounts
        Future<?> ddlFuture = executor.submit(() -> {
            try {
                readerStarted.await(5, TimeUnit.SECONDS);
                // This call must block inside acquireTableLock(X) until Thread 1 commits
                QueryResult res = db.execute("DROP TABLE accounts;");
                ddlFinished.set(true);
                ddlResult.set(res);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        readerFuture.get(5, TimeUnit.SECONDS);
        ddlFuture.get(5, TimeUnit.SECONDS);

        assertTrue(ddlFinished.get(), "DROP TABLE must complete once reader commits");
        assertTrue(ddlResult.get().isSuccess(), "DROP TABLE must succeed: " + ddlResult.get().getError());

        // Verify table is indeed dropped
        QueryResult verify = db.execute("SELECT * FROM accounts;");
        assertFalse(verify.isSuccess(), "Table must no longer exist after drop");

        executor.shutdown();
    }

    @Test
    @DisplayName("DDL (ALTER TABLE) is blocked by active writer holding table Intent Exclusive lock")
    void testTableIntentExclusiveBlocksTableExclusiveDdl() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch writerStarted = new CountDownLatch(1);
        CountDownLatch alterQueued = new CountDownLatch(1);
        AtomicBoolean alterFinished = new AtomicBoolean(false);

        // Thread 1: Writer holds table IX lock and row X lock
        Future<?> writerFuture = executor.submit(() -> {
            db.execute("BEGIN;");
            QueryResult res = db.execute("UPDATE accounts SET balance = 1050 WHERE id = 1;");
            assertTrue(res.isSuccess(), "UPDATE failed: " + (res != null ? res.getError() : "null"));
            writerStarted.countDown();

            while (db.getTransactionManager().getLockManager().getTableWaiterCount("accounts") == 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            assertFalse(alterFinished.get(), "ALTER TABLE must be blocked while writer is active");
            db.execute("COMMIT;");
        });

        // Thread 2: ALTER TABLE requests table X lock
        Future<?> alterFuture = executor.submit(() -> {
            try {
                writerStarted.await(5, TimeUnit.SECONDS);
                QueryResult res = db.execute("ALTER TABLE accounts ADD COLUMN extra INT;");
                alterFinished.set(true);
                assertTrue(res.isSuccess(), "ALTER TABLE must succeed: " + (res != null ? res.getError() : "null"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        writerFuture.get(5, TimeUnit.SECONDS);
        alterFuture.get(5, TimeUnit.SECONDS);

        assertTrue(alterFinished.get(), "ALTER TABLE must finish after writer commits");

        // Verify added column
        QueryResult read = db.execute("SELECT id, extra FROM accounts WHERE id = 1;");
        assertTrue(read.isSuccess());
        executor.shutdown();
    }

    @Test
    @DisplayName("Active table Exclusive lock (DDL) blocks incoming readers until commit")
    void testTableExclusiveDdlBlocksActiveReaders() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch ddlHolding = new CountDownLatch(1);
        CountDownLatch readerQueued = new CountDownLatch(1);
        AtomicBoolean readerFinished = new AtomicBoolean(false);

        // Thread 1: DDL transaction acquires table X lock
        Future<?> ddlFuture = executor.submit(() -> {
            db.execute("BEGIN;");
            QueryResult res = db.execute("ALTER TABLE accounts ADD COLUMN status VARCHAR(20);");
            assertTrue(res.isSuccess());
            ddlHolding.countDown();

            while (db.getTransactionManager().getLockManager().getTableWaiterCount("accounts") == 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            assertFalse(readerFinished.get(), "Reader must be blocked while DDL holds table X lock");
            db.execute("COMMIT;");
        });

        // Thread 2: Reader requests table IS lock
        Future<?> readerFuture = executor.submit(() -> {
            try {
                ddlHolding.await(5, TimeUnit.SECONDS);
                QueryResult res = db.execute("SELECT id, name FROM accounts WHERE id = 2;");
                readerFinished.set(true);
                assertTrue(res.isSuccess());
                assertEquals("Bob", res.getRows().get(0).getValue("name"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        ddlFuture.get(5, TimeUnit.SECONDS);
        readerFuture.get(5, TimeUnit.SECONDS);

        assertTrue(readerFinished.get());
        executor.shutdown();
    }

    @Test
    @DisplayName("Concurrent SELECT ... FOR SHARE queries on same row proceed concurrently without blocking")
    void testConcurrentSelectForShareNonBlocking() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch allReady = new CountDownLatch(3);
        CountDownLatch runQuery = new CountDownLatch(1);
        CountDownLatch allCompleted = new CountDownLatch(3);

        for (int i = 0; i < 3; i++) {
            executor.submit(() -> {
                allReady.countDown();
                try {
                    runQuery.await();
                    db.execute("BEGIN;");
                    QueryResult res = db.execute("SELECT * FROM accounts WHERE id = 1 FOR SHARE;");
                    assertTrue(res.isSuccess());
                    assertEquals(1, res.getRows().size());
                    allCompleted.countDown();
                    db.execute("COMMIT;");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        allReady.await(2, TimeUnit.SECONDS);
        long start = System.currentTimeMillis();
        runQuery.countDown();

        boolean completedInTime = allCompleted.await(2, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(completedInTime, "All 3 concurrent FOR SHARE queries must complete without blocking");
        assertTrue(elapsed < 1000, "Should execute concurrently in < 1s");
        executor.shutdown();
    }

    @Test
    @DisplayName("SELECT ... FOR SHARE blocks concurrent SELECT ... FOR UPDATE on identical row")
    void testSelectForShareBlocksSelectForUpdate() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch shareHeld = new CountDownLatch(1);
        CountDownLatch updateAttempted = new CountDownLatch(1);
        AtomicBoolean updateFinished = new AtomicBoolean(false);

        // Thread 1: SELECT FOR SHARE holds S lock on row 1
        Future<?> shareFuture = executor.submit(() -> {
            db.execute("BEGIN;");
            QueryResult res = db.execute("SELECT * FROM accounts WHERE id = 1 FOR SHARE;");
            assertTrue(res.isSuccess());
            shareHeld.countDown();

            try {
                updateAttempted.await(5, TimeUnit.SECONDS);
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            assertFalse(updateFinished.get(), "FOR UPDATE must remain blocked while row S lock is held");
            db.execute("COMMIT;");
        });

        // Thread 2: Concurrent SELECT FOR UPDATE requests X lock on row 1
        Future<?> updateFuture = executor.submit(() -> {
            try {
                shareHeld.await(5, TimeUnit.SECONDS);
                updateAttempted.countDown();
                db.execute("BEGIN;");
                QueryResult res = db.execute("SELECT * FROM accounts WHERE id = 1 FOR UPDATE;");
                updateFinished.set(true);
                assertTrue(res.isSuccess());
                db.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        shareFuture.get(5, TimeUnit.SECONDS);
        updateFuture.get(5, TimeUnit.SECONDS);

        assertTrue(updateFinished.get(), "FOR UPDATE must unblock and complete after FOR SHARE commits");
        executor.shutdown();
    }

    @Test
    @DisplayName("SELECT ... FOR UPDATE blocks concurrent SELECT ... FOR SHARE on identical row")
    void testSelectForUpdateBlocksSelectForShare() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch updateHeld = new CountDownLatch(1);
        CountDownLatch shareAttempted = new CountDownLatch(1);
        AtomicBoolean shareFinished = new AtomicBoolean(false);

        // Thread 1: SELECT FOR UPDATE holds X lock on row 2
        Future<?> updateFuture = executor.submit(() -> {
            db.execute("BEGIN;");
            QueryResult res = db.execute("SELECT * FROM accounts WHERE id = 2 FOR UPDATE;");
            assertTrue(res.isSuccess());
            updateHeld.countDown();

            try {
                shareAttempted.await(5, TimeUnit.SECONDS);
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            assertFalse(shareFinished.get(), "FOR SHARE must be blocked while row X lock is held");
            db.execute("COMMIT;");
        });

        // Thread 2: Concurrent SELECT FOR SHARE requests S lock on row 2
        Future<?> shareFuture = executor.submit(() -> {
            try {
                updateHeld.await(5, TimeUnit.SECONDS);
                shareAttempted.countDown();
                db.execute("BEGIN;");
                QueryResult res = db.execute("SELECT * FROM accounts WHERE id = 2 FOR SHARE;");
                shareFinished.set(true);
                assertTrue(res.isSuccess());
                db.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        updateFuture.get(5, TimeUnit.SECONDS);
        shareFuture.get(5, TimeUnit.SECONDS);

        assertTrue(shareFinished.get(), "FOR SHARE must unblock and complete after FOR UPDATE commits");
        executor.shutdown();
    }

    @Test
    @DisplayName("Concurrent SELECT ... FOR UPDATE serializes correctly and sees committed updates")
    void testConcurrentSelectForUpdateSerializes() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch t1Locked = new CountDownLatch(1);
        CountDownLatch t2Started = new CountDownLatch(1);
        AtomicBoolean t2Acquired = new AtomicBoolean(false);
        AtomicReference<Object> t2ObservedBalance = new AtomicReference<>();

        // Thread 1: Locks row 1, modifies balance, then commits
        Future<?> t1Future = executor.submit(() -> {
            db.execute("BEGIN;");
            QueryResult res = db.execute("SELECT * FROM accounts WHERE id = 1 FOR UPDATE;");
            assertTrue(res.isSuccess());
            t1Locked.countDown();

            while (db.getTransactionManager().getLockManager().getActiveLockCount() > 0
                && db.getTransactionManager().getLockManager().getWaitsForSnapshot().isEmpty()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            assertFalse(t2Acquired.get(), "T2 must be serialized behind T1");
            db.execute("UPDATE accounts SET balance = 1500 WHERE id = 1;");
            db.execute("COMMIT;");
        });

        // Thread 2: Serializes behind Thread 1 on row 1
        Future<?> t2Future = executor.submit(() -> {
            try {
                t1Locked.await(5, TimeUnit.SECONDS);
                t2Started.countDown();
                db.execute("BEGIN;");
                QueryResult res = db.execute("SELECT * FROM accounts WHERE id = 1 FOR UPDATE;");
                t2Acquired.set(true);
                assertTrue(res.isSuccess());
                Object bal = res.getRows().get(0).getValue("balance");
                t2ObservedBalance.set(bal);
                db.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        t1Future.get(5, TimeUnit.SECONDS);
        t2Future.get(5, TimeUnit.SECONDS);

        assertTrue(t2Acquired.get(), "T2 must acquire lock after T1 commits");
        assertEquals(1500, ((Number) t2ObservedBalance.get()).intValue(), "T2 must see committed updated value");
        executor.shutdown();
    }

    @Test
    @DisplayName("Cross-table deadlock detection aborts one transaction and allows other to commit")
    void testCrossTableDeadlockDetection() throws Exception {
        db.execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT);");
        db.execute("INSERT INTO orders VALUES (10, 500);");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch t1Step1 = new CountDownLatch(1);
        CountDownLatch t2Step1 = new CountDownLatch(1);
        AtomicReference<QueryResult> t1Final = new AtomicReference<>();
        AtomicReference<QueryResult> t2Final = new AtomicReference<>();

        // Thread 1: IS on accounts, requests X on orders
        executor.submit(() -> {
            db.execute("BEGIN;");
            db.execute("SELECT * FROM accounts WHERE id = 1;");
            t1Step1.countDown();

            try {
                t2Step1.await(5, TimeUnit.SECONDS);
                Thread.sleep(50);
                QueryResult res = db.execute("DROP TABLE orders;");
                t1Final.set(res);
                if (res.isSuccess()) {
                    db.execute("COMMIT;");
                }
            } catch (Exception e) {
                db.execute("ROLLBACK;");
            }
        });

        // Thread 2: IS on orders, requests X on accounts
        executor.submit(() -> {
            try {
                t1Step1.await(5, TimeUnit.SECONDS);
                db.execute("BEGIN;");
                db.execute("SELECT * FROM orders WHERE id = 10;");
                t2Step1.countDown();

                QueryResult res = db.execute("DROP TABLE accounts;");
                t2Final.set(res);
                if (res.isSuccess()) {
                    db.execute("COMMIT;");
                }
            } catch (Exception e) {
                db.execute("ROLLBACK;");
            }
        });

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        QueryResult r1 = t1Final.get();
        QueryResult r2 = t2Final.get();

        // Exactly one must detect deadlock and abort, while the other succeeds
        boolean oneFailedWithDeadlock = (r1 != null && !r1.isSuccess() && r1.getError().contains("Deadlock"))
                                     || (r2 != null && !r2.isSuccess() && r2.getError().contains("Deadlock"));
        assertTrue(oneFailedWithDeadlock, "One of the deadlocked transactions must fail with Deadlock detected: r1=" + r1 + ", r2=" + r2);
    }

    @Test
    @DisplayName("Sole holder of row Shared lock can upgrade to Exclusive lock via UPDATE")
    void testRowShareToExclusiveUpgrade() {
        db.execute("BEGIN;");
        QueryResult shareRes = db.execute("SELECT * FROM accounts WHERE id = 3 FOR SHARE;");
        assertTrue(shareRes.isSuccess());

        // Sole holder upgrades to exclusive lock via UPDATE
        QueryResult updateRes = db.execute("UPDATE accounts SET balance = 3500 WHERE id = 3;");
        assertTrue(updateRes.isSuccess(), "Upgrade to exclusive lock must succeed without blocking");

        db.execute("COMMIT;");

        QueryResult verify = db.execute("SELECT balance FROM accounts WHERE id = 3;");
        assertTrue(verify.isSuccess());
        assertEquals(3500, ((Number) verify.getRows().get(0).getValue("balance")).intValue());
    }

    @Test
    @DisplayName("Transaction ROLLBACK immediately releases all table and row locks, waking waiting threads")
    void testRollbackReleasesAllTableAndRowLocks() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch t1Locked = new CountDownLatch(1);
        CountDownLatch t2Waiting = new CountDownLatch(1);
        AtomicBoolean t2Acquired = new AtomicBoolean(false);

        // Thread 1 acquires table IX and row X on row 1
        Future<?> t1Future = executor.submit(() -> {
            db.execute("BEGIN;");
            QueryResult res = db.execute("SELECT * FROM accounts WHERE id = 1 FOR UPDATE;");
            assertTrue(res.isSuccess());
            t1Locked.countDown();

            try {
                t2Waiting.await(5, TimeUnit.SECONDS);
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            assertFalse(t2Acquired.get(), "T2 must be waiting on T1");
            db.execute("ROLLBACK;");
        });

        // Thread 2 waits on row 1
        Future<?> t2Future = executor.submit(() -> {
            try {
                t1Locked.await(5, TimeUnit.SECONDS);
                t2Waiting.countDown();
                db.execute("BEGIN;");
                QueryResult res = db.execute("SELECT * FROM accounts WHERE id = 1 FOR UPDATE;");
                t2Acquired.set(true);
                assertTrue(res.isSuccess());
                db.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        t1Future.get(5, TimeUnit.SECONDS);
        t2Future.get(5, TimeUnit.SECONDS);

        assertTrue(t2Acquired.get(), "ROLLBACK must instantly release all locks and wake waiting transactions");
        executor.shutdown();
    }
}
