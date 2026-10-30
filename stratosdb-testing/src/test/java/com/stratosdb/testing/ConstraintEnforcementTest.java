package com.stratosdb.testing;

import com.stratosdb.common.exceptions.UniqueConstraintViolationException;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireServer;
import com.stratosdb.sql.executor.QueryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ConstraintEnforcementTest {

    @TempDir
    Path tempDir;

    private StratosDB db;
    private StdWireServer server;
    private int port;

    @BeforeEach
    void setUp() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setBufferPoolSize(64);
        db = new StratosDB(config);
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception ignored) {}
        }
        if (db != null) {
            db.shutdown();
        }
    }

    @Test
    void testInlinePrimaryKeyDuplicateRejection() {
        QueryResult create = db.execute("CREATE TABLE users_inline (id INT PRIMARY KEY, name VARCHAR)");
        assertTrue(create.isSuccess(), "CREATE TABLE with inline PK must succeed");

        QueryResult ins1 = db.execute("INSERT INTO users_inline VALUES (1, 'Alice')");
        assertTrue(ins1.isSuccess());

        // Second insert with id=1 must fail with standard SQLSTATE 23505
        QueryResult ins2 = db.execute("INSERT INTO users_inline VALUES (1, 'DuplicateAlice')");
        assertFalse(ins2.isSuccess(), "Duplicate PK insert must fail");
        assertEquals("23505", ins2.getSqlState());
        assertTrue(ins2.getException() instanceof UniqueConstraintViolationException);
        assertTrue(ins2.getError().contains("duplicate key value violates unique constraint"));

        // executeOrThrow must throw UniqueConstraintViolationException
        assertThrows(UniqueConstraintViolationException.class, () ->
            db.executeOrThrow("INSERT INTO users_inline VALUES (1, 'AliceThird')"));

        // Confirm only 1 row exists
        QueryResult sel = db.execute("SELECT * FROM users_inline");
        assertEquals(1, sel.getRows().size());
        assertEquals("Alice", sel.getRows().get(0).getValue("name"));
    }

    @Test
    void testTableLevelPrimaryKeyDuplicateRejection() {
        QueryResult create = db.execute("CREATE TABLE users_tbl (id INT, name VARCHAR, PRIMARY KEY (id))");
        assertTrue(create.isSuccess(), "CREATE TABLE with table-level PK must succeed");

        assertTrue(db.execute("INSERT INTO users_tbl VALUES (10, 'Bob')").isSuccess());

        QueryResult dup = db.execute("INSERT INTO users_tbl VALUES (10, 'DuplicateBob')");
        assertFalse(dup.isSuccess());
        assertEquals("23505", dup.getSqlState());
        assertTrue(dup.getException() instanceof UniqueConstraintViolationException);
    }

    @Test
    void testColumnLevelUniqueConstraint() {
        QueryResult create = db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, email VARCHAR UNIQUE)");
        assertTrue(create.isSuccess());

        assertTrue(db.execute("INSERT INTO accounts VALUES (1, 'alice@example.com')").isSuccess());
        assertTrue(db.execute("INSERT INTO accounts VALUES (2, 'bob@example.com')").isSuccess());

        // Duplicate email with different id must be rejected
        QueryResult dup = db.execute("INSERT INTO accounts VALUES (3, 'alice@example.com')");
        assertFalse(dup.isSuccess());
        assertEquals("23505", dup.getSqlState());
        assertTrue(dup.getException() instanceof UniqueConstraintViolationException);
        assertTrue(dup.getError().contains("accounts_email_key"));

        // Third unique email must succeed
        assertTrue(db.execute("INSERT INTO accounts VALUES (3, 'charlie@example.com')").isSuccess());
        assertEquals(3, db.execute("SELECT * FROM accounts").getRows().size());
    }

    @Test
    void testTableLevelUniqueConstraint() {
        QueryResult create = db.execute("CREATE TABLE products (id INT, code VARCHAR, PRIMARY KEY (id), UNIQUE (code))");
        assertTrue(create.isSuccess());

        assertTrue(db.execute("INSERT INTO products VALUES (1, 'SKU-001')").isSuccess());
        QueryResult dup = db.execute("INSERT INTO products VALUES (2, 'SKU-001')");
        assertFalse(dup.isSuccess());
        assertEquals("23505", dup.getSqlState());
        assertTrue(dup.getException() instanceof UniqueConstraintViolationException);
    }

    @Test
    void testNamedConstraintTableLevel() {
        QueryResult create = db.execute("CREATE TABLE tokens (id INT, token VARCHAR, CONSTRAINT uq_token UNIQUE (token), PRIMARY KEY (id))");
        assertTrue(create.isSuccess());

        assertTrue(db.execute("INSERT INTO tokens VALUES (1, 'TOKEN_ABC')").isSuccess());
        QueryResult dup = db.execute("INSERT INTO tokens VALUES (2, 'TOKEN_ABC')");
        assertFalse(dup.isSuccess());
        assertEquals("23505", dup.getSqlState());
        assertTrue(dup.getException() instanceof UniqueConstraintViolationException);
    }

    @Test
    void testCreateUniqueIndexEnforcement() {
        assertTrue(db.execute("CREATE TABLE items (id INT, serial_no INT)").isSuccess());
        QueryResult createIdx = db.execute("CREATE UNIQUE INDEX idx_serial ON items (serial_no)");
        assertTrue(createIdx.isSuccess());

        assertTrue(db.execute("INSERT INTO items VALUES (1, 9999)").isSuccess());
        QueryResult dup = db.execute("INSERT INTO items VALUES (2, 9999)");
        assertFalse(dup.isSuccess());
        assertEquals("23505", dup.getSqlState());
        assertTrue(dup.getException() instanceof UniqueConstraintViolationException);
    }

    @Test
    void testCreateUniqueIndexRejectsExistingDuplicates() {
        assertTrue(db.execute("CREATE TABLE dup_data (id INT, val INT)").isSuccess());
        assertTrue(db.execute("INSERT INTO dup_data VALUES (1, 42)").isSuccess());
        assertTrue(db.execute("INSERT INTO dup_data VALUES (2, 42)").isSuccess());

        QueryResult createIdx = db.execute("CREATE UNIQUE INDEX idx_val_uq ON dup_data (val)");
        assertFalse(createIdx.isSuccess(), "CREATE UNIQUE INDEX must fail when table contains duplicate values");
        assertEquals("23505", createIdx.getSqlState());
        assertTrue(createIdx.getError().contains("Table contains duplicate values"));
    }

    @Test
    void testUpdatePrimaryKeyToExistingDuplicateFails() {
        assertTrue(db.execute("CREATE TABLE upd_tbl (id INT PRIMARY KEY, val VARCHAR)").isSuccess());
        assertTrue(db.execute("INSERT INTO upd_tbl VALUES (1, 'First')").isSuccess());
        assertTrue(db.execute("INSERT INTO upd_tbl VALUES (2, 'Second')").isSuccess());

        // Attempt to update id 1 to id 2
        QueryResult upd = db.execute("UPDATE upd_tbl SET id = 2 WHERE id = 1");
        assertFalse(upd.isSuccess(), "UPDATE colliding with existing PK must be rejected");
        assertEquals("23505", upd.getSqlState());
        assertTrue(upd.getException() instanceof UniqueConstraintViolationException);

        // Verify both original rows are intact
        QueryResult r1 = db.execute("SELECT * FROM upd_tbl WHERE id = 1");
        assertEquals(1, r1.getRows().size());
        assertEquals("First", r1.getRows().get(0).getValue("val"));

        QueryResult r2 = db.execute("SELECT * FROM upd_tbl WHERE id = 2");
        assertEquals(1, r2.getRows().size());
        assertEquals("Second", r2.getRows().get(0).getValue("val"));
    }

    @Test
    void testUpdateNonKeyColumnSucceedsWithoutSelfConflict() {
        assertTrue(db.execute("CREATE TABLE upd_self (id INT PRIMARY KEY, val VARCHAR)").isSuccess());
        assertTrue(db.execute("INSERT INTO upd_self VALUES (1, 'Initial')").isSuccess());

        // Updating non-key column on row id=1 should NOT conflict with its own PK=1
        QueryResult upd = db.execute("UPDATE upd_self SET val = 'Updated' WHERE id = 1");
        assertTrue(upd.isSuccess(), "Updating non-key column must succeed without self-collision");

        QueryResult sel = db.execute("SELECT * FROM upd_self WHERE id = 1");
        assertEquals("Updated", sel.getRows().get(0).getValue("val"));
    }

    @Test
    void testDeleteAllowsReinsertingKey() {
        assertTrue(db.execute("CREATE TABLE del_test (id INT PRIMARY KEY, val VARCHAR)").isSuccess());
        assertTrue(db.execute("INSERT INTO del_test VALUES (1, 'One')").isSuccess());

        QueryResult del = db.execute("DELETE FROM del_test WHERE id = 1");
        assertTrue(del.isSuccess());

        // Reinserting id 1 after delete must succeed
        QueryResult ins = db.execute("INSERT INTO del_test VALUES (1, 'Reborn')");
        assertTrue(ins.isSuccess(), "Reinserting deleted key must succeed");

        QueryResult sel = db.execute("SELECT * FROM del_test WHERE id = 1");
        assertEquals("Reborn", sel.getRows().get(0).getValue("val"));
    }

    @Test
    void testTransactionRollbackReleasesKey() {
        assertTrue(db.execute("CREATE TABLE tx_test (id INT PRIMARY KEY, val VARCHAR)").isSuccess());

        assertTrue(db.execute("BEGIN").isSuccess());
        assertTrue(db.execute("INSERT INTO tx_test VALUES (50, 'Pending')").isSuccess());
        assertTrue(db.execute("ROLLBACK").isSuccess());

        // After ROLLBACK, key 50 must be available for insert
        QueryResult ins = db.execute("INSERT INTO tx_test VALUES (50, 'CommittedNow')");
        assertTrue(ins.isSuccess(), "Key from rolled-back insert must be insertable");
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testConcurrentPrimaryKeyInserts() throws Exception {
        assertTrue(db.execute("CREATE TABLE conc_pk (id INT PRIMARY KEY, thread_num INT)").isSuccess());

        int threadCount = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger uniqueViolations = new AtomicInteger();

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            futures.add(pool.submit(() -> {
                try {
                    barrier.await();
                    QueryResult res = db.execute("INSERT INTO conc_pk VALUES (1000, " + threadId + ")");
                    if (res.isSuccess()) {
                        successes.incrementAndGet();
                    } else if ("23505".equals(res.getSqlState())) {
                        uniqueViolations.incrementAndGet();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        assertEquals(1, successes.get(), "Exactly one thread must succeed in inserting primary key 1000");
        assertEquals(threadCount - 1, uniqueViolations.get(), "All other concurrent inserts must receive SQLSTATE 23505");

        QueryResult countRes = db.execute("SELECT * FROM conc_pk WHERE id = 1000");
        assertEquals(1, countRes.getRows().size(), "Table must have exactly 1 row with id=1000");
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testConcurrentUniqueColumnInserts() throws Exception {
        assertTrue(db.execute("CREATE TABLE conc_uq (id INT PRIMARY KEY, email VARCHAR UNIQUE)").isSuccess());

        int threadCount = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger uniqueViolations = new AtomicInteger();

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            final int id = i;
            futures.add(pool.submit(() -> {
                try {
                    barrier.await();
                    QueryResult res = db.execute("INSERT INTO conc_uq VALUES (" + id + ", 'same_email@test.com')");
                    if (res.isSuccess()) {
                        successes.incrementAndGet();
                    } else if ("23505".equals(res.getSqlState())) {
                        uniqueViolations.incrementAndGet();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        assertEquals(1, successes.get(), "Exactly one thread must succeed in inserting the unique email");
        assertEquals(threadCount - 1, uniqueViolations.get(), "All other concurrent inserts must receive SQLSTATE 23505");
    }

    @Test
    void testWireProtocolErrorResponseSqlState() throws Exception {
        port = freePort();
        server = new StdWireServer(port, db);
        server.start();
        Thread.sleep(150);

        try (Socket socket = new Socket("localhost", port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            // Startup packet: protocol version 3.0 (196608)
            ByteArrayOutputStream startupBuf = new ByteArrayOutputStream();
            DataOutputStream sOut = new DataOutputStream(startupBuf);
            sOut.writeInt(196608);
            sOut.writeBytes("user\0stratos\0database\0stratos\0\0");
            sOut.flush();
            byte[] startupPayload = startupBuf.toByteArray();

            out.writeInt(startupPayload.length + 4);
            out.write(startupPayload);
            out.flush();

            // Read until ReadyForQuery ('Z')
            drainUntilReady(in);

            // Create table
            sendQuery(out, "CREATE TABLE wire_constraint (id INT PRIMARY KEY, title VARCHAR);");
            drainUntilReady(in);

            // Insert first row
            sendQuery(out, "INSERT INTO wire_constraint VALUES (1, 'First');");
            drainUntilReady(in);

            // Insert duplicate row
            sendQuery(out, "INSERT INTO wire_constraint VALUES (1, 'Duplicate');");

            // Read response: expect ErrorResponse ('E')
            byte type = in.readByte();
            assertEquals((byte) 'E', type, "Server must respond with ErrorResponse ('E') for duplicate key insert");
            int len = in.readInt();
            byte[] errBytes = in.readNBytes(len - 4);

            // Parse error fields: 'S' (severity), 'C' (code/sqlstate), 'M' (message)
            String sqlState = extractErrorField(errBytes, (byte) 'C');
            String message = extractErrorField(errBytes, (byte) 'M');

            assertEquals("23505", sqlState, "Wire protocol error must report standard SQLSTATE 23505");
            assertNotNull(message);
            assertTrue(message.contains("duplicate key value violates unique constraint"));

            // Read trailing 'Z'
            drainUntilReady(in);
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    private void sendQuery(DataOutputStream out, String sql) throws IOException {
        byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
        out.writeByte('Q');
        out.writeInt(sqlBytes.length + 5);
        out.write(sqlBytes);
        out.writeByte(0);
        out.flush();
    }

    private void drainUntilReady(DataInputStream in) throws IOException {
        while (true) {
            byte type = in.readByte();
            int len = in.readInt();
            in.readNBytes(len - 4);
            if (type == 'Z') {
                break;
            }
        }
    }

    private String extractErrorField(byte[] errBytes, byte fieldCode) {
        int i = 0;
        while (i < errBytes.length) {
            byte code = errBytes[i++];
            if (code == 0) break;
            int start = i;
            while (i < errBytes.length && errBytes[i] != 0) {
                i++;
            }
            String val = new String(errBytes, start, i - start, StandardCharsets.UTF_8);
            i++; // skip null terminator
            if (code == fieldCode) {
                return val;
            }
        }
        return null;
    }
}
