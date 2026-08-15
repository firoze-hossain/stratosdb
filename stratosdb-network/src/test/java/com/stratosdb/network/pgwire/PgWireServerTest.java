package com.stratosdb.network.pgwire;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Verifies real PostgreSQL wire-protocol-v3 compatibility - the point of
 * this class isn't "does StratosDB's own code agree with itself," it's
 * "does an actual, unmodified Postgres client work against this server."
 *
 * Two verification strategies, deliberately both present:
 * - A minimal hand-rolled client (RawConnection, below) for tests that must
 *   run everywhere, with no external dependency - this is what proves the
 *   wire format itself (message framing, RowDescription/DataRow/
 *   CommandComplete shapes, NULL encoding, concurrent connections) is
 *   correct. One connection is reused across multiple statements within a
 *   single test, exactly how a real client works - an earlier version of
 *   this helper opened a fresh socket per statement, which silently broke
 *   transaction semantics across separate ThreadLocal sessions (a real bug
 *   in the test, not the server - see PROGRESS.md).
 * - Real `psql` via ProcessBuilder, for tests that specifically need an
 *   actual, unmodified reference client rather than a test's own
 *   understanding of the protocol - skipped gracefully (not failed) if
 *   `psql` isn't on the PATH, since requiring it changes what this test
 *   suite depends on to even build.
 */
class PgWireServerTest {

    @TempDir
    Path tempDir;

    private int port;
    private StratosDB db;
    private PgWireServer server;

    @BeforeEach
    void setUp() throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        server = new PgWireServer(port, db);
        server.start();
        Thread.sleep(200); // give the accept thread a moment to actually be listening
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    private static int freePort() throws Exception {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    // --- core protocol correctness, via the hand-rolled client ---

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void startupHandshakeDeclinesSslThenAcceptsPlaintext() throws Exception {
        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            // SSLRequest - exactly the bytes a real libpq client sends first by default.
            out.writeInt(8);
            out.writeInt(PgWireMessages.SSL_REQUEST_CODE);
            out.flush();
            assertEquals('N', in.readUnsignedByte(), "server must decline SSL with a single 'N' byte");

            sendStartup(out, "testuser", "testdb");
            assertTrue(readUntilReadyForQuery(in), "startup must complete with ReadyForQuery");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void createInsertSelectRoundTrip() throws Exception {
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT, name VARCHAR, val INT)");
            conn.query("INSERT INTO t VALUES (1, 'Alice', 100)");
            conn.query("INSERT INTO t VALUES (2, 'Bob', 200)");

            QueryOutcome result = conn.query("SELECT * FROM t");
            assertEquals(2, result.rowCount);
            assertEquals(List.of("id", "name", "val"), result.columnNames);
            assertEquals("SELECT 2", result.commandTag);
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void nullValuesEncodeCorrectly() throws Exception {
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT, val INT)");
            conn.query("INSERT INTO t VALUES (1, NULL)");
            QueryOutcome result = conn.query("SELECT * FROM t");
            assertEquals(1, result.rowCount);
            assertEquals(2, result.rows.get(0).size());
            assertNull(result.rows.get(0).get(1), "a SQL NULL must decode as a real null, not the string \"null\" or empty string");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void transactionsCommitAndRollbackCorrectly() throws Exception {
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT)");
            conn.query("BEGIN");
            conn.query("INSERT INTO t VALUES (1)");
            conn.query("INSERT INTO t VALUES (2)");
            conn.query("COMMIT");
            assertEquals(2, conn.query("SELECT * FROM t").rowCount);

            conn.query("BEGIN");
            conn.query("INSERT INTO t VALUES (3)");
            conn.query("ROLLBACK");
            assertEquals(2, conn.query("SELECT * FROM t").rowCount, "the rolled-back insert must not be visible");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void failedStatementPoisonsTheTransactionMatchingRealPostgresBehavior() throws Exception {
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT)");
            conn.query("BEGIN");
            conn.query("INSERT INTO t VALUES (1)");
            QueryOutcome badQuery = conn.query("SELECT * FROM nonexistent_table");
            assertTrue(badQuery.error != null);

            QueryOutcome poisonedCheck = conn.query("INSERT INTO t VALUES (2)");
            assertTrue(poisonedCheck.error != null && poisonedCheck.error.contains("aborted"),
                () -> "expected a poisoned-transaction error, got: " + poisonedCheck.error);

            conn.query("ROLLBACK");
            assertEquals(0, conn.query("SELECT * FROM t").rowCount);
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void concurrentConnectionsAllSucceed() throws Exception {
        try (RawConnection setup = connect()) {
            setup.query("CREATE TABLE t (id INT, val INT)");
        }

        int n = 8;
        CountDownLatch latch = new CountDownLatch(n);
        AtomicInteger successes = new AtomicInteger(0);
        for (int i = 0; i < n; i++) {
            final int id = i;
            new Thread(() -> {
                try (RawConnection conn = connect()) {
                    QueryOutcome r = conn.query("INSERT INTO t VALUES (" + id + ", " + (id * 10) + ")");
                    if (r.error == null) successes.incrementAndGet();
                } catch (Exception ignored) {
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS), "all concurrent connections must complete within the timeout");
        assertEquals(n, successes.get(), "every concurrent insert must succeed");
        try (RawConnection check = connect()) {
            assertEquals(n, check.query("SELECT * FROM t").rowCount);
        }
    }

    // --- real psql verification (skips gracefully if psql isn't installed) ---

    private static boolean psqlAvailable() {
        try {
            Process p = new ProcessBuilder("psql", "--version").start();
            return p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void realPsqlClientConnectsAndRunsQueries() throws Exception {
        assumeTrue(psqlAvailable(), "psql not installed - skipping real-client verification");

        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT, name VARCHAR)");
            conn.query("INSERT INTO t VALUES (1, 'Alice')");
        }

        String output = runPsql("SELECT * FROM t");
        assertTrue(output.contains("Alice"), () -> "expected psql's output to contain the row's data, got:\n" + output);
        assertTrue(output.contains("(1 row)"), () -> "expected psql's standard row-count footer, got:\n" + output);
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void realPsqlClientHandlesTransactionsAndErrors() throws Exception {
        assumeTrue(psqlAvailable(), "psql not installed - skipping real-client verification");

        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT)");
        }
        String output = runPsql("BEGIN; INSERT INTO t VALUES (1); SELECT * FROM bogus; INSERT INTO t VALUES (2); ROLLBACK;");
        assertTrue(output.toLowerCase().contains("aborted"),
            () -> "expected psql to show the poisoned-transaction error, got:\n" + output);

        try (RawConnection conn = connect()) {
            assertEquals(0, conn.query("SELECT * FROM t").rowCount, "everything in the rolled-back transaction must be gone");
        }
    }

    private String runPsql(String sql) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
            "psql", "-h", "localhost", "-p", String.valueOf(port), "-U", "testuser", "-d", "testdb", "-c", sql
        );
        pb.environment().put("PGPASSWORD", "unused");
        pb.redirectErrorStream(true);
        Process process = pb.start();
        String output;
        try (InputStream in = process.getInputStream()) {
            output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        boolean exited = process.waitFor(15, TimeUnit.SECONDS);
        assertTrue(exited, "psql did not exit within the timeout");
        return output;
    }

    // --- minimal hand-rolled pg-wire client, used by the core tests above ---

    private record QueryOutcome(int rowCount, List<String> columnNames, List<List<String>> rows, String commandTag, String error) {}

    /** One real connection, reused across multiple statements within a single test - see this class's javadoc for why. */
    private final class RawConnection implements AutoCloseable {
        private final Socket socket;
        private final DataOutputStream out;
        private final DataInputStream in;

        RawConnection() throws IOException {
            socket = new Socket("localhost", port);
            socket.setSoTimeout(8000);
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            sendStartup(out, "testuser", "testdb");
            readUntilReadyForQuery(in);
        }

        QueryOutcome query(String sql) throws IOException {
            byte[] sqlBytes = (sql + "\0").getBytes(StandardCharsets.UTF_8);
            out.writeByte('Q');
            out.writeInt(sqlBytes.length + 4);
            out.write(sqlBytes);
            out.flush();

            List<String> columnNames = new java.util.ArrayList<>();
            List<List<String>> rows = new java.util.ArrayList<>();
            String commandTag = null;
            String error = null;

            while (true) {
                int type = in.readUnsignedByte();
                int len = in.readInt();
                byte[] body = new byte[len - 4];
                in.readFully(body);

                if (type == 'Z') break;
                if (type == 'T') columnNames.addAll(parseRowDescriptionNames(body));
                if (type == 'D') rows.add(parseDataRowValues(body));
                if (type == 'C') commandTag = new String(body, 0, body.length - 1, StandardCharsets.UTF_8);
                if (type == 'E') error = extractErrorMessage(body);
            }
            return new QueryOutcome(rows.size(), columnNames, rows, commandTag, error);
        }

        @Override
        public void close() throws IOException {
            out.writeByte('X');
            out.writeInt(4);
            out.flush();
            socket.close();
        }
    }

    private RawConnection connect() throws IOException {
        return new RawConnection();
    }

    private void sendStartup(DataOutputStream out, String user, String database) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        DataOutputStream bodyOut = new DataOutputStream(body);
        bodyOut.writeInt(PgWireMessages.PROTOCOL_VERSION_3);
        writeCString(bodyOut, "user"); writeCString(bodyOut, user);
        writeCString(bodyOut, "database"); writeCString(bodyOut, database);
        bodyOut.writeByte(0);
        out.writeInt(body.size() + 4);
        out.write(body.toByteArray());
        out.flush();
    }

    private boolean readUntilReadyForQuery(DataInputStream in) throws IOException {
        while (true) {
            int type = in.readUnsignedByte();
            int len = in.readInt();
            byte[] body = new byte[len - 4];
            in.readFully(body);
            if (type == 'Z') return true;
            if (type == 'E') return false;
        }
    }

    private List<String> parseRowDescriptionNames(byte[] body) {
        List<String> names = new java.util.ArrayList<>();
        int pos = 2; // skip field count
        short fieldCount = (short) (((body[0] & 0xFF) << 8) | (body[1] & 0xFF));
        for (int f = 0; f < fieldCount; f++) {
            int nameStart = pos;
            while (body[pos] != 0) pos++;
            names.add(new String(body, nameStart, pos - nameStart, StandardCharsets.UTF_8));
            pos++; // null terminator
            pos += 4 + 2 + 4 + 2 + 4 + 2; // tableOID, attrNum, typeOID, typeSize, typeMod, formatCode
        }
        return names;
    }

    private List<String> parseDataRowValues(byte[] body) {
        List<String> values = new java.util.ArrayList<>();
        int pos = 2;
        short colCount = (short) (((body[0] & 0xFF) << 8) | (body[1] & 0xFF));
        for (int c = 0; c < colCount; c++) {
            int colLen = ((body[pos] & 0xFF) << 24) | ((body[pos + 1] & 0xFF) << 16) | ((body[pos + 2] & 0xFF) << 8) | (body[pos + 3] & 0xFF);
            pos += 4;
            if (colLen == -1) {
                values.add(null);
            } else {
                values.add(new String(body, pos, colLen, StandardCharsets.UTF_8));
                pos += colLen;
            }
        }
        return values;
    }

    private String extractErrorMessage(byte[] body) {
        int pos = 0;
        while (pos < body.length && body[pos] != 0) {
            char fieldType = (char) body[pos];
            pos++;
            int start = pos;
            while (body[pos] != 0) pos++;
            String value = new String(body, start, pos - start, StandardCharsets.UTF_8);
            pos++;
            if (fieldType == 'M') return value;
        }
        return "";
    }

    private void writeCString(DataOutputStream out, String s) throws IOException {
        out.write(s.getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);
    }
}
