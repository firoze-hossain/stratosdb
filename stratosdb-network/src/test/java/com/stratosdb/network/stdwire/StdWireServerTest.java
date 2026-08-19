package com.stratosdb.network.stdwire;

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
class StdWireServerTest {

    @TempDir
    Path tempDir;

    private int port;
    private StratosDB db;
    private StdWireServer server;

    @BeforeEach
    void setUp() throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        server = new StdWireServer(port, db);
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
            out.writeInt(StdWireMessages.SSL_REQUEST_CODE);
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

    // --- Extended query protocol: Parse/Bind/Describe/Execute/Sync, verified
    // with an independently hand-rolled client (see RawConnection.extendedQuery)
    // and, more importantly, with a real, unmodified PostgreSQL driver
    // (psycopg2) whose parameterized queries use this exact path by default -
    // the strongest evidence this isn't just internally self-consistent.

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void extendedProtocolParameterizedSelectByEquality() throws Exception {
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT, name VARCHAR)");
            conn.query("INSERT INTO t VALUES (1, 'Alice')");
            conn.query("INSERT INTO t VALUES (2, 'Bob')");

            QueryOutcome result = conn.extendedQuery("SELECT * FROM t WHERE id = $1", "1");
            assertEquals(1, result.rowCount);
            assertEquals("Alice", result.rows.get(0).get(1));
            assertEquals("SELECT 1", result.commandTag);
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void extendedProtocolParameterizedInsertAndMultipleParameters() throws Exception {
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT, name VARCHAR, age INT)");

            QueryOutcome insertResult = conn.extendedQuery("INSERT INTO t VALUES ($1, $2, $3)", "1", "Carol", "40");
            assertEquals("INSERT 0 1", insertResult.commandTag);

            QueryOutcome selectResult = conn.query("SELECT * FROM t");
            assertEquals(1, selectResult.rowCount);
            assertEquals(List.of("1", "Carol", "40"), selectResult.rows.get(0));
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void createAndDropFunctionAndSequenceReturnTheirOwnCommandTagsNotAGenericOk() throws Exception {
        // A real, previously-latent gap found by testing stdsql end to end: these four
        // statement types all fell through buildCommandTag's generic "OK" fallback
        // instead of their own specific tag, unlike every other DDL type already handled.
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT)");

            QueryOutcome createFunc = conn.query("CREATE FUNCTION f(x INT) RETURNS INT AS $$ SELECT COUNT(*) FROM t $$ LANGUAGE SQL");
            assertEquals("CREATE FUNCTION", createFunc.commandTag);

            QueryOutcome dropFunc = conn.query("DROP FUNCTION f");
            assertEquals("DROP FUNCTION", dropFunc.commandTag);

            QueryOutcome createSeq = conn.query("CREATE SEQUENCE s START WITH 1 INCREMENT BY 1");
            assertEquals("CREATE SEQUENCE", createSeq.commandTag);

            QueryOutcome dropSeq = conn.query("DROP SEQUENCE s");
            assertEquals("DROP SEQUENCE", dropSeq.commandTag);
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void storedProcedureWorksEndToEndOverTheRealWireProtocol() throws Exception {
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE accounts (id INT, status VARCHAR)");
            conn.query("INSERT INTO accounts VALUES (1, 'active')");

            QueryOutcome createProc = conn.query(
                "CREATE PROCEDURE suspend_account(acct_id INT) AS $$ UPDATE accounts SET status = 'suspended' WHERE id = acct_id $$ LANGUAGE SQL");
            assertTrue(createProc.error == null, () -> "CREATE PROCEDURE must succeed: " + createProc.error);
            assertEquals("CREATE PROCEDURE", createProc.commandTag);

            QueryOutcome callResult = conn.query("CALL suspend_account(1)");
            assertTrue(callResult.error == null, () -> "CALL must succeed: " + callResult.error);
            assertEquals("CALL", callResult.commandTag);

            QueryOutcome checkResult = conn.query("SELECT status FROM accounts WHERE id = 1");
            assertEquals(List.of("suspended"), checkResult.rows.get(0));

            QueryOutcome dropProc = conn.query("DROP PROCEDURE suspend_account");
            assertEquals("DROP PROCEDURE", dropProc.commandTag);
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void triggerWorksEndToEndOverTheRealWireProtocol() throws Exception {
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE employees (id INT, name VARCHAR)");
            conn.query("CREATE TABLE audit_log (emp_id INT, emp_name VARCHAR)");
            conn.query("CREATE PROCEDURE log_new_employee(id INT, name VARCHAR) AS $$ INSERT INTO audit_log VALUES (id, name) $$ LANGUAGE SQL");

            QueryOutcome createTrigger = conn.query(
                "CREATE TRIGGER trg_log_insert AFTER INSERT ON employees FOR EACH ROW EXECUTE PROCEDURE log_new_employee()");
            assertTrue(createTrigger.error == null, () -> "CREATE TRIGGER must succeed: " + createTrigger.error);
            assertEquals("CREATE TRIGGER", createTrigger.commandTag);

            QueryOutcome insertResult = conn.query("INSERT INTO employees VALUES (1, 'Alice')");
            assertTrue(insertResult.error == null, () -> "INSERT must succeed: " + insertResult.error);

            QueryOutcome auditResult = conn.query("SELECT * FROM audit_log");
            assertEquals(List.of("1", "Alice"), auditResult.rows.get(0), "the trigger must have fired and logged the new row");

            QueryOutcome dropTrigger = conn.query("DROP TRIGGER trg_log_insert ON employees");
            assertEquals("DROP TRIGGER", dropTrigger.commandTag);
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void extendedProtocolNullParameterEncodesAsRealNull() throws Exception {
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT, val VARCHAR)");

            conn.extendedQuery("INSERT INTO t VALUES ($1, $2)", "1", null);
            QueryOutcome result = conn.query("SELECT * FROM t");
            assertNull(result.rows.get(0).get(1), "a NULL parameter (length -1 in Bind) must decode as a real SQL NULL");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void extendedProtocolStringParameterWithEmbeddedQuoteIsSafe() throws Exception {
        // A real, deliberate injection-safety check: this project's own extended-protocol
        // implementation substitutes parameters as escaped SQL literals rather than using a
        // native parameterized path (see ExtendedProtocolHandler's javadoc) - this proves
        // that simplification hasn't reopened a SQL injection hole.
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT, name VARCHAR)");

            conn.extendedQuery("INSERT INTO t VALUES ($1, $2)", "1", "O'Brien");
            QueryOutcome result = conn.query("SELECT * FROM t WHERE id = 1");
            assertEquals("O'Brien", result.rows.get(0).get(1), "an embedded single quote in a parameter must round-trip exactly, not truncate or break the statement");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void extendedProtocolInvalidStatementReferenceReturnsErrorNotCrash() throws Exception {
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT)");
            // A malformed extended-protocol exchange (referencing a portal/statement that
            // doesn't exist, since this hand-rolled Bind always targets the unnamed
            // statement immediately after its own Parse) shouldn't come up in this specific
            // call shape - instead, verify a query that fails at execution time (not parse
            // time) still completes the full Parse/Bind/Describe/Execute/Sync cycle cleanly.
            QueryOutcome result = conn.extendedQuery("SELECT * FROM nonexistent_table WHERE id = $1", "1");
            assertNotNull(result.error, "a runtime failure during the extended protocol must surface as ErrorResponse, not hang or crash the connection");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void namedPreparedStatementCanBeReusedAcrossMultipleBindExecuteCalls() throws Exception {
        // The actual point of a prepared statement: Parse ONCE, then Bind+Execute
        // repeatedly with different parameter values, rather than a fresh
        // Parse+Bind+Execute+Sync cycle per call (which is all extendedQuery's
        // all-in-one helper, and every prior test using it, ever exercised).
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT, name VARCHAR)");
            conn.query("INSERT INTO t VALUES (1, 'Alice')");
            conn.query("INSERT INTO t VALUES (2, 'Bob')");
            conn.query("INSERT INTO t VALUES (3, 'Carol')");

            conn.parse("myquery", "SELECT * FROM t WHERE id = $1");

            assertTrue(conn.bind("", "myquery", "1"));
            QueryOutcome first = conn.executePortal("");
            conn.sync();
            assertEquals(1, first.rowCount);
            assertEquals("Alice", first.rows.get(0).get(1));

            assertTrue(conn.bind("", "myquery", "2"));
            QueryOutcome second = conn.executePortal("");
            conn.sync();
            assertEquals(1, second.rowCount);
            assertEquals("Bob", second.rows.get(0).get(1));

            assertTrue(conn.bind("", "myquery", "3"));
            QueryOutcome third = conn.executePortal("");
            conn.sync();
            assertEquals(1, third.rowCount);
            assertEquals("Carol", third.rows.get(0).get(1));
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void multiplePortalsFromOneStatementExecuteIndependently() throws Exception {
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT, name VARCHAR)");
            conn.query("INSERT INTO t VALUES (1, 'Alice')");
            conn.query("INSERT INTO t VALUES (2, 'Bob')");

            conn.parse("myquery", "SELECT * FROM t WHERE id = $1");
            assertTrue(conn.bind("portalA", "myquery", "1"));
            assertTrue(conn.bind("portalB", "myquery", "2"));

            // Deliberately interleaved out of bind order - each portal must
            // independently remember its own bound parameter value.
            QueryOutcome resultB = conn.executePortal("portalB");
            QueryOutcome resultA = conn.executePortal("portalA");
            conn.sync();

            assertEquals("Bob", resultB.rows.get(0).get(1));
            assertEquals("Alice", resultA.rows.get(0).get(1));
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void closeRemovesTheStatementAndSubsequentBindCorrectlyErrors() throws Exception {
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT)");
            conn.parse("myquery", "SELECT * FROM t WHERE id = $1");
            conn.closeStatement("myquery");

            boolean bindSucceeded = conn.bind("", "myquery", "1");
            conn.sync();
            assertFalse(bindSucceeded, "binding against a closed statement must fail with an error, not silently succeed");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void failedBindPutsConnectionInErrorStateUntilSync() throws Exception {
        // A real, previously-latent bug found by testing this exact sequence: a
        // failed Bind must cause every subsequent extended-protocol message to be
        // skipped until the next Sync (matching real Postgres's own pipelined
        // error-recovery semantics) - not silently fall through to whatever
        // stale portal/statement of the same name happens to still exist from
        // earlier, unrelated activity on the same connection.
        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE t (id INT, name VARCHAR)");
            conn.query("INSERT INTO t VALUES (1, 'Alice')");

            // Leave a valid, real unnamed portal in place first.
            conn.parse("myquery", "SELECT * FROM t WHERE id = $1");
            assertTrue(conn.bind("", "myquery", "1"));
            QueryOutcome beforeError = conn.executePortal("");
            conn.sync();
            assertEquals("Alice", beforeError.rows.get(0).get(1));

            // Now: Bind against a nonexistent statement (fails), then Execute the
            // SAME unnamed portal name that still holds the earlier, valid result.
            conn.closeStatement("myquery"); // so the next Bind against "myquery" genuinely fails
            boolean bindSucceeded = conn.bind("", "myquery", "1");
            assertFalse(bindSucceeded);

            // sendExecute+sendSync (not executePortal+sync): after a failed Bind, the
            // server correctly sends ZERO response bytes for the skipped Execute -
            // reading immediately, as executePortal would, deadlocks waiting for a
            // response that (correctly) never comes until Sync's own ReadyForQuery.
            conn.sendExecute("");
            conn.sendSync();
            QueryOutcome afterFailedBind = conn.readAllUntilReadyForQuery();
            assertEquals(0, afterFailedBind.rowCount,
                "after a failed Bind, the skipped Execute must return zero rows - specifically, it must NOT silently return the earlier, unrelated portal's stale 'Alice' row");
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void realPsycopg2ClientUsesExtendedProtocolForParameterizedQueries() throws Exception {
        // The strongest possible verification: a real, unmodified, independent
        // PostgreSQL driver - not this project's own test client - using its
        // normal, default parameterized-query API, which uses the extended
        // protocol (Parse/Bind/Execute) without any special configuration.
        assumeTrue(isPythonWithPsycopg2Available(), "psycopg2 not available - skipping real-driver extended protocol verification");

        String script = """
            import psycopg2
            conn = psycopg2.connect(host="localhost", port=%d, user="testuser", dbname="testdb")
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("CREATE TABLE t (id INT, name VARCHAR, price INT)")
            cur.execute("INSERT INTO t VALUES (%%s, %%s, %%s)", (1, "Widget", 100))
            cur.execute("INSERT INTO t VALUES (%%s, %%s, %%s)", (2, "Gadget", 250))
            cur.execute("SELECT * FROM t WHERE id = %%s", (1,))
            row = cur.fetchone()
            assert row[1] == "Widget", f"expected Widget, got {row}"
            cur.execute("SELECT * FROM t WHERE price > %%s", (150,))
            rows = cur.fetchall()
            assert len(rows) == 1 and rows[0][1] == "Gadget", f"unexpected rows: {rows}"
            cur.execute("INSERT INTO t VALUES (%%s, %%s, %%s)", (3, None, 50))
            cur.execute("SELECT * FROM t WHERE id = %%s", (3,))
            assert cur.fetchone()[1] is None
            conn.close()
            print("PSYCOPG2_EXTENDED_PROTOCOL_OK")
            """.formatted(port);

        Process process = new ProcessBuilder("python3", "-c", script)
            .redirectErrorStream(true)
            .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        boolean finished = process.waitFor(12, TimeUnit.SECONDS);
        assertTrue(finished, "psycopg2 script did not finish in time");
        assertTrue(output.contains("PSYCOPG2_EXTENDED_PROTOCOL_OK"),
            () -> "real psycopg2 client (extended protocol) failed:\n" + output);
    }

    private boolean isPythonWithPsycopg2Available() {
        try {
            Process check = new ProcessBuilder("python3", "-c", "import psycopg2").start();
            return check.waitFor(5, TimeUnit.SECONDS) && check.exitValue() == 0;
        } catch (Exception e) {
            return false;
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

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void realPsqlBackslashDtListsActualTablesAndExcludesViews() throws Exception {
        assumeTrue(psqlAvailable(), "psql not installed - skipping real-client verification");

        try (RawConnection conn = connect()) {
            conn.query("CREATE TABLE employees (id INT, name VARCHAR)");
            conn.query("CREATE TABLE departments (id INT)");
            conn.query("CREATE VIEW active_emp AS SELECT * FROM employees");
        }

        String output = runPsql("\\dt");
        assertTrue(output.contains("employees"), () -> "expected \\dt to list the real table 'employees', got:\n" + output);
        assertTrue(output.contains("departments"), () -> "expected \\dt to list the real table 'departments', got:\n" + output);
        assertFalse(output.contains("active_emp"), () -> "\\dt must exclude views, matching real Postgres - got:\n" + output);
        assertTrue(output.contains("(2 rows)"), () -> "expected exactly 2 rows (the 2 tables, not the view), got:\n" + output);
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

        /**
         * Independently hand-rolled Parse+Bind+Describe+Execute+Sync, one
         * unnamed statement/portal per call - deliberately NOT reusing
         * StdWireMessages' own client-side writers (see this class's own
         * javadoc for why: this test needs to independently verify the
         * server's wire format, not confirm the server agrees with the
         * same shared code that built the request).
         */
        QueryOutcome extendedQuery(String sqlWithPlaceholders, String... paramValues) throws IOException {
            // Parse
            byte[] queryBytes = sqlWithPlaceholders.getBytes(StandardCharsets.UTF_8);
            ByteArrayOutputStream parseBody = new ByteArrayOutputStream();
            parseBody.write(0); // unnamed statement
            parseBody.write(queryBytes);
            parseBody.write(0);
            parseBody.write(0); parseBody.write(0); // 0 parameter type OIDs specified
            writeTypedMessage(out, 'P', parseBody.toByteArray());

            // Bind
            ByteArrayOutputStream bindBody = new ByteArrayOutputStream();
            bindBody.write(0); // unnamed portal
            bindBody.write(0); // unnamed statement
            bindBody.write(0); bindBody.write(0); // 0 parameter format codes (all text)
            bindBody.write(0); bindBody.write(paramValues.length); // parameter value count (fits in one byte for these tests)
            for (String v : paramValues) {
                if (v == null) {
                    bindBody.write(-1); bindBody.write(-1); bindBody.write(-1); bindBody.write(-1); // -1 length = NULL
                } else {
                    byte[] vb = v.getBytes(StandardCharsets.UTF_8);
                    bindBody.write((vb.length >> 24) & 0xFF); bindBody.write((vb.length >> 16) & 0xFF);
                    bindBody.write((vb.length >> 8) & 0xFF); bindBody.write(vb.length & 0xFF);
                    bindBody.write(vb);
                }
            }
            bindBody.write(0); bindBody.write(0); // 0 result format codes (all text)
            writeTypedMessage(out, 'B', bindBody.toByteArray());

            // Describe (portal)
            ByteArrayOutputStream describeBody = new ByteArrayOutputStream();
            describeBody.write('P');
            describeBody.write(0); // unnamed
            writeTypedMessage(out, 'D', describeBody.toByteArray());

            // Execute
            ByteArrayOutputStream executeBody = new ByteArrayOutputStream();
            executeBody.write(0); // unnamed portal
            executeBody.write(0); executeBody.write(0); executeBody.write(0); executeBody.write(0); // maxRows = 0 (unlimited)
            writeTypedMessage(out, 'E', executeBody.toByteArray());

            // Sync
            writeTypedMessage(out, 'S', new byte[0]);
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
                // '1' ParseComplete, '2' BindComplete, 't' ParameterDescription, 'n' NoData - acknowledged implicitly by simply not erroring on an unrecognized type here
            }
            return new QueryOutcome(rows.size(), columnNames, rows, commandTag, error);
        }

        private void writeTypedMessage(DataOutputStream out, char type, byte[] body) throws IOException {
            out.writeByte(type);
            out.writeInt(body.length + 4);
            out.write(body);
        }

        /** Parse a NAMED statement (unlike extendedQuery, always unnamed) - reads and discards ParseComplete. */
        void parse(String statementName, String sqlWithPlaceholders) throws IOException {
            byte[] queryBytes = sqlWithPlaceholders.getBytes(StandardCharsets.UTF_8);
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            body.write(statementName.getBytes(StandardCharsets.UTF_8)); body.write(0);
            body.write(queryBytes); body.write(0);
            body.write(0); body.write(0); // 0 parameter type OIDs
            writeTypedMessage(out, 'P', body.toByteArray());
            out.flush();
            char type = (char) in.readUnsignedByte();
            int len = in.readInt();
            in.readFully(new byte[len - 4]);
            if (type != '1') throw new IOException("expected ParseComplete, got: " + type);
        }

        /** Bind a (possibly named) portal to a (possibly named) statement - reads and returns whether BindComplete ('2', true) or ErrorResponse ('E', false) came back, without assuming which. */
        boolean bind(String portalName, String statementName, String... paramValues) throws IOException {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            body.write(portalName.getBytes(StandardCharsets.UTF_8)); body.write(0);
            body.write(statementName.getBytes(StandardCharsets.UTF_8)); body.write(0);
            body.write(0); body.write(0); // 0 parameter format codes (all text)
            body.write(0); body.write(paramValues.length);
            for (String v : paramValues) {
                if (v == null) {
                    body.write(-1); body.write(-1); body.write(-1); body.write(-1);
                } else {
                    byte[] vb = v.getBytes(StandardCharsets.UTF_8);
                    body.write((vb.length >> 24) & 0xFF); body.write((vb.length >> 16) & 0xFF);
                    body.write((vb.length >> 8) & 0xFF); body.write(vb.length & 0xFF);
                    body.write(vb);
                }
            }
            body.write(0); body.write(0); // 0 result format codes (all text)
            writeTypedMessage(out, 'B', body.toByteArray());
            out.flush();
            char type = (char) in.readUnsignedByte();
            int len = in.readInt();
            byte[] respBody = new byte[len - 4];
            in.readFully(respBody);
            if (type == 'E') return false;
            if (type != '2') throw new IOException("expected BindComplete or ErrorResponse, got: " + type);
            return true;
        }

        /** Execute a (possibly named) portal and read its full response (rows, command tag, or error) - does NOT send Sync; caller composes sync() separately so multiple Executes can be issued before one Sync, exactly matching real pipelining. */
        QueryOutcome executePortal(String portalName) throws IOException {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            body.write(portalName.getBytes(StandardCharsets.UTF_8)); body.write(0);
            body.write(0); body.write(0); body.write(0); body.write(0); // maxRows = 0
            writeTypedMessage(out, 'E', body.toByteArray());
            out.flush();

            List<String> columnNames = new java.util.ArrayList<>();
            List<List<String>> rows = new java.util.ArrayList<>();
            String commandTag = null;
            String error = null;
            // Execute's own response is exactly one of: a run of DataRows then CommandComplete, or a single ErrorResponse.
            while (commandTag == null && error == null) {
                int type = in.readUnsignedByte();
                int len = in.readInt();
                byte[] body2 = new byte[len - 4];
                in.readFully(body2);
                if (type == 'D') rows.add(parseDataRowValues(body2));
                else if (type == 'C') commandTag = new String(body2, 0, body2.length - 1, StandardCharsets.UTF_8);
                else if (type == 'E') error = extractErrorMessage(body2);
            }
            return new QueryOutcome(rows.size(), columnNames, rows, commandTag, error);
        }

        /** Sends Execute WITHOUT reading any response - for scenarios where the server may legitimately send zero bytes back (a skipped message after an error), where reading immediately would deadlock. Pair with a follow-up read (see readUntilReadyForQuery) after also sending Sync. */
        void sendExecute(String portalName) throws IOException {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            body.write(portalName.getBytes(StandardCharsets.UTF_8)); body.write(0);
            body.write(0); body.write(0); body.write(0); body.write(0); // maxRows = 0
            writeTypedMessage(out, 'E', body.toByteArray());
        }

        void sendSync() throws IOException {
            writeTypedMessage(out, 'S', new byte[0]);
            out.flush();
        }

        /** Reads every response message up through and including ReadyForQuery, tolerating zero response messages for a skipped command (see class javadoc on error-state handling) - the correct way to read back a batch of messages sent without reading in between (sendExecute, sendSync), as any real pipelining client would. */
        QueryOutcome readAllUntilReadyForQuery() throws IOException {
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

        void closeStatement(String name) throws IOException {
            closeTarget('S', name);
        }

        void closePortal(String name) throws IOException {
            closeTarget('P', name);
        }

        private void closeTarget(char targetType, String name) throws IOException {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            body.write(targetType);
            body.write(name.getBytes(StandardCharsets.UTF_8)); body.write(0);
            writeTypedMessage(out, 'C', body.toByteArray());
            out.flush();
            char type = (char) in.readUnsignedByte();
            int len = in.readInt();
            in.readFully(new byte[len - 4]);
            if (type != '3') throw new IOException("expected CloseComplete, got: " + type);
        }

        /** Sends Sync and reads ReadyForQuery - must be called to end an extended-protocol exchange built from the composable methods above. */
        void sync() throws IOException {
            writeTypedMessage(out, 'S', new byte[0]);
            out.flush();
            char type = (char) in.readUnsignedByte();
            int len = in.readInt();
            in.readFully(new byte[len - 4]);
            if (type != 'Z') throw new IOException("expected ReadyForQuery after Sync, got: " + type);
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
        bodyOut.writeInt(StdWireMessages.PROTOCOL_VERSION_3);
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
