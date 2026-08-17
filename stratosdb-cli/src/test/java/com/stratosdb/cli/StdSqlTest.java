package com.stratosdb.cli;

import com.stratosdb.common.constants.ProtocolConstants;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * StdSql: StratosDB's own native stdwire protocol client - a real
 * implementation, not a wrapper around the JDBC driver. Tested here by
 * feeding it scripted stdin input and capturing stdout, driven against a
 * real, in-process StdWireServer (the same server real psql/psycopg2 are
 * verified against in stratosdb-network's own StdWireServerTest).
 */
class StdSqlTest {

    @TempDir
    Path tempDir;

    private StratosDB db;
    private StdWireServer server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        port = findFreePort();
        server = new StdWireServer(port, db);
        server.start();
        Thread.sleep(200); // let the accept thread actually start listening
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    private int findFreePort() throws Exception {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** Runs StdSql against the real test server with the given scripted stdin lines, returning everything it printed to stdout. */
    private String runStdSql(String... inputLines) throws Exception {
        String scriptedInput = String.join("\n", inputLines) + "\n";
        InputStream originalIn = System.in;
        PrintStream originalOut = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try {
            System.setIn(new ByteArrayInputStream(scriptedInput.getBytes(StandardCharsets.UTF_8)));
            System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
            new StdSql("localhost", port, "testuser", "testdb").start();
        } finally {
            System.setIn(originalIn);
            System.setOut(originalOut);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void connectsAndRunsSimpleQueries() throws Exception {
        String output = runStdSql(
            "CREATE TABLE t (id INT, name VARCHAR)",
            "INSERT INTO t VALUES (1, 'Alice')",
            "SELECT * FROM t",
            "\\q"
        );

        assertTrue(output.contains("CREATE TABLE"), () -> "expected a CREATE TABLE command tag, got:\n" + output);
        assertTrue(output.contains("INSERT 0 1"), () -> "expected an INSERT command tag, got:\n" + output);
        assertTrue(output.contains("Alice"), () -> "expected the inserted row's data, got:\n" + output);
        assertTrue(output.contains("id | name"), () -> "expected a column header row, got:\n" + output);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void reportsErrorsWithoutCrashingTheSession() throws Exception {
        String output = runStdSql(
            "SELECT * FROM nonexistent_table",
            "CREATE TABLE t (id INT)",
            "\\q"
        );

        assertTrue(output.contains("ERROR:"), () -> "expected an ERROR: line for the failed statement, got:\n" + output);
        assertTrue(output.contains("CREATE TABLE"), "the session must continue working after an error, not hang or disconnect");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void bindCommandDrivesTheExtendedQueryProtocol() throws Exception {
        String output = runStdSql(
            "CREATE TABLE t (id INT, name VARCHAR)",
            "INSERT INTO t VALUES (1, 'Alice')",
            "INSERT INTO t VALUES (2, 'Bob')",
            "\\bind SELECT * FROM t WHERE id = $1 | 2",
            "\\q"
        );

        assertTrue(output.contains("ParseComplete"), () -> "expected ParseComplete from the extended protocol exchange, got:\n" + output);
        assertTrue(output.contains("BindComplete"), () -> "expected BindComplete, got:\n" + output);
        assertTrue(output.contains("Bob"), () -> "expected the parameterized query to return Bob's row, got:\n" + output);
        assertFalse(output.contains("Alice"), "the parameterized WHERE id = 2 must not also return Alice's row");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void bindCommandHandlesNullParameter() throws Exception {
        String output = runStdSql(
            "CREATE TABLE t (id INT, val VARCHAR)",
            "\\bind INSERT INTO t VALUES ($1, $2) | 1 | ",
            "SELECT * FROM t",
            "\\q"
        );

        assertTrue(output.contains("NULL"), () -> "expected the NULL parameter to display as NULL in the subsequent SELECT, got:\n" + output);
    }
}
