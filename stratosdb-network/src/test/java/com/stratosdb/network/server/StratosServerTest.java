package com.stratosdb.network.server;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.protocol.WireProtocol;
import com.stratosdb.sql.executor.QueryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real client-server round trips over an actual TCP socket - no mocking of
 * the network layer. Finds a free port itself rather than hardcoding one,
 * so this doesn't flake if something else on the machine holds a fixed
 * port.
 */
class StratosServerTest {

    @TempDir
    Path tempDir;

    private StratosDB db;
    private StratosServer server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setPort(port);
        db = new StratosDB(config);
        server = new StratosServer(port, db);
        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void queryAndResultRoundTripOverARealSocket() throws Exception {
        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            assertTrue(sendAndReceive(out, in, "CREATE TABLE t (id INT, name VARCHAR)").isSuccess());
            assertTrue(sendAndReceive(out, in, "INSERT INTO t VALUES (1, 'Alice')").isSuccess());

            QueryResult selectResult = sendAndReceive(out, in, "SELECT * FROM t WHERE id=1");
            assertTrue(selectResult.isSuccess());
            assertEquals(1, selectResult.getRows().size());
            assertEquals("Alice", selectResult.getRows().get(0).getValue("name"));
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void serverErrorSurvivesTheRoundTripAsAFailedResult() throws Exception {
        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            QueryResult result = sendAndReceive(out, in, "SELECT * FROM nonexistent");
            assertFalse(result.isSuccess());
            assertEquals("Table not found: nonexistent", result.getError());
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void multipleConnectionsShareTheSameUnderlyingData() throws Exception {
        try (Socket a = new Socket("localhost", port)) {
            DataOutputStream outA = new DataOutputStream(new BufferedOutputStream(a.getOutputStream()));
            DataInputStream inA = new DataInputStream(new BufferedInputStream(a.getInputStream()));
            assertTrue(sendAndReceive(outA, inA, "CREATE TABLE shared (id INT)").isSuccess());
            assertTrue(sendAndReceive(outA, inA, "INSERT INTO shared VALUES (42)").isSuccess());
        }

        // A second, separate connection must see what the first committed -
        // this is one shared StratosDB instance behind the server, not one per connection.
        try (Socket b = new Socket("localhost", port)) {
            DataOutputStream outB = new DataOutputStream(new BufferedOutputStream(b.getOutputStream()));
            DataInputStream inB = new DataInputStream(new BufferedInputStream(b.getInputStream()));
            QueryResult result = sendAndReceive(outB, inB, "SELECT * FROM shared");
            assertTrue(result.isSuccess());
            assertEquals(1, result.getRows().size());
            assertEquals(42, result.getRows().get(0).getValue("id"));
        }
    }

    private QueryResult sendAndReceive(DataOutputStream out, DataInputStream in, String sql) throws Exception {
        WireProtocol.writeQuery(out, sql);
        int type = WireProtocol.readMessageType(in);
        assertEquals(WireProtocol.MSG_RESULT, type, "expected a RESULT message back");
        return WireProtocol.readResultBody(in);
    }
}
