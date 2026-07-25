package com.stratosdb.network.server;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.auth.UserStore;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real client-server round trips over an actual TCP socket - no mocking of
 * the network layer. Finds a free port itself rather than hardcoding one,
 * so this doesn't flake if something else on the machine holds a fixed
 * port.
 *
 * Every connection now performs the AUTH handshake as the wire protocol
 * requires (see WireProtocol) - even the tests against a server with no
 * UserStore configured send an (empty, anonymous) AUTH message first,
 * since the server always expects one regardless of whether it actually
 * checks anything.
 */
class StratosServerTest {

    @TempDir
    Path tempDir;

    private StratosDB db;
    private StratosServer server;
    private int port;
    private final List<StratosDB> extraDbs = new ArrayList<>();
    private final List<StratosServer> extraServers = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        port = freePort();
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
        for (StratosServer s : extraServers) s.stop();
        for (StratosDB d : extraDbs) d.shutdown();
    }

    private static int freePort() throws Exception {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void queryAndResultRoundTripOverARealSocket() throws Exception {
        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            authenticate(out, in, "", "");

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
            authenticate(out, in, "", "");

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
            authenticate(outA, inA, "", "");
            assertTrue(sendAndReceive(outA, inA, "CREATE TABLE shared (id INT)").isSuccess());
            assertTrue(sendAndReceive(outA, inA, "INSERT INTO shared VALUES (42)").isSuccess());
        }

        // A second, separate connection must see what the first committed -
        // this is one shared StratosDB instance behind the server, not one per connection.
        try (Socket b = new Socket("localhost", port)) {
            DataOutputStream outB = new DataOutputStream(new BufferedOutputStream(b.getOutputStream()));
            DataInputStream inB = new DataInputStream(new BufferedInputStream(b.getInputStream()));
            authenticate(outB, inB, "", "");
            QueryResult result = sendAndReceive(outB, inB, "SELECT * FROM shared");
            assertTrue(result.isSuccess());
            assertEquals(1, result.getRows().size());
            assertEquals(42, result.getRows().get(0).getValue("id"));
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void serverWithNoUserStoreAcceptsAnyCredentials() throws Exception {
        // Already exercised implicitly above (authenticate() sends empty
        // credentials against the no-auth server in @BeforeEach), but this
        // makes the "open access when unconfigured" contract an explicit,
        // named assertion rather than an incidental side effect of other tests.
        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            WireProtocol.writeAuth(out, "anyone", "anything-at-all");
            assertEquals(WireProtocol.MSG_AUTH_RESULT, WireProtocol.readMessageType(in));
            WireProtocol.AuthResult result = WireProtocol.readAuthResultBody(in);
            assertTrue(result.success(), "a server with no UserStore must accept any credentials");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void serverWithUserStoreRejectsWrongCredentials() throws Exception {
        StratosServer authServer = startServerWithAuth(store -> store.addUser("alice", "correct-password"));
        try (Socket socket = new Socket("localhost", authServer.getPort())) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            WireProtocol.writeAuth(out, "alice", "wrong-password");
            assertEquals(WireProtocol.MSG_AUTH_RESULT, WireProtocol.readMessageType(in));
            WireProtocol.AuthResult result = WireProtocol.readAuthResultBody(in);
            assertFalse(result.success());
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void serverWithUserStoreAcceptsCorrectCredentialsAndThenAllowsQueries() throws Exception {
        StratosServer authServer = startServerWithAuth(store -> store.addUser("alice", "correct-password"));
        try (Socket socket = new Socket("localhost", authServer.getPort())) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            authenticate(out, in, "alice", "correct-password");

            QueryResult result = sendAndReceive(out, in, "SHOW TABLES");
            assertTrue(result.isSuccess(), "an authenticated connection must be able to run queries");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void serverWithUserStoreRejectsAConnectionThatSkipsAuth() throws Exception {
        StratosServer authServer = startServerWithAuth(store -> store.addUser("alice", "correct-password"));
        try (Socket socket = new Socket("localhost", authServer.getPort())) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            // Sends a QUERY where the server expects AUTH first - the server must not
            // treat this as an implicit "no auth needed" and just answer it.
            WireProtocol.writeQuery(out, "SHOW TABLES");
            // The server closes the connection without responding rather than answering -
            // reading past that point should hit end-of-stream, not a valid RESULT message.
            assertThrows(java.io.EOFException.class, () -> WireProtocol.readMessageType(in));
        }
    }

    private StratosServer startServerWithAuth(java.util.function.Consumer<UserStore> configure) throws Exception {
        int p = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.resolve("auth-" + p).toString());
        config.setPort(p);
        StratosDB extraDb = new StratosDB(config);
        UserStore userStore = new UserStore();
        configure.accept(userStore);
        StratosServer authServer = new StratosServer(p, extraDb, userStore, null);
        authServer.start();
        extraDbs.add(extraDb);
        extraServers.add(authServer);
        return authServer;
    }

    private void authenticate(DataOutputStream out, DataInputStream in, String username, String password) throws Exception {
        WireProtocol.writeAuth(out, username, password);
        assertEquals(WireProtocol.MSG_AUTH_RESULT, WireProtocol.readMessageType(in));
        WireProtocol.AuthResult result = WireProtocol.readAuthResultBody(in);
        assertTrue(result.success(), "authentication should have succeeded: " + result.message());
    }

    private QueryResult sendAndReceive(DataOutputStream out, DataInputStream in, String sql) throws Exception {
        WireProtocol.writeQuery(out, sql);
        int type = WireProtocol.readMessageType(in);
        assertEquals(WireProtocol.MSG_RESULT, type, "expected a RESULT message back");
        return WireProtocol.readResultBody(in);
    }
}
