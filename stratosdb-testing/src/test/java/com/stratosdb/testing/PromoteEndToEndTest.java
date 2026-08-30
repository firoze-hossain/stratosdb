package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.replication.ReplicationClient;
import com.stratosdb.network.replication.ReplicationServer;
import com.stratosdb.network.stdwire.StdWireMessages;
import com.stratosdb.network.stdwire.StdWireServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, end-to-end proof that PROMOTE actually works - a real primary,
 * a real replica (real ReplicationClient, real enforced read-only
 * mode), and a real PROMOTE sent over a real connection, not a
 * simulation of any of the three. See StratosHaEndToEndTest for the
 * full, real, automatic failover this is the foundation of.
 */
public class PromoteEndToEndTest {

    private StratosDB primary;
    private StratosDB replica;
    private ReplicationServer replicationServer;
    private ReplicationClient replicationClient;
    private StdWireServer replicaWireServer;

    @AfterEach
    void tearDown() {
        if (replicaWireServer != null) replicaWireServer.stop();
        if (replicationClient != null) replicationClient.stop();
        if (replicationServer != null) replicationServer.stop();
        if (primary != null) primary.shutdown();
        if (replica != null) replica.shutdown();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void promoteStopsReplicationAndEnablesWritesOverARealConnection(@TempDir Path tempDir) throws Exception {
        int replicationPort = freePort();
        DatabaseConfig primaryConfig = new DatabaseConfig();
        primaryConfig.setDataDirectory(tempDir.resolve("primary").toString());
        primary = new StratosDB(primaryConfig);
        replicationServer = new ReplicationServer(replicationPort, primary.getWalManager(), 50);
        replicationServer.start();
        Thread.sleep(200);
        primary.execute("CREATE TABLE t (id INT, name VARCHAR)");
        primary.execute("INSERT INTO t VALUES (1, 'Alice')");

        DatabaseConfig replicaConfig = new DatabaseConfig();
        replicaConfig.setDataDirectory(tempDir.resolve("replica").toString());
        replica = new StratosDB(replicaConfig);
        // Schema/catalog isn't replicated at the SQL layer (a real, separate,
        // already-documented limitation - see ReplicationEndToEndTest) - the
        // replica's own SQL catalog independently needs to know about this table,
        // matching a real setup starting from a base backup of the primary's own
        // catalog.
        replica.execute("CREATE TABLE t (id INT, name VARCHAR)");
        replica.setReadOnly(true);

        replicationClient = new ReplicationClient("localhost", replicationPort, replica.getDiskManager(), replica.getBufferPool());
        replicationClient.start();

        int replicaWirePort = freePort();
        replicaWireServer = new StdWireServer(replicaWirePort, replica);
        replicaWireServer.setReplicationClient(replicationClient);
        replicaWireServer.start();
        Thread.sleep(200);

        long deadline = System.currentTimeMillis() + 10_000;
        while (!replicationClient.isConnected() && System.currentTimeMillis() < deadline) Thread.sleep(50);
        assertTrue(replicationClient.isConnected(), "replica must connect to the primary");

        assertFalse(replica.execute("INSERT INTO t VALUES (2, 'Bob')").isSuccess(), "writes must be rejected on the replica before PROMOTE");
        assertTrue(replica.execute("SELECT * FROM t").isSuccess(), "reads must still work on the read-only replica before PROMOTE");

        RawClient client = new RawClient("localhost", replicaWirePort);
        String promoteError = client.sendQuery("PROMOTE");
        client.close();
        assertNull(promoteError, () -> "PROMOTE over a real connection must succeed: " + promoteError);

        assertFalse(replicationClient.needsResync(), "a normal PROMOTE must not leave the replication client in a resync-required state");
        assertFalse(replica.isReadOnly(), "the instance's own read-only flag must be disabled after PROMOTE");
        assertTrue(replica.execute("INSERT INTO t VALUES (2, 'Bob')").isSuccess(), "writes must succeed on the promoted instance after PROMOTE");

        RawClient secondClient = new RawClient("localhost", replicaWirePort);
        String secondPromoteError = secondClient.sendQuery("PROMOTE");
        secondClient.close();
        assertNotNull(secondPromoteError, "a second PROMOTE must correctly report there's nothing left to promote, not silently succeed again");
        assertTrue(secondPromoteError.contains("nothing to promote"), () -> "the error message should say why: " + secondPromoteError);
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** A minimal, real, trust-authenticated wire-protocol client for sending PROMOTE and reading its own real response. */
    private static class RawClient {
        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;

        RawClient(String host, int port) throws Exception {
            socket = new Socket(host, port);
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            StdWireMessages.writeStartupMessage(out, "anyuser", "anydb");
            out.flush();
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                if (msg.type() == 'Z') break;
            }
        }

        String sendQuery(String sql) throws Exception {
            StdWireMessages.writeQuery(out, sql);
            out.flush();
            String error = null;
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                if (msg.type() == 'E') {
                    error = extractError(msg);
                } else if (msg.type() == 'Z') {
                    return error;
                }
            }
        }

        private String extractError(StdWireMessages.TypedMessage msg) {
            byte[] b = msg.body();
            int pos = 0;
            while (pos < b.length && b[pos] != 0) {
                char field = (char) b[pos]; pos++;
                int start = pos;
                while (b[pos] != 0) pos++;
                String value = new String(b, start, pos - start, StandardCharsets.UTF_8);
                pos++;
                if (field == 'M') return value;
            }
            return "unknown";
        }

        void close() throws Exception {
            socket.close();
        }
    }
}
