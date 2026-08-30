package com.stratosdb.testing;

import com.stratosdb.cli.StratosHa;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.replication.ReplicationClient;
import com.stratosdb.network.replication.ReplicationServer;
import com.stratosdb.network.stdwire.StdWireServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, end-to-end proof that automatic failover actually works - a
 * real primary, a real replica, and a real StratosHa watchdog, all real
 * objects with real sockets between them (see StratosHa's own javadoc
 * for its honestly-stated scope: a single watchdog process, not real
 * distributed consensus). The primary is genuinely shut down mid-test,
 * not simulated as "unreachable" some other way, and the test then
 * waits for the watchdog's own real health-check loop to notice and
 * react - the same real timing a real deployment would experience.
 */
public class StratosHaEndToEndTest {

    private StratosDB primary;
    private StratosDB replica;
    private ReplicationServer replicationServer;
    private ReplicationClient replicationClient;
    private StdWireServer primaryWireServer;
    private StdWireServer replicaWireServer;
    private StratosHa ha;

    @AfterEach
    void tearDown() {
        if (ha != null) ha.stop();
        if (replicaWireServer != null) replicaWireServer.stop();
        if (replicationClient != null) replicationClient.stop();
        if (replicationServer != null) replicationServer.stop();
        if (primaryWireServer != null) primaryWireServer.stop();
        if (primary != null) {
            try {
                primary.shutdown();
            } catch (Exception ignored) {
                // already killed mid-test in the test itself - a second shutdown attempt is expected to be a no-op or fail harmlessly
            }
        }
        if (replica != null) replica.shutdown();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void watchdogAutomaticallyPromotesAReplicaWhenTheRealPrimaryGoesDown(@TempDir Path tempDir) throws Exception {
        int primaryWirePort = freePort();
        int replicationPort = freePort();
        DatabaseConfig primaryConfig = new DatabaseConfig();
        primaryConfig.setDataDirectory(tempDir.resolve("primary").toString());
        primary = new StratosDB(primaryConfig);
        primaryWireServer = new StdWireServer(primaryWirePort, primary);
        primaryWireServer.start();
        replicationServer = new ReplicationServer(replicationPort, primary.getWalManager(), 50);
        replicationServer.start();
        Thread.sleep(200);
        primary.execute("CREATE TABLE t (id INT)");
        primary.execute("INSERT INTO t VALUES (1)");

        DatabaseConfig replicaConfig = new DatabaseConfig();
        replicaConfig.setDataDirectory(tempDir.resolve("replica").toString());
        replica = new StratosDB(replicaConfig);
        replica.execute("CREATE TABLE t (id INT)"); // schema independently known, same reasoning as PromoteEndToEndTest
        replica.setReadOnly(true);
        replicationClient = new ReplicationClient("localhost", replicationPort, replica.getDiskManager(), replica.getBufferPool());
        replicationClient.start();
        int replicaWirePort = freePort();
        replicaWireServer = new StdWireServer(replicaWirePort, replica);
        replicaWireServer.setReplicationClient(replicationClient);
        replicaWireServer.start();

        long deadline = System.currentTimeMillis() + 10_000;
        while (!replicationClient.isConnected() && System.currentTimeMillis() < deadline) Thread.sleep(50);
        assertTrue(replicationClient.isConnected());
        assertTrue(replica.isReadOnly(), "the replica must still be read-only before any failover happens");

        StratosHa.NodeConfig primaryNode = new StratosHa.NodeConfig("localhost", primaryWirePort, "anyuser", null, "anydb");
        StratosHa.NodeConfig replicaNode = new StratosHa.NodeConfig("localhost", replicaWirePort, "anyuser", null, "anydb");
        ha = new StratosHa(primaryNode, List.of(replicaNode), 300, 3);
        ha.start();

        // A real failure, not a simulated one: stop the primary's own wire server
        // and replication server entirely - the watchdog's own real health checks
        // against primaryWirePort will now genuinely fail to connect.
        primaryWireServer.stop();
        replicationServer.stop();
        primary.shutdown();
        primary = null; // tearDown must not attempt a second shutdown on an already-shut-down instance

        long failoverDeadline = System.currentTimeMillis() + 20_000;
        while (!ha.hasTriggeredFailover() && System.currentTimeMillis() < failoverDeadline) {
            Thread.sleep(200);
        }
        assertTrue(ha.hasTriggeredFailover(), "the watchdog must have triggered failover after the primary genuinely became unreachable");

        // The watchdog's own PROMOTE is a real, separate connection - give the
        // replica's own StdWireServer a brief moment to have actually processed it
        // before asserting on the resulting state.
        deadline = System.currentTimeMillis() + 5_000;
        while (replica.isReadOnly() && System.currentTimeMillis() < deadline) Thread.sleep(100);

        assertFalse(replica.isReadOnly(), "the replica must no longer be read-only after the watchdog's own real PROMOTE");
        assertTrue(replica.execute("INSERT INTO t VALUES (2)").isSuccess(), "the newly-promoted instance must genuinely accept writes");
        var result = replica.execute("SELECT * FROM t");
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRows().size(), "both the pre-failover replicated row and the post-promotion write must both be present");
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
