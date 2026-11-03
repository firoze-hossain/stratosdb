package com.stratosdb.testing;

import com.stratosdb.cli.StratosHa;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.consensus.StdRaftCluster;
import com.stratosdb.network.consensus.StdRaftLeaseClient;
import com.stratosdb.network.consensus.StdRaftNode;
import com.stratosdb.network.consensus.StdRaftPeer;
import com.stratosdb.network.replication.ReplicationClient;
import com.stratosdb.network.replication.ReplicationServer;
import com.stratosdb.network.stdwire.StdWireServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests proving distributed consensus failover:
 * 1. Network partition eliminates dual primaries (split-brain) via Raft leader leases
 *    and self-fencing.
 * 2. Automatic failover succeeds in strictly under 3 seconds.
 * 3. Isolated minority watchdogs cannot achieve consensus and cannot trigger false failovers.
 */
public class DistributedFailoverEndToEndTest {

    private StratosDB primaryDb;
    private StratosDB replicaDb;
    private StdWireServer primaryWireServer;
    private StdWireServer replicaWireServer;
    private ReplicationServer replicationServer;
    private ReplicationClient replicationClient;
    private StdRaftLeaseClient leaseClient;
    private StdRaftCluster raftCluster;
    private final List<StratosHa> watchdogs = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (StratosHa ha : watchdogs) {
            try {
                ha.stop();
            } catch (Exception ignored) {}
        }
        watchdogs.clear();

        if (leaseClient != null) {
            try {
                leaseClient.close();
            } catch (Exception ignored) {}
        }
        if (raftCluster != null) {
            try {
                raftCluster.stop();
            } catch (Exception ignored) {}
        }
        if (replicaWireServer != null) {
            try {
                replicaWireServer.stop();
            } catch (Exception ignored) {}
        }
        if (replicationClient != null) {
            try {
                replicationClient.stop();
            } catch (Exception ignored) {}
        }
        if (replicationServer != null) {
            try {
                replicationServer.stop();
            } catch (Exception ignored) {}
        }
        if (primaryWireServer != null) {
            try {
                primaryWireServer.stop();
            } catch (Exception ignored) {}
        }
        if (primaryDb != null) {
            try {
                primaryDb.shutdown();
            } catch (Exception ignored) {}
        }
        if (replicaDb != null) {
            try {
                replicaDb.shutdown();
            } catch (Exception ignored) {}
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void networkPartitionDoesNotCauseDualPrimaries(@TempDir Path tempDir) throws Exception {
        int primaryWirePort = freePort();
        int replicaWirePort = freePort();
        int replicationPort = freePort();

        int raftPort1 = freePort();
        int raftPort2 = freePort();
        int raftPort3 = freePort();

        List<StdRaftPeer> peers = List.of(
            new StdRaftPeer("w1", "127.0.0.1", raftPort1),
            new StdRaftPeer("w2", "127.0.0.1", raftPort2),
            new StdRaftPeer("w3", "127.0.0.1", raftPort3)
        );

        StdRaftNode n1 = new StdRaftNode("w1", "127.0.0.1", raftPort1, peers, 150, 300, 50);
        StdRaftNode n2 = new StdRaftNode("w2", "127.0.0.1", raftPort2, peers, 150, 300, 50);
        StdRaftNode n3 = new StdRaftNode("w3", "127.0.0.1", raftPort3, peers, 150, 300, 50);

        raftCluster = new StdRaftCluster(List.of(n1, n2, n3));
        raftCluster.start();

        StdRaftNode initialLeader = raftCluster.waitForLeader(5000);
        assertNotNull(initialLeader, "A Raft consensus leader must be elected");

        // Primary initialization with consensus leader lease
        DatabaseConfig primaryConfig = new DatabaseConfig();
        primaryConfig.setDataDirectory(tempDir.resolve("primary").toString());
        primaryDb = new StratosDB(primaryConfig);
        primaryWireServer = new StdWireServer(primaryWirePort, primaryDb);
        primaryWireServer.start();

        replicationServer = new ReplicationServer(replicationPort, primaryDb.getWalManager(), 50);
        replicationServer.start();

        primaryDb.execute("CREATE TABLE products (id INT, name TEXT)");
        assertTrue(primaryDb.execute("INSERT INTO products VALUES (1, 'initial')").isSuccess());

        // Attach Raft lease client to primary
        leaseClient = new StdRaftLeaseClient(peers, "127.0.0.1:" + primaryWirePort, "primary", 1000, 200);
        leaseClient.start();
        assertTrue(leaseClient.hasValidLease(), "Primary must acquire initial consensus lease from Raft leader");
        primaryDb.setLeaseChecker(leaseClient::hasValidLease);

        // Replica initialization
        DatabaseConfig replicaConfig = new DatabaseConfig();
        replicaConfig.setDataDirectory(tempDir.resolve("replica").toString());
        replicaDb = new StratosDB(replicaConfig);
        replicaDb.execute("CREATE TABLE products (id INT, name TEXT)");
        replicaDb.setReadOnly(true);

        replicationClient = new ReplicationClient("localhost", replicationPort, replicaDb.getDiskManager(), replicaDb.getBufferPool());
        replicationClient.start();

        replicaWireServer = new StdWireServer(replicaWirePort, replicaDb);
        replicaWireServer.setReplicationClient(replicationClient);
        replicaWireServer.start();

        long connectDeadline = System.currentTimeMillis() + 5000;
        while (!replicationClient.isConnected() && System.currentTimeMillis() < connectDeadline) {
            Thread.sleep(50);
        }
        assertTrue(replicationClient.isConnected());

        // Setup 3 StratosHa watchdogs with Raft nodes
        StratosHa.NodeConfig primaryNode = new StratosHa.NodeConfig("127.0.0.1", primaryWirePort, "anyuser", null, "anydb");
        StratosHa.NodeConfig replicaNode = new StratosHa.NodeConfig("127.0.0.1", replicaWirePort, "anyuser", null, "anydb");

        StratosHa ha1 = new StratosHa(primaryNode, List.of(replicaNode), 150, 2, n1);
        StratosHa ha2 = new StratosHa(primaryNode, List.of(replicaNode), 150, 2, n2);
        StratosHa ha3 = new StratosHa(primaryNode, List.of(replicaNode), 150, 2, n3);
        watchdogs.addAll(List.of(ha1, ha2, ha3));
        ha1.start();
        ha2.start();
        ha3.start();

        // Verify primary still writes before partition
        assertTrue(primaryDb.execute("INSERT INTO products VALUES (2, 'before_partition')").isSuccess());
        Thread.sleep(200); // allow replication stream to propagate row 2 to replica before partition

        // Partition network:
        // Group A (minority): {w1} and Primary
        // Group B (majority): {w2, w3} and Replica
        raftCluster.partition(Set.of("w1"), Set.of("w2", "w3"));
        leaseClient.setPartitionFilter(Set.of("w1")::contains);

        // Simulate network partition for primary: stop wire server and replication server from responding
        // to majority watchdogs, while primary database instance itself continues running
        primaryWireServer.stop();
        replicationServer.stop();

        // Minority node w1 cannot renew primary lease because it lacks quorum
        // Primary lease expires after 1000ms TTL
        long fenceDeadline = System.currentTimeMillis() + 2500;
        while (leaseClient.hasValidLease() && System.currentTimeMillis() < fenceDeadline) {
            Thread.sleep(50);
        }
        assertFalse(leaseClient.hasValidLease(), "Primary lease must expire when isolated from majority quorum");
        assertTrue(primaryDb.isReadOnly(), "Primary must self-fence to read-only once lease expires");

        // Attempting to write to fenced primary must fail with read-only error
        var writeAttemptOnFencedPrimary = primaryDb.execute("INSERT INTO products VALUES (99, 'split_brain_write')");
        assertFalse(writeAttemptOnFencedPrimary.isSuccess(), "Split-brain write on isolated primary must be rejected");
        assertNotNull(writeAttemptOnFencedPrimary.getError());
        assertTrue(writeAttemptOnFencedPrimary.getError().contains("read-only"),
            "Error message must indicate read-only mode: " + writeAttemptOnFencedPrimary.getError());

        // Majority watchdog promotes replica
        long failoverDeadline = System.currentTimeMillis() + 5000;
        while (replicaDb.isReadOnly() && System.currentTimeMillis() < failoverDeadline) {
            Thread.sleep(50);
        }
        assertFalse(replicaDb.isReadOnly(), "Replica must be promoted to primary by majority watchdog");

        // The newly promoted primary accepts writes
        assertTrue(replicaDb.execute("INSERT INTO products VALUES (3, 'after_failover')").isSuccess());

        var result = replicaDb.execute("SELECT * FROM products");
        assertTrue(result.isSuccess());
        // Must contain row 1 (initial), row 2 (before partition), and row 3 (after failover)
        // Must NOT contain row 99 (split_brain_write)
        assertEquals(3, result.getRows().size(), "Zero split-brain writes must be present on promoted primary");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void automaticFailoverSucceedsUnderThreeSeconds(@TempDir Path tempDir) throws Exception {
        int primaryWirePort = freePort();
        int replicaWirePort = freePort();
        int replicationPort = freePort();

        int raftPort1 = freePort();
        int raftPort2 = freePort();
        int raftPort3 = freePort();

        List<StdRaftPeer> peers = List.of(
            new StdRaftPeer("w1", "127.0.0.1", raftPort1),
            new StdRaftPeer("w2", "127.0.0.1", raftPort2),
            new StdRaftPeer("w3", "127.0.0.1", raftPort3)
        );

        StdRaftNode n1 = new StdRaftNode("w1", "127.0.0.1", raftPort1, peers, 150, 300, 50);
        StdRaftNode n2 = new StdRaftNode("w2", "127.0.0.1", raftPort2, peers, 150, 300, 50);
        StdRaftNode n3 = new StdRaftNode("w3", "127.0.0.1", raftPort3, peers, 150, 300, 50);

        raftCluster = new StdRaftCluster(List.of(n1, n2, n3));
        raftCluster.start();

        StdRaftNode leader = raftCluster.waitForLeader(5000);
        assertNotNull(leader, "A Raft consensus leader must be elected");

        DatabaseConfig primaryConfig = new DatabaseConfig();
        primaryConfig.setDataDirectory(tempDir.resolve("primary_perf").toString());
        primaryDb = new StratosDB(primaryConfig);
        primaryWireServer = new StdWireServer(primaryWirePort, primaryDb);
        primaryWireServer.start();

        replicationServer = new ReplicationServer(replicationPort, primaryDb.getWalManager(), 50);
        replicationServer.start();

        primaryDb.execute("CREATE TABLE metrics (id INT, val INT)");
        primaryDb.execute("INSERT INTO metrics VALUES (1, 100)");

        leaseClient = new StdRaftLeaseClient(peers, "127.0.0.1:" + primaryWirePort, "primary", 800, 150);
        leaseClient.start();
        assertTrue(leaseClient.hasValidLease(), "Primary must acquire initial consensus lease");
        primaryDb.setLeaseChecker(leaseClient::hasValidLease);

        DatabaseConfig replicaConfig = new DatabaseConfig();
        replicaConfig.setDataDirectory(tempDir.resolve("replica_perf").toString());
        replicaDb = new StratosDB(replicaConfig);
        replicaDb.execute("CREATE TABLE metrics (id INT, val INT)");
        replicaDb.setReadOnly(true);

        replicationClient = new ReplicationClient("localhost", replicationPort, replicaDb.getDiskManager(), replicaDb.getBufferPool());
        replicationClient.start();

        replicaWireServer = new StdWireServer(replicaWirePort, replicaDb);
        replicaWireServer.setReplicationClient(replicationClient);
        replicaWireServer.start();

        long connectDeadline = System.currentTimeMillis() + 5000;
        while (!replicationClient.isConnected() && System.currentTimeMillis() < connectDeadline) {
            Thread.sleep(50);
        }
        assertTrue(replicationClient.isConnected());

        StratosHa.NodeConfig primaryNode = new StratosHa.NodeConfig("127.0.0.1", primaryWirePort, "anyuser", null, "anydb");
        StratosHa.NodeConfig replicaNode = new StratosHa.NodeConfig("127.0.0.1", replicaWirePort, "anyuser", null, "anydb");

        StratosHa ha1 = new StratosHa(primaryNode, List.of(replicaNode), 120, 2, n1);
        StratosHa ha2 = new StratosHa(primaryNode, List.of(replicaNode), 120, 2, n2);
        StratosHa ha3 = new StratosHa(primaryNode, List.of(replicaNode), 120, 2, n3);
        watchdogs.addAll(List.of(ha1, ha2, ha3));
        ha1.start();
        ha2.start();
        ha3.start();

        // Kill primary process abruptly (both database and its consensus lease client) and measure failover latency
        long start = System.currentTimeMillis();
        leaseClient.stop();
        primaryWireServer.stop();
        replicationServer.stop();
        primaryDb.shutdown();
        primaryDb = null;

        long maxDeadline = start + 3000;
        while (replicaDb.isReadOnly() && System.currentTimeMillis() < maxDeadline) {
            Thread.sleep(50);
        }

        long elapsed = System.currentTimeMillis() - start;
        assertFalse(replicaDb.isReadOnly(), "Replica must be promoted within 3 seconds");
        assertTrue(elapsed < 3000, "Automatic failover must succeed in < 3 seconds (took " + elapsed + "ms)");

        assertTrue(replicaDb.execute("INSERT INTO metrics VALUES (2, 200)").isSuccess(),
            "Promoted replica must accept writes immediately");
        var result = replicaDb.execute("SELECT * FROM metrics");
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRows().size());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void minorityWatchdogPartitionDoesNotTriggerFalseFailover(@TempDir Path tempDir) throws Exception {
        int primaryWirePort = freePort();
        int replicaWirePort = freePort();
        int replicationPort = freePort();

        int raftPort1 = freePort();
        int raftPort2 = freePort();
        int raftPort3 = freePort();

        List<StdRaftPeer> peers = List.of(
            new StdRaftPeer("w1", "127.0.0.1", raftPort1),
            new StdRaftPeer("w2", "127.0.0.1", raftPort2),
            new StdRaftPeer("w3", "127.0.0.1", raftPort3)
        );

        StdRaftNode n1 = new StdRaftNode("w1", "127.0.0.1", raftPort1, peers, 150, 300, 50);
        StdRaftNode n2 = new StdRaftNode("w2", "127.0.0.1", raftPort2, peers, 150, 300, 50);
        StdRaftNode n3 = new StdRaftNode("w3", "127.0.0.1", raftPort3, peers, 150, 300, 50);

        raftCluster = new StdRaftCluster(List.of(n1, n2, n3));
        raftCluster.start();

        StdRaftNode leader = raftCluster.waitForLeader(5000);
        assertNotNull(leader, "A Raft consensus leader must be elected");

        DatabaseConfig primaryConfig = new DatabaseConfig();
        primaryConfig.setDataDirectory(tempDir.resolve("primary_min").toString());
        primaryDb = new StratosDB(primaryConfig);
        primaryWireServer = new StdWireServer(primaryWirePort, primaryDb);
        primaryWireServer.start();

        replicationServer = new ReplicationServer(replicationPort, primaryDb.getWalManager(), 50);
        replicationServer.start();

        primaryDb.execute("CREATE TABLE config (k TEXT, v TEXT)");
        primaryDb.execute("INSERT INTO config VALUES ('status', 'active')");

        leaseClient = new StdRaftLeaseClient(peers, "127.0.0.1:" + primaryWirePort, "primary", 1000, 200);
        leaseClient.start();
        assertTrue(leaseClient.hasValidLease());
        primaryDb.setLeaseChecker(leaseClient::hasValidLease);

        DatabaseConfig replicaConfig = new DatabaseConfig();
        replicaConfig.setDataDirectory(tempDir.resolve("replica_min").toString());
        replicaDb = new StratosDB(replicaConfig);
        replicaDb.execute("CREATE TABLE config (k TEXT, v TEXT)");
        replicaDb.setReadOnly(true);

        replicationClient = new ReplicationClient("localhost", replicationPort, replicaDb.getDiskManager(), replicaDb.getBufferPool());
        replicationClient.start();

        replicaWireServer = new StdWireServer(replicaWirePort, replicaDb);
        replicaWireServer.setReplicationClient(replicationClient);
        replicaWireServer.start();

        long connectDeadline = System.currentTimeMillis() + 5000;
        while (!replicationClient.isConnected() && System.currentTimeMillis() < connectDeadline) {
            Thread.sleep(50);
        }
        assertTrue(replicationClient.isConnected());

        // Point minority watchdog w3 at non-existent primary port to simulate failure from its viewpoint
        StratosHa.NodeConfig unreachablePrimary = new StratosHa.NodeConfig("127.0.0.1", 1, "anyuser", null, "anydb");
        StratosHa.NodeConfig healthyPrimary = new StratosHa.NodeConfig("127.0.0.1", primaryWirePort, "anyuser", null, "anydb");
        StratosHa.NodeConfig replicaNode = new StratosHa.NodeConfig("127.0.0.1", replicaWirePort, "anyuser", null, "anydb");

        StratosHa ha1 = new StratosHa(healthyPrimary, List.of(replicaNode), 150, 2, n1);
        StratosHa ha2 = new StratosHa(healthyPrimary, List.of(replicaNode), 150, 2, n2);
        StratosHa ha3 = new StratosHa(unreachablePrimary, List.of(replicaNode), 150, 2, n3);
        watchdogs.addAll(List.of(ha1, ha2, ha3));

        // Isolate w3 from Raft consensus
        raftCluster.isolate("w3");

        ha1.start();
        ha2.start();
        ha3.start();

        // Wait to ensure ha3 fails its health checks multiple times
        Thread.sleep(1000);

        // Assert that isolated w3 could NOT trigger failover because it lacks Raft leadership and quorum
        assertFalse(ha3.hasTriggeredFailover(), "Isolated minority watchdog must NOT trigger failover");
        assertTrue(replicaDb.isReadOnly(), "Replica must remain in read-only mode");

        // Primary continues to accept writes normally
        assertTrue(primaryDb.execute("INSERT INTO config VALUES ('version', '1.0')").isSuccess());
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
