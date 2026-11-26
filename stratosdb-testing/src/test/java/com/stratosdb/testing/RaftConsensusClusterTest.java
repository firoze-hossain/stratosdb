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
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.wal.SynchronousCommitMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Sprint 09 Week 18: Embedded Raft Consensus Cluster Coordinator
 * for 3-node and 5-node clusters, automated leader election, heartbeats,
 * split-brain prevention, and sub-3-second failover mid-workload.
 */
class RaftConsensusClusterTest {

    private static int findFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testFiveNodeClusterElectionReplicationAndSplitBrainPrevention() throws Exception {
        int[] ports = new int[5];
        for (int i = 0; i < 5; i++) {
            ports[i] = findFreePort();
        }

        try (StdRaftCluster cluster = StdRaftCluster.createLocalCluster(5, ports, 150, 300, 50)) {
            cluster.start();
            StdRaftNode leader1 = cluster.waitForLeader(3000);
            assertNotNull(leader1, "5-node Raft cluster must elect an initial leader in < 3s");
            assertEquals(1, cluster.countLeaders(), "Exactly 1 leader must exist");

            // Acquire and replicate lease entry across 5-node quorum (3/5)
            assertTrue(leader1.handleLeaseAcquire("node-a:5432", "primary", 400).granted());

            // Partition into minority {leader1, one other} vs majority (remaining 3 nodes)
            String leader1Id = leader1.getNodeId();
            String secondId = cluster.getNodes().keySet().stream()
                .filter(id -> !id.equals(leader1Id))
                .findFirst()
                .orElseThrow();
            Set<String> minority = Set.of(leader1Id, secondId);
            Set<String> majority = cluster.getNodes().keySet().stream()
                .filter(id -> !minority.contains(id))
                .collect(java.util.stream.Collectors.toSet());

            long splitStart = System.currentTimeMillis();
            cluster.partition(minority, majority);

            // Majority (3/5) must elect a new leader in < 3s; minority must not commit or split-brain
            StdRaftNode leader2 = cluster.waitForNewLeader(leader1Id, 3000);
            long failoverMs = System.currentTimeMillis() - splitStart;
            assertNotNull(leader2, "Majority partition (3/5) must elect a new leader");
            assertTrue(majority.contains(leader2.getNodeId()), "New leader must be in the majority partition");
            assertTrue(failoverMs < 3000, () -> "5-node failover must complete in < 3s, took " + failoverMs + "ms");
            // Wait for node-a's 400ms lease to expire so node-b can acquire the lease safely without split-brain
            Thread.sleep(450);
            assertTrue(leader2.handleLeaseAcquire("node-b:5432", "primary", 2000).granted(), "New majority leader must commit lease after old lease expiry");

            // Heal partition and verify single leader convergence
            cluster.heal();
            Thread.sleep(300);
            assertEquals(1, cluster.countLeaders(), "After healing partition, cluster must converge to 1 leader (no split-brain)");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testThreeNodeSubThreeSecondFailoverMidWorkload(@TempDir Path tempDir) throws Exception {
        int r1 = findFreePort();
        int r2 = findFreePort();
        int r3 = findFreePort();
        int primaryWirePort = findFreePort();
        int replPort = findFreePort();
        int replicaWirePort = findFreePort();

        List<StdRaftPeer> peers = List.of(
            new StdRaftPeer("n1", "127.0.0.1", r1),
            new StdRaftPeer("n2", "127.0.0.1", r2),
            new StdRaftPeer("n3", "127.0.0.1", r3)
        );
        StdRaftNode n1 = new StdRaftNode("n1", "127.0.0.1", r1, peers, 120, 240, 40);
        StdRaftNode n2 = new StdRaftNode("n2", "127.0.0.1", r2, peers, 120, 240, 40);
        StdRaftNode n3 = new StdRaftNode("n3", "127.0.0.1", r3, peers, 120, 240, 40);

        DatabaseConfig pCfg = new DatabaseConfig();
        pCfg.setDataDirectory(tempDir.resolve("primary").toString());
        pCfg.setSyncWAL(false);
        pCfg.setSynchronousCommit(SynchronousCommitMode.REMOTE_APPLY);
        StratosDB primaryDb = new StratosDB(pCfg);
        StdWireServer primaryWire = new StdWireServer(primaryWirePort, primaryDb);
        ReplicationServer replServer = new ReplicationServer(replPort, primaryDb.getWalManager(), 50, 1, 3000);

        DatabaseConfig repCfg = new DatabaseConfig();
        repCfg.setDataDirectory(tempDir.resolve("replica").toString());
        repCfg.setSyncWAL(false);
        StratosDB replicaDb = new StratosDB(repCfg);
        ReplicationClient replClient = new ReplicationClient(
            "127.0.0.1", replPort, replicaDb.getDiskManager(), replicaDb.getBufferPool(), replicaDb.getTransactionManager(), 0, 0
        );
        StdWireServer replicaWire = new StdWireServer(replicaWirePort, replicaDb);
        replicaWire.setReplicationClient(replClient);

        StdRaftCluster cluster = new StdRaftCluster(List.of(n1, n2, n3));
        StdRaftLeaseClient leaseClient = new StdRaftLeaseClient(peers, "127.0.0.1:" + primaryWirePort, "primary", 800, 150);

        try {
            cluster.start();
            StdRaftNode initialLeader = cluster.waitForLeader(3000);
            assertNotNull(initialLeader, "3-node Raft cluster must elect initial leader");

            primaryWire.start();
            replServer.start();
            leaseClient.start();
            replicaWire.start();
            replClient.start();

            long connDeadline = System.currentTimeMillis() + 3000;
            while (!replClient.isConnected() && System.currentTimeMillis() < connDeadline) {
                Thread.sleep(10);
            }
            assertTrue(replClient.isConnected());

            assertTrue(primaryDb.execute("CREATE TABLE workload (id INT, val TEXT)").isSuccess());
            assertTrue(replicaDb.execute("CREATE TABLE workload (id INT, val TEXT)").isSuccess());
            replicaDb.setReadOnly(true);

            // Mid-workload writes on primary
            for (int i = 1; i <= 10; i++) {
                assertTrue(primaryDb.execute("INSERT INTO workload VALUES (" + i + ", 'before_" + i + "')").isSuccess());
            }

            // Start StratosHa watchdogs on the surviving Raft nodes
            StratosHa.NodeConfig pNodeCfg = new StratosHa.NodeConfig("127.0.0.1", primaryWirePort, "anyuser", null, "anydb");
            StratosHa.NodeConfig rNodeCfg = new StratosHa.NodeConfig("127.0.0.1", replicaWirePort, "anyuser", null, "anydb");
            StratosHa ha1 = new StratosHa(pNodeCfg, List.of(rNodeCfg), 100, 2, n1);
            StratosHa ha2 = new StratosHa(pNodeCfg, List.of(rNodeCfg), 100, 2, n2);
            StratosHa ha3 = new StratosHa(pNodeCfg, List.of(rNodeCfg), 100, 2, n3);
            ha1.start();
            ha2.start();
            ha3.start();

            // Kill primary node + Raft leader mid-workload
            long killTime = System.currentTimeMillis();
            String oldLeaderId = initialLeader.getNodeId();
            initialLeader.stop();
            leaseClient.stop();
            replServer.stop();
            primaryWire.stop();
            primaryDb.shutdown();

            // Cluster must elect new Raft leader AND promote standby in < 3s
            StdRaftNode newLeader = cluster.waitForNewLeader(oldLeaderId, 3000);
            assertNotNull(newLeader, "Remaining 2/3 nodes must elect new Raft leader in < 3s");

            long promoDeadline = System.currentTimeMillis() + 3000;
            while (replicaDb.isReadOnly() && System.currentTimeMillis() < promoDeadline) {
                Thread.sleep(20);
            }
            long totalFailoverMs = System.currentTimeMillis() - killTime;
            assertFalse(replicaDb.isReadOnly(), "Standby must be promoted to writable primary");
            assertTrue(totalFailoverMs < 3000, () -> "Failover must complete in < 3s, took " + totalFailoverMs + "ms");

            // Resume write traffic on promoted leader with all prior rows intact
            QueryResult resumeWrite = replicaDb.execute("INSERT INTO workload VALUES (11, 'after_failover')");
            assertTrue(resumeWrite.isSuccess(), () -> "Promoted leader must accept writes: " + resumeWrite.getError());
            QueryResult countRes = replicaDb.execute("SELECT id FROM workload");
            assertEquals(11, countRes.getRows().size(), "All 10 pre-failover rows + 1 post-failover row must be present");

            ha1.stop();
            ha2.stop();
            ha3.stop();
        } finally {
            leaseClient.stop();
            replClient.stop();
            replServer.stop();
            replicaWire.stop();
            primaryWire.stop();
            replicaDb.shutdown();
            primaryDb.shutdown();
            cluster.stop();
        }
    }
}
