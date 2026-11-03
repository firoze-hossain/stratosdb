package com.stratosdb.network.consensus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test suite verifying the Raft consensus protocol, leader election,
 * quorum enforcement, distributed leader leases, and network partition resilience.
 */
class StdRaftConsensusTest {

    private StdRaftCluster cluster;
    private StdRaftLeaseClient leaseClient;

    @AfterEach
    void tearDown() {
        if (leaseClient != null) {
            leaseClient.stop();
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void threeNodeClusterElectsSingleLeader() throws Exception {
        int p1 = freePort();
        int p2 = freePort();
        int p3 = freePort();

        List<StdRaftPeer> peers1 = List.of(new StdRaftPeer("n2", "localhost", p2), new StdRaftPeer("n3", "localhost", p3));
        List<StdRaftPeer> peers2 = List.of(new StdRaftPeer("n1", "localhost", p1), new StdRaftPeer("n3", "localhost", p3));
        List<StdRaftPeer> peers3 = List.of(new StdRaftPeer("n1", "localhost", p1), new StdRaftPeer("n2", "localhost", p2));

        StdRaftNode n1 = new StdRaftNode("n1", "localhost", p1, peers1, 150, 300, 50);
        StdRaftNode n2 = new StdRaftNode("n2", "localhost", p2, peers2, 150, 300, 50);
        StdRaftNode n3 = new StdRaftNode("n3", "localhost", p3, peers3, 150, 300, 50);

        cluster = new StdRaftCluster(List.of(n1, n2, n3));
        cluster.start();

        StdRaftNode leader = cluster.waitForLeader(5000);
        assertNotNull(leader, "A Raft leader must be elected within 5 seconds");
        assertTrue(leader.isLeader());

        int leaderCount = 0;
        int followerCount = 0;
        for (StdRaftNode node : List.of(n1, n2, n3)) {
            if (node.isLeader()) leaderCount++;
            if (node.getRole() == StdRaftRole.FOLLOWER) followerCount++;
        }

        assertEquals(1, leaderCount, "Exactly one node must be elected leader");
        assertEquals(2, followerCount, "The remaining nodes must be followers");
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void distributedLeaderLeaseAcquisitionAndRenewal() throws Exception {
        int p1 = freePort();
        int p2 = freePort();
        int p3 = freePort();

        List<StdRaftPeer> peers1 = List.of(new StdRaftPeer("n2", "localhost", p2), new StdRaftPeer("n3", "localhost", p3));
        List<StdRaftPeer> peers2 = List.of(new StdRaftPeer("n1", "localhost", p1), new StdRaftPeer("n3", "localhost", p3));
        List<StdRaftPeer> peers3 = List.of(new StdRaftPeer("n1", "localhost", p1), new StdRaftPeer("n2", "localhost", p2));

        StdRaftNode n1 = new StdRaftNode("n1", "localhost", p1, peers1, 150, 300, 50);
        StdRaftNode n2 = new StdRaftNode("n2", "localhost", p2, peers2, 150, 300, 50);
        StdRaftNode n3 = new StdRaftNode("n3", "localhost", p3, peers3, 150, 300, 50);

        cluster = new StdRaftCluster(List.of(n1, n2, n3));
        cluster.start();

        StdRaftNode leader = cluster.waitForLeader(5000);
        assertNotNull(leader);

        List<StdRaftPeer> allPeers = List.of(
            new StdRaftPeer("n1", "localhost", p1),
            new StdRaftPeer("n2", "localhost", p2),
            new StdRaftPeer("n3", "localhost", p3)
        );

        leaseClient = new StdRaftLeaseClient(allPeers, "primary-node-1", "primary", 1000, 200);
        leaseClient.start();

        assertTrue(leaseClient.hasValidLease(), "Lease client must successfully acquire initial lease from leader");
        long firstExpiry = leaseClient.getCurrentExpiresAt();
        assertTrue(firstExpiry > System.currentTimeMillis());

        // Wait for renewal tick
        Thread.sleep(350);
        assertTrue(leaseClient.hasValidLease());
        assertTrue(leaseClient.getCurrentExpiresAt() > firstExpiry, "Lease client should automatically extend expiry");
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void minorityPartitionCannotElectLeader() throws Exception {
        int p1 = freePort();
        int p2 = freePort();
        int p3 = freePort();

        List<StdRaftPeer> peers1 = List.of(new StdRaftPeer("n2", "localhost", p2), new StdRaftPeer("n3", "localhost", p3));
        List<StdRaftPeer> peers2 = List.of(new StdRaftPeer("n1", "localhost", p1), new StdRaftPeer("n3", "localhost", p3));
        List<StdRaftPeer> peers3 = List.of(new StdRaftPeer("n1", "localhost", p1), new StdRaftPeer("n2", "localhost", p2));

        StdRaftNode n1 = new StdRaftNode("n1", "localhost", p1, peers1, 150, 300, 50);
        StdRaftNode n2 = new StdRaftNode("n2", "localhost", p2, peers2, 150, 300, 50);
        StdRaftNode n3 = new StdRaftNode("n3", "localhost", p3, peers3, 150, 300, 50);

        cluster = new StdRaftCluster(List.of(n1, n2, n3));
        cluster.start();

        StdRaftNode initialLeader = cluster.waitForLeader(5000);
        assertNotNull(initialLeader);

        // Partition cluster: Group A = {n1} (minority: 1 node), Group B = {n2, n3} (majority: 2 nodes)
        cluster.partition(Set.of("n1"), Set.of("n2", "n3"));

        // Wait for election timeouts to trigger in partition
        Thread.sleep(600);

        // n1 in minority cannot be leader because quorum is 2
        assertFalse(n1.isLeader(), "Isolated minority node n1 must NEVER be able to become leader");

        // Majority partition {n2, n3} must have a leader
        assertTrue(n2.isLeader() || n3.isLeader(), "Majority partition {n2, n3} must elect/maintain leader");

        // Heal partition
        cluster.heal();
        Thread.sleep(500);

        int totalLeaders = 0;
        for (StdRaftNode n : List.of(n1, n2, n3)) {
            if (n.isLeader()) totalLeaders++;
        }
        assertEquals(1, totalLeaders, "After healing, cluster must converge to exactly one leader");
    }
}
