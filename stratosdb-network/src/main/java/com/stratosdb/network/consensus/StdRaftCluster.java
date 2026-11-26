package com.stratosdb.network.consensus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Orchestrates a cluster of Raft consensus nodes, providing partition injection
 * and leader discovery helpers for multi-node testing and deployment.
 */
public class StdRaftCluster implements AutoCloseable {

    private final Map<String, StdRaftNode> nodes = new ConcurrentHashMap<>();

    public StdRaftCluster(List<StdRaftNode> nodeList) {
        if (nodeList != null) {
            for (StdRaftNode node : nodeList) {
                this.nodes.put(node.getNodeId(), node);
            }
        }
    }

    /**
     * Creates an embedded N-node Raft cluster (typically 3 or 5 nodes) bound on localhost.
     */
    public static StdRaftCluster createLocalCluster(
            int nodeCount,
            int[] ports,
            long electionTimeoutMinMs,
            long electionTimeoutMaxMs,
            long heartbeatIntervalMs) {
        if (ports == null || ports.length != nodeCount || nodeCount < 3) {
            throw new IllegalArgumentException("Cluster requires >= 3 nodes and matching port array");
        }
        List<StdRaftPeer> peers = new ArrayList<>(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            peers.add(new StdRaftPeer("node-" + (i + 1), "127.0.0.1", ports[i]));
        }
        List<StdRaftNode> nodeList = new ArrayList<>(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            nodeList.add(new StdRaftNode(
                "node-" + (i + 1), "127.0.0.1", ports[i], peers,
                electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs
            ));
        }
        return new StdRaftCluster(nodeList);
    }

    public void start() throws IOException {
        for (StdRaftNode node : nodes.values()) {
            node.start();
        }
    }

    public void stop() {
        for (StdRaftNode node : nodes.values()) {
            try {
                node.stop();
            } catch (Exception ignored) {}
        }
    }

    @Override
    public void close() {
        stop();
    }

    public StdRaftNode getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    public Map<String, StdRaftNode> getNodes() {
        return Collections.unmodifiableMap(nodes);
    }

    public StdRaftNode getLeader() {
        for (StdRaftNode node : nodes.values()) {
            if (node.isLeader()) {
                return node;
            }
        }
        return null;
    }

    public StdRaftNode waitForLeader(long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            StdRaftNode leader = getLeader();
            if (leader != null) {
                return leader;
            }
            Thread.sleep(50);
        }
        return null;
    }

    /**
     * Waits until a leader different from {@code previousLeaderId} is elected.
     */
    public StdRaftNode waitForNewLeader(String previousLeaderId, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            for (StdRaftNode node : nodes.values()) {
                if (node.isLeader() && !node.getNodeId().equals(previousLeaderId)) {
                    return node;
                }
            }
            Thread.sleep(25);
        }
        return null;
    }

    public int countLeaders() {
        int count = 0;
        for (StdRaftNode node : nodes.values()) {
            if (node.isLeader()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Splits the cluster into two partitioned network partitions: groupA and groupB.
     * Nodes in groupA can only communicate within groupA.
     * Nodes in groupB can only communicate within groupB.
     */
    public void partition(Set<String> groupA, Set<String> groupB) {
        for (StdRaftNode node : nodes.values()) {
            String id = node.getNodeId();
            if (groupA.contains(id)) {
                node.setPartitionFilter(groupA::contains);
            } else if (groupB.contains(id)) {
                node.setPartitionFilter(groupB::contains);
            }
        }
    }

    /**
     * Isolates a single node from all other nodes in the cluster.
     */
    public void isolate(String nodeId) {
        StdRaftNode target = nodes.get(nodeId);
        if (target != null) {
            target.setPartitionFilter(id -> id.equals(nodeId));
        }
        for (StdRaftNode other : nodes.values()) {
            if (!other.getNodeId().equals(nodeId)) {
                other.setPartitionFilter(id -> !id.equals(nodeId));
            }
        }
    }

    /**
     * Heals all network partitions across the entire cluster.
     */
    public void heal() {
        for (StdRaftNode node : nodes.values()) {
            node.setPartitionFilter(null);
        }
    }
}
