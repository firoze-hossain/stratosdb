package com.stratosdb.network.consensus;

import java.io.IOException;
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
