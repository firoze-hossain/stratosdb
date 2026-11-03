package com.stratosdb.network.consensus;

/**
 * Identifies a peer node in the Raft consensus cluster with its network host and port.
 */
public record StdRaftPeer(String id, String host, int port) {
    @Override
    public String toString() {
        return id + "=" + host + ":" + port;
    }
}
