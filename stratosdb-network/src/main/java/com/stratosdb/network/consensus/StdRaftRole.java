package com.stratosdb.network.consensus;

/**
 * Roles assumed by a node participating in the Raft distributed consensus protocol.
 */
public enum StdRaftRole {
    /**
     * Passive state responding to incoming heartbeats and vote requests.
     */
    FOLLOWER,

    /**
     * Active state campaigning for leadership after an election timeout.
     */
    CANDIDATE,

    /**
     * Authoritative state driving cluster heartbeats, log replication, and distributed leases.
     */
    LEADER
}
