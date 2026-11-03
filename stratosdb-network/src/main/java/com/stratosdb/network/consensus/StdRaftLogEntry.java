package com.stratosdb.network.consensus;

/**
 * An individual entry in the Raft replicated log.
 */
public record StdRaftLogEntry(long index, long term, String command, String data) {}
