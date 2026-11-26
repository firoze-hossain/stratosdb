package com.stratosdb.storage.wal;

/**
 * PostgreSQL-compatible {@code synchronous_commit} levels controlling
 * durability and synchronous streaming replication ACK behavior.
 */
public enum SynchronousCommitMode {
    /** Do not wait for local WAL flush or replica ACK. */
    OFF,
    /** Wait for local WAL flush to disk, do not wait for replica ACK (default for standalone). */
    LOCAL,
    /** Wait for local WAL flush and replica WAL receipt/flush ACK. */
    ON,
    /** Wait for local WAL flush and replica WAL apply ACK (RPO=0, read-after-write on replica). */
    REMOTE_APPLY,
    /** Wait for local WAL flush and quorum of connected replicas to ACK. */
    QUORUM;

    public boolean requiresRemoteAck() {
        return this == ON || this == REMOTE_APPLY || this == QUORUM;
    }

    public static SynchronousCommitMode parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("synchronous_commit cannot be blank");
        }
        return switch (value.trim().toUpperCase()) {
            case "OFF", "FALSE", "0" -> OFF;
            case "LOCAL" -> LOCAL;
            case "ON", "TRUE", "1", "REMOTE_WRITE" -> ON;
            case "REMOTE_APPLY" -> REMOTE_APPLY;
            case "QUORUM" -> QUORUM;
            default -> throw new IllegalArgumentException("Unknown synchronous_commit mode: " + value);
        };
    }
}
