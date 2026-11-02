package com.stratosdb.transaction;

/**
 * Standard transaction isolation levels.
 *
 * READ_COMMITTED: Default isolation level. Each statement acquires a fresh MVCC snapshot,
 * observing changes committed prior to the statement's execution. Non-repeatable reads and
 * phantom reads are possible across statements within the same transaction.
 *
 * REPEATABLE_READ: Preserves the MVCC snapshot captured at transaction begin throughout the
 * entire transaction lifecycle. Non-repeatable reads and phantom reads are prevented.
 *
 * SERIALIZABLE: Equivalent to strict repeatable read with serialized conflict detection.
 *
 * READ_UNCOMMITTED: Treated as READ_COMMITTED under standard MVCC.
 */
public enum IsolationLevel {
    READ_UNCOMMITTED("read uncommitted"),
    READ_COMMITTED("read committed"),
    REPEATABLE_READ("repeatable read"),
    SERIALIZABLE("serializable");

    private final String sqlName;

    IsolationLevel(String sqlName) {
        this.sqlName = sqlName;
    }

    public String getSqlName() {
        return sqlName;
    }

    public static IsolationLevel fromString(String text) {
        if (text == null) {
            return READ_COMMITTED;
        }
        String clean = text.trim().toLowerCase().replace("-", " ").replace("_", " ");
        if (clean.startsWith("'") && clean.endsWith("'") && clean.length() >= 2) {
            clean = clean.substring(1, clean.length() - 1).trim();
        }
        return switch (clean) {
            case "read uncommitted" -> READ_UNCOMMITTED;
            case "read committed" -> READ_COMMITTED;
            case "repeatable read" -> REPEATABLE_READ;
            case "serializable" -> SERIALIZABLE;
            default -> throw new IllegalArgumentException("Unrecognized isolation level: " + text);
        };
    }

    @Override
    public String toString() {
        return sqlName;
    }
}
