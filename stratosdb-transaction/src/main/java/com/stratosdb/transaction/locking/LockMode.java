package com.stratosdb.transaction.locking;

/**
 * Standard multi-granularity lock modes for tables and rows:
 * <ul>
 *   <li><b>IS (Intent Shared)</b>: Table level; indicates intention to read rows under S locks or MVCC.</li>
 *   <li><b>IX (Intent Exclusive)</b>: Table level; indicates intention to modify rows under X locks.</li>
 *   <li><b>S (Shared)</b>: Table or row level; shared read lock allowing concurrent readers.</li>
 *   <li><b>SIX (Shared Intent Exclusive)</b>: Table level; holds table-level S lock and intends row-level X locks.</li>
 *   <li><b>X (Exclusive)</b>: Table or row level; exclusive lock blocking all other readers and writers (e.g. DDL or UPDATE/DELETE).</li>
 * </ul>
 */
public enum LockMode {
    IS,
    IX,
    S,
    SIX,
    X;

    /**
     * Standard Gray / Reuter multi-granularity lock compatibility matrix.
     * Returns true if a lock in this mode can be granted concurrently with an existing lock in {@code other} mode.
     */
    public boolean isCompatible(LockMode other) {
        if (other == null) return true;
        return switch (this) {
            case IS  -> other != X;
            case IX  -> other == IS || other == IX;
            case S   -> other == IS || other == S;
            case SIX -> other == IS;
            case X   -> false;
        };
    }

    /**
     * Returns true if this lock mode is stronger than or equal to {@code other}.
     */
    public boolean isStrongerThanOrEqualTo(LockMode other) {
        if (other == null || this == other) return true;
        if (this == X) return true;
        if (this == SIX) return other == S || other == IX || other == IS;
        if (this == S) return other == IS;
        if (this == IX) return other == IS;
        return false;
    }
}
