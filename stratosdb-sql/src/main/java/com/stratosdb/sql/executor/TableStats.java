package com.stratosdb.sql.executor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * One table's own real, accumulated operational counters - the real
 * pg_stat_user_tables equivalent for a single row. Every field a
 * separate atomic for the same real reason QueryStats itself uses one
 * per field: many different connections can update the SAME table's
 * own counters concurrently, and this must never become a real
 * contention point on the actual query execution path.
 *
 * Real, honestly-stated scope: seq_scan counts every real SELECT
 * against this table, not specifically a sequential scan as opposed to
 * an index scan - this engine's own query planner decides sequential
 * vs. index access deep inside executeSelect's own logic, well past
 * the single, centralized hook point (right after dispatch() returns,
 * in ExecutorEngine.executeInternal) this registry is actually updated
 * from; distinguishing the two would need real instrumentation inside
 * the planner itself, a real, separate, further piece of work not
 * attempted here.
 */
public class TableStats {
    private final AtomicLong seqScans = new AtomicLong(0);
    private final AtomicLong rowsReturned = new AtomicLong(0);
    private final AtomicLong rowsInserted = new AtomicLong(0);
    private final AtomicLong rowsUpdated = new AtomicLong(0);
    private final AtomicLong rowsDeleted = new AtomicLong(0);

    void recordSelect(long rowsReturnedCount) {
        seqScans.incrementAndGet();
        rowsReturned.addAndGet(rowsReturnedCount);
    }

    void recordInsert(long count) {
        rowsInserted.addAndGet(count);
    }

    void recordUpdate(long count) {
        rowsUpdated.addAndGet(count);
    }

    void recordDelete(long count) {
        rowsDeleted.addAndGet(count);
    }

    public long getSeqScans() {
        return seqScans.get();
    }

    public long getRowsReturned() {
        return rowsReturned.get();
    }

    public long getRowsInserted() {
        return rowsInserted.get();
    }

    public long getRowsUpdated() {
        return rowsUpdated.get();
    }

    public long getRowsDeleted() {
        return rowsDeleted.get();
    }
}
