package com.stratosdb.sql.executor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * One normalized query's own accumulated statistics - the real,
 * per-shape counters pg_stat_statements itself tracks (call count,
 * total/min/max execution time, total rows returned). Every field is a
 * separate atomic, not a single lock around a plain object: many
 * different connections can record against the SAME QueryStats
 * instance concurrently (the whole point of normalization - many
 * different literal values, one shared shape), and this must never
 * become a real contention point on the actual query execution path
 * itself.
 */
public class QueryStats {
    private final AtomicLong calls = new AtomicLong(0);
    private final AtomicLong totalTimeNanos = new AtomicLong(0);
    private final AtomicLong minTimeNanos = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxTimeNanos = new AtomicLong(0);
    private final AtomicLong totalRows = new AtomicLong(0);

    void record(long elapsedNanos, long rows) {
        calls.incrementAndGet();
        totalTimeNanos.addAndGet(elapsedNanos);
        totalRows.addAndGet(rows);
        minTimeNanos.updateAndGet(current -> Math.min(current, elapsedNanos));
        maxTimeNanos.updateAndGet(current -> Math.max(current, elapsedNanos));
    }

    public long getCalls() {
        return calls.get();
    }

    public double getTotalTimeMs() {
        return totalTimeNanos.get() / 1_000_000.0;
    }

    public double getMinTimeMs() {
        long calls = getCalls();
        return calls == 0 ? 0.0 : minTimeNanos.get() / 1_000_000.0;
    }

    public double getMaxTimeMs() {
        return maxTimeNanos.get() / 1_000_000.0;
    }

    public double getMeanTimeMs() {
        long callCount = calls.get();
        return callCount == 0 ? 0.0 : getTotalTimeMs() / callCount;
    }

    public long getTotalRows() {
        return totalRows.get();
    }
}
