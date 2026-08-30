package com.stratosdb.sql.executor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The real pg_stat_statements equivalent - a shared, server-wide
 * registry mapping each normalized query shape (see QueryNormalizer)
 * to its own accumulated QueryStats, aggregated across every
 * connection that ever ran a matching query against this same
 * ExecutorEngine instance. One registry per ExecutorEngine, the same
 * real scope pg_stat_statements itself has per real Postgres instance
 * (not per-session, not per-database within a cluster).
 */
public class QueryStatsRegistry {
    private final ConcurrentHashMap<String, QueryStats> statsByNormalizedQuery = new ConcurrentHashMap<>();

    public void record(String sql, long elapsedNanos, long rows) {
        String normalized = QueryNormalizer.normalize(sql);
        statsByNormalizedQuery.computeIfAbsent(normalized, k -> new QueryStats()).record(elapsedNanos, rows);
    }

    /** A real, live snapshot - not a copy that goes stale the moment it's taken, since QueryStats itself is mutable and shared; callers reading a returned QueryStats' own fields see genuinely current values. */
    public Map<String, QueryStats> getAll() {
        return statsByNormalizedQuery;
    }

    public void reset() {
        statsByNormalizedQuery.clear();
    }
}
