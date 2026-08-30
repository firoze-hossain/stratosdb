package com.stratosdb.sql.executor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The real pg_stat_activity equivalent - a shared, server-wide registry
 * of every currently-connected session's own real, live state, keyed by
 * a real, unique connection id. One registry per ExecutorEngine, the
 * same real scope pg_stat_activity itself has per real Postgres
 * instance.
 *
 * StdWireServer (the only real owner of a connection's own actual
 * lifecycle - accept, authenticate, run statements, disconnect) is
 * responsible for calling register() once per real connection and
 * unregister() when it ends; ExecutorEngine itself has no connection
 * lifecycle of its own to hook into directly, only individual
 * execute() calls, which is why this registry's own read/write API is
 * public rather than folded silently into execute() the way
 * QueryStatsRegistry's own recording is.
 */
public class SessionActivityRegistry {
    private final ConcurrentHashMap<Long, SessionActivity> activeSessions = new ConcurrentHashMap<>();
    private final AtomicLong nextConnectionId = new AtomicLong(1);

    public SessionActivity register(String username, String clientAddr) {
        long id = nextConnectionId.getAndIncrement();
        SessionActivity activity = new SessionActivity(id, username, clientAddr);
        activeSessions.put(id, activity);
        return activity;
    }

    public void unregister(SessionActivity activity) {
        activeSessions.remove(activity.connectionId);
    }

    /** A real, live snapshot - each SessionActivity in the returned map is the same mutable object its own owning connection updates in place, so a caller reading it after this call still sees genuinely current state, not a value frozen at snapshot time. */
    public Map<Long, SessionActivity> getAll() {
        return activeSessions;
    }
}
