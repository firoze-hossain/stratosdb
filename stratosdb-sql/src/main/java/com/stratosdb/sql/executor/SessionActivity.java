package com.stratosdb.sql.executor;

/**
 * One connection's own real, live activity state - the real
 * pg_stat_activity equivalent for a single row. Mutable and updated in
 * place by the connection that owns it (see SessionActivityRegistry's
 * own javadoc) as its own state genuinely changes - a snapshot read by
 * SHOW ACTIVITY sees whatever this connection's own state actually is
 * at read time, not a value cached from when the connection first
 * registered.
 */
public class SessionActivity {
    public final long connectionId;
    public final String username;
    public final String clientAddr;
    public final long backendStart;

    public volatile String state = "idle";
    public volatile String query = "";
    public volatile long queryStart = 0;

    public SessionActivity(long connectionId, String username, String clientAddr) {
        this.connectionId = connectionId;
        this.username = username;
        this.clientAddr = clientAddr;
        this.backendStart = System.currentTimeMillis();
    }
}
