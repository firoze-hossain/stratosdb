package com.stratosdb.core;

import com.stratosdb.common.constants.ProtocolConstants;

public class DatabaseConfig {
    private String dataDirectory = "./stratosdb_data";
    private int port = ProtocolConstants.DEFAULT_PORT;
    private int bufferPoolSize = 1024;
    private boolean syncWAL = true;
    private int maxConnections = 100;
    /** Milliseconds between automatic VACUUM passes over every table; 0 (the default) means autovacuum is off - call StratosDB.startAutovacuum explicitly, or set this before construction to auto-start. */
    private long autovacuumIntervalMs = 0;

    public long getAutovacuumIntervalMs() { return autovacuumIntervalMs; }
    public void setAutovacuumIntervalMs(long autovacuumIntervalMs) { this.autovacuumIntervalMs = autovacuumIntervalMs; }

    /** Milliseconds; a statement taking at least this long gets logged at WARN. Negative (the default) disables slow-query logging entirely. */
    private long slowQueryThresholdMs = -1;

    public long getSlowQueryThresholdMs() { return slowQueryThresholdMs; }
    public void setSlowQueryThresholdMs(long slowQueryThresholdMs) { this.slowQueryThresholdMs = slowQueryThresholdMs; }

    public String getDataDirectory() { return dataDirectory; }
    public void setDataDirectory(String dataDirectory) { this.dataDirectory = dataDirectory; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public int getBufferPoolSize() { return bufferPoolSize; }
    public void setBufferPoolSize(int bufferPoolSize) { this.bufferPoolSize = bufferPoolSize; }

    public boolean isSyncWAL() { return syncWAL; }
    public void setSyncWAL(boolean syncWAL) { this.syncWAL = syncWAL; }

    public int getMaxConnections() { return maxConnections; }
    public void setMaxConnections(int maxConnections) { this.maxConnections = maxConnections; }

    /**
     * Returns a real, independent copy of this config with only the data
     * directory changed - everything else (port, buffer pool size, sync
     * WAL, slow-query threshold, autovacuum interval) carried over as-is.
     * Used by {@link StratosCluster} to derive each database's own,
     * separate {@code DatabaseConfig} from one shared, cluster-wide
     * template, since every database in a cluster shares the same real
     * server-level settings but needs its own, independent data directory.
     */
    public DatabaseConfig copyWithDataDirectory(String newDataDirectory) {
        DatabaseConfig copy = new DatabaseConfig();
        copy.setDataDirectory(newDataDirectory);
        copy.setPort(this.port);
        copy.setBufferPoolSize(this.bufferPoolSize);
        copy.setSyncWAL(this.syncWAL);
        copy.setMaxConnections(this.maxConnections);
        copy.setAutovacuumIntervalMs(this.autovacuumIntervalMs);
        copy.setSlowQueryThresholdMs(this.slowQueryThresholdMs);
        return copy;
    }
}