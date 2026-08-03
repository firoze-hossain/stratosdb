package com.stratosdb.core;

public class DatabaseConfig {
    private String dataDirectory = "./stratosdb_data";
    private int port = 5432;
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
}