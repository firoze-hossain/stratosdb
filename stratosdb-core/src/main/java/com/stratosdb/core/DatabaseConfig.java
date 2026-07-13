package com.stratosdb.core;

public class DatabaseConfig {
    private String dataDirectory = "./stratosdb_data";
    private int port = 5432;
    private int bufferPoolSize = 1024;
    private boolean syncWAL = true;
    private int maxConnections = 100;

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