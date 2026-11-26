package com.stratosdb.core;

import com.stratosdb.common.constants.ProtocolConstants;

public class DatabaseConfig {
    private String dataDirectory = "./stratosdb_data";
    private int port = ProtocolConstants.DEFAULT_PORT;
    private int bufferPoolSize = 1024;
    private boolean syncWAL = true;
    private int maxConnections = 100;
    private boolean directAllocation = true;
    /** WAL segment file size in bytes (default 16MB). */
    private long walSegmentSize = 16L * 1024 * 1024;
    /** Number of past 16MB WAL segment files to retain before pruning (default 16). */
    private int walKeepSegments = 16;
    /** Whether to pre-allocate the next 16MB WAL segment in the background (default true). */
    private boolean walPreallocateSegments = true;
    /** Whether group commit coalescing is enabled (default true). */
    private boolean groupCommitEnabled = true;
    /** Maximum number of concurrent transaction commits coalesced per batch/fsync (default 256). */
    private int groupCommitMaxBatchSize = 256;
    /** Delay window in microseconds for follower commit coalescing (default 20 microseconds). */
    private long groupCommitDelayMicros = 20;
    /** Number of hashed lock partitions in LockManager (default 64). */
    private int lockPartitions = 64;
    /** Milliseconds between automatic VACUUM passes over every table; 0 (the default) means autovacuum is off - call StratosDB.startAutovacuum explicitly, or set this before construction to auto-start. */
    private long autovacuumIntervalMs = 0;

    public boolean isDirectAllocation() { return directAllocation; }
    public void setDirectAllocation(boolean directAllocation) { this.directAllocation = directAllocation; }

    public long getWalSegmentSize() { return walSegmentSize; }
    public void setWalSegmentSize(long walSegmentSize) { this.walSegmentSize = walSegmentSize; }

    public int getWalKeepSegments() { return walKeepSegments; }
    public void setWalKeepSegments(int walKeepSegments) { this.walKeepSegments = walKeepSegments; }

    public boolean isWalPreallocateSegments() { return walPreallocateSegments; }
    public void setWalPreallocateSegments(boolean walPreallocateSegments) { this.walPreallocateSegments = walPreallocateSegments; }

    public boolean isGroupCommitEnabled() { return groupCommitEnabled; }
    public void setGroupCommitEnabled(boolean groupCommitEnabled) { this.groupCommitEnabled = groupCommitEnabled; }

    public int getGroupCommitMaxBatchSize() { return groupCommitMaxBatchSize; }
    public void setGroupCommitMaxBatchSize(int groupCommitMaxBatchSize) { this.groupCommitMaxBatchSize = groupCommitMaxBatchSize; }

    public long getGroupCommitDelayMicros() { return groupCommitDelayMicros; }
    public void setGroupCommitDelayMicros(long groupCommitDelayMicros) { this.groupCommitDelayMicros = groupCommitDelayMicros; }

    public int getLockPartitions() { return lockPartitions; }
    public void setLockPartitions(int lockPartitions) { this.lockPartitions = lockPartitions > 0 ? lockPartitions : 64; }

    /**
     * Checks whether Generational ZGC is active in the current JVM runtime.
     * In modern OpenJDK (Java 21+ and Java 25 LTS), ZGC operates in zero-pause generational mode.
     */
    public static boolean isGenerationalZgcActive() {
        for (java.lang.management.GarbageCollectorMXBean gc : java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gc.getName().toUpperCase().contains("ZGC")) {
                return true;
            }
        }
        return false;
    }

    public long getAutovacuumIntervalMs() { return autovacuumIntervalMs; }
    public void setAutovacuumIntervalMs(long autovacuumIntervalMs) { this.autovacuumIntervalMs = autovacuumIntervalMs; }

    /** Milliseconds; a statement taking at least this long gets logged at WARN. Negative (the default) disables slow-query logging entirely. */
    private long slowQueryThresholdMs = -1;
    /** Synchronous commit mode for streaming replication (default LOCAL). */
    private com.stratosdb.storage.wal.SynchronousCommitMode synchronousCommit = com.stratosdb.storage.wal.SynchronousCommitMode.LOCAL;
    /** Quorum of connected replicas required to ACK when synchronous_commit is ON/REMOTE_APPLY/QUORUM. */
    private int synchronousStandbyQuorum = 1;
    /** Maximum milliseconds to wait for synchronous replica ACK before failing commit. */
    private long synchronousReplicationTimeoutMs = 5000;
    /** Transparent Data Encryption (TDE) enabled flag (default false). */
    private boolean tdeEnabled = false;
    /** Master passphrase for PBKDF2-HMAC-SHA256 TDE key derivation (when tdeKeyFile is not set). */
    private String tdePassphrase;
    /** Optional path to external 256-bit AES key file for TDE. */
    private String tdeKeyFile;

    public boolean isTdeEnabled() { return tdeEnabled; }
    public void setTdeEnabled(boolean tdeEnabled) { this.tdeEnabled = tdeEnabled; }

    public String getTdePassphrase() { return tdePassphrase; }
    public void setTdePassphrase(String tdePassphrase) { this.tdePassphrase = tdePassphrase; }

    public String getTdeKeyFile() { return tdeKeyFile; }
    public void setTdeKeyFile(String tdeKeyFile) { this.tdeKeyFile = tdeKeyFile; }

    public com.stratosdb.storage.wal.SynchronousCommitMode getSynchronousCommit() { return synchronousCommit; }
    public void setSynchronousCommit(com.stratosdb.storage.wal.SynchronousCommitMode synchronousCommit) {
        this.synchronousCommit = synchronousCommit != null ? synchronousCommit : com.stratosdb.storage.wal.SynchronousCommitMode.LOCAL;
    }

    public int getSynchronousStandbyQuorum() { return synchronousStandbyQuorum; }
    public void setSynchronousStandbyQuorum(int synchronousStandbyQuorum) {
        this.synchronousStandbyQuorum = Math.max(1, synchronousStandbyQuorum);
    }

    public long getSynchronousReplicationTimeoutMs() { return synchronousReplicationTimeoutMs; }
    public void setSynchronousReplicationTimeoutMs(long synchronousReplicationTimeoutMs) {
        this.synchronousReplicationTimeoutMs = Math.max(100L, synchronousReplicationTimeoutMs);
    }

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
        copy.setDirectAllocation(this.directAllocation);
        copy.setWalSegmentSize(this.walSegmentSize);
        copy.setWalKeepSegments(this.walKeepSegments);
        copy.setWalPreallocateSegments(this.walPreallocateSegments);
        copy.setGroupCommitEnabled(this.groupCommitEnabled);
        copy.setGroupCommitMaxBatchSize(this.groupCommitMaxBatchSize);
        copy.setGroupCommitDelayMicros(this.groupCommitDelayMicros);
        copy.setLockPartitions(this.lockPartitions);
        return copy;
    }
}