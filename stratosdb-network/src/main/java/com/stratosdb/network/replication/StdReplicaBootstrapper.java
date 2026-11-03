package com.stratosdb.network.replication;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Automates bootstrapping a new StratosDB replica over the network from a running primary.
 *
 * Encapsulates the complete workflow:
 *  1. Connects to the primary via {@link StdBaseBackupClient} over PostgreSQL wire protocol.
 *  2. Issues {@code BASE_BACKUP (LABEL '...', FAST, WAL)} and streams the data directory tarball.
 *  3. Unpacks the data directory locally into an empty target directory (no manual file copying).
 *  4. Initializes the replica {@link StratosDB} instance with full system catalogs and tables.
 *  5. Places the replica in enforced read-only mode ({@link StratosDB#setReadOnly(boolean)}).
 *  6. Starts {@link ReplicationClient} resuming from the exact starting epoch and offset of the backup,
 *     establishing live, continuous WAL replication.
 *  7. Optionally starts a {@link StdWireServer} listener for read-only replica queries and promotion.
 */
public final class StdReplicaBootstrapper {
    private static final Logger LOG = LoggerFactory.getLogger(StdReplicaBootstrapper.class);

    private StdReplicaBootstrapper() {}

    public record ReplicaBootstrapConfig(
        String primaryHost,
        int primaryStdWirePort,
        int primaryReplicationPort,
        Path replicaDataDir,
        int replicaStdWirePort,
        String user,
        String database,
        String password,
        String label,
        boolean fast
    ) {
        public ReplicaBootstrapConfig(String primaryHost, int primaryStdWirePort, int primaryReplicationPort, Path replicaDataDir) {
            this(primaryHost, primaryStdWirePort, primaryReplicationPort, replicaDataDir, 0, "stratos", null, null, "replica_bootstrap", true);
        }

        public ReplicaBootstrapConfig(String primaryHost, int primaryStdWirePort, int primaryReplicationPort, Path replicaDataDir, int replicaStdWirePort) {
            this(primaryHost, primaryStdWirePort, primaryReplicationPort, replicaDataDir, replicaStdWirePort, "stratos", null, null, "replica_bootstrap", true);
        }
    }

    public static final class BootstrappedReplica implements AutoCloseable {
        private final StratosDB replicaDb;
        private final ReplicationClient replicationClient;
        private final StdWireServer stdWireServer;
        private final StdBaseBackupClient.StdBaseBackupResult backupResult;

        public BootstrappedReplica(
            StratosDB replicaDb,
            ReplicationClient replicationClient,
            StdWireServer stdWireServer,
            StdBaseBackupClient.StdBaseBackupResult backupResult
        ) {
            this.replicaDb = replicaDb;
            this.replicationClient = replicationClient;
            this.stdWireServer = stdWireServer;
            this.backupResult = backupResult;
        }

        public StratosDB getReplicaDb() {
            return replicaDb;
        }

        public ReplicationClient getReplicationClient() {
            return replicationClient;
        }

        public StdWireServer getStdWireServer() {
            return stdWireServer;
        }

        public StdBaseBackupClient.StdBaseBackupResult getBackupResult() {
            return backupResult;
        }

        /**
         * Promotes this replica to become a primary: stops continuous replication
         * streaming, unlinks replication client from the wire server, and enables
         * write transactions on the underlying StratosDB instance.
         */
        public synchronized void promote() {
            if (replicationClient != null) {
                try {
                    replicationClient.stop();
                } catch (Exception ignored) {}
            }
            if (stdWireServer != null) {
                stdWireServer.setReplicationClient(null);
            }
            if (replicaDb != null) {
                replicaDb.setReadOnly(false);
            }
        }

        @Override
        public void close() {
            if (stdWireServer != null) {
                try {
                    stdWireServer.stop();
                } catch (Exception ignored) {}
            }
            if (replicationClient != null) {
                try {
                    replicationClient.stop();
                } catch (Exception ignored) {}
            }
            if (replicaDb != null) {
                try {
                    replicaDb.shutdown();
                } catch (Exception ignored) {}
            }
        }
    }

    /**
     * Bootstraps a new standby replica node over the network from the specified primary.
     */
    public static BootstrappedReplica bootstrap(ReplicaBootstrapConfig config) throws IOException {
        LOG.info("Bootstrapping replica in {} from primary at {}:{}",
            config.replicaDataDir(), config.primaryHost(), config.primaryStdWirePort());

        Files.createDirectories(config.replicaDataDir());

        // 1. Take base backup over the network
        StdBaseBackupClient.StdBaseBackupResult backupResult;
        try (StdBaseBackupClient backupClient = new StdBaseBackupClient(
                config.primaryHost(),
                config.primaryStdWirePort(),
                config.user(),
                config.database(),
                config.password())) {

            backupClient.connect();
            StdBaseBackupOptions options = new StdBaseBackupOptions(
                config.label(),
                config.fast(),
                true,   // wal
                false,  // progress
                false,  // noWait
                0,      // maxRate
                false,  // tablespaceMap
                false   // noVerifyChecksums
            );
            backupResult = backupClient.executeBaseBackup(config.replicaDataDir(), options);
        }

        LOG.info("Network base backup complete. Start pos: epoch={} lsn={}, extracted {} files",
            backupResult.startEpoch(), backupResult.startLsn(), backupResult.filesExtracted());

        // 2. Initialize replica StratosDB instance
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setDataDirectory(config.replicaDataDir().toString());
        if (config.replicaStdWirePort() > 0) {
            dbConfig.setPort(config.replicaStdWirePort());
        }
        StratosDB replicaDb = new StratosDB(dbConfig);
        replicaDb.setReadOnly(true);

        // 3. Start replication client to stream continuous WAL updates from primary
        ReplicationClient replicationClient = new ReplicationClient(
            config.primaryHost(),
            config.primaryReplicationPort(),
            replicaDb.getDiskManager(),
            replicaDb.getBufferPool(),
            replicaDb.getTransactionManager(),
            backupResult.startEpoch(),
            backupResult.startLsn()
        );
        replicationClient.start();

        // 4. Optionally start stdwire server for replica read queries
        StdWireServer wireServer = null;
        if (config.replicaStdWirePort() > 0) {
            wireServer = new StdWireServer(config.replicaStdWirePort(), replicaDb);
            wireServer.setReplicationClient(replicationClient);
            wireServer.start();
        }

        LOG.info("Replica bootstrapped and replication client active from primary {}:{}",
            config.primaryHost(), config.primaryReplicationPort());

        return new BootstrappedReplica(replicaDb, replicationClient, wireServer, backupResult);
    }
}
