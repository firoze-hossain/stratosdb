package com.stratosdb.testing;

import com.stratosdb.cli.StdBaseBackup;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.replication.ReplicationServer;
import com.stratosdb.network.replication.StdBaseBackupClient;
import com.stratosdb.network.replication.StdBaseBackupOptions;
import com.stratosdb.network.replication.StdReplicaBootstrapper;
import com.stratosdb.network.stdwire.StdWireServer;
import com.stratosdb.sql.executor.QueryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests for Sprint 11 Week 21: Networked Base Backup Protocol.
 *
 * Verifies that:
 *  1. IDENTIFY_SYSTEM replication command returns valid cluster identity, timeline, and WAL LSN.
 *  2. BASE_BACKUP streams raw POSIX tarball of primary data directory over pgwire CopyOut/CopyData.
 *  3. A fresh standby replica node bootstraps automatically over the network into an empty directory
 *     with zero manual file copying.
 *  4. Standby initializes in enforced read-only mode with full schema catalogs and tables accessible.
 *  5. Standby establishes live, continuous WAL replication from primary, seamlessly reflecting
 *     subsequent INSERT, UPDATE, and DELETE operations.
 *  6. PROMOTE turns the standby replica into a standalone primary accepting writes.
 *  7. std_basebackup CLI executes cleanly against a running server.
 */
public class NetworkedBaseBackupEndToEndTest {

    private StratosDB primaryDb;
    private StdWireServer primaryWireServer;
    private ReplicationServer primaryReplicationServer;
    private StdReplicaBootstrapper.BootstrappedReplica replicaHandle;

    @AfterEach
    void tearDown() {
        if (replicaHandle != null) {
            replicaHandle.close();
            replicaHandle = null;
        }
        if (primaryWireServer != null) {
            primaryWireServer.stop();
            primaryWireServer = null;
        }
        if (primaryReplicationServer != null) {
            primaryReplicationServer.stop();
            primaryReplicationServer = null;
        }
        if (primaryDb != null) {
            primaryDb.shutdown();
            primaryDb = null;
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void identifySystemCommandOverWire(@TempDir Path tempDir) throws Exception {
        int stdWirePort = freePort();
        startPrimary(tempDir, stdWirePort, freePort());

        try (StdBaseBackupClient client = new StdBaseBackupClient("localhost", stdWirePort)) {
            client.connect();
            StdBaseBackupClient.IdentifySystemResult idResult = client.identifySystem();

            assertNotNull(idResult);
            assertNotNull(idResult.systemId());
            assertFalse(idResult.systemId().isBlank());
            assertEquals(primaryDb.getWalManager().getWalEpoch(), idResult.timeline());
            assertNotNull(idResult.xlogpos());
            assertTrue(idResult.xlogpos().contains("/"));
            assertEquals("stratos", idResult.dbname());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void baseBackupStreamsTarballOverNetwork(@TempDir Path tempDir) throws Exception {
        int stdWirePort = freePort();
        startPrimary(tempDir, stdWirePort, freePort());

        primaryDb.execute("CREATE TABLE products (id INT, name VARCHAR, price DOUBLE)");
        primaryDb.execute("INSERT INTO products VALUES (1, 'Laptop', 1299.99)");
        primaryDb.execute("INSERT INTO products VALUES (2, 'Mouse', 29.50)");
        primaryDb.execute("CREATE INDEX idx_products_id ON products (id)");

        Path backupDir = tempDir.resolve("network_backup");

        try (StdBaseBackupClient client = new StdBaseBackupClient("localhost", stdWirePort)) {
            client.connect();
            StdBaseBackupOptions options = new StdBaseBackupOptions(
                "backup_test_label",
                true,  // fast
                true,  // wal
                false, // progress
                false, // noWait
                0,     // maxRate
                false,
                false
            );

            StdBaseBackupClient.StdBaseBackupResult result = client.executeBaseBackup(backupDir, options);

            assertNotNull(result);
            assertEquals("backup_test_label", result.label());
            assertTrue(result.filesExtracted() > 0);
            assertTrue(Files.exists(backupDir.resolve("backup_label")));
            assertTrue(Files.exists(backupDir.resolve("products.dat")));
            assertTrue(Files.exists(backupDir.resolve("idx_products_id.dat")));
            assertTrue(Files.exists(backupDir.resolve("stratos_tables.dat")));

            String labelContent = Files.readString(backupDir.resolve("backup_label"));
            assertTrue(labelContent.contains("START WAL LOCATION:"));
            assertTrue(labelContent.contains("backup_test_label"));
        }
    }

    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void standbyBootstrapsAndReplicatesContinuously(@TempDir Path tempDir) throws Exception {
        int primaryWirePort = freePort();
        int primaryReplPort = freePort();
        startPrimary(tempDir, primaryWirePort, primaryReplPort);

        // 1. Initial writes on primary before backup
        primaryDb.execute("CREATE TABLE accounts (id INT, owner VARCHAR, balance DOUBLE)");
        primaryDb.execute("INSERT INTO accounts VALUES (101, 'Alice', 1000.50)");
        primaryDb.execute("INSERT INTO accounts VALUES (102, 'Bob', 2500.00)");

        // 2. Bootstrap replica completely over the network
        Path replicaDir = tempDir.resolve("replica_data");
        int replicaWirePort = freePort();

        StdReplicaBootstrapper.ReplicaBootstrapConfig config = new StdReplicaBootstrapper.ReplicaBootstrapConfig(
            "localhost",
            primaryWirePort,
            primaryReplPort,
            replicaDir,
            replicaWirePort
        );

        replicaHandle = StdReplicaBootstrapper.bootstrap(config);
        assertNotNull(replicaHandle);

        StratosDB replicaDb = replicaHandle.getReplicaDb();
        assertTrue(replicaDb.isReadOnly(), "Bootstrapped replica must start in read-only mode");

        // 3. Query replica directly via SQL: initial data must be present
        QueryResult initialQuery = replicaDb.execute("SELECT * FROM accounts ORDER BY id");
        assertTrue(initialQuery.isSuccess(), "Replica SQL query should succeed");
        assertEquals(2, initialQuery.getRows().size(), "Replica should have 2 rows from base backup");
        assertEquals("Alice", initialQuery.getRows().get(0).getValue("owner"));
        assertEquals("Bob", initialQuery.getRows().get(1).getValue("owner"));

        // 4. Verify read-only enforcement
        QueryResult insertAttempt = replicaDb.execute("INSERT INTO accounts VALUES (103, 'Charlie', 500.0)");
        assertFalse(insertAttempt.isSuccess(), "Insert on read-only replica must be rejected");
        assertTrue(insertAttempt.getError().toLowerCase().contains("read-only"));

        // 5. Perform writes on primary while replica is actively following
        QueryResult insRes = primaryDb.execute("INSERT INTO accounts VALUES (103, 'Charlie', 350.0)");
        assertTrue(insRes.isSuccess(), "Insert should succeed: " + insRes.getError());
        QueryResult updRes = primaryDb.execute("UPDATE accounts SET balance = 1500.5 WHERE id = 101");
        assertTrue(updRes.isSuccess(), "Update should succeed: " + updRes.getError());
        QueryResult delRes = primaryDb.execute("DELETE FROM accounts WHERE id = 102");
        assertTrue(delRes.isSuccess(), "Delete should succeed: " + delRes.getError());

        // 6. Wait for replication stream to apply changes to replica
        waitUntil(() -> {
            QueryResult res = replicaDb.execute("SELECT * FROM accounts ORDER BY id");
            long appliedOffset = replicaHandle.getReplicationClient().getLastAppliedOffset();
            long opsApplied = replicaHandle.getReplicationClient().getApplier().getTotalOpsApplied();
            System.out.printf("CHECK: rows=%d, appliedOffset=%d, opsApplied=%d%n",
                res.getRows() != null ? res.getRows().size() : -1, appliedOffset, opsApplied);
            if (res.isSuccess() && res.getRows() != null) {
                for (int i = 0; i < res.getRows().size(); i++) {
                    System.out.println("  row[" + i + "]: " + res.getRows().get(i).getValues() + ", names=" + res.getRows().get(i).getColumnNames());
                }
            }
            if (!res.isSuccess() || res.getRows().size() != 2) return false;
            Object bal101 = res.getRows().get(0).getValue("balance");
            Object owner103 = res.getRows().get(1).getValue("owner");
            return "Charlie".equals(owner103) && (bal101 instanceof Number n && Math.abs(n.doubleValue() - 1500.5) < 0.001);
        }, 15_000, "Replica caught up with continuous WAL updates");

        // 7. Test replica promotion to new primary
        replicaHandle.promote();
        assertFalse(replicaDb.isReadOnly(), "Replica must no longer be read-only after promotion");

        // 8. Verify promoted replica now accepts writes
        QueryResult postPromoteInsert = replicaDb.execute("INSERT INTO accounts VALUES (104, 'Diana', 999.0)");
        assertTrue(postPromoteInsert.isSuccess(), "Promoted replica should now accept writes");

        QueryResult finalQuery = replicaDb.execute("SELECT * FROM accounts ORDER BY id");
        assertEquals(3, finalQuery.getRows().size(), "Promoted replica should now have 3 rows");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void cliStdBaseBackupExecution(@TempDir Path tempDir) throws Exception {
        int stdWirePort = freePort();
        startPrimary(tempDir, stdWirePort, freePort());

        primaryDb.execute("CREATE TABLE orders (id INT, product VARCHAR)");
        primaryDb.execute("INSERT INTO orders VALUES (1, 'Book')");

        Path cliTargetDir = tempDir.resolve("cli_backup");
        String[] args = {
            "-h", "localhost",
            "-p", String.valueOf(stdWirePort),
            "-D", cliTargetDir.toString(),
            "--label", "cli_test_backup",
            "--fast",
            "--wal"
        };

        StdBaseBackup.main(args);

        assertTrue(Files.exists(cliTargetDir.resolve("orders.dat")));
        assertTrue(Files.exists(cliTargetDir.resolve("backup_label")));
        assertTrue(Files.exists(cliTargetDir.resolve("stratos_tables.dat")));
    }

    private void startPrimary(Path tempDir, int wirePort, int replPort) throws Exception {
        DatabaseConfig primaryConfig = new DatabaseConfig();
        primaryConfig.setDataDirectory(tempDir.resolve("primary_data").toString());
        primaryDb = new StratosDB(primaryConfig);

        primaryReplicationServer = new ReplicationServer(replPort, primaryDb.getWalManager(), 50);
        primaryReplicationServer.start();

        primaryWireServer = new StdWireServer(wirePort, primaryDb);
        primaryWireServer.start();
        Thread.sleep(150);
    }

    private static void waitUntil(BooleanSupplier condition, long timeoutMillis, String description) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(100);
        }
        fail("Timed out waiting for: " + description);
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
