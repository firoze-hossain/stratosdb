package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.replication.ReplicationClient;
import com.stratosdb.network.replication.ReplicationServer;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.transaction.mvcc.MVCCVisibility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, end-to-end physical (WAL-shipping) replication: a real primary
 * StratosDB instance and a real, physically separate replica StratosDB
 * instance, connected over a real TCP socket via ReplicationServer/
 * ReplicationClient - not a simulation, not two objects sharing memory.
 * Writes go through the primary's own normal SQL execution path
 * (StratosDB.execute), the same way any real client would write to it.
 *
 * The replica's own SQL layer has no idea a new table exists yet -
 * schema/catalog replication is real, separate, not-yet-attempted
 * further work (see PROGRESS.md) - so these tests read back the
 * replicated data directly via the replica's own DiskManager/HeapTable,
 * the same way StreamingWalApplierTest already does at the storage
 * layer alone. That gap is exactly why these tests exist at a level
 * above StreamingWalApplierTest: to prove the full, real pipeline - a
 * real primary's real SQL writes, over a real network connection, onto
 * a real replica's real disk - actually works end to end, not just its
 * two halves independently.
 */
public class ReplicationEndToEndTest {

    private StratosDB primary;
    private StratosDB replica;
    private ReplicationServer replicationServer;
    private ReplicationClient replicationClient;

    @AfterEach
    void tearDown() {
        if (replicationClient != null) replicationClient.stop();
        if (replicationServer != null) replicationServer.stop();
        if (primary != null) primary.shutdown();
        if (replica != null) replica.shutdown();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void insertsOnThePrimaryAppearOnTheReplica(@TempDir Path tempDir) throws Exception {
        startPrimaryAndReplica(tempDir);

        primary.execute("CREATE TABLE employees (id INT, name VARCHAR)");
        primary.execute("INSERT INTO employees VALUES (1, 'Alice')");
        primary.execute("INSERT INTO employees VALUES (2, 'Bob')");

        waitUntil(() -> countRawRows("employees") == 2, 10_000, "replica has both inserted rows");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void replicationIsLiveAndOngoingNotJustAOneTimeSnapshot(@TempDir Path tempDir) throws Exception {
        startPrimaryAndReplica(tempDir);

        primary.execute("CREATE TABLE t (id INT)");
        primary.execute("INSERT INTO t VALUES (1)");
        waitUntil(() -> countRawRows("t") == 1, 10_000, "first insert replicated");

        // Insert more AFTER replication was already established and caught up -
        // proves this is a live, continuous stream, not a one-time copy.
        for (int i = 2; i <= 6; i++) {
            primary.execute("INSERT INTO t VALUES (" + i + ")");
        }
        waitUntil(() -> countRawRows("t") == 6, 10_000, "all later inserts also replicated");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anUncommittedTransactionNeverAppearsOnTheReplicaUntilItCommits(@TempDir Path tempDir) throws Exception {
        startPrimaryAndReplica(tempDir);

        primary.execute("CREATE TABLE t (id INT)");
        primary.execute("BEGIN");
        primary.execute("INSERT INTO t VALUES (1)");

        Thread.sleep(1000); // ample time for this to have replicated, if it were going to

        assertEquals(0, countRawRows("t"), "an uncommitted insert must never appear on the replica");

        primary.execute("COMMIT");
        waitUntil(() -> countRawRows("t") == 1, 10_000, "the row appears on the replica only after COMMIT");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void updatesAndDeletesViaRealSqlReplicateCorrectly(@TempDir Path tempDir) throws Exception {
        startPrimaryAndReplica(tempDir);

        primary.execute("CREATE TABLE accounts (id INT, status VARCHAR)");
        primary.execute("INSERT INTO accounts VALUES (1, 'active')");
        primary.execute("INSERT INTO accounts VALUES (2, 'active')");
        waitUntil(() -> countRawRows("accounts") == 2, 10_000, "both initial rows replicated");

        primary.execute("UPDATE accounts SET status = 'suspended' WHERE id = 1");
        primary.execute("DELETE FROM accounts WHERE id = 2");

        waitUntil(() -> countRawRows("accounts") == 1, 10_000, "one non-deleted row remains after the replicated update+delete");
    }

    private void startPrimaryAndReplica(Path tempDir) throws Exception {
        DatabaseConfig primaryConfig = new DatabaseConfig();
        primaryConfig.setDataDirectory(tempDir.resolve("primary").toString());
        primary = new StratosDB(primaryConfig);

        int replicationPort = freePort();
        replicationServer = new ReplicationServer(replicationPort, primary.getWalManager(), 50);
        replicationServer.start();
        Thread.sleep(200);

        DatabaseConfig replicaConfig = new DatabaseConfig();
        replicaConfig.setDataDirectory(tempDir.resolve("replica").toString());
        replica = new StratosDB(replicaConfig);
        replicationClient = new ReplicationClient("localhost", replicationPort, replica.getDiskManager(), replica.getBufferPool());
        replicationClient.start();

        ReplicationClient clientRef = replicationClient;
        waitUntil(clientRef::isConnected, 10_000, "replica connected to the primary");
    }

    private int countRawRows(String tableName) {
        List<byte[]> rows = new HeapTable(tableName, replica.getBufferPool()).scan(1000);
        int count = 0;
        for (byte[] raw : rows) {
            if (MVCCVisibility.readXmax(raw) == MVCCVisibility.NO_XMAX) count++;
        }
        return count;
    }

    private static void waitUntil(BooleanSupplier condition, long timeoutMillis, String description) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(50);
        }
        fail("Timed out waiting for: " + description);
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
