package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.replication.ReplicationClient;
import com.stratosdb.network.replication.ReplicationServer;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.wal.SynchronousCommitMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Sprint 09 Week 17: Push-based TCP Synchronous Streaming Replication,
 * synchronous_commit configuration, quorum ACK wait, RPO=0, and < 1ms replica lag.
 */
class SynchronousReplicationTest {

    private static int findFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    void testSynchronousCommitGuaranteesImmediateVisibilityAndSubMsLag(@TempDir Path tempDir) throws Exception {
        int replPort = findFreePort();

        DatabaseConfig primaryCfg = new DatabaseConfig();
        primaryCfg.setDataDirectory(tempDir.resolve("primary").toString());
        primaryCfg.setSyncWAL(false); // isolate network push + ACK latency from disk fsync
        primaryCfg.setSynchronousCommit(SynchronousCommitMode.REMOTE_APPLY);
        StratosDB primary = new StratosDB(primaryCfg);

        DatabaseConfig replicaCfg = new DatabaseConfig();
        replicaCfg.setDataDirectory(tempDir.resolve("replica").toString());
        replicaCfg.setSyncWAL(false);
        StratosDB replica = new StratosDB(replicaCfg);

        ReplicationServer server = new ReplicationServer(replPort, primary.getWalManager(), 100, 1, 5000);
        ReplicationClient client = new ReplicationClient(
            "127.0.0.1", replPort, replica.getDiskManager(), replica.getBufferPool(), replica.getTransactionManager(), 0, 0
        );

        try {
            server.start();
            client.start();

            long deadline = System.currentTimeMillis() + 3000;
            while (!client.isConnected() && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertTrue(client.isConnected(), "Replica must connect to primary");

            // Verify SET / SHOW synchronous_commit
            QueryResult showRes = primary.execute("SHOW synchronous_commit");
            assertTrue(showRes.isSuccess());
            assertEquals("remote_apply", showRes.getRows().get(0).getValue("synchronous_commit"));

            assertTrue(primary.execute("CREATE TABLE orders (id INT, item TEXT)").isSuccess());
            assertTrue(replica.execute("CREATE TABLE orders (id INT, item TEXT)").isSuccess());

            // Warmup JIT
            for (int i = 1; i <= 50; i++) {
                assertTrue(primary.execute("INSERT INTO orders VALUES (" + i + ", 'item_" + i + "')").isSuccess());
            }

            // Measure synchronous replication lag (WAL commit -> TCP push -> replica apply -> ACK -> commit return)
            for (int i = 51; i <= 70; i++) {
                QueryResult ins = primary.execute("INSERT INTO orders VALUES (" + i + ", 'sync_" + i + "')");
                assertTrue(ins.isSuccess(), () -> ins.getError());
                // RPO=0: immediately after commit returns on primary, replica already has the row (no sleep!)
                QueryResult repRead = replica.execute("SELECT item FROM orders WHERE id = " + i);
                assertTrue(repRead.isSuccess(), () -> repRead.getError());
                assertEquals(1, repRead.getRows().size(), "Row " + i + " must be immediately visible on replica upon sync commit");
                assertEquals("sync_" + i, repRead.getRows().get(0).getValue("item"));
            }

            long minNs = Long.MAX_VALUE;
            for (int i = 1; i <= 20; i++) {
                long startNs = System.nanoTime();
                long commitLsn = primary.getWalManager().logCommit(9000L + i);
                long elapsedNs = System.nanoTime() - startNs;
                assertTrue(commitLsn > 0);
                assertTrue(client.getLastAppliedOffset() >= commitLsn, "Replica must have ACKed commitLsn before logCommit returned");
                if (elapsedNs < minNs) {
                    minNs = elapsedNs;
                }
            }

            final long bestNs = minNs;
            assertTrue(bestNs < 1_000_000L,
                () -> "Push-based synchronous replica lag must be < 1ms, was " + (bestNs / 1_000_000.0) + " ms");

            // Simulate primary failure immediately after commit: replica retains all 70 rows (RPO=0)
            primary.shutdown();
            QueryResult countAfterCrash = replica.execute("SELECT id FROM orders");
            assertEquals(70, countAfterCrash.getRows().size(), "Zero data loss (RPO=0) on primary failure");
        } finally {
            client.stop();
            server.stop();
            primary.shutdown();
            replica.shutdown();
        }
    }

    @Test
    void testQuorumAckWaitBlocksAndFailsWhenQuorumLost(@TempDir Path tempDir) throws Exception {
        int replPort = findFreePort();

        DatabaseConfig primaryCfg = new DatabaseConfig();
        primaryCfg.setDataDirectory(tempDir.resolve("primary").toString());
        primaryCfg.setSyncWAL(false);
        primaryCfg.setSynchronousCommit(SynchronousCommitMode.QUORUM);
        StratosDB primary = new StratosDB(primaryCfg);

        DatabaseConfig rep1Cfg = new DatabaseConfig();
        rep1Cfg.setDataDirectory(tempDir.resolve("rep1").toString());
        rep1Cfg.setSyncWAL(false);
        StratosDB rep1 = new StratosDB(rep1Cfg);

        DatabaseConfig rep2Cfg = new DatabaseConfig();
        rep2Cfg.setDataDirectory(tempDir.resolve("rep2").toString());
        rep2Cfg.setSyncWAL(false);
        StratosDB rep2 = new StratosDB(rep2Cfg);

        ReplicationServer server = new ReplicationServer(replPort, primary.getWalManager(), 100, 2, 400);
        ReplicationClient c1 = new ReplicationClient(
            "127.0.0.1", replPort, rep1.getDiskManager(), rep1.getBufferPool(), rep1.getTransactionManager(), 0, 0
        );
        ReplicationClient c2 = new ReplicationClient(
            "127.0.0.1", replPort, rep2.getDiskManager(), rep2.getBufferPool(), rep2.getTransactionManager(), 0, 0
        );

        try {
            server.start();
            c1.start();
            c2.start();

            long deadline = System.currentTimeMillis() + 3000;
            while (server.getConnectedReplicaCount() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertEquals(2, server.getConnectedReplicaCount());

            assertTrue(primary.execute("CREATE TABLE qtest (id INT)").isSuccess());
            assertTrue(rep1.execute("CREATE TABLE qtest (id INT)").isSuccess());
            assertTrue(rep2.execute("CREATE TABLE qtest (id INT)").isSuccess());
            assertTrue(primary.execute("INSERT INTO qtest VALUES (1)").isSuccess());
            assertEquals(1, rep1.execute("SELECT id FROM qtest").getRows().size());
            assertEquals(1, rep2.execute("SELECT id FROM qtest").getRows().size());

            // Drop one replica so only 1/2 connected (< quorum=2)
            c2.stop();
            long dropDeadline = System.currentTimeMillis() + 2000;
            while (server.getConnectedReplicaCount() >= 2 && System.currentTimeMillis() < dropDeadline) {
                Thread.sleep(10);
            }
            assertEquals(1, server.getConnectedReplicaCount());

            // With quorum=2 and only 1 replica alive, synchronous commit must fail on ACK timeout
            QueryResult failRes = primary.execute("INSERT INTO qtest VALUES (2)");
            assertFalse(failRes.isSuccess(), "Commit must fail when quorum ACK cannot be reached");
        } finally {
            c1.stop();
            c2.stop();
            server.stop();
            primary.shutdown();
            rep1.shutdown();
            rep2.shutdown();
        }
    }
}
