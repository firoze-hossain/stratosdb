package com.stratosdb.testing;

import com.stratosdb.cli.PitrBackup;
import com.stratosdb.cli.PitrRestore;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireServer;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.wal.WalArchiveSink;
import com.stratosdb.storage.wal.WalArchiver;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Sprint 12 Week 23: Continuous WAL Archiving (local & S3)
 * and Microsecond-Precision PITR Restore before accidental DROP TABLE / DELETE.
 */
class ContinuousArchivingAndPitrTest {

    private static int findFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    void testAutomatedWalArchiverToS3SinkAndLocalSink(@TempDir Path tempDir) throws Exception {
        Map<String, byte[]> s3Objects = new ConcurrentHashMap<>();
        HttpServer s3Server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        s3Server.createContext("/stratos-backups", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            if ("PUT".equalsIgnoreCase(method)) {
                byte[] body = exchange.getRequestBody().readAllBytes();
                s3Objects.put(path, body);
                exchange.sendResponseHeaders(200, 0);
                exchange.close();
            } else if ("GET".equalsIgnoreCase(method)) {
                String query = exchange.getRequestURI().getQuery();
                if (query != null && query.startsWith("prefix=")) {
                    String list = String.join("\n", s3Objects.keySet());
                    byte[] resp = list.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, resp.length);
                    exchange.getResponseBody().write(resp);
                    exchange.close();
                } else {
                    byte[] data = s3Objects.get(path);
                    if (data == null) {
                        exchange.sendResponseHeaders(404, 0);
                    } else {
                        exchange.sendResponseHeaders(200, data.length);
                        exchange.getResponseBody().write(data);
                    }
                    exchange.close();
                }
            }
        });
        s3Server.start();

        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataDirectory(tempDir.resolve("db").toString());
        StratosDB db = new StratosDB(cfg);

        int s3Port = s3Server.getAddress().getPort();
        WalArchiveSink s3Sink = new WalArchiveSink.S3WalArchiveSink(
            "http://127.0.0.1:" + s3Port, "stratos-backups", "wal", "AKIA_TEST", "SECRET_TEST"
        );
        WalArchiver archiver = new WalArchiver(db.getWalManager(), s3Sink);
        try {
            archiver.start(50);
            assertTrue(db.execute("CREATE TABLE logs (id INT, msg TEXT)").isSuccess());
            assertTrue(db.execute("INSERT INTO logs VALUES (1, 'archived_to_s3')").isSuccess());

            long deadline = System.currentTimeMillis() + 3000;
            while (s3Sink.listSegments().isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(30);
            }
            var uploaded = s3Sink.listSegments();
            assertFalse(uploaded.isEmpty(), "Automated WalArchiver must upload WAL segments to S3 sink");
            byte[] downloaded = s3Sink.downloadSegment(uploaded.get(0));
            assertTrue(downloaded.length > 0);
        } finally {
            archiver.stop();
            db.shutdown();
            s3Server.stop(0);
        }
    }

    @Test
    void testMicrosecondPitrRestoreRecovers100PercentBeforeAccidentalDropTable(@TempDir Path tempDir) throws Exception {
        Path dataDir = tempDir.resolve("data");
        Path archiveDir = tempDir.resolve("archive");
        Path backupDir = tempDir.resolve("backup");
        Path restoredDir = tempDir.resolve("restored");

        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataDirectory(dataDir.toString());
        StratosDB db = new StratosDB(cfg);
        db.getWalManager().setWalArchiveDirectory(archiveDir.toString());

        int wirePort = findFreePort();
        StdWireServer wireServer = new StdWireServer(wirePort, db);
        wireServer.start();

        Instant beforeDrop;
        try {
            assertTrue(db.execute("CREATE TABLE critical_orders (id INT, amount INT)").isSuccess());
            for (int i = 1; i <= 50; i++) {
                assertTrue(db.execute("INSERT INTO critical_orders VALUES (" + i + ", " + (i * 10) + ")").isSuccess());
            }

            // Take online base backup via PitrBackup
            PitrBackup.main(new String[]{
                "-h", "127.0.0.1",
                "-p", String.valueOf(wirePort),
                "-U", "admin",
                "-d", "stratos",
                "--data-dir", dataDir.toString(),
                "--archive-dir", archiveDir.toString(),
                "--backup-dir", backupDir.toString()
            });

            // Insert 50 more rows AFTER the base backup
            for (int i = 51; i <= 100; i++) {
                assertTrue(db.execute("INSERT INTO critical_orders VALUES (" + i + ", " + (i * 10) + ")").isSuccess());
            }

            Thread.sleep(10);
            beforeDrop = Instant.now();
            Thread.sleep(20);

            // Simulate accidental destructive operation after target timestamp
            assertTrue(db.execute("DELETE FROM critical_orders").isSuccess());
            assertTrue(db.execute("DROP TABLE critical_orders").isSuccess());
            assertTrue(db.execute("CHECKPOINT").isSuccess());
        } finally {
            wireServer.stop();
            db.shutdown();
        }

        // Restore to microsecond timestamp right before the accidental DROP TABLE
        PitrRestore.main(new String[]{
            "--backup-dir", backupDir.toString(),
            "--archive-dir", archiveDir.toString(),
            "--target-dir", restoredDir.toString(),
            "--target-time", beforeDrop.toString()
        });

        DatabaseConfig restoredCfg = new DatabaseConfig();
        restoredCfg.setDataDirectory(restoredDir.toString());
        StratosDB restoredDb = new StratosDB(restoredCfg);
        try {
            QueryResult res = restoredDb.execute("SELECT id FROM critical_orders");
            assertTrue(res.isSuccess(), () -> "Restored table must exist: " + res.getError());
            assertEquals(100, res.getRows().size(), "100% of all 100 rows must be recovered by PITR");
        } finally {
            restoredDb.shutdown();
        }
    }
}
