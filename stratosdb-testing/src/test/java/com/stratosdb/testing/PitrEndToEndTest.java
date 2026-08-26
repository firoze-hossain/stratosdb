package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.auth.UserStore;
import com.stratosdb.network.stdwire.StdWireServer;
import com.stratosdb.sql.executor.QueryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, end-to-end proof that point-in-time recovery actually works:
 * real WAL archiving (see WALManager.setWalArchiveDirectory's own
 * javadoc for why this had to exist at all - this engine's own WAL is
 * truncated to zero the moment it's safely reflected on disk, so
 * without archiving there would be no continuous history to replay at
 * all), a real base backup taken by the real PitrBackup tool
 * connecting over the real wire protocol, and a real point-in-time
 * restore by the real PitrRestore tool - stopping replay exactly at a
 * requested target time, correctly excluding a transaction that
 * committed after it while correctly including one that committed
 * before it.
 *
 * Both tools are invoked as real, separate processes (see
 * runProcess's own javadoc) - not called as in-process library
 * methods - because that's genuinely how a real operator would use
 * them, and because the actual, real bug this feature's own first
 * draft had (see PROGRESS.md: a restored database's fresh
 * TransactionManager never recognized any replayed transaction as
 * committed at all, since PitrRestore never updated the restored
 * directory's own persisted commit log or xid watermark) was only
 * ever caught by running this real, full round trip - every isolated
 * piece looked correct on its own.
 */
public class PitrEndToEndTest {

    private StratosDB db;
    private StdWireServer server;
    private StratosDB restoredDb;
    private StratosDB restoredAllDb;

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
        if (restoredDb != null) restoredDb.shutdown();
        if (restoredAllDb != null) restoredAllDb.shutdown();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void baseBackupPlusArchivedWalRestoresCorrectlyToATargetTime(@TempDir Path tempDir) throws Exception {
        Path dataDir = tempDir.resolve("data");
        Path archiveDir = tempDir.resolve("archive");
        Path backupDir = tempDir.resolve("backup");
        Path restoreDir = tempDir.resolve("restored");
        Path restoreAllDir = tempDir.resolve("restored_all");

        int port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(dataDir.toString());
        db = new StratosDB(config);
        db.getWalManager().setWalArchiveDirectory(archiveDir.toString());

        UserStore userStore = new UserStore();
        userStore.addUser("admin", "adminpass");
        server = new StdWireServer(port, db, userStore);
        server.start();
        Thread.sleep(200);

        db.setCurrentUser("admin");
        db.execute("CREATE ROLE admin WITH LOGIN SUPERUSER");
        db.execute("CREATE TABLE events (id INT, name VARCHAR)");
        db.execute("INSERT INTO events VALUES (1, 'before-backup')");

        // A real base backup, taken by the real tool as a real, separate process.
        int backupExit = runProcess(new String[]{
            "com.stratosdb.cli.PitrBackup",
            "-h", "localhost", "-p", String.valueOf(port), "-U", "admin", "-d", "anydb",
            "--data-dir", dataDir.toString(), "--archive-dir", archiveDir.toString(), "--backup-dir", backupDir.toString()
        }, "adminpass");
        assertEquals(0, backupExit, "PitrBackup must exit cleanly");
        assertTrue(java.nio.file.Files.exists(backupDir.resolve("backup_label")), "backup_label must be created");

        db.execute("INSERT INTO events VALUES (2, 'after-backup-before-target')");
        Instant targetTime = Instant.now();
        Thread.sleep(50);
        db.execute("INSERT INTO events VALUES (3, 'the-bad-event')");
        db.execute("CHECKPOINT");

        server.stop();
        db.shutdown();
        db = null;
        server = null;

        // Restore to a point in time strictly before "the-bad-event" committed.
        int restoreExit = runProcess(new String[]{
            "com.stratosdb.cli.PitrRestore",
            "--backup-dir", backupDir.toString(), "--archive-dir", archiveDir.toString(),
            "--target-dir", restoreDir.toString(), "--target-time", targetTime.toString()
        }, null);
        assertEquals(0, restoreExit, "PitrRestore must exit cleanly");

        DatabaseConfig restoredConfig = new DatabaseConfig();
        restoredConfig.setDataDirectory(restoreDir.toString());
        restoredDb = new StratosDB(restoredConfig);
        QueryResult result = restoredDb.execute("SELECT * FROM events");
        assertTrue(result.isSuccess(), () -> "SELECT on the restored database must succeed: " + result.getError());
        assertEquals(2, result.getRows().size(), "restored database must have exactly the 2 events committed before the target time");
        assertTrue(result.getRows().stream().anyMatch(r -> r.getValue("id").equals(1)), "event 1 (before the backup) must be present");
        assertTrue(result.getRows().stream().anyMatch(r -> r.getValue("id").equals(2)), "event 2 (after the backup, before the target time) must be present");
        assertTrue(result.getRows().stream().noneMatch(r -> r.getValue("id").equals(3)), "event 3 (committed after the target time) must be correctly excluded");

        // Restore with no target time at all - everything available must be included.
        int restoreAllExit = runProcess(new String[]{
            "com.stratosdb.cli.PitrRestore",
            "--backup-dir", backupDir.toString(), "--archive-dir", archiveDir.toString(),
            "--target-dir", restoreAllDir.toString()
        }, null);
        assertEquals(0, restoreAllExit);

        DatabaseConfig restoredAllConfig = new DatabaseConfig();
        restoredAllConfig.setDataDirectory(restoreAllDir.toString());
        restoredAllDb = new StratosDB(restoredAllConfig);
        QueryResult resultAll = restoredAllDb.execute("SELECT * FROM events");
        assertEquals(3, resultAll.getRows().size(), "restore with no target time must include every event, including the one after the (unused) target time");
    }

    /** Real subprocess invocation - matching how these are genuinely real CLI tools with their own main()/System.exit-style flows and environment-variable-based password input, not something safely callable in-process the way a library method would be. */
    private static int runProcess(String[] mainArgs, String password) throws Exception {
        String[] fullCommand = new String[mainArgs.length + 3];
        fullCommand[0] = "java";
        fullCommand[1] = "-cp";
        fullCommand[2] = System.getProperty("java.class.path");
        System.arraycopy(mainArgs, 0, fullCommand, 3, mainArgs.length);

        ProcessBuilder pb = new ProcessBuilder(fullCommand);
        pb.redirectErrorStream(true);
        if (password != null) {
            pb.environment().put("STDSQL_PASSWORD", password);
        }
        Process process = pb.start();
        String output = new String(process.getInputStream().readAllBytes());
        int exit = process.waitFor();
        if (exit != 0) {
            System.err.println("Process output:\n" + output);
        }
        return exit;
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
