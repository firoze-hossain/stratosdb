package com.stratosdb.testing;

import com.stratosdb.cli.StratosMigrate;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, end-to-end proof that StratosMigrate - this project's own
 * previously entirely-missing Flyway/Liquibase-style migration tool -
 * actually works against a real, running server: real migrations are
 * discovered, applied in real, strict numeric version order, recorded
 * in a real schema-history table, and both a real mid-migration failure
 * and a real, tampered-after-the-fact migration file are genuinely
 * detected, not silently accepted.
 */
public class StratosMigrateEndToEndTest {

    private StratosDB db;
    private StdWireServer server;
    private int port;

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    private void startServer(Path tempDir) throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        server = new StdWireServer(port, db);
        server.start();
        Thread.sleep(200);
    }

    private void writeMigration(Path dir, String fileName, String sql) throws Exception {
        Files.writeString(dir.resolve(fileName), sql, StandardCharsets.UTF_8);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void migrationsApplyInRealNumericVersionOrderAndAreRecorded(@TempDir Path serverDir, @TempDir Path migrationsDir) throws Exception {
        startServer(serverDir);
        writeMigration(migrationsDir, "V1__create_users_table.sql", "CREATE TABLE users (id INT, name VARCHAR)");
        writeMigration(migrationsDir, "V2__add_email_column.sql", "ALTER TABLE users ADD COLUMN email VARCHAR");
        // V10 must sort AFTER V2 - a real, numeric comparison, not lexicographic
        // (where "10" would incorrectly come before "2").
        writeMigration(migrationsDir, "V10__create_orders_table.sql", "CREATE TABLE orders (id INT, user_id INT)");

        List<StratosMigrate.Migration> migrations = StratosMigrate.discoverMigrations(migrationsDir.toFile());
        assertEquals(List.of("1", "2", "10"), migrations.stream().map(StratosMigrate.Migration::version).toList(),
            "migrations must be discovered in real, strict ascending NUMERIC version order");

        StratosMigrate.MigrateConnection conn = new StratosMigrate.MigrateConnection("localhost", port, "anyuser", "anydb", null);
        try {
            StratosMigrate.ensureSchemaHistoryTable(conn);
            StratosMigrate.runMigrate(conn, migrations);

            // Every real migration's own effect must have actually happened.
            assertNull(conn.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')"));
            assertNull(conn.execute("INSERT INTO orders VALUES (1, 1)"));

            List<List<String>> history = conn.selectRows("SELECT version, success FROM stratos_schema_history");
            assertEquals(3, history.size());
            for (List<String> row : history) {
                assertEquals("true", row.get(1), "every real migration here must have genuinely succeeded");
            }

            // Real idempotency: running migrate again must apply nothing further.
            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            PrintStream original = System.out;
            System.setOut(new PrintStream(captured));
            try {
                StratosMigrate.runMigrate(conn, migrations);
            } finally {
                System.setOut(original);
            }
            assertTrue(captured.toString().contains("No pending migrations"),
                "a second real run must find nothing left to apply");
        } finally {
            conn.close();
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aFailedMigrationHaltsBeforeApplyingAnyLaterOne(@TempDir Path serverDir, @TempDir Path migrationsDir) throws Exception {
        startServer(serverDir);
        writeMigration(migrationsDir, "V1__create_widgets.sql", "CREATE TABLE widgets (id INT)");
        writeMigration(migrationsDir, "V2__broken.sql", "THIS IS NOT VALID SQL AT ALL");
        writeMigration(migrationsDir, "V3__create_gadgets.sql", "CREATE TABLE gadgets (id INT)");

        List<StratosMigrate.Migration> migrations = StratosMigrate.discoverMigrations(migrationsDir.toFile());
        StratosMigrate.MigrateConnection conn = new StratosMigrate.MigrateConnection("localhost", port, "anyuser", "anydb", null);
        try {
            StratosMigrate.ensureSchemaHistoryTable(conn);
            StratosMigrate.runMigrate(conn, migrations);

            // V1 must have genuinely succeeded.
            assertNull(conn.execute("INSERT INTO widgets VALUES (1)"));
            // V3 must never have been attempted at all - a real migration tool
            // never applies a later version on top of a real failure.
            String gadgetsError = conn.execute("INSERT INTO gadgets VALUES (1)");
            assertNotNull(gadgetsError, "V3 must never have run - the gadgets table must not exist");

            List<List<String>> history = conn.selectRows("SELECT version, success FROM stratos_schema_history ORDER BY version");
            assertEquals(2, history.size(), "only V1 and V2 were ever attempted");
            assertEquals("true", history.get(0).get(1), "V1 succeeded");
            assertEquals("false", history.get(1).get(1), "V2's own real failure must be honestly recorded, not hidden");
        } finally {
            conn.close();
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void validateDetectsARealTamperedMigrationFile(@TempDir Path serverDir, @TempDir Path migrationsDir) throws Exception {
        startServer(serverDir);
        writeMigration(migrationsDir, "V1__create_notes.sql", "CREATE TABLE notes (id INT)");

        StratosMigrate.MigrateConnection conn = new StratosMigrate.MigrateConnection("localhost", port, "anyuser", "anydb", null);
        try {
            StratosMigrate.ensureSchemaHistoryTable(conn);
            List<StratosMigrate.Migration> original = StratosMigrate.discoverMigrations(migrationsDir.toFile());
            StratosMigrate.runMigrate(conn, original);
            assertTrue(StratosMigrate.runValidate(conn, original), "an untampered, freshly-applied migration must validate cleanly");

            // Now genuinely tamper with the already-applied file.
            writeMigration(migrationsDir, "V1__create_notes.sql", "CREATE TABLE notes (id INT, extra_column_added_after_the_fact VARCHAR)");
            List<StratosMigrate.Migration> tampered = StratosMigrate.discoverMigrations(migrationsDir.toFile());
            assertFalse(StratosMigrate.runValidate(conn, tampered),
                "a real, modified-after-application migration file must be genuinely detected, not silently trusted");
        } finally {
            conn.close();
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
