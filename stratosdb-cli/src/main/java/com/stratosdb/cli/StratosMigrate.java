package com.stratosdb.cli;

import com.stratosdb.network.auth.ScramClient;
import com.stratosdb.network.auth.ScramSha256;
import com.stratosdb.network.stdwire.StdWireMessages;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Console;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

/**
 * A real Flyway/Liquibase-style migration tool - this project's own
 * previously entirely-missing gap, deliberately built after real
 * {@code ALTER TABLE} support already existed (see PROJECT_PLAN.md's
 * own framing: a migration tool is far more useful once a schema can
 * actually evolve incrementally, not just be created once).
 *
 * Real, standard versioned-migration conventions, matching real
 * Flyway's own naming and behavior closely enough that anyone already
 * familiar with it needs no new mental model:
 *   - Migration files are named {@code V<version>__<description>.sql}
 *     (e.g. {@code V1__create_users_table.sql},
 *     {@code V2__add_email_column.sql}) in a given directory, applied in
 *     real, strict ascending version order - never file-system order,
 *     which is not guaranteed to match version order at all.
 *   - A real, own schema-history table ({@code stratos_schema_history})
 *     records every applied migration's own version, description, a
 *     real CRC32 checksum of its own file content, when it ran, and
 *     whether it succeeded - the same real record real Flyway itself
 *     keeps, for the same real reason: a migration must never be
 *     silently re-run, and a modified-after-the-fact migration file
 *     must be detectable, not silently trusted.
 *   - {@code migrate}: applies every real, pending (not-yet-recorded)
 *     migration in order, stopping at the first real failure rather
 *     than skipping ahead - a partially-migrated schema is a real,
 *     serious problem a migration tool must never paper over.
 *   - {@code info}: shows every discovered migration's own real status
 *     (applied/pending) without changing anything.
 *   - {@code validate}: recomputes every already-applied migration's
 *     own real checksum against its current file content and reports
 *     any real mismatch - catching the real, dangerous case where an
 *     already-run migration file was edited afterward, which would
 *     otherwise drift silently from what the schema's own history
 *     claims actually ran.
 *
 * Real, honest limitations, matching this whole project's own
 * established standard of naming what isn't done rather than leaving
 * it implicit:
 *   - No real rollback/undo migrations (Flyway's own paid-tier
 *     feature; Liquibase's own {@code rollback}) - this is a real,
 *     forward-only tool, matching Flyway Community's own real, free
 *     scope.
 *   - Each migration's own SQL is sent as a single, real, possibly
 *     multi-statement {@code Query} message (this engine's own
 *     existing multi-statement-per-message support, see
 *     {@code StdWireServer.splitStatements}) - not wrapped in an
 *     explicit {@code BEGIN}/{@code COMMIT} by this tool itself, since
 *     a migration file may legitimately want its own transactional
 *     boundaries (or intentionally none, e.g. around a DDL statement);
 *     forcing one here would take that real choice away from the
 *     migration's own author.
 *   - Connects over the real wire protocol with a small, self-contained
 *     client (modeled on {@code StratosDump}'s own proven SCRAM-auth
 *     logic), not this project's own JDBC driver - the same, real,
 *     deliberate choice {@code StratosBench} already made, and for the
 *     same reason (see that class's own javadoc): an unexplained hang
 *     was found in a standalone JDBC reproduction while building an
 *     unrelated feature, isolated but never fully root-caused.
 */
public class StratosMigrate {

    private static final Pattern MIGRATION_FILENAME = Pattern.compile("^V(\\d+(?:\\.\\d+)*)__(.+)\\.sql$");

    public static void main(String[] args) throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

        Args parsed = Args.parse(args);
        if (parsed == null) {
            printUsage();
            System.exit(1);
            return;
        }

        List<Migration> migrations = discoverMigrations(parsed.migrationsDir);
        MigrateConnection conn = new MigrateConnection(parsed.host, parsed.port, parsed.user, parsed.database, parsed.password);
        try {
            ensureSchemaHistoryTable(conn);
            switch (parsed.command) {
                case "migrate" -> runMigrate(conn, migrations);
                case "info" -> runInfo(conn, migrations);
                case "validate" -> {
                    if (!runValidate(conn, migrations)) {
                        System.exit(1);
                    }
                }
                default -> throw new IllegalStateException("Unknown command: " + parsed.command);
            }
        } finally {
            conn.close();
        }
    }

    private static void printUsage() {
        System.out.println("""
            StratosMigrate - a real Flyway/Liquibase-style migration tool for StratosDB

            Usage:
              StratosMigrate <command> -m migrationsDir -h host -p port -U user -d database [--password pw]

            Commands:
              migrate    Apply every pending migration, in real, strict ascending version order
              info       Show every discovered migration's own real status (applied/pending)
              validate   Check every already-applied migration's own real checksum against its current file

            Migration files must be named V<version>__<description>.sql (e.g. V1__create_users_table.sql,
            V2__add_email_column.sql) inside the given migrations directory.

            Options:
              -m migrationsDir   Directory containing V<version>__<description>.sql files (required)
              -h host            Server host (default localhost)
              -p port            Server port (required)
              -U user            Username (required)
              -d database        Database name (default stratos)
              --password pw      Password (prompted interactively if the server requires SCRAM and this is omitted)
            """);
    }

    // --- Migration discovery -----------------------------------------------

    public record Migration(String version, String description, String fileName, String checksum, String sql) {}

    public static List<Migration> discoverMigrations(File dir) throws IOException {
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + dir);
        }
        File[] files = dir.listFiles((d, name) -> name.endsWith(".sql"));
        List<Migration> migrations = new ArrayList<>();
        if (files == null) return migrations;
        for (File f : files) {
            Matcher m = MIGRATION_FILENAME.matcher(f.getName());
            if (!m.matches()) {
                System.out.println("Skipping file that doesn't match V<version>__<description>.sql: " + f.getName());
                continue;
            }
            String version = m.group(1);
            String description = m.group(2).replace('_', ' ');
            String sql = Files.readString(f.toPath(), StandardCharsets.UTF_8);
            migrations.add(new Migration(version, description, f.getName(), checksum(sql), sql));
        }
        migrations.sort(Comparator.comparing(mig -> parseVersionForSort(mig.version())));
        return migrations;
    }

    /** Real version comparison must be numeric, not lexicographic - "V10" must sort after "V2", not before it (lexicographically, "10" < "2"). Pads each real, dot-separated segment to a fixed width so a plain string comparison afterward still sorts correctly. */
    private static String parseVersionForSort(String version) {
        StringBuilder sb = new StringBuilder();
        for (String part : version.split("\\.")) {
            sb.append(String.format("%020d", Long.parseLong(part))).append('.');
        }
        return sb.toString();
    }

    private static String checksum(String content) {
        CRC32 crc = new CRC32();
        crc.update(content.getBytes(StandardCharsets.UTF_8));
        return Long.toHexString(crc.getValue());
    }

    // --- Schema history table -----------------------------------------------

    public static void ensureSchemaHistoryTable(MigrateConnection conn) throws IOException {
        // A real, idempotent "create if missing" - this engine's own CREATE
        // TABLE has no IF NOT EXISTS clause, so a real error here (from a
        // second, later run) is expected and silently ignored, matching the
        // same, established pattern already used elsewhere in this project.
        conn.execute("CREATE TABLE stratos_schema_history (version VARCHAR, description VARCHAR, "
            + "checksum VARCHAR, applied_at VARCHAR, success BOOLEAN)");
    }

    private static List<String> appliedVersions(MigrateConnection conn) throws IOException {
        List<String> versions = new ArrayList<>();
        for (List<String> row : conn.selectRows("SELECT version FROM stratos_schema_history WHERE success = true")) {
            versions.add(row.get(0));
        }
        return versions;
    }

    // --- Commands -----------------------------------------------------------

    public static void runMigrate(MigrateConnection conn, List<Migration> migrations) throws IOException {
        List<String> applied = appliedVersions(conn);
        int appliedCount = 0;
        for (Migration mig : migrations) {
            if (applied.contains(mig.version())) {
                continue;
            }
            System.out.println("Applying " + mig.fileName() + " (" + mig.description() + ")...");
            String error = conn.execute(mig.sql());
            String appliedAt = java.time.Instant.now().toString();
            boolean success = error == null;
            conn.execute("INSERT INTO stratos_schema_history VALUES ('" + mig.version() + "', '"
                + escapeSql(mig.description()) + "', '" + mig.checksum() + "', '" + appliedAt + "', " + success + ")");
            if (!success) {
                System.err.println("Migration V" + mig.version() + " FAILED: " + error);
                System.err.println("Stopping - a real migration tool never applies a later version on top of a real failure.");
                return;
            }
            System.out.println("  OK");
            appliedCount++;
        }
        System.out.println(appliedCount == 0 ? "No pending migrations - schema is already up to date." : "Applied " + appliedCount + " migration(s).");
    }

    public static void runInfo(MigrateConnection conn, List<Migration> migrations) throws IOException {
        List<String> succeeded = appliedVersions(conn);
        List<String> attempted = new ArrayList<>();
        for (List<String> row : conn.selectRows("SELECT version FROM stratos_schema_history")) {
            attempted.add(row.get(0));
        }
        System.out.printf("%-10s %-40s %-10s%n", "Version", "Description", "Status");
        System.out.println("-".repeat(62));
        for (Migration mig : migrations) {
            String status = succeeded.contains(mig.version()) ? "Applied"
                : attempted.contains(mig.version()) ? "Failed" : "Pending";
            System.out.printf("%-10s %-40s %-10s%n", "V" + mig.version(), mig.description(), status);
        }
    }

    /** Returns true if every already-applied migration's own checksum still matches its current file - false on any real mismatch. A real return value rather than calling System.exit() directly, so this method stays safely callable from a test (which shares the same JVM as every other test) - only main() itself decides the real, meaningful process exit code from this. */
    public static boolean runValidate(MigrateConnection conn, List<Migration> migrations) throws IOException {
        List<List<String>> historyRows = conn.selectRows("SELECT version, checksum FROM stratos_schema_history WHERE success = true");
        boolean allValid = true;
        for (List<String> row : historyRows) {
            String version = row.get(0);
            String recordedChecksum = row.get(1);
            Migration onDisk = migrations.stream().filter(m -> m.version().equals(version)).findFirst().orElse(null);
            if (onDisk == null) {
                System.out.println("V" + version + ": applied in the database, but its own migration file is now missing from disk");
                allValid = false;
                continue;
            }
            if (!onDisk.checksum().equals(recordedChecksum)) {
                System.out.println("V" + version + ": MISMATCH - this migration was modified after it was already applied "
                    + "(recorded checksum " + recordedChecksum + ", current file checksum " + onDisk.checksum() + ")");
                allValid = false;
            } else {
                System.out.println("V" + version + ": OK");
            }
        }
        System.out.println(allValid ? "\nAll applied migrations are valid." : "\nValidation FAILED - see mismatches above.");
        return allValid;
    }

    private static String escapeSql(String s) {
        return s.replace("'", "''");
    }

    // --- Argument parsing -----------------------------------------------------

    private static final class Args {
        String command;
        File migrationsDir;
        String host = "localhost";
        int port = -1;
        String user;
        String database = "stratos";
        String password;

        static Args parse(String[] argv) {
            if (argv.length == 0) return null;
            Args a = new Args();
            a.command = argv[0];
            if (!a.command.equals("migrate") && !a.command.equals("info") && !a.command.equals("validate")) {
                System.err.println("Unknown command: " + a.command);
                return null;
            }
            for (int i = 1; i < argv.length; i++) {
                switch (argv[i]) {
                    case "-m" -> a.migrationsDir = new File(argv[++i]);
                    case "-h" -> a.host = argv[++i];
                    case "-p" -> a.port = Integer.parseInt(argv[++i]);
                    case "-U" -> a.user = argv[++i];
                    case "-d" -> a.database = argv[++i];
                    case "--password" -> a.password = argv[++i];
                    default -> {
                        System.err.println("Unrecognized argument: " + argv[i]);
                        return null;
                    }
                }
            }
            if (a.migrationsDir == null || a.port < 0 || a.user == null) {
                System.err.println("Missing required argument: -m, -p, and -U are always required");
                return null;
            }
            return a;
        }
    }

    // --- A minimal, real, SCRAM-capable wire-protocol client - see
    // StratosBench's own MigrateConnection-equivalent (BenchConnection) for
    // the same, established pattern and the same reasoning for why this is
    // a separate, self-contained copy rather than a shared refactor.

    public static final class MigrateConnection {
        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;

        public MigrateConnection(String host, int port, String user, String database, String password) throws IOException {
            socket = new Socket(host, port);
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            StdWireMessages.writeStartupMessage(out, user, database);
            out.flush();
            readStartupResponses(user, password);
        }

        private void readStartupResponses(String user, String password) throws IOException {
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                switch (msg.type()) {
                    case 'R' -> {
                        int authCode = readAuthCode(msg);
                        if (authCode == 10) {
                            performScramHandshake(user, password);
                        }
                    }
                    case 'S', 'K' -> { /* ParameterStatus / BackendKeyData - not needed here */ }
                    case 'Z' -> { return; }
                    case 'E' -> throw new IOException("Server rejected startup: " + extractError(msg));
                    default -> { /* ignore */ }
                }
            }
        }

        private void performScramHandshake(String username, String password) throws IOException {
            if (password == null) {
                password = promptForPassword(username);
            }
            ScramClient scram = new ScramClient(username, password);
            String clientFirstMessage = scram.buildClientFirstMessage();
            writeSaslInitialResponse(clientFirstMessage);

            StdWireMessages.TypedMessage continueMsg = StdWireMessages.readTypedMessage(in);
            if (continueMsg.type() != 'R' || readAuthCode(continueMsg) != 11) {
                throw new IOException("Expected AuthenticationSASLContinue during SCRAM handshake");
            }
            String serverFirstMessage = new String(continueMsg.body(), 4, continueMsg.body().length - 4, StandardCharsets.UTF_8);

            String clientFinalMessage = scram.buildClientFinalMessage(serverFirstMessage);
            byte[] cfBytes = clientFinalMessage.getBytes(StandardCharsets.UTF_8);
            out.writeByte('p');
            out.writeInt(cfBytes.length + 4);
            out.write(cfBytes);
            out.flush();

            StdWireMessages.TypedMessage finalMsg = StdWireMessages.readTypedMessage(in);
            if (finalMsg.type() == 'E') {
                throw new IOException("Authentication failed: " + extractError(finalMsg));
            }
            if (finalMsg.type() != 'R' || readAuthCode(finalMsg) != 12) {
                throw new IOException("Expected AuthenticationSASLFinal during SCRAM handshake");
            }
            String serverFinalMessage = new String(finalMsg.body(), 4, finalMsg.body().length - 4, StandardCharsets.UTF_8);
            if (!scram.verifyServerFinalMessage(serverFinalMessage)) {
                throw new IOException("Server's SCRAM signature did not verify - possible impersonation, aborting");
            }
        }

        private void writeSaslInitialResponse(String clientFirstMessage) throws IOException {
            byte[] mechanismBytes = ScramSha256.MECHANISM_NAME.getBytes(StandardCharsets.UTF_8);
            byte[] dataBytes = clientFirstMessage.getBytes(StandardCharsets.UTF_8);
            int bodyLen = mechanismBytes.length + 1 + 4 + dataBytes.length;
            out.writeByte('p');
            out.writeInt(bodyLen + 4);
            out.write(mechanismBytes);
            out.writeByte(0);
            out.writeInt(dataBytes.length);
            out.write(dataBytes);
            out.flush();
        }

        private String promptForPassword(String username) throws IOException {
            Console console = System.console();
            String promptText = "Password for user " + username + ": ";
            if (console != null) {
                char[] chars = console.readPassword(promptText);
                return chars == null ? "" : new String(chars);
            }
            System.out.print(promptText);
            System.out.flush();
            Scanner scanner = new Scanner(System.in);
            return scanner.hasNextLine() ? scanner.nextLine() : "";
        }

        private static int readAuthCode(StdWireMessages.TypedMessage msg) {
            byte[] b = msg.body();
            return ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
        }

        private String extractError(StdWireMessages.TypedMessage msg) {
            byte[] b = msg.body();
            int pos = 0;
            while (pos < b.length && b[pos] != 0) {
                char field = (char) b[pos]; pos++;
                int start = pos;
                while (b[pos] != 0) pos++;
                String value = new String(b, start, pos - start, StandardCharsets.UTF_8);
                pos++;
                if (field == 'M') return value;
            }
            return "unknown error";
        }

        /** Runs a statement (possibly several, semicolon-separated), returning null on success or the real error message on the FIRST failure encountered. */
        public String execute(String sql) throws IOException {
            StdWireMessages.writeQuery(out, sql);
            out.flush();
            String error = null;
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                if (msg.type() == 'E' && error == null) {
                    error = extractError(msg);
                } else if (msg.type() == 'Z') {
                    return error;
                }
            }
        }

        /** Runs a real SELECT and returns every real row's own column values as text - used for reading back this tool's own schema-history table. */
        public List<List<String>> selectRows(String sql) throws IOException {
            StdWireMessages.writeQuery(out, sql);
            out.flush();
            List<List<String>> rows = new ArrayList<>();
            String error = null;
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                if (msg.type() == 'E') {
                    error = extractError(msg);
                } else if (msg.type() == 'D') {
                    rows.add(parseDataRow(msg.body()));
                } else if (msg.type() == 'Z') {
                    if (error != null) {
                        throw new IOException("Query failed: " + error);
                    }
                    return rows;
                }
            }
        }

        /** Mirrors writeDataRow's own exact wire format (see StdWireMessages): Int16 count, then per value an Int32 length (-1 = NULL) followed by that many UTF-8 bytes. */
        private List<String> parseDataRow(byte[] b) {
            List<String> values = new ArrayList<>();
            int pos = 0;
            int count = ((b[pos] & 0xFF) << 8) | (b[pos + 1] & 0xFF);
            pos += 2;
            for (int i = 0; i < count; i++) {
                int len = ((b[pos] & 0xFF) << 24) | ((b[pos + 1] & 0xFF) << 16) | ((b[pos + 2] & 0xFF) << 8) | (b[pos + 3] & 0xFF);
                pos += 4;
                if (len == -1) {
                    values.add(null);
                } else {
                    values.add(new String(b, pos, len, StandardCharsets.UTF_8));
                    pos += len;
                }
            }
            return values;
        }

        public void close() {
            try { socket.close(); } catch (IOException ignored) { }
        }
    }
}
