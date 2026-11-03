package com.stratosdb.cli;

import com.stratosdb.network.replication.StdBaseBackupClient;
import com.stratosdb.network.replication.StdBaseBackupOptions;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Networked base backup CLI tool for StratosDB, modeled on PostgreSQL's {@code pg_basebackup}.
 *
 * Connects to a running StratosDB instance over the PostgreSQL wire protocol, issues
 * {@code BASE_BACKUP}, and streams the complete raw tarball/pages of the database data directory
 * over the replication connection directly into a destination directory.
 *
 * Enables new replicas to bootstrap over the network with zero manual file copying.
 */
public class StdBaseBackup {

    public static void main(String[] args) {
        String host = "localhost";
        int port = com.stratosdb.common.constants.ProtocolConstants.DEFAULT_STDWIRE_PORT;
        String user = System.getProperty("user.name", "stratos");
        String database = null;
        String targetDir = null;
        String label = "std_basebackup_" + System.currentTimeMillis();
        boolean fast = true;
        boolean wal = true;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-h" -> host = requireArg(args, ++i, "-h");
                case "-p" -> port = Integer.parseInt(requireArg(args, ++i, "-p"));
                case "-U" -> user = requireArg(args, ++i, "-U");
                case "-d" -> database = requireArg(args, ++i, "-d");
                case "-D", "--pgdata" -> targetDir = requireArg(args, ++i, args[i - 1]);
                case "--label" -> label = requireArg(args, ++i, "--label");
                case "--fast", "-c", "fast" -> fast = true;
                case "--wal", "-X" -> wal = true;
                case "--help" -> {
                    printUsage();
                    return;
                }
                default -> {
                    System.err.println("Unrecognized argument: " + args[i]);
                    printUsage();
                    System.exit(1);
                    return;
                }
            }
        }

        if (targetDir == null) {
            System.err.println("Error: Target directory (-D) must be specified.");
            printUsage();
            System.exit(1);
            return;
        }

        String password = System.getenv("STDSQL_PASSWORD");

        System.out.printf("Initiating networked base backup from %s:%d into %s...%n", host, port, targetDir);
        try (StdBaseBackupClient client = new StdBaseBackupClient(host, port, user, database, password)) {
            client.connect();

            StdBaseBackupOptions options = new StdBaseBackupOptions(
                label,
                fast,
                wal,
                false, // progress
                false, // noWait
                0,     // maxRate
                false, // tablespaceMap
                false  // noVerifyChecksums
            );

            StdBaseBackupClient.StdBaseBackupResult result = client.executeBaseBackup(Path.of(targetDir), options);

            System.out.println("Base backup completed successfully:");
            System.out.printf("  Destination:      %s%n", result.targetDir());
            System.out.printf("  Starting LSN:     epoch=%d, lsn=%d%n", result.startEpoch(), result.startLsn());
            System.out.printf("  Ending LSN:       epoch=%d, lsn=%d%n", result.endEpoch(), result.endLsn());
            System.out.printf("  Files Extracted:  %d%n", result.filesExtracted());
            System.out.printf("  Label:            %s%n", result.label());
        } catch (IOException e) {
            System.err.println("Base backup failed: " + e.getMessage());
            System.exit(2);
        }
    }

    private static String requireArg(String[] args, int index, String flag) {
        if (index >= args.length) {
            System.err.println("Missing required argument for " + flag);
            printUsage();
            System.exit(1);
        }
        return args[index];
    }

    private static void printUsage() {
        System.err.println("Usage: std_basebackup -h <host> -p <port> -U <user> -d <database> -D <target_dir> [options]");
        System.err.println("Options:");
        System.err.println("  -h <host>         Server host (default: localhost)");
        System.err.println("  -p <port>         Server port (default: 5432)");
        System.err.println("  -U <user>         Username (default: current user)");
        System.err.println("  -d <database>     Database name");
        System.err.println("  -D <target_dir>   Target directory for backup (required)");
        System.err.println("  --label <label>   Label for backup");
        System.err.println("  --fast            Request fast checkpoint on primary (default: true)");
        System.err.println("  --wal             Include WAL files in backup");
    }
}
