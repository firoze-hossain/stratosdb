package com.stratosdb.cli;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A real base backup tool - this project's own concrete answer to
 * "there's no way to take a base backup, archive WAL continuously, and
 * restore to an arbitrary past moment." Modeled on real Postgres's own
 * pg_basebackup, adapted for this engine's own real, honest scope: a
 * local-filesystem copy rather than a networked file-streaming
 * protocol, since this engine has no such protocol and building one
 * would be its own separate, large piece of work - this tool assumes
 * it runs on the same machine (or has filesystem access to the same
 * data/archive directories) as the server it's backing up, the same
 * assumption stratosdump already makes for its own connection.
 *
 * What a base backup actually is here: a real CHECKPOINT (forcing
 * every dirty page durably to disk and archiving/truncating whatever
 * WAL had accumulated - see CheckpointStatement's own javadoc),
 * followed by a plain filesystem copy of the data directory's own
 * table/index/catalog files (NOT the wal/ subdirectory itself - the
 * active, ongoing WAL is irrelevant to a base backup; PitrRestore
 * replays from the ARCHIVE directory instead), plus a small
 * backup_label file recording exactly which archived WAL segment's
 * own contents are already reflected in the files just copied -
 * PitrRestore only ever needs to replay archived segments strictly
 * AFTER that one.
 *
 * Real, honestly-stated limitation: CHECKPOINT requires a superuser
 * connection (see ExecutorEngine.executeCheckpoint's own javadoc), so
 * this tool does too. This is NOT a fully non-blocking online backup
 * the way real Postgres's own base backup protocol is - WALManager's
 * own checkpointAndArchive has a real, stated concurrency caveat
 * (a race against an in-flight writer that already reserved a WAL
 * position but hasn't written there yet) that this tool inherits
 * rather than solves - safest run when write traffic is low.
 */
public class PitrBackup {

    private static final Pattern ARCHIVE_SEGMENT_PATTERN = Pattern.compile("(\\d{12})\\.walseg");

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = com.stratosdb.common.constants.ProtocolConstants.DEFAULT_STDWIRE_PORT;
        String user = System.getProperty("user.name", "stratos");
        String database = null;
        String dataDir = null;
        String archiveDir = null;
        String backupDir = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-h" -> host = requireArg(args, ++i, "-h");
                case "-p" -> port = Integer.parseInt(requireArg(args, ++i, "-p"));
                case "-U" -> user = requireArg(args, ++i, "-U");
                case "-d" -> database = requireArg(args, ++i, "-d");
                case "--data-dir" -> dataDir = requireArg(args, ++i, "--data-dir");
                case "--archive-dir" -> archiveDir = requireArg(args, ++i, "--archive-dir");
                case "--backup-dir" -> backupDir = requireArg(args, ++i, "--backup-dir");
                default -> {
                    System.err.println("Unrecognized argument: " + args[i]);
                    printUsage();
                    return;
                }
            }
        }
        if (dataDir == null || archiveDir == null || backupDir == null) {
            printUsage();
            return;
        }
        if (database == null) {
            database = user;
        }
        String password = System.getenv("STDSQL_PASSWORD");

        StratosDump conn;
        try {
            conn = new StratosDump(host, port, user, database, password);
        } catch (IOException e) {
            System.err.println("Could not connect to StratosDB stdwire server at " + host + ":" + port + " - " + e.getMessage());
            return;
        }

        try {
            System.err.println("Running CHECKPOINT...");
            String checkpointError = conn.executeSql("CHECKPOINT");
            if (checkpointError != null) {
                System.err.println("CHECKPOINT failed: " + checkpointError
                    + " (CHECKPOINT requires a superuser connection - see ExecutorEngine.executeCheckpoint's own javadoc)");
                return;
            }
        } finally {
            conn.close();
        }

        // The archive directory's own state, observed directly via the shared
        // filesystem - simpler and more robust than asking the server for a number
        // that could already be stale by the time this tool acts on it (see this
        // class's own javadoc for why a local-filesystem approach is the real,
        // honest scope here rather than a networked protocol).
        long startingSegment = findHighestArchiveSegment(archiveDir);
        System.err.println("Starting WAL segment for this backup: " + startingSegment
            + " (restore replays only segments strictly after this one)");

        System.err.println("Copying data directory...");
        copyDataDirectoryExcludingWal(Path.of(dataDir), Path.of(backupDir));

        writeBackupLabel(Path.of(backupDir), startingSegment);

        System.err.println("Base backup complete: " + backupDir);
    }

    private static void printUsage() {
        System.err.println("Usage: stratosbackup -h host -p port -U user -d database --data-dir <server_data_dir> --archive-dir <wal_archive_dir> --backup-dir <destination_dir>");
        System.err.println("  Requires a superuser connection - CHECKPOINT is superuser-only.");
    }

    /** The highest-numbered *.walseg file currently in archiveDir, or 0 if none exist yet (a base backup taken before any WAL was ever archived - restore then simply has nothing to replay, which is correct). */
    static long findHighestArchiveSegment(String archiveDir) {
        java.io.File dir = new java.io.File(archiveDir);
        java.io.File[] files = dir.listFiles();
        long highest = 0;
        if (files != null) {
            for (java.io.File f : files) {
                Matcher m = ARCHIVE_SEGMENT_PATTERN.matcher(f.getName());
                if (m.matches()) {
                    highest = Math.max(highest, Long.parseLong(m.group(1)));
                }
            }
        }
        return highest;
    }

    /** Recursively copies sourceDataDir to destBackupDir, skipping the "wal" subdirectory entirely - the active WAL is irrelevant to a base backup, since PitrRestore replays from the separate archive directory instead. */
    static void copyDataDirectoryExcludingWal(Path sourceDataDir, Path destBackupDir) throws IOException {
        Files.createDirectories(destBackupDir);
        Path walDir = sourceDataDir.resolve("wal");
        Files.walkFileTree(sourceDataDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                if (dir.equals(walDir)) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                Path target = destBackupDir.resolve(sourceDataDir.relativize(dir));
                Files.createDirectories(target);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path target = destBackupDir.resolve(sourceDataDir.relativize(file));
                Files.copy(file, target, StandardCopyOption.REPLACE_EXISTING);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * backup_label: the one piece of metadata PitrRestore actually needs -
     * which archived WAL segment's own contents are already reflected in
     * this backup's own copied files, so replay knows to start strictly
     * after it, never re-applying (or, worse, skipping) anything.
     */
    static void writeBackupLabel(Path backupDir, long startingSegment) throws IOException {
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(backupDir.resolve("backup_label")))) {
            writer.println("START_WAL_SEGMENT: " + startingSegment);
            writer.println("BACKUP_TIME: " + Instant.now());
        }
    }

    private static String requireArg(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException(flag + " requires a value");
        }
        return args[index];
    }
}
