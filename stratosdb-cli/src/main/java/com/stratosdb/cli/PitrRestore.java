package com.stratosdb.cli;

import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.wal.PitrWalReplayer;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Point-in-time restore - the other half of this project's own real
 * disaster-recovery story (see PitrBackup's own javadoc for base
 * backups and WAL archiving, the two things this tool depends on
 * already existing). Rebuilds a fresh, standalone data directory from
 * a base backup plus every archived WAL segment strictly after that
 * backup's own starting point, optionally stopping at a specific
 * target time rather than replaying everything available - "what did
 * this database look like at 2:59pm, right before someone ran that bad
 * DROP TABLE at 3:00pm" being the actual, real scenario this whole
 * feature exists for.
 *
 * The output of this tool is an ordinary, real data directory - no
 * separate "restored mode" or special startup flag needed. Point a
 * normal StratosDB/StdWireServerMain at it and it starts up exactly
 * like any other data directory, because after replay, that's exactly
 * what it is.
 */
public class PitrRestore {

    private static final Pattern ARCHIVE_SEGMENT_PATTERN = Pattern.compile("(\\d{12})\\.walseg");

    public static void main(String[] args) throws Exception {
        String backupDir = null;
        String archiveDir = null;
        String targetDataDir = null;
        String targetTimeArg = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--backup-dir" -> backupDir = requireArg(args, ++i, "--backup-dir");
                case "--archive-dir" -> archiveDir = requireArg(args, ++i, "--archive-dir");
                case "--target-dir" -> targetDataDir = requireArg(args, ++i, "--target-dir");
                case "--target-time" -> targetTimeArg = requireArg(args, ++i, "--target-time");
                default -> {
                    System.err.println("Unrecognized argument: " + args[i]);
                    printUsage();
                    return;
                }
            }
        }
        if (backupDir == null || archiveDir == null || targetDataDir == null) {
            printUsage();
            return;
        }

        Long targetTimeMillis = null;
        if (targetTimeArg != null) {
            try {
                targetTimeMillis = Instant.parse(targetTimeArg).toEpochMilli();
            } catch (Exception e) {
                System.err.println("--target-time must be an ISO-8601 instant, e.g. 2026-08-26T15:00:00Z - got: " + targetTimeArg);
                return;
            }
        }

        Path target = Path.of(targetDataDir);
        if (Files.exists(target) && Files.list(target).findAny().isPresent()) {
            System.err.println("--target-dir must not already exist or must be empty - refusing to overwrite " + targetDataDir);
            return;
        }

        System.err.println("Copying base backup to " + targetDataDir + "...");
        copyDirectory(Path.of(backupDir), target);

        long startingSegment = readBackupLabel(target);
        System.err.println("Base backup's own starting WAL segment: " + startingSegment);

        List<Long> segmentsToReplay = findSegmentsAfter(archiveDir, startingSegment);
        System.err.println("Archived segments to replay: " + segmentsToReplay
            + (segmentsToReplay.isEmpty() ? " (none - restored state is exactly the base backup itself)" : ""));

        List<byte[]> segmentBytes = new ArrayList<>();
        for (Long seq : segmentsToReplay) {
            String fileName = String.format("%012d.walseg", seq);
            segmentBytes.add(Files.readAllBytes(Path.of(archiveDir, fileName)));
        }

        DiskManager diskManager = new DiskManager(targetDataDir);
        PitrWalReplayer.ReplayResult result = PitrWalReplayer.replay(segmentBytes, diskManager, targetTimeMillis);
        System.err.println(result.summary());
        diskManager.close();

        // The restored directory's own TransactionManager (created fresh, the
        // next time any real StratosDB actually starts against this directory)
        // has no way to know any of this replayed history happened unless these
        // two files - copied from the base backup as they stood AT BACKUP TIME,
        // reflecting nothing replayed since - are updated to also cover it. See
        // TransactionManager's own javadoc for exactly why both exist: without
        // this step, every replayed row's own xmin would appear to belong to a
        // transaction the restored database's fresh commit log has genuinely
        // never heard of, and MVCCVisibility would correctly (given that
        // otherwise-accurate but incomplete information) treat every one of
        // them as not yet committed - hidden from every future query, even
        // though the row's own bytes are sitting right there on disk. Found
        // by testing this real, actual round trip, not by inspection.
        appendToCommitLog(target, result.appliedXids());
        advanceXidWatermark(target, result.highestXidSeen());

        System.err.println("Restore complete: " + targetDataDir
            + " - start a normal server against this directory to use the restored database.");
    }

    /** Appends xids to the restored directory's own commit_log.dat, in the exact same raw-8-byte-per-xid format TransactionManager itself already reads and writes - see its own loadPersistedCommitLog/appendToCommitLog. */
    private static void appendToCommitLog(Path targetDataDir, java.util.Set<Long> xids) throws IOException {
        if (xids.isEmpty()) return;
        Path commitLogPath = targetDataDir.resolve("commit_log.dat");
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(commitLogPath.toFile(), "rw")) {
            raf.seek(raf.length());
            for (long xid : xids) {
                raf.writeLong(xid);
            }
            raf.getFD().sync();
        }
    }

    /** Ensures the restored directory's own xid_counter.txt is at least highestXidSeen - never lowers an existing, already-higher watermark, only ever advances it, so the restored database's own future transactions can never collide with an xid already used (and now potentially present on disk) before the restore. */
    private static void advanceXidWatermark(Path targetDataDir, long highestXidSeen) throws IOException {
        if (highestXidSeen == 0) return;
        Path counterPath = targetDataDir.resolve("xid_counter.txt");
        long existing = 0;
        if (Files.exists(counterPath)) {
            try {
                existing = Long.parseLong(Files.readString(counterPath).trim());
            } catch (Exception ignored) {
                // a malformed or unreadable existing watermark - treat as 0, matching
                // TransactionManager's own fallback behavior for the same case
            }
        }
        long newWatermark = Math.max(existing, highestXidSeen);
        Files.writeString(counterPath, String.valueOf(newWatermark));
    }

    private static void printUsage() {
        System.err.println("Usage: stratosrestore-pitr --backup-dir <base_backup_dir> --archive-dir <wal_archive_dir> --target-dir <new_data_dir> [--target-time <ISO-8601 instant>]");
        System.err.println("  Without --target-time, replays every available archived WAL segment (restore to the most recent point possible).");
    }

    static long readBackupLabel(Path targetDataDir) throws IOException {
        Path labelFile = targetDataDir.resolve("backup_label");
        for (String line : Files.readAllLines(labelFile)) {
            if (line.startsWith("START_WAL_SEGMENT: ")) {
                return Long.parseLong(line.substring("START_WAL_SEGMENT: ".length()).trim());
            }
        }
        throw new IOException("backup_label in " + targetDataDir + " has no START_WAL_SEGMENT line - is this a real base backup directory?");
    }

    /** Every *.walseg sequence number in archiveDir strictly greater than afterSegment, in ascending (replay) order. */
    static List<Long> findSegmentsAfter(String archiveDir, long afterSegment) {
        java.io.File dir = new java.io.File(archiveDir);
        java.io.File[] files = dir.listFiles();
        List<Long> result = new ArrayList<>();
        if (files != null) {
            for (java.io.File f : files) {
                Matcher m = ARCHIVE_SEGMENT_PATTERN.matcher(f.getName());
                if (m.matches()) {
                    long seq = Long.parseLong(m.group(1));
                    if (seq > afterSegment) {
                        result.add(seq);
                    }
                }
            }
        }
        return result.stream().sorted().collect(Collectors.toList());
    }

    private static void copyDirectory(Path source, Path dest) throws IOException {
        Files.createDirectories(dest);
        Files.walkFileTree(source, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Files.createDirectories(dest.resolve(source.relativize(dir)));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.copy(file, dest.resolve(source.relativize(file)), StandardCopyOption.REPLACE_EXISTING);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static String requireArg(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException(flag + " requires a value");
        }
        return args[index];
    }
}
