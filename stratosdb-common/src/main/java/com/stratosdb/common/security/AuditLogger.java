package com.stratosdb.common.security;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Immutable, tamper-evident cryptographic audit logger that writes SHA-256
 * hash-chained event records for DDL, DCL, and authentication events.
 *
 * <p>Each record stores {@code prevHash} and {@code hash = SHA-256(seq|ts|category|actor|action|detail|prevHash)}.
 * Modifying, deleting, or reordering any record breaks the hash chain and is
 * immediately detected by {@link #verifyIntegrity()}.
 */
public final class AuditLogger {
    public static final String GENESIS_HASH = "0".repeat(64);

    public record AuditRecord(
        long sequence,
        long timestampMs,
        String category,
        String actor,
        String action,
        String detail,
        String prevHash,
        String hash
    ) {}

    public record VerificationResult(
        boolean valid,
        long recordCount,
        long brokenAtSequence,
        String reason
    ) {}

    private final Path logPath;
    private long nextSequence = 1L;
    private String lastHash = GENESIS_HASH;

    public AuditLogger(Path logPath) {
        this.logPath = logPath;
        initFromExisting();
    }

    private synchronized void initFromExisting() {
        try {
            if (logPath.getParent() != null) {
                Files.createDirectories(logPath.getParent());
            }
            if (!Files.exists(logPath)) {
                return;
            }
            List<String> lines = Files.readAllLines(logPath, StandardCharsets.UTF_8);
            for (String line : lines) {
                if (line.isBlank()) continue;
                AuditRecord rec = parseLine(line);
                if (rec != null) {
                    nextSequence = rec.sequence() + 1;
                    lastHash = rec.hash();
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialize AuditLogger at " + logPath, e);
        }
    }

    public synchronized AuditRecord logEvent(String category, String actor, String action, String detail) {
        long seq = nextSequence++;
        long ts = System.currentTimeMillis();
        String safeCat = sanitize(category);
        String safeActor = sanitize(actor == null || actor.isBlank() ? "system" : actor);
        String safeAction = sanitize(action);
        String safeDetail = sanitize(detail);
        String prev = lastHash;
        String hash = computeHash(seq, ts, safeCat, safeActor, safeAction, safeDetail, prev);
        AuditRecord record = new AuditRecord(seq, ts, safeCat, safeActor, safeAction, safeDetail, prev, hash);
        String line = formatLine(record) + "\n";
        try {
            Files.writeString(
                logPath,
                line,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND,
                StandardOpenOption.DSYNC
            );
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append audit record to " + logPath, e);
        }
        lastHash = hash;
        return record;
    }

    public synchronized VerificationResult verifyIntegrity() {
        if (!Files.exists(logPath)) {
            return new VerificationResult(true, 0L, -1L, "No audit log file yet");
        }
        try {
            List<String> lines = Files.readAllLines(logPath, StandardCharsets.UTF_8);
            long expectedSeq = 1L;
            String expectedPrevHash = GENESIS_HASH;
            long count = 0L;

            for (String line : lines) {
                if (line.isBlank()) continue;
                AuditRecord rec = parseLine(line);
                if (rec == null) {
                    return new VerificationResult(false, count, expectedSeq, "Malformed audit record at sequence " + expectedSeq);
                }
                if (rec.sequence() != expectedSeq) {
                    return new VerificationResult(false, count, expectedSeq,
                        "Sequence mismatch: expected " + expectedSeq + " but found " + rec.sequence());
                }
                if (!expectedPrevHash.equals(rec.prevHash())) {
                    return new VerificationResult(false, count, rec.sequence(),
                        "Previous hash mismatch at sequence " + rec.sequence());
                }
                String recomputed = computeHash(
                    rec.sequence(), rec.timestampMs(), rec.category(), rec.actor(),
                    rec.action(), rec.detail(), rec.prevHash()
                );
                if (!recomputed.equals(rec.hash())) {
                    return new VerificationResult(false, count, rec.sequence(),
                        "Hash mismatch (tampered record) at sequence " + rec.sequence());
                }
                expectedPrevHash = rec.hash();
                expectedSeq++;
                count++;
            }
            return new VerificationResult(true, count, -1L, "OK");
        } catch (IOException e) {
            return new VerificationResult(false, 0L, -1L, "IO error reading audit log: " + e.getMessage());
        }
    }

    public synchronized List<AuditRecord> readAllRecords() throws IOException {
        if (!Files.exists(logPath)) {
            return List.of();
        }
        List<String> lines = Files.readAllLines(logPath, StandardCharsets.UTF_8);
        List<AuditRecord> records = new ArrayList<>(lines.size());
        for (String line : lines) {
            if (line.isBlank()) continue;
            AuditRecord rec = parseLine(line);
            if (rec != null) {
                records.add(rec);
            }
        }
        return records;
    }

    public Path getLogPath() {
        return logPath;
    }

    private static String sanitize(String s) {
        if (s == null) return "";
        return s.replace('\r', ' ').replace('\n', ' ').replace('|', '/');
    }

    private static String formatLine(AuditRecord r) {
        return r.sequence() + "|" + r.timestampMs() + "|" + r.category() + "|"
            + r.actor() + "|" + r.action() + "|" + r.detail() + "|"
            + r.prevHash() + "|" + r.hash();
    }

    private static AuditRecord parseLine(String line) {
        String[] parts = line.split("\\|", -1);
        if (parts.length != 8) {
            return null;
        }
        try {
            return new AuditRecord(
                Long.parseLong(parts[0]),
                Long.parseLong(parts[1]),
                parts[2], parts[3], parts[4], parts[5], parts[6], parts[7]
            );
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String computeHash(long seq, long ts, String cat, String actor, String action, String detail, String prevHash) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            String payload = seq + "|" + ts + "|" + cat + "|" + actor + "|" + action + "|" + detail + "|" + prevHash;
            byte[] digest = md.digest(payload.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
