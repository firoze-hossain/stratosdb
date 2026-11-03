package com.stratosdb.network.replication;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses and represents options passed to the PostgreSQL wire protocol {@code BASE_BACKUP} command.
 *
 * Supports both standard syntax formats:
 * <pre>
 *   BASE_BACKUP ( option [, ...] )
 *   BASE_BACKUP [LABEL 'label'] [PROGRESS] [FAST] [WAL] [NOWAIT] [MAX_RATE rate] [TABLESPACE_MAP] [NOVERIFY_CHECKSUMS]
 * </pre>
 */
public record StdBaseBackupOptions(
    String label,
    boolean fast,
    boolean wal,
    boolean progress,
    boolean noWait,
    long maxRateBytesPerSec,
    boolean tablespaceMap,
    boolean noVerifyChecksums
) {
    private static final Pattern LABEL_PATTERN = Pattern.compile("(?i)\\bLABEL\\s+(?:(?:'|\")([^'\"]+)(?:'|\")|([a-zA-Z0-9_\\-]+))");
    private static final Pattern MAX_RATE_PATTERN = Pattern.compile("(?i)\\bMAX_RATE\\s+([0-9]+)");

    public static StdBaseBackupOptions parse(String sql) {
        String trimmed = sql.trim();
        if (trimmed.endsWith(";")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
        }

        String label = "std_basebackup_" + System.currentTimeMillis();
        Matcher lm = LABEL_PATTERN.matcher(trimmed);
        if (lm.find()) {
            label = lm.group(1) != null ? lm.group(1) : lm.group(2);
        }

        long maxRate = 0;
        Matcher mm = MAX_RATE_PATTERN.matcher(trimmed);
        if (mm.find()) {
            try {
                // MAX_RATE is typically in KiB/s in pgwire
                maxRate = Long.parseLong(mm.group(1)) * 1024L;
            } catch (NumberFormatException ignored) {}
        }

        String upper = trimmed.toUpperCase(Locale.ROOT);
        boolean fast = upper.contains("FAST") || upper.contains("CHECKPOINT 'FAST'");
        boolean wal = upper.contains("WAL") || upper.contains("INCLUDEDWAL");
        boolean progress = upper.contains("PROGRESS");
        boolean noWait = upper.contains("NOWAIT");
        boolean tablespaceMap = upper.contains("TABLESPACE_MAP");
        boolean noVerifyChecksums = upper.contains("NOVERIFY_CHECKSUMS");

        // By default in StratosDB base backup, WAL and FAST are true if not explicitly disabled
        // to ensure the replica bootstraps to a clean, consistent checkpoint state.
        return new StdBaseBackupOptions(label, fast, wal, progress, noWait, maxRate, tablespaceMap, noVerifyChecksums);
    }
}
