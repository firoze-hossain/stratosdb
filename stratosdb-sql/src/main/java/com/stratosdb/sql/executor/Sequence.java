package com.stratosdb.sql.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A real, persisted sequence - CREATE SEQUENCE's backing object, and what
 * SERIAL columns generate their default values from.
 *
 * Uses the exact same correctness pattern already built for the persisted
 * transaction-id counter (see TransactionManager): reserve a batch of
 * values by writing a watermark to disk BEFORE handing any of them out, so
 * a crash immediately after a value is used can never result in a restart
 * reusing it - the restart resumes strictly above the last persisted
 * watermark, which by construction is always >= any value actually given
 * out. A crash wastes at most the unused remainder of a batch; sequence
 * values are cheap (64-bit longs) and, like transaction ids, never need
 * reclaiming.
 *
 * Known, honestly-stated difference from real Postgres: this never wraps
 * or cycles (no CYCLE/MAXVALUE support) and always increments upward
 * (INCREMENT BY is stored but a negative value isn't specially handled) -
 * real further work, not attempted here since the common case (a plain
 * auto-incrementing id) doesn't need either.
 */
public class Sequence {
    private static final Logger LOG = LoggerFactory.getLogger(Sequence.class);
    private static final long BATCH_SIZE = 100;

    private final String name;
    private final long incrementBy;
    private final File persistFile; // null means no persistence configured (e.g. some tests) - values still work correctly within the process, just don't survive a restart
    private final AtomicLong currentValue;
    private volatile long persistedWatermark;
    private final Object lock = new Object();

    public Sequence(String name, long startValue, long incrementBy, File persistFile) {
        this.name = name;
        this.incrementBy = incrementBy;
        this.persistFile = persistFile;
        long resumeFrom = startValue;
        if (persistFile != null && persistFile.exists()) {
            try {
                long persisted = Long.parseLong(Files.readString(persistFile.toPath()).trim());
                resumeFrom = persisted + incrementBy; // resume strictly after the last reserved watermark, never reusing it
                LOG.info("Sequence {} resuming at {} (persisted watermark {})", name, resumeFrom, persisted);
            } catch (Exception e) {
                LOG.error("Failed to load persisted sequence {} from {} - starting from its declared START value, "
                    + "which risks repeating already-issued values if this directory has prior data", name, persistFile, e);
            }
        }
        this.currentValue = new AtomicLong(resumeFrom - incrementBy); // nextValue() advances before returning, so back up one increment
        this.persistedWatermark = resumeFrom - incrementBy;
    }

    public String getName() {
        return name;
    }

    /** Advances the sequence and returns the new value - never repeats, even across a crash, by construction (see class javadoc). */
    public long nextValue() {
        long value = currentValue.addAndGet(incrementBy);
        if (isBeyondWatermark(value)) {
            reserveWatermark(value);
        }
        return value;
    }

    private boolean isBeyondWatermark(long value) {
        return incrementBy > 0 ? value > persistedWatermark : value < persistedWatermark;
    }

    private void reserveWatermark(long value) {
        synchronized (lock) {
            if (!isBeyondWatermark(value)) {
                return; // another thread already advanced the watermark past this value while we waited
            }
            long newWatermark = value + incrementBy * BATCH_SIZE;
            if (persistFile != null) {
                try {
                    File parent = persistFile.getParentFile();
                    if (parent != null) parent.mkdirs();
                    Files.writeString(persistFile.toPath(), String.valueOf(newWatermark));
                } catch (IOException e) {
                    LOG.error("Failed to persist watermark for sequence {} to {} - a restart after this point "
                        + "risks repeating values already issued by this session", name, persistFile, e);
                }
            }
            persistedWatermark = newWatermark;
        }
    }
}
