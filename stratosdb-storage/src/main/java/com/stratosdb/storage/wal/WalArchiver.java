package com.stratosdb.storage.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Automated continuous WAL archiver that streams completed WAL segments
 * (and on-demand active WAL increments) to a {@link WalArchiveSink} (local or S3).
 */
public class WalArchiver implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(WalArchiver.class);

    private final WALManager walManager;
    private final WalArchiveSink sink;
    private final AtomicLong lastArchivedLsn = new AtomicLong(0);
    private final AtomicLong nextSequence = new AtomicLong(1);
    private ScheduledExecutorService scheduler;

    public WalArchiver(WALManager walManager, WalArchiveSink sink) {
        this.walManager = walManager;
        this.sink = sink;
    }

    public synchronized void start(long intervalMs) {
        stop();
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "wal-archiver");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                archiveNow();
            } catch (Exception e) {
                LOG.warn("WAL archiver pass failed: {}", e.getMessage());
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Archives any newly-written WAL bytes since the last archived LSN to the sink.
     * Returns the archived segment sequence number, or -1 if no new bytes exist.
     */
    public synchronized long archiveNow() throws IOException {
        long upToLsn = walManager.getCurrentLSN();
        long fromLsn = lastArchivedLsn.get();
        if (upToLsn <= fromLsn) {
            return -1L;
        }
        byte[] bytes = walManager.readBytesFrom(fromLsn);
        if (bytes == null || bytes.length == 0) {
            return -1L;
        }
        long seq = nextSequence.getAndIncrement();
        String name = String.format("%012d.walseg", seq);
        sink.uploadSegment(name, bytes);
        lastArchivedLsn.set(upToLsn);
        LOG.info("WalArchiver uploaded {} ({} bytes, LSN {}..{})", name, bytes.length, fromLsn, upToLsn);
        return seq;
    }

    public long getLastArchivedLsn() {
        return lastArchivedLsn.get();
    }
}
