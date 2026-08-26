package com.stratosdb.storage.wal;

import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.SlottedPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Real, multi-segment WAL replay with an optional target-time cutoff -
 * the actual mechanics behind point-in-time recovery (see PitrBackup's
 * own javadoc for the rest of that story). A genuinely separate
 * implementation from WALManager.recover() rather than a modification
 * of it: recover() is this engine's own critical, already-well-tested
 * crash-recovery path, operating on exactly one active WAL file with
 * no notion of a time cutoff at all - risking a change to its own
 * internal structure to also support PITR's different requirements
 * (many archived segment files, replayed in order, with transactions
 * committed after a target time correctly excluded) was a real,
 * avoidable risk to something already working. This class re-derives
 * the same real two-pass redo logic (see WALManager.recover()'s own
 * javadoc for why two passes are needed at all: only a transaction
 * with a matching OP_COMMIT record is ever replayed) independently,
 * but generalized across an ordered list of in-memory WAL segment
 * byte arrays instead of one live FileChannel.
 */
public class PitrWalReplayer {
    private static final Logger LOG = LoggerFactory.getLogger(PitrWalReplayer.class);

    /** What actually happened during a replay - PitrRestore needs more than just a human-readable summary: it must also update the restored directory's own persisted commit log and XID watermark (see TransactionManager's own javadoc for why those exist and why a restored database's fresh TransactionManager would otherwise never recognize any replayed transaction as committed at all). */
    public record ReplayResult(String summary, Set<Long> appliedXids, long highestXidSeen) {}

    /**
     * Replays segments, in the given order, against diskManager - only
     * transactions that both (a) have a matching OP_COMMIT record
     * somewhere in the segments and (b) committed at or before
     * targetTimeMillis (or unconditionally, if targetTimeMillis is
     * null - meaning "replay everything available," the normal case
     * when no specific point in time was requested) are actually
     * applied.
     */
    public static ReplayResult replay(List<byte[]> segments, DiskManager diskManager, Long targetTimeMillis) {
        Map<Long, Long> committedXidToTimestamp = new HashMap<>();
        for (byte[] segment : segments) {
            collectCommits(segment, committedXidToTimestamp);
        }

        long highestXidSeen = 0;
        for (long xid : committedXidToTimestamp.keySet()) {
            highestXidSeen = Math.max(highestXidSeen, xid);
        }

        Set<Long> xidsToApply = new HashSet<>();
        for (Map.Entry<Long, Long> entry : committedXidToTimestamp.entrySet()) {
            if (targetTimeMillis == null || entry.getValue() <= targetTimeMillis) {
                xidsToApply.add(entry.getKey());
            }
        }

        Map<String, SlottedPage> dirtyPages = new HashMap<>();
        int replayedOps = 0;
        for (byte[] segment : segments) {
            replayedOps += replaySegment(segment, xidsToApply, diskManager, dirtyPages);
        }

        for (Map.Entry<String, SlottedPage> entry : dirtyPages.entrySet()) {
            String tableName = entry.getKey().substring(0, entry.getKey().lastIndexOf(':'));
            diskManager.writePage(tableName, entry.getValue());
        }

        String summary = "PITR replay complete: " + segments.size() + " segment(s), "
            + replayedOps + " operation(s) from " + xidsToApply.size() + " committed transaction(s) applied"
            + (targetTimeMillis != null ? " (target time: " + java.time.Instant.ofEpochMilli(targetTimeMillis) + ")" : " (no target time - replayed everything available)")
            + ", across " + dirtyPages.size() + " page(s)";
        LOG.info(summary);
        return new ReplayResult(summary, xidsToApply, highestXidSeen);
    }

    private static void collectCommits(byte[] segment, Map<Long, Long> out) {
        Cursor c = new Cursor(segment);
        while (c.hasRemaining()) {
            int opType = c.readInt();
            switch (opType) {
                case WALManager.OP_INSERT -> {
                    c.readLong();
                    c.readLengthPrefixedString();
                    c.readLong();
                    c.readInt();
                    int len = c.readInt();
                    c.skip(len);
                }
                case WALManager.OP_DELETE -> {
                    c.readLong();
                    c.readLengthPrefixedString();
                    c.readLong();
                    c.readInt();
                }
                case WALManager.OP_UPDATE -> {
                    c.readLong();
                    c.readLengthPrefixedString();
                    c.readLong();
                    c.readInt();
                    int oldLen = c.readInt();
                    c.skip(oldLen);
                    int newLen = c.readInt();
                    c.skip(newLen);
                }
                case WALManager.OP_COMMIT -> {
                    long xid = c.readLong();
                    long ts = c.readLong();
                    out.put(xid, ts);
                }
                case WALManager.OP_CHECKPOINT -> c.readLong();
                default -> {
                    LOG.warn("Unknown WAL record type {} during PITR pass 1 - stopping this segment's scan", opType);
                    return;
                }
            }
        }
    }

    private static int replaySegment(byte[] segment, Set<Long> xidsToApply, DiskManager diskManager, Map<String, SlottedPage> dirtyPages) {
        int replayedOps = 0;
        Cursor c = new Cursor(segment);
        while (c.hasRemaining()) {
            int opType = c.readInt();
            switch (opType) {
                case WALManager.OP_INSERT -> {
                    long xid = c.readLong();
                    String tableName = c.readLengthPrefixedString();
                    long pageId = c.readLong();
                    c.readInt(); // logged slot - redo re-derives it deterministically, same as WALManager.recover()
                    int len = c.readInt();
                    byte[] tupleData = c.readBytes(len);
                    if (xidsToApply.contains(xid)) {
                        loadOrGetDirtyPage(diskManager, dirtyPages, tableName, pageId).insertTuple(tupleData);
                        replayedOps++;
                    }
                }
                case WALManager.OP_DELETE -> {
                    long xid = c.readLong();
                    String tableName = c.readLengthPrefixedString();
                    long pageId = c.readLong();
                    int slot = c.readInt();
                    if (xidsToApply.contains(xid)) {
                        loadOrGetDirtyPage(diskManager, dirtyPages, tableName, pageId).deleteTuple(slot);
                        replayedOps++;
                    }
                }
                case WALManager.OP_UPDATE -> {
                    long xid = c.readLong();
                    String tableName = c.readLengthPrefixedString();
                    long pageId = c.readLong();
                    int slot = c.readInt();
                    int oldLen = c.readInt();
                    c.skip(oldLen);
                    int newLen = c.readInt();
                    byte[] newData = c.readBytes(newLen);
                    if (xidsToApply.contains(xid)) {
                        loadOrGetDirtyPage(diskManager, dirtyPages, tableName, pageId).updateTuple(slot, newData);
                        replayedOps++;
                    }
                }
                case WALManager.OP_COMMIT -> {
                    c.readLong();
                    c.readLong();
                }
                case WALManager.OP_CHECKPOINT -> c.readLong();
                default -> {
                    LOG.warn("Unknown WAL record type {} during PITR pass 2 - stopping this segment's scan", opType);
                    return replayedOps;
                }
            }
        }
        return replayedOps;
    }

    private static SlottedPage loadOrGetDirtyPage(DiskManager diskManager, Map<String, SlottedPage> dirtyPages, String tableName, long pageId) {
        String key = tableName + ":" + pageId;
        return dirtyPages.computeIfAbsent(key, k -> {
            com.stratosdb.storage.page.Page raw = diskManager.readPage(tableName, pageId);
            SlottedPage page = new SlottedPage(pageId);
            page.getBuffer().put(raw.getBytes());
            page.getBuffer().flip();
            return page;
        });
    }

    /** A tiny, self-contained byte[] cursor - the in-memory equivalent of WALManager's own exact-width FileChannel readers, needed here since PITR replay operates on whole segments already read into memory (see PitrRestore), not a live channel. */
    private static final class Cursor {
        private final byte[] data;
        private int pos = 0;

        Cursor(byte[] data) {
            this.data = data;
        }

        boolean hasRemaining() {
            return pos < data.length;
        }

        int readInt() {
            ByteBuffer buf = ByteBuffer.wrap(data, pos, 4);
            pos += 4;
            return buf.getInt();
        }

        long readLong() {
            ByteBuffer buf = ByteBuffer.wrap(data, pos, 8);
            pos += 8;
            return buf.getLong();
        }

        byte[] readBytes(int n) {
            byte[] out = new byte[n];
            System.arraycopy(data, pos, out, 0, n);
            pos += n;
            return out;
        }

        String readLengthPrefixedString() {
            int len = readInt();
            return new String(readBytes(len));
        }

        void skip(int n) {
            pos += n;
        }
    }
}
