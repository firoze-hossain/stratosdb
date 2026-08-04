package com.stratosdb.transaction;

import com.stratosdb.transaction.locking.LockManager;
import com.stratosdb.transaction.mvcc.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks transaction lifecycle and commit status well enough to support real
 * snapshot-isolation visibility (see mvcc.MVCCVisibility) and owns the
 * LockManager writers use for conflict detection.
 *
 * Persists two things across restarts, fixing a real gap found while testing
 * savepoints' crash-safety (see PROGRESS.md for the full story of how this
 * was found): the xid counter (so a restart never reuses an xid another
 * session already used - the actual bug: two unrelated transactions from
 * different sessions could get the same xid number, making a row's
 * visibility after a restart depend on incidental coincidence rather than
 * being reliably correct) and a commit log (so "did xid X commit" has a real
 * answer for xids from a previous session, not just "no, I've never heard of
 * it," which is what an empty in-memory set silently means every restart).
 *
 * Known simplification, stated plainly: the persisted commit log is
 * append-only and never compacted - it grows by one entry per committed
 * transaction, forever. A horizon/vacuum-style mechanism to safely forget
 * xids old enough that no live snapshot (current or future, after another
 * restart) could ever need them is real further work, not attempted here -
 * the same kind of bound `committedXids` itself has always needed even
 * in-memory, just now also relevant to a file that outlives the process.
 */
public class TransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

    /** How many xids get reserved (persisted) at once. A crash after using some of a batch just wastes the rest of it - xids are cheap, 64-bit, and never need reclaiming. */
    private static final long XID_BATCH_SIZE = 1000;

    private final AtomicLong nextXID = new AtomicLong(1);
    private final ThreadLocal<Transaction> currentTransaction = new ThreadLocal<>();

    private final Set<Long> activeXids = ConcurrentHashMap.newKeySet();
    private final Set<Long> committedXids = ConcurrentHashMap.newKeySet();
    private final Set<Long> abortedXids = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<Long, Transaction> transactionsByXid = new ConcurrentHashMap<>();

    private final LockManager lockManager = new LockManager();

    private final String dataDirectory;
    private volatile long persistedXidWatermark = 0; // xids up to and including this value are safe to use without another disk write
    private RandomAccessFile commitLogFile; // kept open for the process lifetime - opening/closing per commit would be needless I/O
    private final Object commitLogLock = new Object(); // serializes appends - commit() can be called from multiple threads

    public TransactionManager() {
        this(null);
    }

    public TransactionManager(String dataDirectory) {
        this.dataDirectory = dataDirectory;
        loadPersistedXidWatermark();
        loadPersistedCommitLog();
        openCommitLogForAppend();
    }

    private java.io.File xidCounterFile() {
        return dataDirectory == null ? null : new java.io.File(dataDirectory, "xid_counter.txt");
    }

    private java.io.File commitLogPath() {
        return dataDirectory == null ? null : new java.io.File(dataDirectory, "commit_log.dat");
    }

    private void loadPersistedXidWatermark() {
        java.io.File file = xidCounterFile();
        if (file == null || !file.exists()) return;
        try {
            long persisted = Long.parseLong(Files.readString(file.toPath()).trim());
            persistedXidWatermark = persisted;
            nextXID.set(persisted + 1); // resume strictly after the highest reserved value, never reusing one
            LOG.info("Resuming xid counter at {} (persisted watermark {})", nextXID.get(), persisted);
        } catch (Exception e) {
            LOG.error("Failed to load persisted xid counter from {} - starting from 1, which risks xid reuse if this directory has prior data", file, e);
        }
    }

    private void loadPersistedCommitLog() {
        java.io.File file = commitLogPath();
        if (file == null || !file.exists()) return;
        int loaded = 0;
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            while (true) {
                try {
                    committedXids.add(raf.readLong());
                    loaded++;
                } catch (EOFException e) {
                    break;
                }
            }
            LOG.info("Loaded {} previously-committed xid(s) from the persisted commit log", loaded);
        } catch (Exception e) {
            LOG.error("Failed to load persisted commit log from {}", file, e);
        }
    }

    private void openCommitLogForAppend() {
        java.io.File file = commitLogPath();
        if (file == null) return;
        try {
            commitLogFile = new RandomAccessFile(file, "rw");
            commitLogFile.seek(commitLogFile.length()); // append position
        } catch (Exception e) {
            LOG.error("Failed to open commit log {} for appending - commit status will not persist across a restart", file, e);
            commitLogFile = null;
        }
    }

    /**
     * Starts a new transaction and captures its snapshot: the set of xids
     * that are still active (not yet committed or aborted) at this exact
     * moment. That set is what lets MVCCVisibility tell "committed before I
     * started" apart from "committed after I started but before I read."
     */
    public Transaction begin() {
        long xid = nextXID.getAndIncrement();
        reserveXidWatermarkIfNeeded(xid);
        Set<Long> activeAtStart = new HashSet<>(activeXids); // snapshot BEFORE adding self
        activeXids.add(xid);

        Transaction tx = new Transaction(xid, new Snapshot(xid, activeAtStart));
        transactionsByXid.put(xid, tx);
        currentTransaction.set(tx);
        LOG.debug("Began transaction {} with snapshot {}", xid, tx.getSnapshot());
        return tx;
    }

    /**
     * Persists a new watermark BEFORE xid is allowed to be used, whenever
     * xid would exceed the currently-persisted one - so a crash immediately
     * after this xid is used can never result in a restart reusing it (the
     * restart would resume strictly after the persisted watermark, which by
     * construction is always >= xid at the point xid is actually handed out).
     */
    private void reserveXidWatermarkIfNeeded(long xid) {
        if (xid <= persistedXidWatermark) {
            return;
        }
        synchronized (this) {
            if (xid <= persistedXidWatermark) {
                return; // another thread already advanced it past xid while we waited
            }
            long newWatermark = xid + XID_BATCH_SIZE;
            java.io.File file = xidCounterFile();
            if (file != null) {
                try {
                    Files.writeString(file.toPath(), String.valueOf(newWatermark));
                } catch (IOException e) {
                    LOG.error("Failed to persist xid watermark to {} - a restart after this point risks xid reuse", file, e);
                }
            }
            persistedXidWatermark = newWatermark;
        }
    }

    public void commit(Transaction tx) {
        if (tx == null) return;
        tx.commit();
        committedXids.add(tx.getXID());
        appendToCommitLog(tx.getXID());
        activeXids.remove(tx.getXID());
        transactionsByXid.remove(tx.getXID());
        lockManager.releaseAll(tx.getXID());
        if (tx.equals(currentTransaction.get())) {
            currentTransaction.remove();
        }
        LOG.debug("Committed transaction {}", tx.getXID());
    }

    private void appendToCommitLog(long xid) {
        if (commitLogFile == null) return;
        synchronized (commitLogLock) {
            try {
                commitLogFile.writeLong(xid);
                commitLogFile.getFD().sync(); // durable before commit() returns - a crash right after must not lose this
            } catch (IOException e) {
                LOG.error("Failed to persist commit of xid {} to the commit log - it will not be recognized as committed after a restart", xid, e);
            }
        }
    }

    public void abort(Transaction tx) {
        if (tx == null) return;
        tx.abort();
        abortedXids.add(tx.getXID());
        activeXids.remove(tx.getXID());
        transactionsByXid.remove(tx.getXID());
        lockManager.releaseAll(tx.getXID());
        if (tx.equals(currentTransaction.get())) {
            currentTransaction.remove();
        }
        LOG.debug("Aborted transaction {}", tx.getXID());
    }

    /** True iff xid committed (as opposed to still active, aborted, or unknown). */
    public boolean isCommitted(long xid) {
        return committedXids.contains(xid);
    }

    public boolean isActive(long xid) {
        return activeXids.contains(xid);
    }

    public boolean isAborted(long xid) {
        return abortedXids.contains(xid);
    }

    /**
     * The horizon vacuum needs: a tuple version superseded by a committed
     * xmax strictly less than this is guaranteed unreachable by every
     * currently active transaction (every active snapshot already started
     * after that xmax committed, so MVCCVisibility would already correctly
     * hide the old version from all of them) - safe to physically reclaim.
     *
     * Returns nextXID's current value if there are no active transactions
     * at all: with nothing active, nothing has a floor to respect, and any
     * future transaction will be assigned an xid >= that value anyway, so
     * it can't possibly need to see anything older.
     */
    public long getOldestActiveXid() {
        long min = Long.MAX_VALUE;
        for (long xid : activeXids) {
            if (xid < min) min = xid;
        }
        return min == Long.MAX_VALUE ? nextXID.get() : min;
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    /** Closes the persisted commit log's file handle. Safe to call more than once. */
    public void close() {
        synchronized (commitLogLock) {
            if (commitLogFile != null) {
                try {
                    commitLogFile.close();
                } catch (IOException e) {
                    LOG.error("Failed to close commit log", e);
                }
                commitLogFile = null;
            }
        }
    }

    // --- ThreadLocal convenience API, kept for callers that want an implicit
    // "current transaction per calling thread" model instead of passing
    // Transaction objects around explicitly. ---

    public Transaction getCurrentTransaction() {
        return currentTransaction.get();
    }

    public void commit() {
        commit(currentTransaction.get());
    }

    public void rollback() {
        abort(currentTransaction.get());
    }
}
