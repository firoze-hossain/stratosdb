package com.stratosdb.storage.heap;

import com.stratosdb.common.exceptions.DeadlockException;
import com.stratosdb.common.exceptions.TransactionException;
import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.page.SlottedPage;
import com.stratosdb.transaction.TransactionManager;
import com.stratosdb.transaction.locking.LockManager;
import com.stratosdb.transaction.mvcc.MVCCVisibility;
import com.stratosdb.transaction.mvcc.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Heap table implementation
 */
public class HeapTable {
    private static final Logger LOG = LoggerFactory.getLogger(HeapTable.class);
    
    private final String name;
    private final BufferPool bufferPool;
    /**
     * Atomic, not a plain long: now that insert() uses fine-grained,
     * per-page latches instead of one coarse table-wide synchronized
     * lock (see insert()'s own javadoc), multiple threads can genuinely
     * read and race to advance this field concurrently while deciding
     * whether to extend the table with a new page - a plain long here
     * would risk a stale, un-published read across threads with no
     * happens-before relationship at all.
     */
    private final java.util.concurrent.atomic.AtomicLong lastPageId;

    /**
     * The visibility map: bit i set means page i is currently "all
     * visible" - every tuple on it has a committed xmin and NO xmax at
     * all (never deleted/updated, ever - not just "the delete is old
     * enough to reclaim"). That distinction matters: a tuple with any
     * xmax set has visibility that genuinely differs between an old
     * snapshot and a new one, even before vacuum's horizon check makes
     * it safe to physically reclaim - so "all visible" must mean nothing
     * on the page has any xmax, not just "nothing reclaimable remains."
     *
     * Only ever SET by vacuum() (after physically reclaiming anything
     * reclaimable, checking what's left), and CLEARED by insert()/
     * update()/deleteMvcc() the moment they touch a page - inserting a
     * new (possibly uncommitted) tuple, or giving an existing tuple an
     * xmax, both invalidate the "everything here is safely visible to
     * everyone" guarantee until the next vacuum re-establishes it.
     *
     * This is what index-only scans (see ExecutorEngine) actually check
     * before trusting an index's key value directly instead of fetching
     * the heap tuple to verify visibility.
     */
    private final BitSet visibilityMap = new BitSet();

    public HeapTable(String name, BufferPool bufferPool) {
        this.name = name;
        this.bufferPool = bufferPool;
        // Discover how many pages this table actually has on disk. Previously this
        // was hardcoded to 0, meaning every HeapTable object - including one freshly
        // constructed after a restart - only ever looked at page 0, no matter how
        // many pages the table really had. scan()/insert() silently ignored every
        // page beyond the first for any table that survived a restart.
        long existingPages = bufferPool.getTablePageCount(name);
        this.lastPageId = new java.util.concurrent.atomic.AtomicLong(existingPages > 0 ? existingPages - 1 : 0);
    }

    /** Whether pageId is currently known to be "all visible" - see visibilityMap's own javadoc for the exact guarantee this means. Conservatively false (never crashes) for a pageId never observed. */
    public boolean isAllVisible(long pageId) {
        return pageId >= 0 && pageId <= Integer.MAX_VALUE && visibilityMap.get((int) pageId);
    }

    /**
     * Insert a tuple.
     *
     * Real, fine-grained page latching, not one coarse table-wide lock:
     * each existing page is tried under its OWN write latch, held only
     * for that page's own insert attempt - concurrent inserts landing on
     * different pages of the same table now genuinely proceed in
     * parallel, rather than serializing through one lock regardless of
     * which page each one actually targets.
     *
     * The real, separate risk this replaces a coarse lock's brute-force
     * fix for (a real, serious bug found while testing sequence-generated
     * ids under concurrency, though not specific to sequences at all):
     * two threads both deciding to extend the table with a new page could
     * race on lastPageId itself - not a page's own contents, but the
     * decision of which page id even IS the new one. Solved here with a
     * real compareAndSet retry loop: a thread that loses the race to
     * publish its own candidate new page id (because a concurrent thread
     * already advanced lastPageId past it) doesn't silently double-use a
     * page id or corrupt anything - it simply retries with a fresh
     * candidate. The same loop also handles the rarer case where a "new"
     * page is found already full by the time this thread's own write
     * latch on it is granted (another thread got there first and filled
     * it) - retried, not assumed to succeed.
     */
    public InsertResult insert(byte[] tupleData) {
        // Fast path: try every existing page under its own write latch, held only
        // for this one insert attempt on that specific page.
        long snapshotLast = lastPageId.get();
        for (long pageId = 0; pageId <= snapshotLast; pageId++) {
            SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
            page.getLatch().writeLock().lock();
            try {
                if (page.hasSpace(tupleData)) {
                    int slot = page.insertTuple(tupleData);
                    if (slot != -1) {
                        bufferPool.markDirty(name, pageId);
                        visibilityMap.clear((int) pageId); // a new (possibly uncommitted) tuple just landed here
                        LOG.debug("Inserted at {}/{}", pageId, slot);
                        return new InsertResult(pageId, slot);
                    }
                }
            } finally {
                page.getLatch().writeLock().unlock();
                bufferPool.unpinPage(name, pageId);
            }
        }

        // No existing page had space - extend the table. This MUST go through the
        // buffer pool (getPage), the same as every existing page above - otherwise
        // the page only exists in a local variable, is never registered in the
        // pool's cache, and is silently dropped on the floor: flushAll()/eviction
        // can only persist pages they know about.
        while (true) {
            long observedLast = lastPageId.get();
            long candidatePageId = observedLast + 1;
            SlottedPage newPage = (SlottedPage) bufferPool.getPage(name, candidatePageId);
            newPage.getLatch().writeLock().lock();
            try {
                int slot = newPage.insertTuple(tupleData);
                if (slot == -1) {
                    // A concurrent thread already claimed and filled this exact candidate
                    // page before we got its write latch - retry with a fresh candidate
                    // rather than assuming this attempt succeeded.
                    lastPageId.compareAndSet(observedLast, candidatePageId);
                    continue;
                }
                bufferPool.markDirty(name, candidatePageId);
                visibilityMap.clear((int) candidatePageId); // a brand new page always starts NOT all-visible
                lastPageId.compareAndSet(observedLast, candidatePageId);
                LOG.debug("Created new page {} for insertion", candidatePageId);
                return new InsertResult(candidatePageId, slot);
            } finally {
                newPage.getLatch().writeLock().unlock();
                bufferPool.unpinPage(name, candidatePageId);
            }
        }
    }

    /**
     * Scan all tuples
     */
    public List<byte[]> scan() {
        List<byte[]> results = new ArrayList<>();
        
        for (long pageId = 0; pageId <= lastPageId.get(); pageId++) {
            SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
            page.getLatch().readLock().lock();
            try {
                List<Integer> slots = page.getValidSlots();
                for (int slot : slots) {
                    byte[] tuple = page.readTuple(slot);
                    if (tuple != null) {
                        results.add(tuple);
                    }
                }
            } finally {
                page.getLatch().readLock().unlock();
                bufferPool.unpinPage(name, pageId);
            }
        }
        
        LOG.debug("Scanned {} tuples from table {}", results.size(), name);
        return results;
    }
    
    /**
     * Scan with limit
     */
    public List<byte[]> scan(int limit) {
        List<byte[]> results = new ArrayList<>();
        int count = 0;
        
        for (long pageId = 0; pageId <= lastPageId.get() && count < limit; pageId++) {
            SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
            page.getLatch().readLock().lock();
            try {
                List<Integer> slots = page.getValidSlots();
                for (int slot : slots) {
                    if (count >= limit) break;
                    byte[] tuple = page.readTuple(slot);
                    if (tuple != null) {
                        results.add(tuple);
                        count++;
                    }
                }
            } finally {
                page.getLatch().readLock().unlock();
                bufferPool.unpinPage(name, pageId);
            }
        }
        
        return results;
    }
    
    /**
     * Delete by page and slot
     */
    public boolean delete(long pageId, int slot) {
        SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
        page.getLatch().writeLock().lock();
        try {
            page.deleteTuple(slot);
            bufferPool.markDirty(name, pageId);
        } finally {
            page.getLatch().writeLock().unlock();
            bufferPool.unpinPage(name, pageId);
        }
        LOG.debug("Deleted {}/{}", pageId, slot);
        return true;
    }
    
    /**
     * Update by page and slot
     */
    public boolean update(long pageId, int slot, byte[] newData) {
        SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
        boolean result;
        page.getLatch().writeLock().lock();
        try {
            result = page.updateTuple(slot, newData);
            if (result) {
                bufferPool.markDirty(name, pageId);
                visibilityMap.clear((int) pageId); // e.g. an xmax was just set (deleteMvcc's tombstone) - visibility on this page is now snapshot-dependent
            }
        } finally {
            page.getLatch().writeLock().unlock();
            bufferPool.unpinPage(name, pageId);
        }
        return result;
    }
    
    /**
     * Get tuple by page and slot
     */
    public byte[] readTuple(long pageId, int slot) {
        SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
        page.getLatch().readLock().lock();
        try {
            return page.readTuple(slot);
        } finally {
            page.getLatch().readLock().unlock();
            bufferPool.unpinPage(name, pageId);
        }
    }
    
    public String getName() { return name; }
    public long getLastPageId() { return lastPageId.get(); }

    // --- MVCC-aware API ---
    // These sit on top of the raw methods above rather than replacing them:
    // every stored row is the raw payload with a 16-byte (xmin, xmax) header
    // prepended (see MVCCVisibility). The raw insert/scan/delete/update above
    // are untouched so existing callers (e.g. the Week 1 crash-recovery test)
    // keep working exactly as before against tables that don't use MVCC.

    /** A scanned row together with its physical position, needed so callers can target UPDATE/DELETE at it. */
    public record PositionedRow(long pageId, int slot, byte[] stored) {}

    /** Like scan(), but keeps (pageId, slot) around instead of returning bare payload bytes. */
    public List<PositionedRow> scanPositioned() {
        List<PositionedRow> results = new ArrayList<>();
        for (long pageId = 0; pageId <= lastPageId.get(); pageId++) {
            SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
            page.getLatch().readLock().lock();
            try {
                for (int slot : page.getValidSlots()) {
                    byte[] stored = page.readTuple(slot);
                    if (stored != null) {
                        results.add(new PositionedRow(pageId, slot, stored));
                    }
                }
            } finally {
                page.getLatch().readLock().unlock();
                bufferPool.unpinPage(name, pageId);
            }
        }
        return results;
    }

    /** Inserts payload as a new row version created by xid (xmax left unset - i.e. currently live). */
    public InsertResult insertMvcc(byte[] payload, long xid) {
        byte[] stored = MVCCVisibility.wrap(payload, xid, MVCCVisibility.NO_XMAX);
        return insert(stored);
    }

    /** Returns the payload bytes of every row version visible to this snapshot. */
    public List<byte[]> scanMvcc(Snapshot snapshot, TransactionManager txnManager) {
        List<byte[]> visible = new ArrayList<>();
        for (PositionedRow row : scanPositioned()) {
            if (MVCCVisibility.isVisible(row.stored(), snapshot, txnManager)) {
                visible.add(MVCCVisibility.readPayload(row.stored()));
            }
        }
        return visible;
    }

    public record VacuumResult(int reclaimedVersions, int pagesCompacted) {}

    /**
     * Reclaims space from row versions that are truly dead: superseded by
     * a committed xmax strictly less than horizonXid (see
     * TransactionManager.getOldestActiveXid - the caller is responsible
     * for computing that horizon, since it needs a live view of every
     * currently active transaction, which belongs to TransactionManager,
     * not this class).
     *
     * A version below the horizon is safe to physically remove because
     * every currently active transaction's snapshot was taken after that
     * xmax committed - MVCCVisibility would already correctly hide the old
     * version from every one of them. A version at or above the horizon is
     * left untouched even if it looks "old," because some active
     * transaction's snapshot might genuinely still need to see it -
     * reclaiming it early would be a real correctness bug (a still-running
     * transaction losing a row version its own snapshot is entitled to),
     * not just a missed optimization.
     *
     * Physical reclamation, not just marking: SlottedPage.deleteTuple()
     * marks the dead version's slot invalid, then SlottedPage.defragment()
     * compacts out its actual bytes so insert() (which already scans
     * existing pages for free space before allocating a new one) can
     * genuinely reuse that space for a future row.
     */
    public VacuumResult vacuum(long horizonXid, TransactionManager txnManager) {
        int reclaimedVersions = 0;
        int pagesCompacted = 0;

        // lastPageId, not bufferPool.getTablePageCount(name): that queries the
        // on-disk file size directly, which only grows as pages are actually
        // flushed - a page still dirty-only in the buffer pool (the common
        // case for anything written earlier in the same process, before any
        // checkpoint/close) wouldn't be counted yet, and vacuum would silently
        // scan zero pages. lastPageId is this table's own in-memory record of
        // every page it has ever allocated, which is exactly what insert()
        // and scan() already rely on for the same reason.
        for (long pageId = 0; pageId <= lastPageId.get(); pageId++) {
            SlottedPage page = (SlottedPage) bufferPool.getPage(name, pageId);
            page.getLatch().writeLock().lock();
            try {
                boolean anyReclaimed = false;
                for (int slot : page.getValidSlots()) {
                    byte[] stored = page.readTuple(slot);
                    if (stored == null) {
                        continue;
                    }
                    long xmax = MVCCVisibility.readXmax(stored);
                    if (xmax != MVCCVisibility.NO_XMAX
                            && txnManager.isCommitted(xmax)
                            && xmax < horizonXid) {
                        page.deleteTuple(slot);
                        reclaimedVersions++;
                        anyReclaimed = true;
                    }
                }
                if (anyReclaimed) {
                    page.defragment();
                    bufferPool.markDirty(name, pageId);
                    pagesCompacted++;
                }

                // Visibility map: after reclaiming whatever was safe to reclaim,
                // this page qualifies as "all visible" only if EVERY remaining
                // tuple has a committed xmin and NO xmax at all - not merely "no
                // xmax below the horizon." A tuple with any xmax set (even one
                // not yet old enough to reclaim) has visibility that genuinely
                // differs between snapshots, which disqualifies the whole page
                // regardless of how much dead space vacuum just cleaned up.
                boolean allVisible = true;
                for (int slot : page.getValidSlots()) {
                    byte[] stored = page.readTuple(slot);
                    if (stored == null) {
                        continue;
                    }
                    long xmin = MVCCVisibility.readXmin(stored);
                    long xmax = MVCCVisibility.readXmax(stored);
                    if (!txnManager.isCommitted(xmin) || xmax != MVCCVisibility.NO_XMAX) {
                        allVisible = false;
                        break;
                    }
                }
                if (allVisible) {
                    visibilityMap.set((int) pageId);
                } else {
                    visibilityMap.clear((int) pageId);
                }
            } finally {
                page.getLatch().writeLock().unlock();
                bufferPool.unpinPage(name, pageId);
            }
        }

        LOG.debug("Vacuumed {}: reclaimed {} dead row version(s) across {} page(s)", name, reclaimedVersions, pagesCompacted);
        return new VacuumResult(reclaimedVersions, pagesCompacted);
    }

    /**
     * Deletes (tombstones) the row at (pageId, slot) on behalf of xid, after
     * taking an exclusive lock on it. Returns false if the row is not visible
     * to this transaction's snapshot (already deleted by someone else, or
     * never existed) rather than throwing - a delete of something you can't
     * see is a no-op, not a conflict.
     */
    public boolean deleteMvcc(long pageId, int slot, long xid, Snapshot snapshot,
                               TransactionManager txnManager, LockManager lockManager) throws DeadlockException {
        lockManager.acquireExclusive(new LockManager.RowId(name, pageId, slot), xid);
        byte[] stored = readTuple(pageId, slot);
        if (stored == null || !MVCCVisibility.isVisible(stored, snapshot, txnManager)) {
            return false;
        }
        byte[] tombstoned = MVCCVisibility.withXmax(stored, xid);
        return update(pageId, slot, tombstoned);
    }

    /**
     * Updates the row at (pageId, slot) on behalf of xid: tombstones the old
     * version in place and inserts newPayload as a brand-new version (which
     * may land on a different page/slot - MVCC row identity here is "this
     * table's data", not a fixed physical location tracked across versions).
     * Throws TransactionException if the row is not visible to this
     * transaction's snapshot - unlike delete, a write conflict on UPDATE is
     * treated as an error the caller should see, not a silent no-op.
     */
    public InsertResult updateMvcc(long pageId, int slot, byte[] newPayload, long xid, Snapshot snapshot,
                                    TransactionManager txnManager, LockManager lockManager) throws DeadlockException {
        lockManager.acquireExclusive(new LockManager.RowId(name, pageId, slot), xid);
        byte[] stored = readTuple(pageId, slot);
        if (stored == null || !MVCCVisibility.isVisible(stored, snapshot, txnManager)) {
            throw new TransactionException("Write conflict: row " + pageId + "/" + slot
                + " is not visible to transaction " + xid);
        }
        byte[] tombstoned = MVCCVisibility.withXmax(stored, xid);
        update(pageId, slot, tombstoned);
        return insertMvcc(newPayload, xid);
    }

    public static class InsertResult {
        public final long pageId;
        public final int slot;
        
        public InsertResult(long pageId, int slot) {
            this.pageId = pageId;
            this.slot = slot;
        }
    }
}