package com.stratosdb.index.hash;

import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.page.BTreePage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A disk-backed static hash index: long keys mapping to row locations
 * (RID = heap pageId + slot), built on the same Page/BufferPool
 * infrastructure as BTreeIndex and the heap storage engine - no second
 * storage path, and it reuses BTreePage's existing leaf layout as the
 * bucket page format (a bucket doesn't need B+Tree's ordering invariant,
 * just somewhere to hold (key, RID) pairs and a "next page" pointer for
 * overflow chaining, both of which BTreePage's leaf format already
 * provides for free).
 *
 * Design: NUM_BUCKETS fixed buckets, chosen at construction and never
 * resized (static hashing - real Postgres hash indexes support bucket
 * splitting as they grow; this doesn't). Page 0 is metadata; pages
 * 1..NUM_BUCKETS are each a bucket's primary page. A bucket that
 * overflows its primary page links to an overflow page via the same
 * nextLeafPageId field BTreePage already uses for its own leaf chain -
 * borrowed here for an unrelated purpose (bucket chaining, not sorted
 * leaf traversal), which is a legitimate reuse of the same "next page"
 * mechanism, not a hack specific to B+Trees.
 *
 * What this does: point insert, point search (single or all matches for
 * a duplicate key), and point delete by (key, RID) - real hashing (not a
 * linear scan pretending to be a hash index), verified at a scale that
 * forces real overflow chains, not just single-bucket cases.
 *
 * What this deliberately does NOT do, stated plainly: range scans (hashing
 * destroys key order on purpose - a caller needing ordered access should
 * use BTreeIndex), and no dynamic bucket splitting as the index grows
 * (a fixed bucket count chosen up front; heavy skew or a much larger
 * dataset than anticipated degrades toward long overflow chains rather
 * than rehashing - the standard tradeoff of static over extendible
 * hashing, real further work if it's ever needed).
 */
public class HashIndex implements com.stratosdb.index.KeyValueIndex {
    private static final Logger LOG = LoggerFactory.getLogger(HashIndex.class);

    private static final long META_PAGE_ID = 0;
    private static final int NUM_BUCKETS = 251; // a prime bucket count spreads hashCode() collisions more evenly than a round number

    private final String indexName;
    private final BufferPool bufferPool;
    private long nextPageId;

    public HashIndex(String indexName, BufferPool bufferPool) {
        this.indexName = indexName;
        this.bufferPool = bufferPool;

        long existingPages = bufferPool.getTablePageCount(indexName);
        if (existingPages <= 1) {
            initializeNewIndex();
        }
        this.nextPageId = Math.max(existingPages, NUM_BUCKETS + 1);
    }

    private void initializeNewIndex() {
        for (long bucket = 0; bucket < NUM_BUCKETS; bucket++) {
            long pageId = bucketPrimaryPageId(bucket);
            BTreePage page = bufferPool.getPage(indexName, pageId, BTreePage.FACTORY);
            page.setLeafContents(List.of(), List.of());
            page.setNextLeafPageId(-1); // no overflow page yet
            bufferPool.markDirty(indexName, pageId);
            bufferPool.unpinPage(indexName, pageId);
        }
        LOG.debug("Initialized new hash index '{}' with {} buckets", indexName, NUM_BUCKETS);
    }

    private long bucketPrimaryPageId(long bucket) {
        return bucket + 1; // page 0 is reserved for metadata (see class javadoc); buckets start at page 1
    }

    private long bucketFor(long key) {
        // Long.hashCode() can be negative; floorMod gives a proper 0..NUM_BUCKETS-1 result either way.
        return Math.floorMod(Long.hashCode(key), NUM_BUCKETS);
    }

    public void insert(long key, BTreePage.RID rid) {
        long pageId = bucketPrimaryPageId(bucketFor(key));
        BTreePage page = bufferPool.getPage(indexName, pageId, BTreePage.FACTORY);

        // Walk the overflow chain to the last page, so a new entry lands in
        // the first page that has room rather than always appending a new
        // overflow page even when an earlier one in the chain has space
        // (which can happen after deletes free room in an earlier page).
        while (page.getKeyCount() >= BTreePage.MAX_LEAF_KEYS && page.getNextLeafPageId() != -1) {
            long nextId = page.getNextLeafPageId();
            bufferPool.unpinPage(indexName, pageId);
            pageId = nextId;
            page = bufferPool.getPage(indexName, pageId, BTreePage.FACTORY);
        }

        if (page.getKeyCount() >= BTreePage.MAX_LEAF_KEYS) {
            // This page (the last in the chain) is full too - allocate a new overflow page.
            long newPageId = nextPageId++;
            BTreePage newPage = bufferPool.getPage(indexName, newPageId, BTreePage.FACTORY);
            newPage.setLeafContents(List.of(key), List.of(rid));
            newPage.setNextLeafPageId(-1);
            bufferPool.markDirty(indexName, newPageId);
            bufferPool.unpinPage(indexName, newPageId);

            page.setNextLeafPageId(newPageId);
            bufferPool.markDirty(indexName, pageId);
            bufferPool.unpinPage(indexName, pageId);
        } else {
            List<Long> keys = new ArrayList<>(page.getKeys());
            List<BTreePage.RID> values = new ArrayList<>(page.getLeafValues());
            keys.add(key);
            values.add(rid);
            page.setLeafContents(keys, values);
            bufferPool.markDirty(indexName, pageId);
            bufferPool.unpinPage(indexName, pageId);
        }
    }

    /** The first matching RID for this key, or null if none. Use searchAll if duplicate keys are possible and all matches matter. */
    public BTreePage.RID search(long key) {
        List<BTreePage.RID> all = searchAll(key);
        return all.isEmpty() ? null : all.get(0);
    }

    public List<BTreePage.RID> searchAll(long key) {
        List<BTreePage.RID> results = new ArrayList<>();
        long pageId = bucketPrimaryPageId(bucketFor(key));
        while (pageId != -1) {
            BTreePage page = bufferPool.getPage(indexName, pageId, BTreePage.FACTORY);
            List<Long> keys = page.getKeys();
            List<BTreePage.RID> values = page.getLeafValues();
            for (int i = 0; i < keys.size(); i++) {
                if (keys.get(i) == key) {
                    results.add(values.get(i));
                }
            }
            long nextPage = page.getNextLeafPageId();
            bufferPool.unpinPage(indexName, pageId);
            pageId = nextPage;
        }
        return results;
    }

    /** Removes the exact (key, rid) pair. A no-op if that exact pair isn't present. */
    public void delete(long key, BTreePage.RID rid) {
        long pageId = bucketPrimaryPageId(bucketFor(key));
        while (pageId != -1) {
            BTreePage page = bufferPool.getPage(indexName, pageId, BTreePage.FACTORY);
            List<Long> keys = new ArrayList<>(page.getKeys());
            List<BTreePage.RID> values = new ArrayList<>(page.getLeafValues());

            int removeIdx = -1;
            for (int i = 0; i < keys.size(); i++) {
                if (keys.get(i) == key && values.get(i).equals(rid)) {
                    removeIdx = i;
                    break;
                }
            }
            long nextPage = page.getNextLeafPageId();
            if (removeIdx != -1) {
                keys.remove(removeIdx);
                values.remove(removeIdx);
                page.setLeafContents(keys, values);
                bufferPool.markDirty(indexName, pageId);
                bufferPool.unpinPage(indexName, pageId);
                return; // exact pair found and removed - done (a bucket page left underfull is fine, see class javadoc)
            }
            bufferPool.unpinPage(indexName, pageId);
            pageId = nextPage;
        }
        // Exact pair not found anywhere in the chain - a no-op, matching BTreeIndex.delete's contract.
    }

    public static int bucketCount() {
        return NUM_BUCKETS;
    }
}
