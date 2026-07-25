package com.stratosdb.index.btree;

import com.stratosdb.common.exceptions.StorageException;
import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.page.BTreePage;
import com.stratosdb.storage.page.Page;
import com.stratosdb.storage.page.PageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A disk-backed B+Tree index: long keys mapping to row locations (RID =
 * heap pageId + slot). Built on the same Page/BufferPool infrastructure as
 * the heap storage engine - no second storage path.
 *
 * Page 0 of the index's file is reserved for metadata (root page id).
 * Every other page is a BTreePage (leaf or internal node).
 *
 * What this does: point insert, point search, and range scan, with correct
 * node splitting (leaf and internal) as the tree grows - verified by
 * actually forcing multi-level splits in tests, not just single-page cases.
 *
 * What this deliberately does NOT do yet, stated plainly rather than
 * glossed over: deletion (no merge/redistribute logic), concurrent
 * insert/search from multiple threads (no latching - callers must
 * serialize writers themselves for now), and integration into the query
 * planner (nothing in ExecutorEngine chooses an index scan yet - that's
 * the very next piece once this is verified solid). Duplicate keys are
 * supported (searchAll/rangeScan return every match); deleting a specific
 * duplicate is therefore not as simple as "remove this key" even once
 * deletion exists, and that's worth remembering when it's built.
 */
public class BTreeIndex {
    private static final Logger LOG = LoggerFactory.getLogger(BTreeIndex.class);

    private static final long META_PAGE_ID = 0;
    private static final long INITIAL_ROOT_PAGE_ID = 1;
    private static final int META_MAGIC = 0x53545242; // "STRB"
    private static final int META_MAGIC_OFFSET = 24; // right after Page's base 24-byte header
    private static final int META_ROOT_OFFSET = 28;

    private static final PageFactory<Page> RAW_PAGE_FACTORY = new PageFactory<>() {
        @Override
        public Page createEmpty(long pageId) {
            return new Page(pageId);
        }

        @Override
        public Page wrap(long pageId, byte[] existingBytes) {
            return new Page(pageId, existingBytes);
        }
    };

    private final String indexName;
    private final BufferPool bufferPool;
    private long rootPageId;
    private long nextPageId;

    public BTreeIndex(String indexName, BufferPool bufferPool) {
        this.indexName = indexName;
        this.bufferPool = bufferPool;

        long existingPages = bufferPool.getTablePageCount(indexName);
        if (existingPages <= 1) {
            initializeNewIndex();
        } else {
            loadMetadata();
        }
        this.nextPageId = Math.max(existingPages, INITIAL_ROOT_PAGE_ID + 1);
    }

    private void initializeNewIndex() {
        BTreePage root = bufferPool.getPage(indexName, INITIAL_ROOT_PAGE_ID, BTreePage.FACTORY);
        root.setLeafContents(List.of(), List.of());
        // setLeafContents deliberately doesn't touch nextLeafPageId (splits manage
        // that field explicitly). A brand-new root has no next leaf at all, and
        // that must be set here - the underlying page's bytes otherwise carry
        // whatever Page's generic header initialization left behind, which
        // decodes to a nonsense "next page" value once every later split
        // propagates it forward as "whatever the pre-split page's next was."
        root.setNextLeafPageId(-1);
        bufferPool.markDirty(indexName, INITIAL_ROOT_PAGE_ID);
        bufferPool.unpinPage(indexName, INITIAL_ROOT_PAGE_ID);

        this.rootPageId = INITIAL_ROOT_PAGE_ID;
        saveMetadata();
        LOG.debug("Initialized new B+Tree index '{}' with empty root at page {}", indexName, INITIAL_ROOT_PAGE_ID);
    }

    private void loadMetadata() {
        Page meta = bufferPool.getPage(indexName, META_PAGE_ID, RAW_PAGE_FACTORY);
        int magic = meta.getBuffer().getInt(META_MAGIC_OFFSET);
        if (magic != META_MAGIC) {
            bufferPool.unpinPage(indexName, META_PAGE_ID);
            throw new StorageException("Index metadata page for '" + indexName + "' is missing or corrupt");
        }
        this.rootPageId = meta.getBuffer().getLong(META_ROOT_OFFSET);
        bufferPool.unpinPage(indexName, META_PAGE_ID);
        LOG.debug("Loaded B+Tree index '{}', root at page {}", indexName, rootPageId);
    }

    private void saveMetadata() {
        Page meta = bufferPool.getPage(indexName, META_PAGE_ID, RAW_PAGE_FACTORY);
        meta.getBuffer().putInt(META_MAGIC_OFFSET, META_MAGIC);
        meta.getBuffer().putLong(META_ROOT_OFFSET, rootPageId);
        meta.setDirty(true);
        bufferPool.markDirty(indexName, META_PAGE_ID);
        bufferPool.unpinPage(indexName, META_PAGE_ID);
    }

    private long allocatePageId() {
        return nextPageId++;
    }

    // --- point insert ---

    public void insert(long key, BTreePage.RID rid) {
        SplitResult result = insertRecursive(rootPageId, key, rid);
        if (result != null) {
            long newRootId = allocatePageId();
            BTreePage newRoot = bufferPool.getPage(indexName, newRootId, BTreePage.FACTORY);
            newRoot.setInternalContents(List.of(result.splitKey), List.of(rootPageId, result.newPageId));
            bufferPool.markDirty(indexName, newRootId);
            bufferPool.unpinPage(indexName, newRootId);

            rootPageId = newRootId;
            saveMetadata();
            LOG.debug("Root split: new root {} for index '{}'", newRootId, indexName);
        }
    }

    private record SplitResult(long splitKey, long newPageId) {}

    private SplitResult insertRecursive(long pageId, long key, BTreePage.RID rid) {
        BTreePage page = bufferPool.getPage(indexName, pageId, BTreePage.FACTORY);

        if (page.isLeaf()) {
            List<Long> keys = new ArrayList<>(page.getKeys());
            List<BTreePage.RID> values = new ArrayList<>(page.getLeafValues());

            int idx = Collections.binarySearch(keys, key);
            int insertPos = idx >= 0 ? idx : -(idx + 1);
            keys.add(insertPos, key);
            values.add(insertPos, rid);

            if (keys.size() <= BTreePage.MAX_LEAF_KEYS) {
                page.setLeafContents(keys, values);
                bufferPool.markDirty(indexName, pageId);
                bufferPool.unpinPage(indexName, pageId);
                return null;
            }

            // Split: left half stays on this page, right half moves to a new leaf.
            int mid = keys.size() / 2;
            List<Long> rightKeys = new ArrayList<>(keys.subList(mid, keys.size()));
            List<BTreePage.RID> rightValues = new ArrayList<>(values.subList(mid, values.size()));
            List<Long> leftKeys = new ArrayList<>(keys.subList(0, mid));
            List<BTreePage.RID> leftValues = new ArrayList<>(values.subList(0, mid));

            long newPageId = allocatePageId();
            BTreePage newLeaf = bufferPool.getPage(indexName, newPageId, BTreePage.FACTORY);
            newLeaf.setLeafContents(rightKeys, rightValues);
            newLeaf.setNextLeafPageId(page.getNextLeafPageId());
            bufferPool.markDirty(indexName, newPageId);
            bufferPool.unpinPage(indexName, newPageId);

            page.setLeafContents(leftKeys, leftValues);
            page.setNextLeafPageId(newPageId);
            bufferPool.markDirty(indexName, pageId);
            bufferPool.unpinPage(indexName, pageId);

            // The smallest key now in the new right leaf becomes the separator -
            // "child to the right of this separator holds keys >= separator".
            return new SplitResult(rightKeys.get(0), newPageId);
        }

        // Internal node.
        List<Long> keys = page.getKeys();
        List<Long> children = page.getChildren();
        int childIdx = findChildIndex(keys, key);
        long childPageId = children.get(childIdx);

        SplitResult childSplit = insertRecursive(childPageId, key, rid);
        if (childSplit == null) {
            bufferPool.unpinPage(indexName, pageId);
            return null;
        }

        List<Long> newKeys = new ArrayList<>(keys);
        List<Long> newChildren = new ArrayList<>(children);
        newKeys.add(childIdx, childSplit.splitKey());
        newChildren.add(childIdx + 1, childSplit.newPageId());

        if (newKeys.size() <= BTreePage.MAX_INTERNAL_KEYS) {
            page.setInternalContents(newKeys, newChildren);
            bufferPool.markDirty(indexName, pageId);
            bufferPool.unpinPage(indexName, pageId);
            return null;
        }

        // Split internal node: the middle key is PROMOTED (removed from both
        // sides, not copied) - that's what distinguishes an internal split
        // from a leaf split.
        int mid = newKeys.size() / 2;
        long promotedKey = newKeys.get(mid);

        List<Long> leftKeys = new ArrayList<>(newKeys.subList(0, mid));
        List<Long> leftChildren = new ArrayList<>(newChildren.subList(0, mid + 1));
        List<Long> rightKeys = new ArrayList<>(newKeys.subList(mid + 1, newKeys.size()));
        List<Long> rightChildren = new ArrayList<>(newChildren.subList(mid + 1, newChildren.size()));

        long newPageId = allocatePageId();
        BTreePage newInternal = bufferPool.getPage(indexName, newPageId, BTreePage.FACTORY);
        newInternal.setInternalContents(rightKeys, rightChildren);
        bufferPool.markDirty(indexName, newPageId);
        bufferPool.unpinPage(indexName, newPageId);

        page.setInternalContents(leftKeys, leftChildren);
        bufferPool.markDirty(indexName, pageId);
        bufferPool.unpinPage(indexName, pageId);

        return new SplitResult(promotedKey, newPageId);
    }

    /** child[i] holds keys < keys[i]; child[i+1] holds keys >= keys[i]. */
    private int findChildIndex(List<Long> keys, long key) {
        int idx = 0;
        while (idx < keys.size() && key >= keys.get(idx)) {
            idx++;
        }
        return idx;
    }

    // --- point search ---

    /** Returns one matching RID, or null if the key isn't present. Use rangeScan for all matches of a duplicate key. */
    public BTreePage.RID search(long key) {
        BTreePage leaf = descendToLeaf(key);
        List<Long> keys = leaf.getKeys();
        List<BTreePage.RID> values = leaf.getLeafValues();
        int idx = Collections.binarySearch(keys, key);
        BTreePage.RID result = idx >= 0 ? values.get(idx) : null;
        bufferPool.unpinPage(indexName, leaf.getPageId());
        return result;
    }

    private BTreePage descendToLeaf(long key) {
        long pageId = rootPageId;
        BTreePage page = bufferPool.getPage(indexName, pageId, BTreePage.FACTORY);
        while (!page.isLeaf()) {
            List<Long> keys = page.getKeys();
            List<Long> children = page.getChildren();
            long childPageId = children.get(findChildIndex(keys, key));
            bufferPool.unpinPage(indexName, pageId);
            pageId = childPageId;
            page = bufferPool.getPage(indexName, pageId, BTreePage.FACTORY);
        }
        return page;
    }

    // --- range scan ---

    /** All RIDs with key in [fromKey, toKey], inclusive, in ascending key order. */
    public List<BTreePage.RID> rangeScan(long fromKey, long toKey) {
        List<BTreePage.RID> results = new ArrayList<>();
        BTreePage page = descendToLeaf(fromKey);

        while (true) {
            List<Long> keys = page.getKeys();
            List<BTreePage.RID> values = page.getLeafValues();
            boolean stop = false;
            for (int i = 0; i < keys.size(); i++) {
                long k = keys.get(i);
                if (k > toKey) {
                    stop = true;
                    break;
                }
                if (k >= fromKey) {
                    results.add(values.get(i));
                }
            }
            long nextId = page.getNextLeafPageId();
            bufferPool.unpinPage(indexName, page.getPageId());
            if (stop || nextId < 0) {
                break;
            }
            page = bufferPool.getPage(indexName, nextId, BTreePage.FACTORY);
        }
        return results;
    }

    /** All RIDs for an exact (possibly duplicated) key. */
    public List<BTreePage.RID> searchAll(long key) {
        return rangeScan(key, key);
    }

    public long getRootPageId() {
        return rootPageId;
    }
}
