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
 * What this does: point insert, point search, range scan, and point delete
 * by (key, RID) - correct node splitting on insert and correct node
 * underflow handling on delete (borrow from a sibling, or merge with one
 * and propagate upward, including collapsing the root when the tree
 * shrinks), verified by actually forcing multi-level splits and merges in
 * tests, not just single-page cases.
 *
 * What this deliberately does NOT do yet, stated plainly rather than
 * glossed over: concurrent insert/search/delete from multiple threads (no
 * latching - callers must serialize writers themselves for now), and no
 * page reuse after a merge orphans a page (the file doesn't shrink; a
 * free-space map to reclaim that space is a separate, further piece of
 * work - see PROJECT_PLAN.md Phase A). Deletion removes an exact (key,
 * RID) pair, not "the first row with this key" - necessary because
 * duplicate keys are supported (searchAll/rangeScan return every match),
 * so the caller must know which specific row it's removing.
 */
public class BTreeIndex implements com.stratosdb.index.KeyValueIndex {
    private static final Logger LOG = LoggerFactory.getLogger(BTreeIndex.class);

    private static final long META_PAGE_ID = 0;
    private static final long INITIAL_ROOT_PAGE_ID = 1;
    private static final int META_MAGIC = 0x53545242; // "STRB"
    private static final int META_MAGIC_OFFSET = 24; // right after Page's base 24-byte header
    private static final int META_ROOT_OFFSET = 28;

    // A node below this many keys (and not the root, which is exempt) must
    // borrow from a sibling or merge. Using floor(max/2) rather than
    // ceil(max/2) is a standard, valid choice - it just affects overall
    // space utilization slightly, not correctness, as long as insert and
    // delete agree on it (they don't need to for correctness here, since
    // insert's split threshold and delete's underflow threshold are
    // independent invariants that both hold simultaneously).
    private static final int MIN_LEAF_KEYS = BTreePage.MAX_LEAF_KEYS / 2;
    private static final int MIN_INTERNAL_KEYS = BTreePage.MAX_INTERNAL_KEYS / 2;

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

    // --- delete ---

    /**
     * Removes the exact (key, rid) pair. A no-op (not an error) if that
     * exact pair isn't present - matches the get-out-of-the-way semantics
     * of most delete APIs rather than forcing every caller to check
     * existence first.
     */
    public void delete(long key, BTreePage.RID rid) {
        deleteRecursive(rootPageId, key, rid);

        // If deletion propagated all the way up and left the root as an
        // internal node with zero keys (one remaining child), that child
        // becomes the new root - the tree's height shrinks by one.
        BTreePage root = bufferPool.getPage(indexName, rootPageId, BTreePage.FACTORY);
        if (!root.isLeaf() && root.getKeyCount() == 0) {
            long onlyChild = root.getChildren().get(0);
            bufferPool.unpinPage(indexName, rootPageId);
            rootPageId = onlyChild;
            saveMetadata();
            LOG.debug("Root collapsed to page {} for index '{}'", rootPageId, indexName);
        } else {
            bufferPool.unpinPage(indexName, rootPageId);
        }
    }

    /** Returns true if the page at pageId underflowed (below minimum, and not the root) after the delete. */
    private boolean deleteRecursive(long pageId, long key, BTreePage.RID rid) {
        BTreePage page = bufferPool.getPage(indexName, pageId, BTreePage.FACTORY);

        if (page.isLeaf()) {
            List<Long> keys = new ArrayList<>(page.getKeys());
            List<BTreePage.RID> values = new ArrayList<>(page.getLeafValues());

            int removeIdx = -1;
            for (int i = 0; i < keys.size(); i++) {
                if (keys.get(i) == key && values.get(i).equals(rid)) {
                    removeIdx = i;
                    break;
                }
            }
            if (removeIdx == -1) {
                bufferPool.unpinPage(indexName, pageId);
                return false; // exact pair not found - nothing to do
            }
            keys.remove(removeIdx);
            values.remove(removeIdx);
            page.setLeafContents(keys, values);
            bufferPool.markDirty(indexName, pageId);

            boolean isRoot = (pageId == rootPageId);
            bufferPool.unpinPage(indexName, pageId);
            return !isRoot && keys.size() < MIN_LEAF_KEYS;
        }

        List<Long> keys = page.getKeys();
        List<Long> children = page.getChildren();
        int childIdx = findChildIndex(keys, key);
        long childPageId = children.get(childIdx);

        boolean childUnderflowed = deleteRecursive(childPageId, key, rid);
        if (!childUnderflowed) {
            bufferPool.unpinPage(indexName, pageId);
            return false;
        }

        boolean selfUnderflowed = rebalanceChild(page, pageId, childIdx);
        bufferPool.unpinPage(indexName, pageId);
        return selfUnderflowed;
    }

    /**
     * The child at parentChildren[childIdx] underflowed. Try to borrow a
     * key from a sibling that has more than the minimum first (cheaper,
     * and keeps the tree denser); only merge if neither sibling can lend.
     * Returns true if the parent itself now underflows as a result.
     */
    private boolean rebalanceChild(BTreePage parent, long parentPageId, int childIdx) {
        List<Long> parentKeys = new ArrayList<>(parent.getKeys());
        List<Long> parentChildren = new ArrayList<>(parent.getChildren());
        long childPageId = parentChildren.get(childIdx);
        BTreePage child = bufferPool.getPage(indexName, childPageId, BTreePage.FACTORY);

        if (childIdx > 0) {
            long leftId = parentChildren.get(childIdx - 1);
            BTreePage left = bufferPool.getPage(indexName, leftId, BTreePage.FACTORY);
            if (canLend(left)) {
                borrowFromLeft(parent, parentKeys, parentChildren, childIdx, child, left);
                bufferPool.markDirty(indexName, parentPageId);
                bufferPool.unpinPage(indexName, childPageId);
                bufferPool.unpinPage(indexName, leftId);
                return false;
            }
            bufferPool.unpinPage(indexName, leftId);
        }

        if (childIdx < parentChildren.size() - 1) {
            long rightId = parentChildren.get(childIdx + 1);
            BTreePage right = bufferPool.getPage(indexName, rightId, BTreePage.FACTORY);
            if (canLend(right)) {
                borrowFromRight(parent, parentKeys, parentChildren, childIdx, child, right);
                bufferPool.markDirty(indexName, parentPageId);
                bufferPool.unpinPage(indexName, childPageId);
                bufferPool.unpinPage(indexName, rightId);
                return false;
            }
            bufferPool.unpinPage(indexName, rightId);
        }

        // Neither sibling can lend without underflowing itself - merge.
        // Prefer merging with the left sibling when one exists (arbitrary
        // but consistent choice); otherwise merge with the right.
        if (childIdx > 0) {
            long leftId = parentChildren.get(childIdx - 1);
            BTreePage left = bufferPool.getPage(indexName, leftId, BTreePage.FACTORY);
            mergeChildren(parent, parentKeys, parentChildren, childIdx - 1, left, childIdx, child);
            bufferPool.unpinPage(indexName, childPageId);
            bufferPool.unpinPage(indexName, leftId);
        } else {
            long rightId = parentChildren.get(childIdx + 1);
            BTreePage right = bufferPool.getPage(indexName, rightId, BTreePage.FACTORY);
            mergeChildren(parent, parentKeys, parentChildren, childIdx, child, childIdx + 1, right);
            bufferPool.unpinPage(indexName, childPageId);
            bufferPool.unpinPage(indexName, rightId);
        }

        boolean isParentRoot = (parentPageId == rootPageId);
        int minForParent = parent.isLeaf() ? MIN_LEAF_KEYS : MIN_INTERNAL_KEYS;
        return !isParentRoot && parent.getKeyCount() < minForParent;
    }

    private boolean canLend(BTreePage sibling) {
        int min = sibling.isLeaf() ? MIN_LEAF_KEYS : MIN_INTERNAL_KEYS;
        return sibling.getKeyCount() > min;
    }

    /** Moves left sibling's last entry to become child's new first entry, updating the parent's separator. */
    private void borrowFromLeft(BTreePage parent, List<Long> parentKeys, List<Long> parentChildren,
                                 int childIdx, BTreePage child, BTreePage left) {
        if (child.isLeaf()) {
            List<Long> leftKeys = new ArrayList<>(left.getKeys());
            List<BTreePage.RID> leftValues = new ArrayList<>(left.getLeafValues());
            long borrowedKey = leftKeys.remove(leftKeys.size() - 1);
            BTreePage.RID borrowedValue = leftValues.remove(leftValues.size() - 1);
            left.setLeafContents(leftKeys, leftValues);
            bufferPool.markDirty(indexName, left.getPageId());

            List<Long> childKeys = new ArrayList<>(child.getKeys());
            List<BTreePage.RID> childValues = new ArrayList<>(child.getLeafValues());
            childKeys.add(0, borrowedKey);
            childValues.add(0, borrowedValue);
            child.setLeafContents(childKeys, childValues);
            bufferPool.markDirty(indexName, child.getPageId());

            parentKeys.set(childIdx - 1, borrowedKey); // separator = child's new first key
        } else {
            List<Long> leftKeys = new ArrayList<>(left.getKeys());
            List<Long> leftChildren = new ArrayList<>(left.getChildren());
            long separator = parentKeys.get(childIdx - 1);
            long movedUpKey = leftKeys.remove(leftKeys.size() - 1);
            long movedChild = leftChildren.remove(leftChildren.size() - 1);
            left.setInternalContents(leftKeys, leftChildren);
            bufferPool.markDirty(indexName, left.getPageId());

            List<Long> childKeys = new ArrayList<>(child.getKeys());
            List<Long> childChildren = new ArrayList<>(child.getChildren());
            childKeys.add(0, separator); // parent's old separator becomes child's new first key
            childChildren.add(0, movedChild);
            child.setInternalContents(childKeys, childChildren);
            bufferPool.markDirty(indexName, child.getPageId());

            parentKeys.set(childIdx - 1, movedUpKey); // left sibling's last key rotates up through the parent
        }
        parent.setInternalContents(parentKeys, parentChildren);
    }

    /** Moves right sibling's first entry to become child's new last entry, updating the parent's separator. */
    private void borrowFromRight(BTreePage parent, List<Long> parentKeys, List<Long> parentChildren,
                                  int childIdx, BTreePage child, BTreePage right) {
        if (child.isLeaf()) {
            List<Long> rightKeys = new ArrayList<>(right.getKeys());
            List<BTreePage.RID> rightValues = new ArrayList<>(right.getLeafValues());
            long borrowedKey = rightKeys.remove(0);
            BTreePage.RID borrowedValue = rightValues.remove(0);
            right.setLeafContents(rightKeys, rightValues);
            bufferPool.markDirty(indexName, right.getPageId());

            List<Long> childKeys = new ArrayList<>(child.getKeys());
            List<BTreePage.RID> childValues = new ArrayList<>(child.getLeafValues());
            childKeys.add(borrowedKey);
            childValues.add(borrowedValue);
            child.setLeafContents(childKeys, childValues);
            bufferPool.markDirty(indexName, child.getPageId());

            // canLend() already guaranteed right still has >= 1 key left after the removal above.
            parentKeys.set(childIdx, rightKeys.get(0));
        } else {
            List<Long> rightKeys = new ArrayList<>(right.getKeys());
            List<Long> rightChildren = new ArrayList<>(right.getChildren());
            long separator = parentKeys.get(childIdx);
            long movedUpKey = rightKeys.remove(0);
            long movedChild = rightChildren.remove(0);
            right.setInternalContents(rightKeys, rightChildren);
            bufferPool.markDirty(indexName, right.getPageId());

            List<Long> childKeys = new ArrayList<>(child.getKeys());
            List<Long> childChildren = new ArrayList<>(child.getChildren());
            childKeys.add(separator);
            childChildren.add(movedChild);
            child.setInternalContents(childKeys, childChildren);
            bufferPool.markDirty(indexName, child.getPageId());

            parentKeys.set(childIdx, movedUpKey);
        }
        parent.setInternalContents(parentKeys, parentChildren);
    }

    /**
     * Merges rightPage's contents into leftPage (leftPage survives,
     * rightPage becomes an orphaned, unreferenced page - its space isn't
     * reclaimed, see this class's javadoc), removing the separator key
     * between them from the parent along with rightPage's child pointer.
     */
    private void mergeChildren(BTreePage parent, List<Long> parentKeys, List<Long> parentChildren,
                                int leftIdx, BTreePage leftPage, int rightIdx, BTreePage rightPage) {
        long separator = parentKeys.get(leftIdx);

        if (leftPage.isLeaf()) {
            List<Long> mergedKeys = new ArrayList<>(leftPage.getKeys());
            List<BTreePage.RID> mergedValues = new ArrayList<>(leftPage.getLeafValues());
            mergedKeys.addAll(rightPage.getKeys());
            mergedValues.addAll(rightPage.getLeafValues());
            leftPage.setLeafContents(mergedKeys, mergedValues);
            leftPage.setNextLeafPageId(rightPage.getNextLeafPageId()); // skip the now-orphaned rightPage in the leaf chain
            bufferPool.markDirty(indexName, leftPage.getPageId());
        } else {
            List<Long> mergedKeys = new ArrayList<>(leftPage.getKeys());
            List<Long> mergedChildren = new ArrayList<>(leftPage.getChildren());
            mergedKeys.add(separator); // the parent's separator comes down into the merged node
            mergedKeys.addAll(rightPage.getKeys());
            mergedChildren.addAll(rightPage.getChildren());
            leftPage.setInternalContents(mergedKeys, mergedChildren);
            bufferPool.markDirty(indexName, leftPage.getPageId());
        }

        parentKeys.remove(leftIdx);
        parentChildren.remove(rightIdx);
        parent.setInternalContents(parentKeys, parentChildren);
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
