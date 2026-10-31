package com.stratosdb.index.btree;

import com.stratosdb.common.exceptions.StorageException;
import com.stratosdb.common.exceptions.UniqueConstraintViolationException;
import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.page.BTreePage;
import com.stratosdb.storage.page.Page;
import com.stratosdb.storage.page.PageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A concurrent disk-backed B+Tree index with multi-page latch crabbing (coupling).
 *
 * Latches (Page.getLatch()) protect physical B+Tree pages during concurrent operations:
 * - Read Latch Crabbing (Coupling): search and rangeScan acquire a read latch on the child
 *   page BEFORE releasing the read latch on the parent page, ensuring concurrent splits
 *   cannot relocate keys undetected.
 * - Sibling Latch Crabbing: rangeScan advances along leaf chains by acquiring the read latch
 *   on nextLeafPageId before releasing the read latch on the current leaf.
 * - Write Latch Crabbing on Insertion & Splits: write latches are acquired hand-over-hand
 *   down the tree; ancestors are released immediately whenever a safe (non-full) child
 *   is reached. On leaf split, write latches are held on the split chain up to the lowest
 *   safe ancestor.
 * - Concurrency & Deadlock Freedom: all latch acquisitions follow a strict topological
 *   order (top-to-bottom for tree descent, left-to-right for sibling traversal), guaranteeing
 *   zero deadlocks between concurrent readers and writers under arbitrary concurrency.
 */
public class BTreeIndex implements com.stratosdb.index.KeyValueIndex {
    private static final Logger LOG = LoggerFactory.getLogger(BTreeIndex.class);

    private static final long META_PAGE_ID = 0;
    private static final long INITIAL_ROOT_PAGE_ID = 1;
    private static final int META_MAGIC = 0x53545242; // "STRB"
    private static final int META_MAGIC_OFFSET = 24; // right after Page's base 24-byte header
    private static final int META_ROOT_OFFSET = 28;
    private static final int META_FLAGS_OFFSET = 36;

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

        @Override
        public Page wrap(long pageId, java.nio.ByteBuffer directBuffer) {
            return new Page(pageId, directBuffer);
        }
    };

    private final String indexName;
    private final BufferPool bufferPool;
    private com.stratosdb.storage.wal.WALManager walManager;
    private volatile long rootPageId;
    private final AtomicLong nextPageId;
    private volatile boolean unique;

    /**
     * Root latch: protects transitions to rootPageId (such as when the root splits or collapses).
     * Traversals acquire readLock on rootLatch briefly to read rootPageId and latch the root page,
     * releasing rootLatch immediately once the root page latch is acquired.
     */
    private final ReentrantReadWriteLock rootLatch = new ReentrantReadWriteLock();

    public BTreeIndex(String indexName, BufferPool bufferPool) {
        this(indexName, bufferPool, null, false);
    }

    public BTreeIndex(String indexName, BufferPool bufferPool, boolean unique) {
        this(indexName, bufferPool, null, unique);
    }

    public BTreeIndex(String indexName, BufferPool bufferPool, com.stratosdb.storage.wal.WALManager walManager) {
        this(indexName, bufferPool, walManager, false);
    }

    public BTreeIndex(String indexName, BufferPool bufferPool, com.stratosdb.storage.wal.WALManager walManager, boolean unique) {
        this.indexName = indexName;
        this.bufferPool = bufferPool;
        this.walManager = walManager;
        this.unique = unique;

        long existingPages = bufferPool.getTablePageCount(indexName);
        if (existingPages <= 1) {
            initializeNewIndex();
        } else {
            loadMetadata();
        }
        this.nextPageId = new AtomicLong(Math.max(existingPages, INITIAL_ROOT_PAGE_ID + 1));
    }

    @Override
    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        rootLatch.writeLock().lock();
        try {
            this.unique = unique;
            saveMetadata();
        } finally {
            rootLatch.writeLock().unlock();
        }
    }

    public com.stratosdb.storage.wal.WALManager getWalManager() {
        return walManager;
    }

    public void setWalManager(com.stratosdb.storage.wal.WALManager walManager) {
        this.walManager = walManager;
    }

    private void initializeNewIndex() {
        rootLatch.writeLock().lock();
        try {
            BTreePage root = bufferPool.getPage(indexName, INITIAL_ROOT_PAGE_ID, BTreePage.FACTORY);
            root.getLatch().writeLock().lock();
            try {
                root.setLeafContents(List.of(), List.of());
                root.setNextLeafPageId(-1);
                root.setPageLSN(0L);
                bufferPool.markDirty(indexName, INITIAL_ROOT_PAGE_ID);
            } finally {
                root.getLatch().writeLock().unlock();
                bufferPool.unpinPage(indexName, INITIAL_ROOT_PAGE_ID);
            }
            bufferPool.flushPage(indexName, INITIAL_ROOT_PAGE_ID);

            this.rootPageId = INITIAL_ROOT_PAGE_ID;
            saveMetadata();
            LOG.debug("Initialized new B+Tree index '{}' with empty root at page {}", indexName, INITIAL_ROOT_PAGE_ID);
        } finally {
            rootLatch.writeLock().unlock();
        }
    }

    private void loadMetadata() {
        Page meta = bufferPool.getPage(indexName, META_PAGE_ID, RAW_PAGE_FACTORY);
        meta.getLatch().readLock().lock();
        try {
            int magic = meta.getBuffer().getInt(META_MAGIC_OFFSET);
            if (magic != META_MAGIC) {
                throw new StorageException("Index metadata page for '" + indexName + "' is missing or corrupt");
            }
            this.rootPageId = meta.getBuffer().getLong(META_ROOT_OFFSET);
            int flags = meta.getBuffer().getInt(META_FLAGS_OFFSET);
            if ((flags & 1) != 0) {
                this.unique = true;
            }
            LOG.debug("Loaded B+Tree index '{}', root at page {}, unique={}", indexName, rootPageId, unique);
        } finally {
            meta.getLatch().readLock().unlock();
            bufferPool.unpinPage(indexName, META_PAGE_ID);
        }
    }

    private void saveMetadata() {
        Page meta = bufferPool.getPage(indexName, META_PAGE_ID, RAW_PAGE_FACTORY);
        meta.getLatch().writeLock().lock();
        try {
            meta.getBuffer().putInt(META_MAGIC_OFFSET, META_MAGIC);
            meta.getBuffer().putLong(META_ROOT_OFFSET, rootPageId);
            meta.getBuffer().putInt(META_FLAGS_OFFSET, unique ? 1 : 0);
            meta.setDirty(true);
            bufferPool.markDirty(indexName, META_PAGE_ID);
        } finally {
            meta.getLatch().writeLock().unlock();
            bufferPool.unpinPage(indexName, META_PAGE_ID);
        }
        bufferPool.flushPage(indexName, META_PAGE_ID);
    }

    private long allocatePageId() {
        return nextPageId.getAndIncrement();
    }

    // --- Point Insert ---

    @Override
    public void insert(long key, BTreePage.RID rid) {
        insert(0L, key, rid);
    }

    @Override
    public void insert(long xid, long key, BTreePage.RID rid) {
        // Optimistic pass: read latch crabbing down to leaf, write latch only on the leaf
        if (tryOptimisticInsert(xid, key, rid)) {
            return;
        }
        // Pessimistic fallback: write latch crabbing down to leaf on split
        pessimisticInsert(xid, key, rid);
    }

    private boolean isSafeForInsert(BTreePage page) {
        if (page.isLeaf()) {
            return page.getKeyCount() < BTreePage.MAX_LEAF_KEYS;
        } else {
            return page.getKeyCount() < BTreePage.MAX_INTERNAL_KEYS;
        }
    }

    /**
     * Optimistic insert pass: crabs down internal nodes holding read latches.
     * At the leaf level, acquires write latch on the leaf and releases the parent read latch.
     * If the leaf is not full, inserts directly in O(1) memory and releases latches.
     * Returns true on success, or false if the leaf is full and requires a split.
     */
    private boolean tryOptimisticInsert(long xid, long key, BTreePage.RID rid) {
        rootLatch.readLock().lock();
        long currentRootId = this.rootPageId;
        BTreePage root = bufferPool.getPage(indexName, currentRootId, BTreePage.FACTORY);

        if (root.isLeaf()) {
            rootLatch.readLock().unlock();
            bufferPool.unpinPage(indexName, currentRootId);
            return false; // let pessimistic pass handle root leaf
        }

        root.getLatch().readLock().lock();
        rootLatch.readLock().unlock();

        BTreePage curr = root;
        try {
            while (true) {
                List<Long> keys = curr.getKeys();
                List<Long> children = curr.getChildren();
                int childIdx = findChildIndex(keys, key);
                long childPageId = children.get(childIdx);

                BTreePage child = bufferPool.getPage(indexName, childPageId, BTreePage.FACTORY);

                if (child.isLeaf()) {
                    // Child is leaf: acquire WRITE latch on leaf child
                    child.getLatch().writeLock().lock();

                    // Release parent read latch
                    long parentId = curr.getPageId();
                    curr.getLatch().readLock().unlock();
                    bufferPool.unpinPage(indexName, parentId);
                    curr = null; // parent successfully released

                    try {
                        // Check Lehman-Yao right-sibling traversal if concurrent split occurred
                        while (child.getNextLeafPageId() > 0) {
                            List<Long> leafKeys = child.getKeys();
                            if (!leafKeys.isEmpty() && key > leafKeys.get(leafKeys.size() - 1)) {
                                long nextId = child.getNextLeafPageId();
                                BTreePage next = bufferPool.getPage(indexName, nextId, BTreePage.FACTORY);
                                next.getLatch().writeLock().lock();
                                if (!next.getKeys().isEmpty() && key >= next.getKeys().get(0)) {
                                    long prevId = child.getPageId();
                                    child.getLatch().writeLock().unlock();
                                    bufferPool.unpinPage(indexName, prevId);
                                    child = next;
                                    continue;
                                }
                                next.getLatch().writeLock().unlock();
                                bufferPool.unpinPage(indexName, nextId);
                            }
                            break;
                        }

                        List<Long> leafKeys = new ArrayList<>(child.getKeys());
                        List<BTreePage.RID> leafValues = new ArrayList<>(child.getLeafValues());

                        if (unique) {
                            int idx = Collections.binarySearch(leafKeys, key);
                            if (idx >= 0) {
                                throw new UniqueConstraintViolationException(
                                    null, indexName, key,
                                    "duplicate key value violates unique constraint \"" + indexName + "\": Key (" + key + ") already exists.");
                            }
                        }

                        if (leafKeys.size() < BTreePage.MAX_LEAF_KEYS) {
                            int idx = Collections.binarySearch(leafKeys, key);
                            int insertPos = idx >= 0 ? idx : -(idx + 1);
                            leafKeys.add(insertPos, key);
                            leafValues.add(insertPos, rid);

                            long lsn = 0;
                            if (walManager != null) {
                                lsn = walManager.logIndexInsert(indexName, xid, child.getPageId(), key, rid.pageId(), rid.slot());
                            }
                            if (lsn > 0) {
                                child.setPageLSN(lsn);
                            }
                            child.setLeafContents(leafKeys, leafValues);
                            bufferPool.markDirty(indexName, child.getPageId());
                            return true;
                        }

                        // Leaf is full: must split
                        return false;
                    } finally {
                        long leafId = child.getPageId();
                        child.getLatch().writeLock().unlock();
                        bufferPool.unpinPage(indexName, leafId);
                    }
                } else {
                    // Child is internal: crab down with read latch
                    child.getLatch().readLock().lock();
                    long parentId = curr.getPageId();
                    curr.getLatch().readLock().unlock();
                    bufferPool.unpinPage(indexName, parentId);
                    curr = child;
                }
            }
        } finally {
            if (curr != null) {
                long pageId = curr.getPageId();
                curr.getLatch().readLock().unlock();
                bufferPool.unpinPage(indexName, pageId);
            }
        }
    }

    private record HeldFrame(BTreePage page, int childIdx) {}

    private record SplitResult(long splitKey, long newPageId, long newRootId, long lsn) {}

    /**
     * Pessimistic insert pass: performs structural modification (split) under SMO write lock
     * and page write latches down to the leaf.
     */
    private void pessimisticInsert(long xid, long key, BTreePage.RID rid) {
        rootLatch.writeLock().lock();
        List<HeldFrame> heldFrames = new ArrayList<>();

        try {
            long currentRootId = this.rootPageId;
            BTreePage root = bufferPool.getPage(indexName, currentRootId, BTreePage.FACTORY);
            root.getLatch().writeLock().lock();
            heldFrames.add(new HeldFrame(root, -1));

            BTreePage curr = root;
            while (!curr.isLeaf()) {
                List<Long> keys = curr.getKeys();
                List<Long> children = curr.getChildren();
                int childIdx = findChildIndex(keys, key);
                long childPageId = children.get(childIdx);

                heldFrames.set(heldFrames.size() - 1, new HeldFrame(curr, childIdx));

                BTreePage child = bufferPool.getPage(indexName, childPageId, BTreePage.FACTORY);
                child.getLatch().writeLock().lock(); // Acquire write latch down to leaf

                heldFrames.add(new HeldFrame(child, -1));
                curr = child;
            }

            // Now curr is the leaf page
            List<Long> leafKeys = new ArrayList<>(curr.getKeys());
            List<BTreePage.RID> leafValues = new ArrayList<>(curr.getLeafValues());

            if (unique) {
                int idx = Collections.binarySearch(leafKeys, key);
                if (idx >= 0) {
                    throw new UniqueConstraintViolationException(
                        null, indexName, key,
                        "duplicate key value violates unique constraint \"" + indexName + "\": Key (" + key + ") already exists.");
                }
            }

            int idx = Collections.binarySearch(leafKeys, key);
            int insertPos = idx >= 0 ? idx : -(idx + 1);
            leafKeys.add(insertPos, key);
            leafValues.add(insertPos, rid);

            if (leafKeys.size() <= BTreePage.MAX_LEAF_KEYS) {
                // Leaf fits without split
                long lsn = 0;
                if (walManager != null) {
                    lsn = walManager.logIndexInsert(indexName, xid, curr.getPageId(), key, rid.pageId(), rid.slot());
                }
                if (lsn > 0) curr.setPageLSN(lsn);
                curr.setLeafContents(leafKeys, leafValues);
                bufferPool.markDirty(indexName, curr.getPageId());
                return;
            }

            // Leaf split: left half stays in curr, right half moves to newLeaf
            int mid = leafKeys.size() / 2;
            List<Long> rightKeys = new ArrayList<>(leafKeys.subList(mid, leafKeys.size()));
            List<BTreePage.RID> rightValues = new ArrayList<>(leafValues.subList(mid, leafValues.size()));
            List<Long> leftKeys = new ArrayList<>(leafKeys.subList(0, mid));
            List<BTreePage.RID> leftValues = new ArrayList<>(leafValues.subList(0, mid));

            long newPageId = allocatePageId();
            long oldNext = curr.getNextLeafPageId();
            long splitKey = rightKeys.get(0);
            long newRootId = (heldFrames.size() == 1) ? allocatePageId() : -1L;
            long parentPageId = (heldFrames.size() > 1) ? heldFrames.get(heldFrames.size() - 2).page().getPageId() : -1L;

            long lsn = 0;
            if (walManager != null) {
                lsn = walManager.logIndexSplit(indexName, xid, true, curr.getPageId(), newPageId,
                    splitKey, oldNext, parentPageId, newRootId,
                    rightKeys, rightValues, null, leftKeys, leftValues, null);
            }

            BTreePage newLeaf = bufferPool.getPage(indexName, newPageId, BTreePage.FACTORY);
            newLeaf.getLatch().writeLock().lock();
            try {
                if (lsn > 0) newLeaf.setPageLSN(lsn);
                newLeaf.setLeafContents(rightKeys, rightValues);
                newLeaf.setNextLeafPageId(oldNext);
                bufferPool.markDirty(indexName, newPageId);
            } finally {
                newLeaf.getLatch().writeLock().unlock();
                bufferPool.unpinPage(indexName, newPageId);
            }

            if (lsn > 0) curr.setPageLSN(lsn);
            curr.setLeafContents(leftKeys, leftValues);
            curr.setNextLeafPageId(newPageId);
            bufferPool.markDirty(indexName, curr.getPageId());

            SplitResult split = new SplitResult(splitKey, newPageId, newRootId, lsn);

            // Propagate split up heldFrames
            for (int level = heldFrames.size() - 2; level >= 0; level--) {
                HeldFrame parentFrame = heldFrames.get(level);
                BTreePage parent = parentFrame.page();
                List<Long> parentKeys = new ArrayList<>(parent.getKeys());
                List<Long> parentChildren = new ArrayList<>(parent.getChildren());

                int childIdx = parentFrame.childIdx();
                parentKeys.add(childIdx, split.splitKey());
                parentChildren.add(childIdx + 1, split.newPageId());

                if (parentKeys.size() <= BTreePage.MAX_INTERNAL_KEYS) {
                    if (split.lsn() > 0) parent.setPageLSN(split.lsn());
                    parent.setInternalContents(parentKeys, parentChildren);
                    bufferPool.markDirty(indexName, parent.getPageId());
                    split = null;
                    break;
                }

                // Internal split
                int midInt = parentKeys.size() / 2;
                long promotedKey = parentKeys.get(midInt);

                List<Long> leftIntKeys = new ArrayList<>(parentKeys.subList(0, midInt));
                List<Long> leftIntChildren = new ArrayList<>(parentChildren.subList(0, midInt + 1));
                List<Long> rightIntKeys = new ArrayList<>(parentKeys.subList(midInt + 1, parentKeys.size()));
                List<Long> rightIntChildren = new ArrayList<>(parentChildren.subList(midInt + 1, parentChildren.size()));

                long newIntId = allocatePageId();
                long newRootIdForParent = (level == 0) ? allocatePageId() : -1L;
                long grandParentId = (level > 0) ? heldFrames.get(level - 1).page().getPageId() : -1L;

                long intLsn = 0;
                if (walManager != null) {
                    intLsn = walManager.logIndexSplit(indexName, xid, false, parent.getPageId(), newIntId,
                        promotedKey, -1L, grandParentId, newRootIdForParent,
                        rightIntKeys, null, rightIntChildren, leftIntKeys, null, leftIntChildren);
                }

                BTreePage newInternal = bufferPool.getPage(indexName, newIntId, BTreePage.FACTORY);
                newInternal.getLatch().writeLock().lock();
                try {
                    if (intLsn > 0) newInternal.setPageLSN(intLsn);
                    newInternal.setInternalContents(rightIntKeys, rightIntChildren);
                    bufferPool.markDirty(indexName, newIntId);
                } finally {
                    newInternal.getLatch().writeLock().unlock();
                    bufferPool.unpinPage(indexName, newIntId);
                }

                if (intLsn > 0) parent.setPageLSN(intLsn);
                parent.setInternalContents(leftIntKeys, leftIntChildren);
                bufferPool.markDirty(indexName, parent.getPageId());

                split = new SplitResult(promotedKey, newIntId, newRootIdForParent, intLsn);
            }

            if (split != null) {
                // Root split: allocate new root
                long finalNewRootId = split.newRootId() != -1 ? split.newRootId() : allocatePageId();
                BTreePage newRoot = bufferPool.getPage(indexName, finalNewRootId, BTreePage.FACTORY);
                newRoot.getLatch().writeLock().lock();
                try {
                    newRoot.setInternalContents(List.of(split.splitKey()), List.of(rootPageId, split.newPageId()));
                    if (split.lsn() > 0) newRoot.setPageLSN(split.lsn());
                    bufferPool.markDirty(indexName, finalNewRootId);
                } finally {
                    newRoot.getLatch().writeLock().unlock();
                    bufferPool.unpinPage(indexName, finalNewRootId);
                }

                this.rootPageId = finalNewRootId;
                saveMetadata();
                LOG.debug("Root split: new root {} for index '{}'", finalNewRootId, indexName);
            }
        } finally {
            for (HeldFrame frame : heldFrames) {
                try {
                    long pid = frame.page().getPageId();
                    frame.page().getLatch().writeLock().unlock();
                    bufferPool.unpinPage(indexName, pid);
                } catch (Exception e) {
                    LOG.error("Failed unlocking/unpinning page {}", frame.page().getPageId(), e);
                }
            }
            heldFrames.clear();
            rootLatch.writeLock().unlock();
        }
    }

    /** child[i] holds keys < keys[i]; child[i+1] holds keys >= keys[i]. */
    private int findChildIndex(List<Long> keys, long key) {
        int idx = 0;
        while (idx < keys.size() && key >= keys.get(idx)) {
            idx++;
        }
        return idx;
    }

    // --- Point Delete ---

    @Override
    public void delete(long key, BTreePage.RID rid) {
        delete(0L, key, rid);
    }

    @Override
    public void delete(long xid, long key, BTreePage.RID rid) {
        if (tryOptimisticDelete(xid, key, rid)) {
            return;
        }
        pessimisticDelete(xid, key, rid);
    }

    private boolean tryOptimisticDelete(long xid, long key, BTreePage.RID rid) {
        rootLatch.readLock().lock();
        long currentRootId = this.rootPageId;
        BTreePage root = bufferPool.getPage(indexName, currentRootId, BTreePage.FACTORY);

        if (root.isLeaf()) {
            rootLatch.readLock().unlock();
            bufferPool.unpinPage(indexName, currentRootId);
            return false; // let pessimistic handle root leaf
        }

        root.getLatch().readLock().lock();
        rootLatch.readLock().unlock();

        BTreePage curr = root;
        try {
            while (true) {
                List<Long> keys = curr.getKeys();
                List<Long> children = curr.getChildren();
                int childIdx = findChildIndex(keys, key);
                long childPageId = children.get(childIdx);

                BTreePage child = bufferPool.getPage(indexName, childPageId, BTreePage.FACTORY);

                if (child.isLeaf()) {
                    child.getLatch().writeLock().lock();
                    long parentId = curr.getPageId();
                    curr.getLatch().readLock().unlock();
                    bufferPool.unpinPage(indexName, parentId);
                    curr = null;

                    try {
                        List<Long> leafKeys = new ArrayList<>(child.getKeys());
                        List<BTreePage.RID> leafValues = new ArrayList<>(child.getLeafValues());

                        int removeIdx = -1;
                        for (int i = 0; i < leafKeys.size(); i++) {
                            if (leafKeys.get(i) == key && leafValues.get(i).equals(rid)) {
                                removeIdx = i;
                                break;
                            }
                        }
                        if (removeIdx == -1) {
                            return true; // not found: nothing to delete
                        }

                        // If removing this key would cause an underflow, trigger pessimistic rebalance
                        if (leafKeys.size() - 1 < MIN_LEAF_KEYS) {
                            return false;
                        }

                        leafKeys.remove(removeIdx);
                        leafValues.remove(removeIdx);
                        long lsn = 0;
                        if (walManager != null) {
                            lsn = walManager.logIndexDelete(indexName, xid, child.getPageId(), key, rid.pageId(), rid.slot());
                        }
                        if (lsn > 0) child.setPageLSN(lsn);
                        child.setLeafContents(leafKeys, leafValues);
                        bufferPool.markDirty(indexName, child.getPageId());
                        return true;
                    } finally {
                        long leafId = child.getPageId();
                        child.getLatch().writeLock().unlock();
                        bufferPool.unpinPage(indexName, leafId);
                    }
                } else {
                    child.getLatch().readLock().lock();
                    long parentId = curr.getPageId();
                    curr.getLatch().readLock().unlock();
                    bufferPool.unpinPage(indexName, parentId);
                    curr = child;
                }
            }
        } finally {
            if (curr != null) {
                long pageId = curr.getPageId();
                curr.getLatch().readLock().unlock();
                bufferPool.unpinPage(indexName, pageId);
            }
        }
    }

    private void pessimisticDelete(long xid, long key, BTreePage.RID rid) {
        rootLatch.writeLock().lock();
        try {
            long currentRootId = this.rootPageId;
            BTreePage root = bufferPool.getPage(indexName, currentRootId, BTreePage.FACTORY);
            root.getLatch().writeLock().lock();
            try {
                deleteRecursive(xid, root, key, rid);

                // If root collapsed
                if (!root.isLeaf() && root.getKeyCount() == 0) {
                    long onlyChild = root.getChildren().get(0);
                    this.rootPageId = onlyChild;
                    saveMetadata();
                    LOG.debug("Root collapsed to page {} for index '{}'", rootPageId, indexName);
                }
            } finally {
                root.getLatch().writeLock().unlock();
                bufferPool.unpinPage(indexName, currentRootId);
            }
        } finally {
            rootLatch.writeLock().unlock();
        }
    }

    private boolean deleteRecursive(long xid, BTreePage page, long key, BTreePage.RID rid) {
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
                return false;
            }
            keys.remove(removeIdx);
            values.remove(removeIdx);
            long lsn = 0;
            if (walManager != null) {
                lsn = walManager.logIndexDelete(indexName, xid, page.getPageId(), key, rid.pageId(), rid.slot());
            }
            if (lsn > 0) page.setPageLSN(lsn);
            page.setLeafContents(keys, values);
            bufferPool.markDirty(indexName, page.getPageId());

            boolean isRoot = (page.getPageId() == rootPageId);
            return !isRoot && keys.size() < MIN_LEAF_KEYS;
        }

        List<Long> keys = page.getKeys();
        List<Long> children = page.getChildren();
        int childIdx = findChildIndex(keys, key);
        long childPageId = children.get(childIdx);

        BTreePage child = bufferPool.getPage(indexName, childPageId, BTreePage.FACTORY);
        child.getLatch().writeLock().lock(); // Latch crabbing: acquire child write latch before releasing parent

        try {
            boolean childUnderflowed = deleteRecursive(xid, child, key, rid);
            if (!childUnderflowed) {
                return false;
            }
            // Child underflowed: rebalance while child's write latch is STILL HELD (zero gap)
            return rebalanceHeldChild(xid, page, childIdx, child);
        } finally {
            child.getLatch().writeLock().unlock();
            bufferPool.unpinPage(indexName, childPageId);
        }
    }

    private boolean rebalanceHeldChild(long xid, BTreePage parent, int childIdx, BTreePage child) {
        List<Long> parentKeys = new ArrayList<>(parent.getKeys());
        List<Long> parentChildren = new ArrayList<>(parent.getChildren());

        if (childIdx > 0) {
            long leftId = parentChildren.get(childIdx - 1);
            BTreePage left = bufferPool.getPage(indexName, leftId, BTreePage.FACTORY);
            left.getLatch().writeLock().lock();
            try {
                if (canLend(left)) {
                    borrowFromLeft(parent, parentKeys, parentChildren, childIdx, child, left);
                    bufferPool.markDirty(indexName, parent.getPageId());
                    return false;
                }
            } finally {
                left.getLatch().writeLock().unlock();
                bufferPool.unpinPage(indexName, leftId);
            }
        }

        if (childIdx < parentChildren.size() - 1) {
            long rightId = parentChildren.get(childIdx + 1);
            BTreePage right = bufferPool.getPage(indexName, rightId, BTreePage.FACTORY);
            right.getLatch().writeLock().lock();
            try {
                if (canLend(right)) {
                    borrowFromRight(parent, parentKeys, parentChildren, childIdx, child, right);
                    bufferPool.markDirty(indexName, parent.getPageId());
                    return false;
                }
            } finally {
                right.getLatch().writeLock().unlock();
                bufferPool.unpinPage(indexName, rightId);
            }
        }

        // Neither sibling can lend: merge
        if (childIdx > 0) {
            long leftId = parentChildren.get(childIdx - 1);
            BTreePage left = bufferPool.getPage(indexName, leftId, BTreePage.FACTORY);
            left.getLatch().writeLock().lock();
            try {
                mergeChildren(xid, parent, parentKeys, parentChildren, childIdx - 1, left, childIdx, child);
            } finally {
                left.getLatch().writeLock().unlock();
                bufferPool.unpinPage(indexName, leftId);
            }
        } else {
            long rightId = parentChildren.get(childIdx + 1);
            BTreePage right = bufferPool.getPage(indexName, rightId, BTreePage.FACTORY);
            right.getLatch().writeLock().lock();
            try {
                mergeChildren(xid, parent, parentKeys, parentChildren, childIdx, child, childIdx + 1, right);
            } finally {
                right.getLatch().writeLock().unlock();
                bufferPool.unpinPage(indexName, rightId);
            }
        }

        boolean isParentRoot = (parent.getPageId() == rootPageId);
        int minForParent = parent.isLeaf() ? MIN_LEAF_KEYS : MIN_INTERNAL_KEYS;
        return !isParentRoot && parent.getKeyCount() < minForParent;
    }

    private boolean canLend(BTreePage sibling) {
        int min = sibling.isLeaf() ? MIN_LEAF_KEYS : MIN_INTERNAL_KEYS;
        return sibling.getKeyCount() > min;
    }

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

            parentKeys.set(childIdx - 1, borrowedKey);
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
            childKeys.add(0, separator);
            childChildren.add(0, movedChild);
            child.setInternalContents(childKeys, childChildren);
            bufferPool.markDirty(indexName, child.getPageId());

            parentKeys.set(childIdx - 1, movedUpKey);
        }
        parent.setInternalContents(parentKeys, parentChildren);
    }

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

    private void mergeChildren(long xid, BTreePage parent, List<Long> parentKeys, List<Long> parentChildren,
                                int leftIdx, BTreePage leftPage, int rightIdx, BTreePage rightPage) {
        long separator = parentKeys.get(leftIdx);

        List<Long> updatedParentKeys = new ArrayList<>(parentKeys);
        List<Long> updatedParentChildren = new ArrayList<>(parentChildren);
        updatedParentKeys.remove(leftIdx);
        updatedParentChildren.remove(rightIdx);

        boolean isCollapsed = (parent.getPageId() == rootPageId && updatedParentKeys.isEmpty());
        long newRootId = isCollapsed ? leftPage.getPageId() : -1L;

        long lsn = 0;
        if (leftPage.isLeaf()) {
            List<Long> mergedKeys = new ArrayList<>(leftPage.getKeys());
            List<BTreePage.RID> mergedValues = new ArrayList<>(leftPage.getLeafValues());
            mergedKeys.addAll(rightPage.getKeys());
            mergedValues.addAll(rightPage.getLeafValues());

            if (walManager != null) {
                lsn = walManager.logIndexMerge(indexName, xid, true, parent.getPageId(),
                    leftPage.getPageId(), rightPage.getPageId(), separator,
                    rightPage.getNextLeafPageId(), isCollapsed, newRootId,
                    mergedKeys, mergedValues, null, updatedParentKeys, updatedParentChildren);
            }
            if (lsn > 0) {
                leftPage.setPageLSN(lsn);
                parent.setPageLSN(lsn);
            }
            leftPage.setLeafContents(mergedKeys, mergedValues);
            leftPage.setNextLeafPageId(rightPage.getNextLeafPageId());
            bufferPool.markDirty(indexName, leftPage.getPageId());
        } else {
            List<Long> mergedKeys = new ArrayList<>(leftPage.getKeys());
            List<Long> mergedChildren = new ArrayList<>(leftPage.getChildren());
            mergedKeys.add(separator);
            mergedKeys.addAll(rightPage.getKeys());
            mergedChildren.addAll(rightPage.getChildren());

            if (walManager != null) {
                lsn = walManager.logIndexMerge(indexName, xid, false, parent.getPageId(),
                    leftPage.getPageId(), rightPage.getPageId(), separator,
                    -1L, isCollapsed, newRootId,
                    mergedKeys, null, mergedChildren, updatedParentKeys, updatedParentChildren);
            }
            if (lsn > 0) {
                leftPage.setPageLSN(lsn);
                parent.setPageLSN(lsn);
            }
            leftPage.setInternalContents(mergedKeys, mergedChildren);
            bufferPool.markDirty(indexName, leftPage.getPageId());
        }

        parentKeys.remove(leftIdx);
        parentChildren.remove(rightIdx);
        parent.setInternalContents(parentKeys, parentChildren);
        bufferPool.markDirty(indexName, parent.getPageId());
    }

    // --- Point Search with Read Latch Crabbing ---

    @Override
    public BTreePage.RID search(long key) {
        rootLatch.readLock().lock();
        long currentRootId = this.rootPageId;
        BTreePage curr = bufferPool.getPage(indexName, currentRootId, BTreePage.FACTORY);
        curr.getLatch().readLock().lock();
        rootLatch.readLock().unlock();

        try {
            while (!curr.isLeaf()) {
                List<Long> keys = curr.getKeys();
                List<Long> children = curr.getChildren();
                int childIdx = findChildIndex(keys, key);
                long childPageId = children.get(childIdx);

                BTreePage child = bufferPool.getPage(indexName, childPageId, BTreePage.FACTORY);
                child.getLatch().readLock().lock(); // Latch coupling: child before parent

                long parentId = curr.getPageId();
                curr.getLatch().readLock().unlock();
                bufferPool.unpinPage(indexName, parentId);

                curr = child;
            }

            // Lehman-Yao right-sibling check
            while (curr.getNextLeafPageId() > 0) {
                List<Long> keys = curr.getKeys();
                if (!keys.isEmpty() && key > keys.get(keys.size() - 1)) {
                    long nextId = curr.getNextLeafPageId();
                    BTreePage next = bufferPool.getPage(indexName, nextId, BTreePage.FACTORY);
                    next.getLatch().readLock().lock();
                    if (!next.getKeys().isEmpty() && key >= next.getKeys().get(0)) {
                        long prevId = curr.getPageId();
                        curr.getLatch().readLock().unlock();
                        bufferPool.unpinPage(indexName, prevId);
                        curr = next;
                        continue;
                    }
                    next.getLatch().readLock().unlock();
                    bufferPool.unpinPage(indexName, nextId);
                }
                break;
            }

            List<Long> keys = curr.getKeys();
            List<BTreePage.RID> values = curr.getLeafValues();
            int idx = Collections.binarySearch(keys, key);
            return idx >= 0 ? values.get(idx) : null;
        } finally {
            long leafId = curr.getPageId();
            curr.getLatch().readLock().unlock();
            bufferPool.unpinPage(indexName, leafId);
        }
    }

    // --- Range Scan with Leaf Sibling Latch Crabbing ---

    public List<BTreePage.RID> rangeScan(long fromKey, long toKey) {
        List<BTreePage.RID> results = new ArrayList<>();

        rootLatch.readLock().lock();
        long currentRootId = this.rootPageId;
        BTreePage curr = bufferPool.getPage(indexName, currentRootId, BTreePage.FACTORY);
        curr.getLatch().readLock().lock();
        rootLatch.readLock().unlock();

        try {
            while (!curr.isLeaf()) {
                List<Long> keys = curr.getKeys();
                List<Long> children = curr.getChildren();
                int childIdx = findChildIndex(keys, fromKey);
                long childPageId = children.get(childIdx);

                BTreePage child = bufferPool.getPage(indexName, childPageId, BTreePage.FACTORY);
                child.getLatch().readLock().lock(); // Latch coupling: child before parent

                long parentId = curr.getPageId();
                curr.getLatch().readLock().unlock();
                bufferPool.unpinPage(indexName, parentId);

                curr = child;
            }

            // Lehman-Yao right sibling check for fromKey
            while (curr.getNextLeafPageId() > 0) {
                List<Long> keys = curr.getKeys();
                if (!keys.isEmpty() && fromKey > keys.get(keys.size() - 1)) {
                    long nextId = curr.getNextLeafPageId();
                    BTreePage next = bufferPool.getPage(indexName, nextId, BTreePage.FACTORY);
                    next.getLatch().readLock().lock();
                    if (!next.getKeys().isEmpty() && fromKey >= next.getKeys().get(0)) {
                        long prevId = curr.getPageId();
                        curr.getLatch().readLock().unlock();
                        bufferPool.unpinPage(indexName, prevId);
                        curr = next;
                        continue;
                    }
                    next.getLatch().readLock().unlock();
                    bufferPool.unpinPage(indexName, nextId);
                }
                break;
            }

            // Scan leaf sibling chain with latch crabbing
            while (true) {
                List<Long> keys = curr.getKeys();
                List<BTreePage.RID> values = curr.getLeafValues();
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

                long nextId = curr.getNextLeafPageId();
                if (stop || nextId < 0) {
                    break;
                }

                BTreePage next = bufferPool.getPage(indexName, nextId, BTreePage.FACTORY);
                next.getLatch().readLock().lock(); // Latch coupling: next leaf before current leaf

                long prevId = curr.getPageId();
                curr.getLatch().readLock().unlock();
                bufferPool.unpinPage(indexName, prevId);

                curr = next;
            }

            return results;
        } finally {
            long leafId = curr.getPageId();
            curr.getLatch().readLock().unlock();
            bufferPool.unpinPage(indexName, leafId);
        }
    }

    @Override
    public List<BTreePage.RID> searchAll(long key) {
        return rangeScan(key, key);
    }

    public long getRootPageId() {
        return rootPageId;
    }

    /**
     * Validates structural consistency of the B+Tree:
     * - Key ordering across internal nodes and leaves.
     * - Leaf sibling chain connectivity and ordering.
     * - Zero orphan pointers and zero broken links.
     */
    public int validateTree() {
        rootLatch.readLock().lock();
        try {
            return validateTreeInternal();
        } finally {
            rootLatch.readLock().unlock();
        }
    }

    private int validateTreeInternal() {
        if (rootPageId <= 0) {
            throw new IllegalStateException("Corrupt root page id: " + rootPageId);
        }
        BTreePage root = bufferPool.getPage(indexName, rootPageId, BTreePage.FACTORY);
        root.getLatch().readLock().lock();
        java.util.Set<Long> pagesReachableFromRoot = new java.util.HashSet<>();
        pagesReachableFromRoot.add(rootPageId);
        java.util.Set<Long> leavesFromRoot = new java.util.HashSet<>();
        long leftmostLeafId = -1;

        try {
            if (root.isLeaf()) {
                leavesFromRoot.add(rootPageId);
                leftmostLeafId = rootPageId;
                validateLeaf(root);
            } else {
                validateInternalNode(root, pagesReachableFromRoot, leavesFromRoot);
                long currId = rootPageId;
                while (true) {
                    BTreePage curr = bufferPool.getPage(indexName, currId, BTreePage.FACTORY);
                    curr.getLatch().readLock().lock();
                    long pinnedId = currId;
                    try {
                        if (curr.isLeaf()) {
                            leftmostLeafId = pinnedId;
                            break;
                        }
                        currId = curr.getChildren().get(0);
                    } finally {
                        curr.getLatch().readLock().unlock();
                        bufferPool.unpinPage(indexName, pinnedId);
                    }
                }
            }
        } finally {
            root.getLatch().readLock().unlock();
            bufferPool.unpinPage(indexName, rootPageId);
        }

        // Traverse leaf sibling chain starting from leftmost leaf
        java.util.Set<Long> leavesInChain = new java.util.HashSet<>();
        long currLeafId = leftmostLeafId;
        Long previousMaxKey = null;
        int totalKeys = 0;

        while (currLeafId != -1) {
            if (leavesInChain.contains(currLeafId)) {
                throw new IllegalStateException("Cycle detected in leaf sibling links at page " + currLeafId);
            }
            leavesInChain.add(currLeafId);

            BTreePage leaf = bufferPool.getPage(indexName, currLeafId, BTreePage.FACTORY);
            leaf.getLatch().readLock().lock();
            try {
                if (!leaf.isLeaf()) {
                    throw new IllegalStateException("Leaf chain node " + currLeafId + " is not marked as a leaf");
                }
                List<Long> keys = leaf.getKeys();
                validateLeaf(leaf);

                if (!keys.isEmpty()) {
                    if (previousMaxKey != null && keys.get(0) < previousMaxKey) {
                        throw new IllegalStateException("Broken key ordering across leaf sibling chain: "
                            + previousMaxKey + " followed by " + keys.get(0) + " on page " + currLeafId);
                    }
                    previousMaxKey = keys.get(keys.size() - 1);
                }
                totalKeys += keys.size();

                long nextId = leaf.getNextLeafPageId();
                currLeafId = nextId;
            } finally {
                leaf.getLatch().readLock().unlock();
                bufferPool.unpinPage(indexName, leaf.getPageId());
            }
        }

        // Verify zero orphan pointers and zero broken links
        java.util.Set<Long> orphanFromRoot = new java.util.HashSet<>(leavesFromRoot);
        orphanFromRoot.removeAll(leavesInChain);
        if (!orphanFromRoot.isEmpty()) {
            throw new IllegalStateException("Broken links detected: leaves reachable from root not in sibling chain: " + orphanFromRoot);
        }

        java.util.Set<Long> orphanInChain = new java.util.HashSet<>(leavesInChain);
        orphanInChain.removeAll(leavesFromRoot);
        if (!orphanInChain.isEmpty()) {
            throw new IllegalStateException("Orphan pointers detected: leaves in sibling chain unreachable from root: " + orphanInChain);
        }

        return totalKeys;
    }

    private void validateInternalNode(BTreePage page, java.util.Set<Long> visitedPages, java.util.Set<Long> leaves) {
        List<Long> keys = page.getKeys();
        List<Long> children = page.getChildren();
        if (children.size() != keys.size() + 1) {
            throw new IllegalStateException("Internal page " + page.getPageId() + " has " + keys.size() + " keys but " + children.size() + " children");
        }
        for (int i = 1; i < keys.size(); i++) {
            if (keys.get(i) < keys.get(i - 1)) {
                throw new IllegalStateException("Internal page " + page.getPageId() + " keys not sorted: " + keys);
            }
        }
        for (long childId : children) {
            if (childId <= 0) {
                throw new IllegalStateException("Invalid child page id " + childId + " in page " + page.getPageId());
            }
            if (visitedPages.contains(childId)) {
                throw new IllegalStateException("Cycle or duplicate reference to child page " + childId);
            }
            visitedPages.add(childId);
            BTreePage child = bufferPool.getPage(indexName, childId, BTreePage.FACTORY);
            child.getLatch().readLock().lock();
            try {
                if (child.isLeaf()) {
                    leaves.add(childId);
                    validateLeaf(child);
                } else {
                    validateInternalNode(child, visitedPages, leaves);
                }
            } finally {
                child.getLatch().readLock().unlock();
                bufferPool.unpinPage(indexName, childId);
            }
        }
    }

    private void validateLeaf(BTreePage leaf) {
        List<Long> keys = leaf.getKeys();
        List<BTreePage.RID> values = leaf.getLeafValues();
        if (keys.size() != values.size()) {
            throw new IllegalStateException("Leaf page " + leaf.getPageId() + " key count " + keys.size() + " != value count " + values.size());
        }
        for (int i = 1; i < keys.size(); i++) {
            if (keys.get(i) < keys.get(i - 1)) {
                throw new IllegalStateException("Leaf page " + leaf.getPageId() + " keys not sorted: " + keys);
            }
        }
    }
}
