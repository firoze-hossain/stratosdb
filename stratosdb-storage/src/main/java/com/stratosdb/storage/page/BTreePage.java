package com.stratosdb.storage.page;

import com.stratosdb.common.constants.PageConstants;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A B+Tree node, laid out in an 8KB page.
 *
 * This intentionally does NOT reuse SlottedPage's tuple/slot-directory
 * layout - a tree node is a sorted key array plus a parallel array of either
 * child page ids (internal node) or row locations (leaf node), which is a
 * completely different shape from "a bag of variable-length tuples." It
 * does reuse the same Page base class, the same PAGE_SIZE, and the same
 * buffer pool (via PageFactory) as everything else, per the project's rule
 * of one storage path rather than a second one bolted on for indexes.
 *
 * Header (overwrites the generic Page header's byte range - a B+Tree node
 * has no use for pd_lower/pd_upper's slotted-page meaning):
 *   [0]  int  isLeaf (1 = leaf, 0 = internal)
 *   [4]  int  keyCount
 *   [8]  long nextLeafPageId (-1 if none; unused/ignored for internal nodes)
 *   [16..24) reserved
 *
 * Body, starting at HEADER_SIZE (24):
 *   keys: keyCount * long (8 bytes each), always kept sorted ascending
 *   leaf values:     keyCount * RID (pageId: long 8B, slot: int 4B = 12B each)
 *   internal values: (keyCount + 1) * long child page ids (8 bytes each)
 */
public class BTreePage extends Page {
    private static final int HEADER_SIZE = PageConstants.HEADER_SIZE; // 24
    private static final int KEY_SIZE = 8;
    private static final int RID_SIZE = 12; // long pageId + int slot
    private static final int CHILD_SIZE = 8;

    public static final int MAX_LEAF_KEYS = (PageConstants.PAGE_SIZE - HEADER_SIZE) / (KEY_SIZE + RID_SIZE);
    // n keys + (n+1) children must fit: n*KEY_SIZE + (n+1)*CHILD_SIZE <= available
    public static final int MAX_INTERNAL_KEYS =
        ((PageConstants.PAGE_SIZE - HEADER_SIZE) - CHILD_SIZE) / (KEY_SIZE + CHILD_SIZE);

    public record RID(long pageId, int slot) {}

    public static final PageFactory<BTreePage> FACTORY = new PageFactory<>() {
        @Override
        public BTreePage createEmpty(long pageId) {
            return new BTreePage(pageId);
        }

        @Override
        public BTreePage wrap(long pageId, byte[] existingBytes) {
            return new BTreePage(pageId, existingBytes);
        }
    };

    public BTreePage(long pageId) {
        super(pageId);
        setLeaf(true);
        setKeyCount(0);
        setNextLeafPageId(-1);
    }

    public BTreePage(long pageId, byte[] existingBytes) {
        super(pageId, existingBytes);
    }

    public boolean isLeaf() {
        return getBuffer().getInt(0) == 1;
    }

    public void setLeaf(boolean leaf) {
        getBuffer().putInt(0, leaf ? 1 : 0);
    }

    public int getKeyCount() {
        return getBuffer().getInt(4);
    }

    private void setKeyCount(int count) {
        getBuffer().putInt(4, count);
    }

    public long getNextLeafPageId() {
        return getBuffer().getLong(8);
    }

    public void setNextLeafPageId(long pageId) {
        getBuffer().putLong(8, pageId);
    }

    public List<Long> getKeys() {
        List<Long> keys = new ArrayList<>();
        int count = getKeyCount();
        ByteBuffer buf = getBuffer();
        for (int i = 0; i < count; i++) {
            keys.add(buf.getLong(HEADER_SIZE + i * KEY_SIZE));
        }
        return keys;
    }

    private int leafValuesOffset() {
        return HEADER_SIZE + MAX_LEAF_KEYS * KEY_SIZE;
    }

    private int internalChildrenOffset() {
        return HEADER_SIZE + MAX_INTERNAL_KEYS * KEY_SIZE;
    }

    public List<RID> getLeafValues() {
        if (!isLeaf()) throw new IllegalStateException("Not a leaf page");
        List<RID> values = new ArrayList<>();
        int count = getKeyCount();
        ByteBuffer buf = getBuffer();
        int base = leafValuesOffset();
        for (int i = 0; i < count; i++) {
            long pageId = buf.getLong(base + i * RID_SIZE);
            int slot = buf.getInt(base + i * RID_SIZE + 8);
            values.add(new RID(pageId, slot));
        }
        return values;
    }

    public List<Long> getChildren() {
        if (isLeaf()) throw new IllegalStateException("Not an internal page");
        List<Long> children = new ArrayList<>();
        int count = getKeyCount();
        ByteBuffer buf = getBuffer();
        int base = internalChildrenOffset();
        for (int i = 0; i <= count; i++) {
            children.add(buf.getLong(base + i * CHILD_SIZE));
        }
        return children;
    }

    /** Overwrites this leaf's entire key/value contents. Caller must keep keys sorted. */
    public void setLeafContents(List<Long> keys, List<RID> values) {
        if (keys.size() != values.size()) throw new IllegalArgumentException("keys/values size mismatch");
        if (keys.size() > MAX_LEAF_KEYS) throw new IllegalStateException("Leaf overflow: " + keys.size() + " > " + MAX_LEAF_KEYS);
        setLeaf(true);
        setKeyCount(keys.size());
        ByteBuffer buf = getBuffer();
        for (int i = 0; i < keys.size(); i++) {
            buf.putLong(HEADER_SIZE + i * KEY_SIZE, keys.get(i));
        }
        int base = leafValuesOffset();
        for (int i = 0; i < values.size(); i++) {
            buf.putLong(base + i * RID_SIZE, values.get(i).pageId());
            buf.putInt(base + i * RID_SIZE + 8, values.get(i).slot());
        }
        setDirty(true);
    }

    /** Overwrites this internal node's entire key/child contents. children.size() must be keys.size()+1. */
    public void setInternalContents(List<Long> keys, List<Long> children) {
        if (children.size() != keys.size() + 1) throw new IllegalArgumentException("children must be keys+1");
        if (keys.size() > MAX_INTERNAL_KEYS) throw new IllegalStateException("Internal overflow: " + keys.size() + " > " + MAX_INTERNAL_KEYS);
        setLeaf(false);
        setKeyCount(keys.size());
        ByteBuffer buf = getBuffer();
        for (int i = 0; i < keys.size(); i++) {
            buf.putLong(HEADER_SIZE + i * KEY_SIZE, keys.get(i));
        }
        int base = internalChildrenOffset();
        for (int i = 0; i < children.size(); i++) {
            buf.putLong(base + i * CHILD_SIZE, children.get(i));
        }
        setDirty(true);
    }

    @Override
    public String toString() {
        return String.format("BTreePage[id=%d, leaf=%s, keys=%d]", getPageId(), isLeaf(), getKeyCount());
    }
}
