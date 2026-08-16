package com.stratosdb.index.bitmap;

import com.stratosdb.storage.page.BTreePage;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A real bitmap index: one bit per row per distinct indexed value,
 * rather than B+Tree/hash's one entry per row. Ideal for low-cardinality
 * columns (a handful of distinct values - status flags, categories,
 * booleans) where a plain B+Tree/hash entry per row wastes space
 * proportional to row count for very little selectivity benefit, and
 * where combining conditions on MULTIPLE such columns via bitwise
 * AND/OR (see searchCombined) is cheap and exact - unlike combining two
 * separate B+Tree scans, which would need an explicit intersection step
 * over row identifiers.
 *
 * Rows are tracked by position in an internal, append-only RID list
 * (ridPositions) rather than by (pageId, slot) directly, since a BitSet
 * needs a dense integer position space to be efficient - position i in
 * ridPositions corresponds to bit i in every value's BitSet.
 *
 * Known, honestly-stated limitation: no support for deleting a row's
 * bit (a deleted row's bit simply stays set forever) - real further
 * work, matching how this project's B+Tree index has its own,
 * separately-documented delete-support gaps. A high-cardinality column
 * (many distinct values, e.g. an id) would create nearly as many
 * bitmaps as there are rows, which is exactly the wrong shape for this
 * structure - real Postgres warns about the same thing.
 */
public class BitmapIndex {
    private final String name;
    private final Map<Object, BitSet> valueToBits = new HashMap<>();
    private final List<BTreePage.RID> ridPositions = new ArrayList<>();

    public BitmapIndex(String name) {
        this.name = name;
    }

    public void insert(Object value, BTreePage.RID rid) {
        int position = ridPositions.size();
        ridPositions.add(rid);
        valueToBits.computeIfAbsent(value, v -> new BitSet()).set(position);
    }

    /** All RIDs whose indexed column exactly equals this value - the bitmap index's basic equality lookup, same role as KeyValueIndex.searchAll for B+Tree/hash. */
    public List<BTreePage.RID> search(Object value) {
        BitSet bits = valueToBits.get(value);
        List<BTreePage.RID> result = new ArrayList<>();
        if (bits == null) {
            return result;
        }
        for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
            result.add(ridPositions.get(i));
        }
        return result;
    }

    /**
     * The bitmap index's real, distinguishing capability: combine
     * multiple equality conditions via bitwise AND/OR - cheap and exact,
     * unlike intersecting two separate row-identifier lists from
     * different index types. Returns the matching RIDs directly.
     */
    public List<BTreePage.RID> searchCombined(List<Object> values, boolean and) {
        BitSet combined = null;
        for (Object value : values) {
            BitSet bits = valueToBits.getOrDefault(value, new BitSet());
            if (combined == null) {
                combined = (BitSet) bits.clone();
            } else if (and) {
                combined.and(bits);
            } else {
                combined.or(bits);
            }
        }
        List<BTreePage.RID> result = new ArrayList<>();
        if (combined == null) {
            return result;
        }
        for (int i = combined.nextSetBit(0); i >= 0; i = combined.nextSetBit(i + 1)) {
            result.add(ridPositions.get(i));
        }
        return result;
    }

    public String getName() { return name; }
    public int getDistinctValueCount() { return valueToBits.size(); }
}
