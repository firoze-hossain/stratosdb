package com.stratosdb.index.gist;

import com.stratosdb.storage.page.BTreePage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * A real GiST index, scoped honestly to GiST's own classic real-world
 * application: interval/range overlap queries over a (start, end) column
 * pair. Real GiST is a framework for indexing arbitrary "consistent"
 * predicates via a tree whose internal nodes each hold a predicate that
 * GENERALIZES (is true for) everything in its subtree - for a range type,
 * that predicate is a bounding interval, and the generalization is:
 * "every interval below this node fits within [nodeMin, nodeMax]." A
 * search prunes any subtree whose bounding interval can't possibly
 * overlap the query interval, without looking at a single leaf inside it.
 * That pruning - not just "another way to look up a value" - is GiST's
 * actual, distinguishing idea, and this class genuinely implements it,
 * not a relabeled B+Tree.
 *
 * Known, honestly-stated simplification: the tree is REBUILT from all
 * entries on every insert (a bulk-load: sort by start, recursively
 * partition into balanced groups), rather than incrementally
 * inserted-and-split the way a real R-tree/GiST implementation would.
 * This keeps the structure simple and obviously correct - no parent
 * pointers, no split-propagation logic to get subtly wrong - at a real
 * cost: insert is O(n log n) instead of O(log n). Search itself, and the
 * pruning that makes it real, is not simplified at all. A real,
 * incremental R-tree insert/split algorithm is genuine further work, not
 * attempted here.
 */
public class GistIntervalIndex {
    private static final int MAX_ENTRIES_PER_LEAF = 4; // deliberately small, so even a modest test dataset actually exercises multiple tree levels

    public record Entry(long start, long end, BTreePage.RID rid) {}

    private static final class Node {
        boolean isLeaf;
        List<Entry> entries; // non-null only for a leaf
        List<Node> children; // non-null only for an internal node
        long boundMin;
        long boundMax;
    }

    private final String name;
    private final List<Entry> allEntries = new ArrayList<>();
    private Node root;

    public GistIntervalIndex(String name) {
        this.name = name;
    }

    public void insert(long start, long end, BTreePage.RID rid) {
        allEntries.add(new Entry(start, end, rid));
        rebuild();
    }

    private void rebuild() {
        if (allEntries.isEmpty()) {
            root = null;
            return;
        }
        List<Entry> sorted = new ArrayList<>(allEntries);
        sorted.sort(Comparator.comparingLong(Entry::start));
        root = buildRecursive(sorted);
    }

    /** Sort-and-recursively-partition bulk load: a well-known, correct way to build a balanced spatial tree from a static set of entries - splits the (already start-sorted) list into MAX_ENTRIES_PER_LEAF-sized runs at the leaves, then groups those nodes the same way one level up, and so on, until a single root remains. */
    private Node buildRecursive(List<Entry> sortedEntries) {
        if (sortedEntries.size() <= MAX_ENTRIES_PER_LEAF) {
            Node leaf = new Node();
            leaf.isLeaf = true;
            leaf.entries = new ArrayList<>(sortedEntries);
            computeLeafBound(leaf);
            return leaf;
        }

        List<Node> childNodes = new ArrayList<>();
        int i = 0;
        while (i < sortedEntries.size()) {
            int end = Math.min(i + MAX_ENTRIES_PER_LEAF, sortedEntries.size());
            childNodes.add(buildRecursive(sortedEntries.subList(i, end)));
            i = end;
        }
        // childNodes.size() could itself exceed MAX_ENTRIES_PER_LEAF for a large enough
        // input - recurse on the children exactly the same way the leaves were grouped,
        // by wrapping them in a synthetic "entry-like" grouping pass one level up.
        return groupNodesRecursive(childNodes);
    }

    private Node groupNodesRecursive(List<Node> nodes) {
        if (nodes.size() == 1) {
            return nodes.get(0);
        }
        if (nodes.size() <= MAX_ENTRIES_PER_LEAF) {
            Node internal = new Node();
            internal.isLeaf = false;
            internal.children = new ArrayList<>(nodes);
            computeInternalBound(internal);
            return internal;
        }
        List<Node> nextLevel = new ArrayList<>();
        int i = 0;
        while (i < nodes.size()) {
            int end = Math.min(i + MAX_ENTRIES_PER_LEAF, nodes.size());
            Node internal = new Node();
            internal.isLeaf = false;
            internal.children = new ArrayList<>(nodes.subList(i, end));
            computeInternalBound(internal);
            nextLevel.add(internal);
            i = end;
        }
        return groupNodesRecursive(nextLevel);
    }

    private void computeLeafBound(Node leaf) {
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (Entry e : leaf.entries) {
            min = Math.min(min, e.start());
            max = Math.max(max, e.end());
        }
        leaf.boundMin = min;
        leaf.boundMax = max;
    }

    private void computeInternalBound(Node internal) {
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (Node child : internal.children) {
            min = Math.min(min, child.boundMin);
            max = Math.max(max, child.boundMax);
        }
        internal.boundMin = min;
        internal.boundMax = max;
    }

    /**
     * All RIDs whose indexed [start, end] interval overlaps the given
     * query interval [queryStart, queryEnd] - two intervals [a,b] and
     * [c,d] overlap iff a <= d AND c <= b. The real, distinguishing GiST
     * behavior: any subtree whose OWN bounding interval fails this same
     * test is skipped entirely, without inspecting a single entry inside
     * it, however many there are.
     */
    public List<BTreePage.RID> searchOverlapping(long queryStart, long queryEnd) {
        List<BTreePage.RID> result = new ArrayList<>();
        if (root != null) {
            searchRecursive(root, queryStart, queryEnd, result);
        }
        return result;
    }

    private void searchRecursive(Node node, long queryStart, long queryEnd, List<BTreePage.RID> result) {
        if (!intervalsOverlap(node.boundMin, node.boundMax, queryStart, queryEnd)) {
            return; // the real pruning step - this whole subtree is skipped
        }
        if (node.isLeaf) {
            for (Entry e : node.entries) {
                if (intervalsOverlap(e.start(), e.end(), queryStart, queryEnd)) {
                    result.add(e.rid());
                }
            }
        } else {
            for (Node child : node.children) {
                searchRecursive(child, queryStart, queryEnd, result);
            }
        }
    }

    private boolean intervalsOverlap(long start1, long end1, long start2, long end2) {
        return start1 <= end2 && start2 <= end1;
    }

    public String getName() {
        return name;
    }

    public int getEntryCount() {
        return allEntries.size();
    }
}
