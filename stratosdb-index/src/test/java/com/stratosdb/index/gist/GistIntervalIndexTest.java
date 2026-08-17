package com.stratosdb.index.gist;

import com.stratosdb.storage.page.BTreePage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * GistIntervalIndex: a real interval-overlap index, GiST's own classic
 * real-world application. The property that actually matters here is
 * pruning correctness at scale - a tree with a small MAX_ENTRIES_PER_LEAF
 * (4) is used specifically so a modest test dataset already spans several
 * tree levels, not just one leaf, and a brute-force linear scan is used
 * as the independent oracle for every non-trivial case rather than
 * hand-computing expected results.
 */
class GistIntervalIndexTest {

    @Test
    void findsOverlappingIntervalsAmongNonOverlappingOnes() {
        GistIntervalIndex index = new GistIntervalIndex("idx");
        index.insert(1, 5, new BTreePage.RID(0, 0));
        index.insert(10, 15, new BTreePage.RID(0, 1));
        index.insert(20, 25, new BTreePage.RID(0, 2));

        List<BTreePage.RID> result = index.searchOverlapping(3, 6);
        assertEquals(1, result.size());
        assertEquals(new BTreePage.RID(0, 0), result.get(0));
    }

    @Test
    void findsMultipleOverlappingIntervals() {
        GistIntervalIndex index = new GistIntervalIndex("idx");
        index.insert(1, 5, new BTreePage.RID(0, 0));
        index.insert(4, 8, new BTreePage.RID(0, 1));
        index.insert(10, 15, new BTreePage.RID(0, 2));

        List<BTreePage.RID> result = index.searchOverlapping(3, 6);
        Set<BTreePage.RID> resultSet = new HashSet<>(result);
        assertEquals(Set.of(new BTreePage.RID(0, 0), new BTreePage.RID(0, 1)), resultSet);
    }

    @Test
    void returnsEmptyForAGapBetweenIntervals() {
        GistIntervalIndex index = new GistIntervalIndex("idx");
        index.insert(1, 5, new BTreePage.RID(0, 0));
        index.insert(10, 15, new BTreePage.RID(0, 1));

        assertTrue(index.searchOverlapping(6, 9).isEmpty());
    }

    @Test
    void boundaryTouchCountsAsOverlap() {
        // [1,5] and a query of [5,10] share the single point 5 - inclusive intervals, so this must count as overlapping.
        GistIntervalIndex index = new GistIntervalIndex("idx");
        index.insert(1, 5, new BTreePage.RID(0, 0));

        List<BTreePage.RID> result = index.searchOverlapping(5, 10);
        assertEquals(1, result.size());
    }

    @Test
    void oneIntervalFullyContainingAnotherOverlapsCorrectly() {
        GistIntervalIndex index = new GistIntervalIndex("idx");
        index.insert(1, 100, new BTreePage.RID(0, 0)); // a wide interval
        index.insert(40, 50, new BTreePage.RID(0, 1)); // fully inside it

        assertEquals(2, index.searchOverlapping(45, 45).size(), "a query interval fully inside both stored intervals must match both");
    }

    @Test
    void searchCorrectnessAtScaleAgainstABruteForceOracle() {
        // MAX_ENTRIES_PER_LEAF is 4 - 200 entries forces several real tree
        // levels, and correctness is checked against independent brute-force
        // linear scans (the oracle), not hand-computed expected values.
        GistIntervalIndex index = new GistIntervalIndex("idx");
        Random random = new Random(42);
        record Interval(long start, long end, BTreePage.RID rid) {}
        List<Interval> allIntervals = new ArrayList<>();

        for (int i = 0; i < 200; i++) {
            long start = random.nextInt(1000);
            long end = start + random.nextInt(50);
            BTreePage.RID rid = new BTreePage.RID(i / 20, i % 20);
            index.insert(start, end, rid);
            allIntervals.add(new Interval(start, end, rid));
        }

        assertEquals(200, index.getEntryCount());

        for (int trial = 0; trial < 30; trial++) {
            long queryStart = random.nextInt(1000);
            long queryEnd = queryStart + random.nextInt(50);

            Set<BTreePage.RID> expected = new HashSet<>();
            for (Interval iv : allIntervals) {
                if (iv.start() <= queryEnd && queryStart <= iv.end()) {
                    expected.add(iv.rid());
                }
            }

            Set<BTreePage.RID> actual = new HashSet<>(index.searchOverlapping(queryStart, queryEnd));
            assertEquals(expected, actual, () -> "mismatch for query [" + queryStart + "," + queryEnd + "]");
        }
    }

    @Test
    void emptyIndexReturnsEmptyResultsWithoutError() {
        GistIntervalIndex index = new GistIntervalIndex("idx");
        assertTrue(index.searchOverlapping(1, 10).isEmpty());
    }
}
