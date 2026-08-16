package com.stratosdb.index.brin;

import java.util.ArrayList;
import java.util.List;

/**
 * A real BRIN (Block Range Index): instead of one entry per row (like
 * B+Tree/hash), stores a (min, max) summary per contiguous range of
 * table pages. Extremely lightweight - a few bytes per range rather
 * than per row - which makes it cheap to build and maintain, at the
 * cost of being "lossy": it can only ever RULE OUT ranges that
 * definitely can't contain a match, never confirm one, so any range it
 * doesn't rule out still needs its actual rows checked. This tradeoff
 * is the whole point of BRIN, matching real Postgres: it's designed
 * for large tables where the indexed column is naturally correlated
 * with physical row order (an auto-incrementing id, a timestamp
 * column rows were inserted in order of), where a handful of range
 * summaries can skip the vast majority of the table for a range query.
 *
 * Known, honestly-stated limitation: PAGES_PER_RANGE is fixed rather
 * than configurable per index (real Postgres's own default is 128;
 * this is deliberately smaller so a modest, page-count-small test
 * table can still exercise multiple ranges) - real further work if
 * this were ever tuned for genuinely large tables.
 */
public class BrinIndex {
    public static final int PAGES_PER_RANGE = 4;

    public static class RangeSummary {
        public final long startPageId;
        public final long endPageId; // inclusive
        private Long min;
        private Long max;

        RangeSummary(long startPageId, long endPageId) {
            this.startPageId = startPageId;
            this.endPageId = endPageId;
        }

        void observe(long value) {
            if (min == null || value < min) min = value;
            if (max == null || value > max) max = value;
        }

        /** True if this range's (min, max) makes it IMPOSSIBLE to satisfy the given bounds - the only thing BRIN can ever be certain of. A range with no observed rows yet can never satisfy anything. */
        boolean isDefinitelyExcludedBy(Long lowerBound, boolean lowerInclusive, Long upperBound, boolean upperInclusive) {
            if (min == null) {
                return true; // empty range
            }
            if (lowerBound != null) {
                boolean rangeMaxTooLow = lowerInclusive ? (max < lowerBound) : (max <= lowerBound);
                if (rangeMaxTooLow) return true;
            }
            if (upperBound != null) {
                boolean rangeMinTooHigh = upperInclusive ? (min > upperBound) : (min >= upperBound);
                if (rangeMinTooHigh) return true;
            }
            return false;
        }

        public Long getMin() { return min; }
        public Long getMax() { return max; }
    }

    private final String name;
    private final List<RangeSummary> ranges = new ArrayList<>();

    public BrinIndex(String name) {
        this.name = name;
    }

    private RangeSummary rangeFor(long pageId) {
        int rangeIndex = (int) (pageId / PAGES_PER_RANGE);
        while (ranges.size() <= rangeIndex) {
            long start = (long) ranges.size() * PAGES_PER_RANGE;
            ranges.add(new RangeSummary(start, start + PAGES_PER_RANGE - 1));
        }
        return ranges.get(rangeIndex);
    }

    /** Records one row's indexed value as belonging to the page it's physically stored on - called once per row during index build, and once per new row on every subsequent INSERT. */
    public void observe(long pageId, long value) {
        rangeFor(pageId).observe(value);
    }

    /**
     * Returns the page ranges that MIGHT contain a matching row for the
     * given bounds (null bound = unbounded on that side) - every range
     * NOT returned here is provably excluded and can be skipped entirely
     * without checking a single row. Callers must still check actual rows
     * within the returned ranges; BRIN never confirms a match by itself.
     */
    public List<RangeSummary> candidateRanges(Long lowerBound, boolean lowerInclusive, Long upperBound, boolean upperInclusive) {
        List<RangeSummary> result = new ArrayList<>();
        for (RangeSummary r : ranges) {
            if (!r.isDefinitelyExcludedBy(lowerBound, lowerInclusive, upperBound, upperInclusive)) {
                result.add(r);
            }
        }
        return result;
    }

    public String getName() { return name; }
    public int getRangeCount() { return ranges.size(); }
}
