package com.stratosdb.sql.statistics;

import java.util.Objects;

/**
 * Detailed statistical profile for a single column.
 *
 * Tracks row/page counts, null fraction, average serialized byte width,
 * HyperLogLog-estimated distinct count, Most Common Values (MCV) with empirical frequencies,
 * an equi-depth histogram for non-MCV data, min/max bounds, and physical correlation.
 *
 * Implements realistic selectivity calculation for equality, inequality, null-checks,
 * and range predicates.
 */
public record ColumnStatistics(
    String tableName,
    String columnName,
    long rowCount,
    long pageCount,
    double nullFraction,
    int avgWidth,
    double distinctCount,
    MostCommonValues mcv,
    EquiDepthHistogram histogram,
    Double min,
    Double max,
    double correlation
) {
    public ColumnStatistics {
        if (mcv == null) mcv = MostCommonValues.empty();
        if (histogram == null) histogram = EquiDepthHistogram.empty();
    }

    public ColumnStatistics(long distinctCount, Double min, Double max) {
        this("", "", 0L, 0L, 0.0, 4, (double) distinctCount, MostCommonValues.empty(), EquiDepthHistogram.empty(), min, max, 0.0);
    }

    /**
     * Estimates selectivity for an equality comparison (col = literal).
     */
    public double estimateEqualitySelectivity(Object literal) {
        if (literal == null) {
            return nullFraction;
        }

        // 1. If in MCV list, return its exact empirical frequency
        if (mcv != null && mcv.contains(literal)) {
            return mcv.getFrequency(literal);
        }

        // 2. Otherwise, distribute remaining probability uniformly across non-MCV distinct values
        double sumMcv = mcv != null ? mcv.getSumFrequency() : 0.0;
        double remainingProb = Math.max(0.0, 1.0 - nullFraction - sumMcv);
        int mcvCount = mcv != null ? mcv.size() : 0;
        double remainingDistinct = Math.max(1.0, distinctCount - mcvCount);

        double selectivity = remainingProb / remainingDistinct;
        return Math.max(0.0, Math.min(1.0, selectivity));
    }

    /**
     * Estimates selectivity for a range comparison [lo, hi].
     */
    public double estimateRangeSelectivity(Double lo, Double hi) {
        if (rowCount == 0) {
            return 0.0;
        }

        // 1. Contribution from MCV values matching [lo, hi]
        double mcvContrib = mcv != null ? mcv.rangeFrequency(lo, hi) : 0.0;

        // 2. Contribution from histogram for remaining population
        double sumMcv = mcv != null ? mcv.getSumFrequency() : 0.0;
        double remainingFrac = Math.max(0.0, 1.0 - nullFraction - sumMcv);

        double histFrac;
        if (histogram != null && !histogram.isEmpty()) {
            histFrac = histogram.estimateFractionRange(lo, hi);
        } else if (min != null && max != null && max > min) {
            double rangeLo = (lo != null) ? Math.max(min, lo) : min;
            double rangeHi = (hi != null) ? Math.min(max, hi) : max;
            double width = Math.max(0.0, rangeHi - rangeLo);
            histFrac = Math.min(1.0, width / (max - min));
        } else {
            histFrac = 0.5;
        }

        double totalSelectivity = mcvContrib + (histFrac * remainingFrac);
        return Math.max(0.0, Math.min(1.0, totalSelectivity));
    }

    /**
     * Estimates selectivity for an arbitrary comparison operator.
     */
    public double estimateSelectivity(String operator, long lo, long hi) {
        if (operator.equals("=")) {
            return estimateEqualitySelectivity(lo);
        }
        if (operator.equals("!=")) {
            return Math.max(0.0, 1.0 - nullFraction - estimateEqualitySelectivity(lo));
        }
        if (operator.equalsIgnoreCase("IS NULL")) {
            return nullFraction;
        }
        if (operator.equalsIgnoreCase("IS NOT NULL")) {
            return Math.max(0.0, 1.0 - nullFraction);
        }

        Double loVal = (lo == Long.MIN_VALUE) ? null : (double) lo;
        Double hiVal = (hi == Long.MAX_VALUE) ? null : (double) hi;
        return estimateRangeSelectivity(loVal, hiVal);
    }

    public long distinctCountLong() {
        return Math.max(1, Math.round(distinctCount));
    }
}
