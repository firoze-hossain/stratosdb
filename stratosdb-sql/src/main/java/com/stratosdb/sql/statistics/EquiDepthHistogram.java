package com.stratosdb.sql.statistics;

import com.stratosdb.sql.executor.JsonParser;

import java.util.*;

/**
 * An equi-depth histogram partitioning non-MCV column values into equal-frequency buckets.
 *
 * Each bucket represents 1 / B fraction of the non-MCV population.
 * Range predicate selectivity is computed using linear interpolation within the matching bucket,
 * providing high accuracy on non-uniform distributions where standard uniform min/max models fail.
 */
public class EquiDepthHistogram {
    private final List<Double> bounds;

    public EquiDepthHistogram(List<Double> bounds) {
        if (bounds == null) {
            this.bounds = List.of();
        } else {
            this.bounds = Collections.unmodifiableList(new ArrayList<>(bounds));
        }
    }

    public static EquiDepthHistogram empty() {
        return new EquiDepthHistogram(List.of());
    }

    public static EquiDepthHistogram build(List<Double> sortedValues, int maxBuckets) {
        if (sortedValues == null || sortedValues.isEmpty()) {
            return empty();
        }
        int n = sortedValues.size();
        int numBuckets = Math.min(Math.max(1, maxBuckets), Math.max(1, n / 5));
        if (numBuckets <= 1) {
            return new EquiDepthHistogram(List.of(sortedValues.get(0), sortedValues.get(n - 1)));
        }

        List<Double> bucketBounds = new ArrayList<>(numBuckets + 1);
        bucketBounds.add(sortedValues.get(0));
        for (int i = 1; i < numBuckets; i++) {
            int idx = (int) Math.round((double) i * (n - 1) / numBuckets);
            bucketBounds.add(sortedValues.get(idx));
        }
        bucketBounds.add(sortedValues.get(n - 1));
        return new EquiDepthHistogram(bucketBounds);
    }

    public int getBucketCount() {
        return Math.max(0, bounds.size() - 1);
    }

    public List<Double> getBounds() {
        return bounds;
    }

    public boolean isEmpty() {
        return bounds.size() < 2;
    }

    public double estimateFractionLessThan(double value) {
        int b = getBucketCount();
        if (b == 0) {
            return 0.5;
        }
        double min = bounds.get(0);
        double max = bounds.get(b);
        if (value <= min) {
            return 0.0;
        }
        if (value >= max) {
            return 1.0;
        }

        // Binary search or linear scan through bucket boundaries
        for (int j = 0; j < b; j++) {
            double lo = bounds.get(j);
            double hi = bounds.get(j + 1);
            if (value >= lo && value <= hi) {
                double bucketWidth = hi - lo;
                double fracWithinBucket = (bucketWidth > 0) ? (value - lo) / bucketWidth : 0.5;
                return Math.max(0.0, Math.min(1.0, (j + fracWithinBucket) / b));
            }
        }
        return 1.0;
    }

    public double estimateFractionGreaterThan(double value) {
        return Math.max(0.0, 1.0 - estimateFractionLessThan(value));
    }

    public double estimateFractionRange(Double lo, Double hi) {
        if (isEmpty()) {
            return 1.0;
        }
        if (lo == null && hi == null) {
            return 1.0;
        }
        if (lo == null) {
            return estimateFractionLessThan(hi);
        }
        if (hi == null) {
            return estimateFractionGreaterThan(lo);
        }
        if (hi < lo) {
            return 0.0;
        }
        return Math.max(0.0, Math.min(1.0, estimateFractionLessThan(hi) - estimateFractionLessThan(lo)));
    }

    public String toJson() {
        return JsonParser.toJsonText(bounds);
    }

    @SuppressWarnings("unchecked")
    public static EquiDepthHistogram fromJson(String json) {
        if (json == null || json.isBlank()) {
            return empty();
        }
        try {
            Object parsed = JsonParser.parse(json);
            if (parsed instanceof List<?> list) {
                List<Double> b = new ArrayList<>();
                for (Object item : list) {
                    if (item instanceof Number num) {
                        b.add(num.doubleValue());
                    }
                }
                if (b.size() >= 2) {
                    return new EquiDepthHistogram(b);
                }
            }
        } catch (Exception ignored) {}
        return empty();
    }

    @Override
    public String toString() {
        return "Histogram{buckets=" + getBucketCount() + ", bounds=" + bounds + "}";
    }
}
