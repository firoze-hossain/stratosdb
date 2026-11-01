package com.stratosdb.sql.statistics;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class ColumnStatisticsTest {

    @Test
    public void testEqualitySelectivityWithMcv() {
        // Table with 1000 rows, 100 distinct values, 10% nulls.
        // MCV has "hot" with 60% frequency, "warm" with 20% frequency.
        MostCommonValues mcv = new MostCommonValues(List.of("hot", "warm"), List.of(0.60, 0.20));
        ColumnStatistics cs = new ColumnStatistics(
            "orders", "status", 1000L, 10L, 0.10, 8, 100.0,
            mcv, EquiDepthHistogram.empty(), null, null, 0.0
        );

        // Equality on MCV "hot" -> 0.60
        assertEquals(0.60, cs.estimateEqualitySelectivity("hot"), 1e-6);

        // Equality on MCV "warm" -> 0.20
        assertEquals(0.20, cs.estimateEqualitySelectivity("warm"), 1e-6);

        // Equality on non-MCV value "rare":
        // Remaining probability = 1.0 - 0.10 (null) - 0.80 (mcv) = 0.10
        // Remaining distinct values = 100 - 2 = 98
        // Expected selectivity = 0.10 / 98 ≈ 0.00102
        double rareSelectivity = cs.estimateEqualitySelectivity("rare");
        assertEquals(0.10 / 98.0, rareSelectivity, 1e-5);

        // Null equality
        assertEquals(0.10, cs.estimateEqualitySelectivity(null), 1e-6);
    }

    @Test
    public void testRangeSelectivityWithHistogram() {
        // Values partitioned into [0, 50, 100, 500, 1000]
        EquiDepthHistogram hist = new EquiDepthHistogram(List.of(0.0, 50.0, 100.0, 500.0, 1000.0));
        ColumnStatistics cs = new ColumnStatistics(
            "metrics", "latency", 1000L, 5L, 0.0, 4, 800.0,
            MostCommonValues.empty(), hist, 0.0, 1000.0, 0.95
        );

        // Range <= 100 covers 2 out of 4 buckets -> 0.50
        double selLe100 = cs.estimateSelectivity("<=", Long.MIN_VALUE, 100);
        assertEquals(0.50, selLe100, 0.05);

        // Range > 500 covers 1 out of 4 buckets -> 0.25
        double selGt500 = cs.estimateSelectivity(">", 500, Long.MAX_VALUE);
        assertEquals(0.25, selGt500, 0.05);

        // IS NULL -> 0.0
        assertEquals(0.0, cs.estimateSelectivity("IS NULL", 0, 0), 1e-6);
        // IS NOT NULL -> 1.0
        assertEquals(1.0, cs.estimateSelectivity("IS NOT NULL", 0, 0), 1e-6);
    }
}
