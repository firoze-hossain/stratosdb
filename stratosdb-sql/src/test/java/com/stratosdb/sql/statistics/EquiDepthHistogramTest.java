package com.stratosdb.sql.statistics;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class EquiDepthHistogramTest {

    @Test
    public void testEmptyHistogram() {
        EquiDepthHistogram h = EquiDepthHistogram.empty();
        assertTrue(h.isEmpty());
        assertEquals(0, h.getBucketCount());
        assertEquals(0.5, h.estimateFractionLessThan(10.0));
        assertEquals(1.0, h.estimateFractionRange(5.0, 15.0));
    }

    @Test
    public void testUniformDistributionBuckets() {
        // Values from 0 to 1000
        List<Double> values = new ArrayList<>();
        for (int i = 0; i <= 1000; i++) {
            values.add((double) i);
        }

        // Build 10 buckets
        EquiDepthHistogram h = EquiDepthHistogram.build(values, 10);
        assertEquals(10, h.getBucketCount());
        assertEquals(11, h.getBounds().size());
        assertEquals(0.0, h.getBounds().get(0), 1e-6);
        assertEquals(1000.0, h.getBounds().get(10), 1e-6);

        // Value <= 0
        assertEquals(0.0, h.estimateFractionLessThan(0.0), 1e-6);
        // Value >= 1000
        assertEquals(1.0, h.estimateFractionLessThan(1000.0), 1e-6);

        // Value = 500 should be ~0.50
        double frac500 = h.estimateFractionLessThan(500.0);
        assertEquals(0.5, frac500, 0.05);

        // Range [200, 800] should be ~0.60
        double rangeFrac = h.estimateFractionRange(200.0, 800.0);
        assertEquals(0.6, rangeFrac, 0.05);
    }

    @Test
    public void testClusteredDistribution() {
        // Skewed data: 800 values between 0.0 and 10.0; 200 values between 10.0 and 1000.0
        List<Double> values = new ArrayList<>();
        for (int i = 0; i < 800; i++) {
            values.add(i * 10.0 / 800.0); // 0 to 10
        }
        for (int i = 0; i < 200; i++) {
            values.add(10.0 + i * 990.0 / 200.0); // 10 to 1000
        }
        Collections.sort(values);

        EquiDepthHistogram h = EquiDepthHistogram.build(values, 10);
        assertEquals(10, h.getBucketCount());

        // In a naive uniform model (min=0, max=1000), fraction <= 10 would be estimated as 10/1000 = 1%.
        // But with our equi-depth histogram, 80% of data is <= 10, so estimate should be ~0.80!
        double frac10 = h.estimateFractionLessThan(10.0);
        assertEquals(0.8, frac10, 0.08);

        // Greater than 10 should be ~0.20
        double fracGt10 = h.estimateFractionGreaterThan(10.0);
        assertEquals(0.2, fracGt10, 0.08);
    }

    @Test
    public void testJsonRoundTrip() {
        List<Double> bounds = List.of(0.0, 10.0, 25.0, 50.0, 100.0);
        EquiDepthHistogram h = new EquiDepthHistogram(bounds);
        String json = h.toJson();
        assertNotNull(json);

        EquiDepthHistogram restored = EquiDepthHistogram.fromJson(json);
        assertFalse(restored.isEmpty());
        assertEquals(4, restored.getBucketCount());
        assertEquals(bounds, restored.getBounds());
    }
}
