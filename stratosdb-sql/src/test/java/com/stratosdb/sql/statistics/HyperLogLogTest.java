package com.stratosdb.sql.statistics;

import org.junit.jupiter.api.Test;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

public class HyperLogLogTest {

    @Test
    public void testEmptyHll() {
        HyperLogLog hll = new HyperLogLog();
        assertEquals(0, hll.estimate());
    }

    @Test
    public void testSmallCardinalityLinearCounting() {
        HyperLogLog hll = new HyperLogLog();
        int n = 50;
        for (int i = 0; i < n; i++) {
            hll.add("item_" + i);
        }
        long est = hll.estimate();
        // With linear counting on small cardinality (50), error should be very small
        assertTrue(Math.abs(est - n) <= 3, "Expected estimate close to " + n + ", got " + est);
    }

    @Test
    public void testMediumCardinality() {
        HyperLogLog hll = new HyperLogLog();
        int n = 1000;
        for (int i = 0; i < n; i++) {
            hll.add("user_" + i);
        }
        long est = hll.estimate();
        // Expected standard error is ~2.3%. Allow 3 * standard error (~7%)
        double error = Math.abs(est - n) / (double) n;
        assertTrue(error < 0.08, "Error should be < 8%, got " + (error * 100) + "% (est=" + est + ")");
    }

    @Test
    public void testLargeCardinality() {
        HyperLogLog hll = new HyperLogLog();
        int n = 20000;
        for (int i = 0; i < n; i++) {
            hll.add("row_key_" + i);
        }
        long est = hll.estimate();
        double error = Math.abs(est - n) / (double) n;
        assertTrue(error < 0.05, "Error should be < 5%, got " + (error * 100) + "% (est=" + est + ")");
    }

    @Test
    public void testDuplicateInsertsDoNotInflate() {
        HyperLogLog hll = new HyperLogLog();
        for (int rep = 0; rep < 50; rep++) {
            for (int i = 0; i < 100; i++) {
                hll.add("val_" + i);
            }
        }
        long est = hll.estimate();
        assertTrue(Math.abs(est - 100) <= 5, "Duplicate additions should not inflate estimate: " + est);
    }

    @Test
    public void testMerge() {
        HyperLogLog hll1 = new HyperLogLog();
        HyperLogLog hll2 = new HyperLogLog();

        for (int i = 0; i < 500; i++) {
            hll1.add("item_" + i);
        }
        for (int i = 250; i < 750; i++) { // 250 overlapping, 250 new -> total distinct 750
            hll2.add("item_" + i);
        }

        hll1.merge(hll2);
        long est = hll1.estimate();
        double error = Math.abs(est - 750) / 750.0;
        assertTrue(error < 0.07, "Merged error should be < 7%, got " + (error * 100) + "% (est=" + est + ")");
    }

    @Test
    public void testSerializationRoundTrip() {
        HyperLogLog hll = new HyperLogLog();
        for (int i = 0; i < 300; i++) {
            hll.add("element_" + i);
        }
        byte[] bytes = hll.toByteArray();
        assertNotNull(bytes);

        HyperLogLog restored = HyperLogLog.fromByteArray(bytes);
        assertEquals(hll.estimate(), restored.estimate());
    }

    @Test
    public void testDifferentTypes() {
        HyperLogLog hll = new HyperLogLog();
        hll.add(12345);
        hll.add(12345L);
        hll.add(3.14159);
        hll.add("hello");
        hll.add(true);
        hll.add(null); // null should be safely ignored
        assertTrue(hll.estimate() >= 4);
    }
}
