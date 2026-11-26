package com.stratosdb.storage.vector;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for native off-heap columnar VectorBatch and primitive vectors
 * (IntVector, LongVector, DoubleVector, ByteVector).
 */
public class VectorBatchTest {

    @Test
    void testVectorBatchNativeMemoryAndAlignment() {
        try (VectorBatch batch = new VectorBatch(
                VectorType.INT, VectorType.LONG, VectorType.DOUBLE, VectorType.BYTE)) {
            assertEquals(1024, batch.getCapacity());
            assertEquals(4, batch.getColumnCount());
            assertEquals(0, batch.getNumRows());

            for (int i = 0; i < batch.getColumnCount(); i++) {
                ColumnVector col = batch.getColumn(i);
                assertTrue(col.getDataSegment().isNative(), "Data segment must be off-heap native memory");
                assertTrue(col.getNullBitmapSegment().isNative(), "Null bitmap must be off-heap native memory");
                assertEquals(0L, col.getDataSegment().address() % ColumnVector.SIMD_ALIGNMENT,
                    "Data segment must be 64-byte aligned for SIMD");
                assertEquals(0L, col.getNullBitmapSegment().address() % ColumnVector.SIMD_ALIGNMENT,
                    "Null bitmap must be 64-byte aligned for SIMD");
            }

            // Expected data footprint: 1024 * (4 + 8 + 8 + 1) = 21,504 bytes + 4 * 128B null bitmaps = 22,016 bytes
            assertEquals(22016L, batch.getTotalNativeMemoryBytes());
        }
    }

    @Test
    void testAllFourVectorTypesAndNullBitmask() {
        try (VectorBatch batch = new VectorBatch(
                VectorType.INT, VectorType.LONG, VectorType.DOUBLE, VectorType.BYTE)) {
            IntVector ints = batch.getIntVector(0);
            LongVector longs = batch.getLongVector(1);
            DoubleVector doubles = batch.getDoubleVector(2);
            ByteVector bytes = batch.getByteVector(3);

            for (int i = 0; i < 1024; i++) {
                if (i % 10 == 0) {
                    ints.appendNull();
                    longs.appendNull();
                    doubles.appendNull();
                    bytes.appendNull();
                } else {
                    ints.append(i);
                    longs.append(i * 1000_000L);
                    doubles.append(i * 1.5);
                    bytes.appendBoolean(i % 2 == 1);
                }
            }
            batch.setNumRows(1024);

            assertEquals(1024, batch.getNumRows());
            assertTrue(ints.hasNulls());
            assertTrue(longs.hasNulls());
            assertTrue(doubles.hasNulls());
            assertTrue(bytes.hasNulls());

            for (int i = 0; i < 1024; i++) {
                if (i % 10 == 0) {
                    assertTrue(ints.isNull(i));
                    assertTrue(longs.isNull(i));
                    assertTrue(doubles.isNull(i));
                    assertTrue(bytes.isNull(i));
                } else {
                    assertFalse(ints.isNull(i));
                    assertEquals(i, ints.get(i));
                    assertEquals(i * 1000_000L, longs.get(i));
                    assertEquals(i * 1.5, doubles.get(i), 1e-9);
                    assertEquals(i % 2 == 1, bytes.getBoolean(i));
                }
            }

            batch.reset();
            assertEquals(0, batch.getNumRows());
            assertFalse(ints.hasNulls());
            assertFalse(ints.isNull(0));
        }
    }

    @Test
    void testBulkCopyAndZeroAllocationReset() {
        ThreadMXBean threadBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        try (VectorBatch batch = new VectorBatch(VectorType.INT, VectorType.DOUBLE)) {
            int[] srcInts = new int[1024];
            double[] srcDoubles = new double[1024];
            for (int i = 0; i < 1024; i++) {
                srcInts[i] = i + 1;
                srcDoubles[i] = (i + 1) * 2.0;
            }

            // Warmup JIT (10,000 iterations for C2 compilation)
            for (int w = 0; w < 10_000; w++) {
                batch.reset();
                batch.getIntVector(0).copyFrom(srcInts, 0, 0, 1024);
                batch.getDoubleVector(1).copyFrom(srcDoubles, 0, 0, 1024);
                batch.setNumRows(1024);
                batch.getIntVector(0).sum();
                batch.getDoubleVector(1).sum();
            }
            threadBean.getCurrentThreadAllocatedBytes();

            long before = threadBean.getCurrentThreadAllocatedBytes();
            long intSum = 0;
            double dblSum = 0;
            for (int iter = 0; iter < 1000; iter++) {
                batch.reset();
                batch.getIntVector(0).copyFrom(srcInts, 0, 0, 1024);
                batch.getDoubleVector(1).copyFrom(srcDoubles, 0, 0, 1024);
                batch.setNumRows(1024);
                intSum += batch.getIntVector(0).sum();
                dblSum += batch.getDoubleVector(1).sum();
            }
            long after = threadBean.getCurrentThreadAllocatedBytes();

            assertEquals(0L, after - before, "VectorBatch reuse loop must allocate 0 heap bytes");
            assertEquals(524800L * 1000L, intSum);
            assertEquals(1049600.0 * 1000.0, dblSum, 1e-6);
        }
    }

    @Test
    void testSequentialScanMemoryThroughputExceeds4GBPerSec() {
        // Use larger contiguous batch capacity (e.g., 1024-element batches repeated)
        // Each batch of (LONG + DOUBLE + INT) = 1024 * (8 + 8 + 4) = 20,480 bytes
        try (VectorBatch batch = new VectorBatch(VectorType.LONG, VectorType.DOUBLE, VectorType.INT)) {
            LongVector lv = batch.getLongVector(0);
            DoubleVector dv = batch.getDoubleVector(1);
            IntVector iv = batch.getIntVector(2);
            for (int i = 0; i < 1024; i++) {
                lv.set(i, i + 1L);
                dv.set(i, (i + 1) * 0.5);
                iv.set(i, i);
            }
            batch.setNumRows(1024);

            // Warmup JIT for SIMD auto-vectorization
            long sinkL = 0;
            double sinkD = 0;
            for (int i = 0; i < 20_000; i++) {
                sinkL += lv.sum() + iv.sum();
                sinkD += dv.sum();
            }

            int iterations = 200_000; // 200,000 * 1024 * 20 bytes = ~4.096 GB scanned
            long bytesPerIter = 1024L * (Long.BYTES + Double.BYTES + Integer.BYTES);
            long totalBytes = (long) iterations * bytesPerIter;

            long startNs = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                sinkL += lv.sum() + iv.sum();
                sinkD += dv.sum();
            }
            long elapsedNs = System.nanoTime() - startNs;

            double gbPerSec = (totalBytes / 1e9) / (elapsedNs / 1e9);
            System.out.printf("VectorBatch sequential scan throughput: %.2f GB/sec (sinkL=%d, sinkD=%.1f)%n",
                gbPerSec, sinkL, sinkD);

            assertTrue(gbPerSec > 4.0,
                "Expected sequential scan memory throughput > 4.0 GB/sec, measured " + gbPerSec + " GB/sec");
        }
    }
}
