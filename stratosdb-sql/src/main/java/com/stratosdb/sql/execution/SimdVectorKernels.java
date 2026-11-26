package com.stratosdb.sql.execution;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * Hardware-accelerated SIMD kernels powered by Java 25 {@code jdk.incubator.vector}
 * compiler intrinsics operating directly on 64-byte-aligned off-heap {@link MemorySegment}s.
 */
public final class SimdVectorKernels {
    private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

    public static final VectorSpecies<Integer> SPECIES_INT = IntVector.SPECIES_PREFERRED;
    public static final VectorSpecies<Long> SPECIES_LONG = LongVector.SPECIES_PREFERRED;
    public static final VectorSpecies<Double> SPECIES_DOUBLE = DoubleVector.SPECIES_PREFERRED;
    public static final VectorSpecies<Byte> SPECIES_BYTE = ByteVector.SPECIES_PREFERRED;

    private SimdVectorKernels() {}

    public static VectorOperators.Comparison toVectorOp(String op) {
        return switch (op) {
            case "=" -> VectorOperators.EQ;
            case "!=", "<>" -> VectorOperators.NE;
            case "<" -> VectorOperators.LT;
            case "<=" -> VectorOperators.LE;
            case ">" -> VectorOperators.GT;
            case ">=" -> VectorOperators.GE;
            default -> throw new IllegalArgumentException("Unsupported SIMD comparison operator: " + op);
        };
    }

    // =========================================================================
    // SIMD Filter Kernels (MemorySegment -> 1024-bit long[] mask)
    // =========================================================================

    public static void filterInt(MemorySegment dataSeg, MemorySegment nullBitmap, boolean hasNulls,
                                 int count, VectorOperators.Comparison op, int threshold, long[] outMask) {
        clearMask(outMask, count);
        final int lanes = SPECIES_INT.length();
        final int bound = SPECIES_INT.loopBound(count);
        final IntVector vThresh = IntVector.broadcast(SPECIES_INT, threshold);

        int i = 0;
        for (; i < bound; i += lanes) {
            IntVector v = IntVector.fromMemorySegment(SPECIES_INT, dataSeg, (long) i * Integer.BYTES, NATIVE_ORDER);
            long bits = v.compare(op, vThresh).toLong();
            if (bits != 0L) {
                int wordIdx = i >>> 6;
                outMask[wordIdx] |= (bits << (i & 63));
            }
        }
        for (; i < count; i++) {
            int val = dataSeg.getAtIndex(ValueLayout.JAVA_INT, i);
            if (compareInt(val, op, threshold)) {
                outMask[i >>> 6] |= (1L << (i & 63));
            }
        }
        if (hasNulls) {
            maskOutNulls(outMask, nullBitmap, count);
        }
    }

    public static void filterLong(MemorySegment dataSeg, MemorySegment nullBitmap, boolean hasNulls,
                                  int count, VectorOperators.Comparison op, long threshold, long[] outMask) {
        clearMask(outMask, count);
        final int lanes = SPECIES_LONG.length();
        final int bound = SPECIES_LONG.loopBound(count);
        final LongVector vThresh = LongVector.broadcast(SPECIES_LONG, threshold);

        int i = 0;
        for (; i < bound; i += lanes) {
            LongVector v = LongVector.fromMemorySegment(SPECIES_LONG, dataSeg, (long) i * Long.BYTES, NATIVE_ORDER);
            long bits = v.compare(op, vThresh).toLong();
            if (bits != 0L) {
                int wordIdx = i >>> 6;
                outMask[wordIdx] |= (bits << (i & 63));
            }
        }
        for (; i < count; i++) {
            long val = dataSeg.getAtIndex(ValueLayout.JAVA_LONG, i);
            if (compareLong(val, op, threshold)) {
                outMask[i >>> 6] |= (1L << (i & 63));
            }
        }
        if (hasNulls) {
            maskOutNulls(outMask, nullBitmap, count);
        }
    }

    public static void filterDouble(MemorySegment dataSeg, MemorySegment nullBitmap, boolean hasNulls,
                                    int count, VectorOperators.Comparison op, double threshold, long[] outMask) {
        clearMask(outMask, count);
        final int lanes = SPECIES_DOUBLE.length();
        final int bound = SPECIES_DOUBLE.loopBound(count);
        final DoubleVector vThresh = DoubleVector.broadcast(SPECIES_DOUBLE, threshold);

        int i = 0;
        for (; i < bound; i += lanes) {
            DoubleVector v = DoubleVector.fromMemorySegment(SPECIES_DOUBLE, dataSeg, (long) i * Double.BYTES, NATIVE_ORDER);
            long bits = v.compare(op, vThresh).toLong();
            if (bits != 0L) {
                int wordIdx = i >>> 6;
                outMask[wordIdx] |= (bits << (i & 63));
            }
        }
        for (; i < count; i++) {
            double val = dataSeg.getAtIndex(ValueLayout.JAVA_DOUBLE, i);
            if (compareDouble(val, op, threshold)) {
                outMask[i >>> 6] |= (1L << (i & 63));
            }
        }
        if (hasNulls) {
            maskOutNulls(outMask, nullBitmap, count);
        }
    }

    public static void filterByte(MemorySegment dataSeg, MemorySegment nullBitmap, boolean hasNulls,
                                  int count, VectorOperators.Comparison op, byte threshold, long[] outMask) {
        clearMask(outMask, count);
        final int lanes = SPECIES_BYTE.length();
        final int bound = SPECIES_BYTE.loopBound(count);
        final ByteVector vThresh = ByteVector.broadcast(SPECIES_BYTE, threshold);

        int i = 0;
        for (; i < bound; i += lanes) {
            ByteVector v = ByteVector.fromMemorySegment(SPECIES_BYTE, dataSeg, i, NATIVE_ORDER);
            long bits = v.compare(op, vThresh).toLong();
            if (bits != 0L) {
                int wordIdx = i >>> 6;
                outMask[wordIdx] |= (bits << (i & 63));
            }
        }
        for (; i < count; i++) {
            byte val = dataSeg.get(ValueLayout.JAVA_BYTE, i);
            if (compareByte(val, op, threshold)) {
                outMask[i >>> 6] |= (1L << (i & 63));
            }
        }
        if (hasNulls) {
            maskOutNulls(outMask, nullBitmap, count);
        }
    }

    public static void filterIntColumnPair(MemorySegment leftSeg, MemorySegment leftNulls, boolean leftHasNulls,
                                           MemorySegment rightSeg, MemorySegment rightNulls, boolean rightHasNulls,
                                           int count, VectorOperators.Comparison op, long[] outMask) {
        clearMask(outMask, count);
        final int lanes = SPECIES_INT.length();
        final int bound = SPECIES_INT.loopBound(count);
        int i = 0;
        for (; i < bound; i += lanes) {
            IntVector lv = IntVector.fromMemorySegment(SPECIES_INT, leftSeg, (long) i * Integer.BYTES, NATIVE_ORDER);
            IntVector rv = IntVector.fromMemorySegment(SPECIES_INT, rightSeg, (long) i * Integer.BYTES, NATIVE_ORDER);
            long bits = lv.compare(op, rv).toLong();
            if (bits != 0L) {
                outMask[i >>> 6] |= (bits << (i & 63));
            }
        }
        for (; i < count; i++) {
            if (compareInt(leftSeg.getAtIndex(ValueLayout.JAVA_INT, i), op, rightSeg.getAtIndex(ValueLayout.JAVA_INT, i))) {
                outMask[i >>> 6] |= (1L << (i & 63));
            }
        }
        if (leftHasNulls) maskOutNulls(outMask, leftNulls, count);
        if (rightHasNulls) maskOutNulls(outMask, rightNulls, count);
    }

    public static void filterDoubleColumnPair(MemorySegment leftSeg, MemorySegment leftNulls, boolean leftHasNulls,
                                              MemorySegment rightSeg, MemorySegment rightNulls, boolean rightHasNulls,
                                              int count, VectorOperators.Comparison op, long[] outMask) {
        clearMask(outMask, count);
        final int lanes = SPECIES_DOUBLE.length();
        final int bound = SPECIES_DOUBLE.loopBound(count);
        int i = 0;
        for (; i < bound; i += lanes) {
            DoubleVector lv = DoubleVector.fromMemorySegment(SPECIES_DOUBLE, leftSeg, (long) i * Double.BYTES, NATIVE_ORDER);
            DoubleVector rv = DoubleVector.fromMemorySegment(SPECIES_DOUBLE, rightSeg, (long) i * Double.BYTES, NATIVE_ORDER);
            long bits = lv.compare(op, rv).toLong();
            if (bits != 0L) {
                outMask[i >>> 6] |= (bits << (i & 63));
            }
        }
        for (; i < count; i++) {
            if (compareDouble(leftSeg.getAtIndex(ValueLayout.JAVA_DOUBLE, i), op, rightSeg.getAtIndex(ValueLayout.JAVA_DOUBLE, i))) {
                outMask[i >>> 6] |= (1L << (i & 63));
            }
        }
        if (leftHasNulls) maskOutNulls(outMask, leftNulls, count);
        if (rightHasNulls) maskOutNulls(outMask, rightNulls, count);
    }

    // =========================================================================
    // SIMD Aggregation Kernels (SUM, MIN, MAX, COUNT)
    // =========================================================================

    public static long sumInt(MemorySegment dataSeg, int count) {
        long total = 0L;
        final int lanes = SPECIES_INT.length();
        final int bound = SPECIES_INT.loopBound(count);
        int i = 0;
        for (; i < bound; i += lanes) {
            IntVector v = IntVector.fromMemorySegment(SPECIES_INT, dataSeg, (long) i * Integer.BYTES, NATIVE_ORDER);
            total += v.reduceLanesToLong(VectorOperators.ADD);
        }
        for (; i < count; i++) {
            total += dataSeg.getAtIndex(ValueLayout.JAVA_INT, i);
        }
        return total;
    }

    public static long sumLong(MemorySegment dataSeg, int count) {
        LongVector acc = LongVector.zero(SPECIES_LONG);
        final int lanes = SPECIES_LONG.length();
        final int bound = SPECIES_LONG.loopBound(count);
        int i = 0;
        for (; i < bound; i += lanes) {
            LongVector v = LongVector.fromMemorySegment(SPECIES_LONG, dataSeg, (long) i * Long.BYTES, NATIVE_ORDER);
            acc = acc.add(v);
        }
        long total = acc.reduceLanes(VectorOperators.ADD);
        for (; i < count; i++) {
            total += dataSeg.getAtIndex(ValueLayout.JAVA_LONG, i);
        }
        return total;
    }

    public static double sumDouble(MemorySegment dataSeg, int count) {
        DoubleVector acc = DoubleVector.zero(SPECIES_DOUBLE);
        final int lanes = SPECIES_DOUBLE.length();
        final int bound = SPECIES_DOUBLE.loopBound(count);
        int i = 0;
        for (; i < bound; i += lanes) {
            DoubleVector v = DoubleVector.fromMemorySegment(SPECIES_DOUBLE, dataSeg, (long) i * Double.BYTES, NATIVE_ORDER);
            acc = acc.add(v);
        }
        double total = acc.reduceLanes(VectorOperators.ADD);
        for (; i < count; i++) {
            total += dataSeg.getAtIndex(ValueLayout.JAVA_DOUBLE, i);
        }
        return total;
    }

    public static int minInt(MemorySegment dataSeg, int count) {
        if (count <= 0) return Integer.MAX_VALUE;
        IntVector acc = IntVector.broadcast(SPECIES_INT, Integer.MAX_VALUE);
        final int lanes = SPECIES_INT.length();
        final int bound = SPECIES_INT.loopBound(count);
        int i = 0;
        for (; i < bound; i += lanes) {
            IntVector v = IntVector.fromMemorySegment(SPECIES_INT, dataSeg, (long) i * Integer.BYTES, NATIVE_ORDER);
            acc = acc.min(v);
        }
        int min = acc.reduceLanes(VectorOperators.MIN);
        for (; i < count; i++) {
            int val = dataSeg.getAtIndex(ValueLayout.JAVA_INT, i);
            if (val < min) min = val;
        }
        return min;
    }

    public static int maxInt(MemorySegment dataSeg, int count) {
        if (count <= 0) return Integer.MIN_VALUE;
        IntVector acc = IntVector.broadcast(SPECIES_INT, Integer.MIN_VALUE);
        final int lanes = SPECIES_INT.length();
        final int bound = SPECIES_INT.loopBound(count);
        int i = 0;
        for (; i < bound; i += lanes) {
            IntVector v = IntVector.fromMemorySegment(SPECIES_INT, dataSeg, (long) i * Integer.BYTES, NATIVE_ORDER);
            acc = acc.max(v);
        }
        int max = acc.reduceLanes(VectorOperators.MAX);
        for (; i < count; i++) {
            int val = dataSeg.getAtIndex(ValueLayout.JAVA_INT, i);
            if (val > max) max = val;
        }
        return max;
    }

    public static double minDouble(MemorySegment dataSeg, int count) {
        if (count <= 0) return Double.POSITIVE_INFINITY;
        DoubleVector acc = DoubleVector.broadcast(SPECIES_DOUBLE, Double.POSITIVE_INFINITY);
        final int lanes = SPECIES_DOUBLE.length();
        final int bound = SPECIES_DOUBLE.loopBound(count);
        int i = 0;
        for (; i < bound; i += lanes) {
            DoubleVector v = DoubleVector.fromMemorySegment(SPECIES_DOUBLE, dataSeg, (long) i * Double.BYTES, NATIVE_ORDER);
            acc = acc.min(v);
        }
        double min = acc.reduceLanes(VectorOperators.MIN);
        for (; i < count; i++) {
            double val = dataSeg.getAtIndex(ValueLayout.JAVA_DOUBLE, i);
            if (val < min) min = val;
        }
        return min;
    }

    public static double maxDouble(MemorySegment dataSeg, int count) {
        if (count <= 0) return Double.NEGATIVE_INFINITY;
        DoubleVector acc = DoubleVector.broadcast(SPECIES_DOUBLE, Double.NEGATIVE_INFINITY);
        final int lanes = SPECIES_DOUBLE.length();
        final int bound = SPECIES_DOUBLE.loopBound(count);
        int i = 0;
        for (; i < bound; i += lanes) {
            DoubleVector v = DoubleVector.fromMemorySegment(SPECIES_DOUBLE, dataSeg, (long) i * Double.BYTES, NATIVE_ORDER);
            acc = acc.max(v);
        }
        double max = acc.reduceLanes(VectorOperators.MAX);
        for (; i < count; i++) {
            double val = dataSeg.getAtIndex(ValueLayout.JAVA_DOUBLE, i);
            if (val > max) max = val;
        }
        return max;
    }

    public static int countNonNull(MemorySegment nullBitmap, int count) {
        int words = (count + 63) >>> 6;
        int nullCount = 0;
        for (int w = 0; w < words; w++) {
            long word = nullBitmap.getAtIndex(ValueLayout.JAVA_LONG, w);
            if (w == words - 1 && (count & 63) != 0) {
                word &= (1L << (count & 63)) - 1L;
            }
            nullCount += Long.bitCount(word);
        }
        return count - nullCount;
    }

    // =========================================================================
    // Bitmask Helpers
    // =========================================================================

    public static void clearMask(long[] mask, int count) {
        int words = (count + 63) >>> 6;
        for (int w = 0; w < words; w++) {
            mask[w] = 0L;
        }
    }

    public static void fillMask(long[] mask, int count) {
        int fullWords = count >>> 6;
        for (int w = 0; w < fullWords; w++) {
            mask[w] = -1L;
        }
        int rem = count & 63;
        if (rem != 0) {
            mask[fullWords] = (1L << rem) - 1L;
        }
    }

    public static void maskOutNulls(long[] mask, MemorySegment nullBitmap, int count) {
        int words = (count + 63) >>> 6;
        for (int w = 0; w < words; w++) {
            mask[w] &= ~nullBitmap.getAtIndex(ValueLayout.JAVA_LONG, w);
        }
    }

    public static int materializeSelection(long[] mask, int count, int[] selectionOut) {
        int words = (count + 63) >>> 6;
        int outCount = 0;
        for (int w = 0; w < words; w++) {
            long bits = mask[w];
            int base = w << 6;
            while (bits != 0L) {
                int bit = Long.numberOfTrailingZeros(bits);
                int idx = base + bit;
                if (idx < count) {
                    selectionOut[outCount++] = idx;
                }
                bits &= (bits - 1L);
            }
        }
        return outCount;
    }

    private static boolean compareInt(int a, VectorOperators.Comparison op, int b) {
        if (op == VectorOperators.EQ) return a == b;
        if (op == VectorOperators.NE) return a != b;
        if (op == VectorOperators.LT) return a < b;
        if (op == VectorOperators.LE) return a <= b;
        if (op == VectorOperators.GT) return a > b;
        if (op == VectorOperators.GE) return a >= b;
        return false;
    }

    private static boolean compareLong(long a, VectorOperators.Comparison op, long b) {
        if (op == VectorOperators.EQ) return a == b;
        if (op == VectorOperators.NE) return a != b;
        if (op == VectorOperators.LT) return a < b;
        if (op == VectorOperators.LE) return a <= b;
        if (op == VectorOperators.GT) return a > b;
        if (op == VectorOperators.GE) return a >= b;
        return false;
    }

    private static boolean compareDouble(double a, VectorOperators.Comparison op, double b) {
        if (op == VectorOperators.EQ) return a == b;
        if (op == VectorOperators.NE) return a != b;
        if (op == VectorOperators.LT) return a < b;
        if (op == VectorOperators.LE) return a <= b;
        if (op == VectorOperators.GT) return a > b;
        if (op == VectorOperators.GE) return a >= b;
        return false;
    }

    private static boolean compareByte(byte a, VectorOperators.Comparison op, byte b) {
        if (op == VectorOperators.EQ) return a == b;
        if (op == VectorOperators.NE) return a != b;
        if (op == VectorOperators.LT) return a < b;
        if (op == VectorOperators.LE) return a <= b;
        if (op == VectorOperators.GT) return a > b;
        if (op == VectorOperators.GE) return a >= b;
        return false;
    }

    // Keep VectorMask import referenced cleanly
    @SuppressWarnings("unused")
    private static void unusedMaskRef(VectorMask<Integer> m) {}
}
