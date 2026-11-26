package com.stratosdb.storage.vector;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Columnar vector holding flat 64-bit IEEE-754 doubles in 64-byte-aligned native memory.
 */
public final class DoubleVector extends ColumnVector {

    public DoubleVector() {
        this(DEFAULT_CAPACITY, null);
    }

    public DoubleVector(int capacity) {
        this(capacity, null);
    }

    public DoubleVector(int capacity, Arena externalArena) {
        super(VectorType.DOUBLE, capacity, externalArena);
    }

    public double get(int index) {
        return dataSegment.getAtIndex(ValueLayout.JAVA_DOUBLE, index);
    }

    public void set(int index, double value) {
        dataSegment.setAtIndex(ValueLayout.JAVA_DOUBLE, index, value);
    }

    public void append(double value) {
        dataSegment.setAtIndex(ValueLayout.JAVA_DOUBLE, size++, value);
    }

    public void appendNull() {
        setNull(size++);
    }

    public void fill(double value) {
        for (int i = 0; i < capacity; i++) {
            dataSegment.setAtIndex(ValueLayout.JAVA_DOUBLE, i, value);
        }
    }

    public void copyFrom(double[] src, int srcOffset, int dstOffset, int length) {
        MemorySegment.copy(src, srcOffset, dataSegment, ValueLayout.JAVA_DOUBLE, (long) dstOffset * Double.BYTES, length);
    }

    public void copyTo(double[] dst, int srcOffset, int dstOffset, int length) {
        MemorySegment.copy(dataSegment, ValueLayout.JAVA_DOUBLE, (long) srcOffset * Double.BYTES, dst, dstOffset, length);
    }

    /**
     * Computes the sum of all active (or non-null) elements using a tight contiguous loop
     * suitable for HotSpot C2 SIMD auto-vectorization.
     */
    public double sum() {
        double total = 0.0;
        int n = this.size;
        MemorySegment seg = this.dataSegment;
        if (!hasNulls) {
            for (int i = 0; i < n; i++) {
                total += seg.getAtIndex(ValueLayout.JAVA_DOUBLE, i);
            }
        } else {
            for (int i = 0; i < n; i++) {
                if (!isNull(i)) {
                    total += seg.getAtIndex(ValueLayout.JAVA_DOUBLE, i);
                }
            }
        }
        return total;
    }
}
