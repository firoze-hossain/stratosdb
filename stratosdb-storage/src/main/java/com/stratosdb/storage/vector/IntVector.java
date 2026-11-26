package com.stratosdb.storage.vector;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Columnar vector holding flat 32-bit signed integers in 64-byte-aligned native memory.
 */
public final class IntVector extends ColumnVector {

    public IntVector() {
        this(DEFAULT_CAPACITY, null);
    }

    public IntVector(int capacity) {
        this(capacity, null);
    }

    public IntVector(int capacity, Arena externalArena) {
        super(VectorType.INT, capacity, externalArena);
    }

    public int get(int index) {
        return dataSegment.getAtIndex(ValueLayout.JAVA_INT, index);
    }

    public void set(int index, int value) {
        dataSegment.setAtIndex(ValueLayout.JAVA_INT, index, value);
    }

    public void append(int value) {
        dataSegment.setAtIndex(ValueLayout.JAVA_INT, size++, value);
    }

    public void appendNull() {
        setNull(size++);
    }

    public void fill(int value) {
        for (int i = 0; i < capacity; i++) {
            dataSegment.setAtIndex(ValueLayout.JAVA_INT, i, value);
        }
    }

    public void copyFrom(int[] src, int srcOffset, int dstOffset, int length) {
        MemorySegment.copy(src, srcOffset, dataSegment, ValueLayout.JAVA_INT, (long) dstOffset * Integer.BYTES, length);
    }

    public void copyTo(int[] dst, int srcOffset, int dstOffset, int length) {
        MemorySegment.copy(dataSegment, ValueLayout.JAVA_INT, (long) srcOffset * Integer.BYTES, dst, dstOffset, length);
    }

    /**
     * Computes the sum of all active (or non-null) elements using a tight contiguous loop
     * suitable for HotSpot C2 SIMD auto-vectorization.
     */
    public long sum() {
        long total = 0L;
        int n = this.size;
        MemorySegment seg = this.dataSegment;
        if (!hasNulls) {
            for (int i = 0; i < n; i++) {
                total += seg.getAtIndex(ValueLayout.JAVA_INT, i);
            }
        } else {
            for (int i = 0; i < n; i++) {
                if (!isNull(i)) {
                    total += seg.getAtIndex(ValueLayout.JAVA_INT, i);
                }
            }
        }
        return total;
    }
}
