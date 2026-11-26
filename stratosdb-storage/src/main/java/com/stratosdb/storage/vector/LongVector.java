package com.stratosdb.storage.vector;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Columnar vector holding flat 64-bit signed longs in 64-byte-aligned native memory.
 */
public final class LongVector extends ColumnVector {

    public LongVector() {
        this(DEFAULT_CAPACITY, null);
    }

    public LongVector(int capacity) {
        this(capacity, null);
    }

    public LongVector(int capacity, Arena externalArena) {
        super(VectorType.LONG, capacity, externalArena);
    }

    public long get(int index) {
        return dataSegment.getAtIndex(ValueLayout.JAVA_LONG, index);
    }

    public void set(int index, long value) {
        dataSegment.setAtIndex(ValueLayout.JAVA_LONG, index, value);
    }

    public void append(long value) {
        dataSegment.setAtIndex(ValueLayout.JAVA_LONG, size++, value);
    }

    public void appendNull() {
        setNull(size++);
    }

    public void fill(long value) {
        for (int i = 0; i < capacity; i++) {
            dataSegment.setAtIndex(ValueLayout.JAVA_LONG, i, value);
        }
    }

    public void copyFrom(long[] src, int srcOffset, int dstOffset, int length) {
        MemorySegment.copy(src, srcOffset, dataSegment, ValueLayout.JAVA_LONG, (long) dstOffset * Long.BYTES, length);
    }

    public void copyTo(long[] dst, int srcOffset, int dstOffset, int length) {
        MemorySegment.copy(dataSegment, ValueLayout.JAVA_LONG, (long) srcOffset * Long.BYTES, dst, dstOffset, length);
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
                total += seg.getAtIndex(ValueLayout.JAVA_LONG, i);
            }
        } else {
            for (int i = 0; i < n; i++) {
                if (!isNull(i)) {
                    total += seg.getAtIndex(ValueLayout.JAVA_LONG, i);
                }
            }
        }
        return total;
    }
}
