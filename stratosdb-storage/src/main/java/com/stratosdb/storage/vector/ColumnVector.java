package com.stratosdb.storage.vector;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Base class for flat, 64-byte-aligned native off-heap columnar vectors.
 * Each vector holds up to {@code capacity} (default 1024) primitive elements
 * in contiguous off-heap {@link MemorySegment} storage, paired with a compact
 * 64-byte-aligned off-heap null-mask bitmap.
 *
 * Designed for zero-allocation vectorized scans and SIMD auto-vectorization.
 */
public abstract class ColumnVector implements AutoCloseable {
    public static final int DEFAULT_CAPACITY = 1024;
    public static final long SIMD_ALIGNMENT = 64L;

    protected final VectorType type;
    protected final int capacity;
    protected final MemorySegment dataSegment;
    protected final MemorySegment nullBitmapSegment;
    protected final int bitmapLongCount;

    private final Arena ownedArena;
    protected int size;
    protected boolean hasNulls;

    protected ColumnVector(VectorType type, int capacity, Arena externalArena) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive: " + capacity);
        }
        this.type = type;
        this.capacity = capacity;
        this.ownedArena = (externalArena == null) ? Arena.ofConfined() : null;
        Arena arena = (externalArena != null) ? externalArena : this.ownedArena;

        long dataBytes = (long) capacity * type.getByteWidth();
        this.dataSegment = arena.allocate(dataBytes, SIMD_ALIGNMENT);

        this.bitmapLongCount = (capacity + 63) >>> 6;
        long bitmapBytes = (long) bitmapLongCount * Long.BYTES;
        this.nullBitmapSegment = arena.allocate(bitmapBytes, SIMD_ALIGNMENT);
        this.nullBitmapSegment.fill((byte) 0);

        this.size = 0;
        this.hasNulls = false;
    }

    /**
     * Resets the vector for reuse without any heap or off-heap allocations.
     */
    public final void reset() {
        this.size = 0;
        if (this.hasNulls) {
            this.nullBitmapSegment.fill((byte) 0);
            this.hasNulls = false;
        }
    }

    public final VectorType getType() {
        return type;
    }

    public final int getCapacity() {
        return capacity;
    }

    public final int getSize() {
        return size;
    }

    public final void setSize(int size) {
        if (size < 0 || size > capacity) {
            throw new IndexOutOfBoundsException("Size " + size + " out of bounds [0, " + capacity + "]");
        }
        this.size = size;
    }

    public final boolean hasNulls() {
        return hasNulls;
    }

    /**
     * Returns true if the element at {@code index} is marked NULL.
     * Bit = 1 in nullBitmapSegment means NULL; 0 means non-null.
     */
    public final boolean isNull(int index) {
        if (!hasNulls) {
            return false;
        }
        long word = nullBitmapSegment.getAtIndex(ValueLayout.JAVA_LONG, index >>> 6);
        return (word & (1L << (index & 63))) != 0L;
    }

    /**
     * Marks the element at {@code index} as NULL.
     */
    public final void setNull(int index) {
        int wordIdx = index >>> 6;
        long word = nullBitmapSegment.getAtIndex(ValueLayout.JAVA_LONG, wordIdx);
        nullBitmapSegment.setAtIndex(ValueLayout.JAVA_LONG, wordIdx, word | (1L << (index & 63)));
        hasNulls = true;
    }

    /**
     * Clears the NULL bit for {@code index}.
     */
    public final void setNotNull(int index) {
        if (!hasNulls) {
            return;
        }
        int wordIdx = index >>> 6;
        long word = nullBitmapSegment.getAtIndex(ValueLayout.JAVA_LONG, wordIdx);
        nullBitmapSegment.setAtIndex(ValueLayout.JAVA_LONG, wordIdx, word & ~(1L << (index & 63)));
    }

    public final MemorySegment getDataSegment() {
        return dataSegment;
    }

    public final MemorySegment getNullBitmapSegment() {
        return nullBitmapSegment;
    }

    /**
     * Returns total native off-heap bytes allocated for data + null bitmap.
     */
    public final long getMemoryFootprintBytes() {
        return dataSegment.byteSize() + nullBitmapSegment.byteSize();
    }

    @Override
    public void close() {
        if (ownedArena != null && ownedArena.scope().isAlive()) {
            ownedArena.close();
        }
    }
}
