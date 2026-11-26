package com.stratosdb.storage.vector;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Columnar vector holding flat 8-bit signed bytes (and booleans) in 64-byte-aligned native memory.
 */
public final class ByteVector extends ColumnVector {

    public ByteVector() {
        this(DEFAULT_CAPACITY, null);
    }

    public ByteVector(int capacity) {
        this(capacity, null);
    }

    public ByteVector(int capacity, Arena externalArena) {
        super(VectorType.BYTE, capacity, externalArena);
    }

    public byte get(int index) {
        return dataSegment.get(ValueLayout.JAVA_BYTE, index);
    }

    public void set(int index, byte value) {
        dataSegment.set(ValueLayout.JAVA_BYTE, index, value);
    }

    public boolean getBoolean(int index) {
        return dataSegment.get(ValueLayout.JAVA_BYTE, index) != 0;
    }

    public void setBoolean(int index, boolean value) {
        dataSegment.set(ValueLayout.JAVA_BYTE, index, value ? (byte) 1 : (byte) 0);
    }

    public void append(byte value) {
        dataSegment.set(ValueLayout.JAVA_BYTE, size++, value);
    }

    public void appendBoolean(boolean value) {
        dataSegment.set(ValueLayout.JAVA_BYTE, size++, value ? (byte) 1 : (byte) 0);
    }

    public void appendNull() {
        setNull(size++);
    }

    public void fill(byte value) {
        dataSegment.fill(value);
    }

    public void copyFrom(byte[] src, int srcOffset, int dstOffset, int length) {
        MemorySegment.copy(src, srcOffset, dataSegment, ValueLayout.JAVA_BYTE, dstOffset, length);
    }

    public void copyTo(byte[] dst, int srcOffset, int dstOffset, int length) {
        MemorySegment.copy(dataSegment, ValueLayout.JAVA_BYTE, srcOffset, dst, dstOffset, length);
    }
}
