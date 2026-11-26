package com.stratosdb.storage.vector;

/**
 * Physical primitive data types supported by native SIMD columnar vectors.
 */
public enum VectorType {
    INT(Integer.BYTES),
    LONG(Long.BYTES),
    DOUBLE(Double.BYTES),
    BYTE(Byte.BYTES);

    private final int byteWidth;

    VectorType(int byteWidth) {
        this.byteWidth = byteWidth;
    }

    /**
     * Returns the fixed byte width of each primitive element in native memory.
     */
    public int getByteWidth() {
        return byteWidth;
    }
}
