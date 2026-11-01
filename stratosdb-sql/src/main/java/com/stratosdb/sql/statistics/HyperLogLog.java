package com.stratosdb.sql.statistics;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * HyperLogLog cardinality estimator implementing Flajolet et al. with 64-bit hashing
 * and small-range Linear Counting correction.
 *
 * Configured with p=11 (m=2048 registers), offering a theoretical standard error of
 * 1.04 / sqrt(m) ≈ 2.29%. Uses 64-bit MurmurHash3 to eliminate large-cardinality saturation
 * for datasets up to 10^15 elements.
 */
public class HyperLogLog {
    public static final int DEFAULT_PRECISION = 11; // m = 2048 registers
    private final int p;
    private final int m;
    private final double alphaM;
    private final byte[] registers;

    public HyperLogLog() {
        this(DEFAULT_PRECISION);
    }

    public HyperLogLog(int p) {
        if (p < 4 || p > 16) {
            throw new IllegalArgumentException("Precision p must be between 4 and 16, got: " + p);
        }
        this.p = p;
        this.m = 1 << p;
        this.alphaM = computeAlpha(this.m);
        this.registers = new byte[this.m];
    }

    private HyperLogLog(int p, byte[] registers) {
        this.p = p;
        this.m = 1 << p;
        this.alphaM = computeAlpha(this.m);
        this.registers = registers.clone();
    }

    private static double computeAlpha(int m) {
        if (m == 16) return 0.673;
        if (m == 32) return 0.697;
        if (m == 64) return 0.709;
        return 0.7213 / (1.0 + 1.079 / m);
    }

    public void add(Object value) {
        if (value == null) {
            return;
        }
        long hash = hash64(value);
        addHash(hash);
    }

    public void addHash(long hash) {
        int index = (int) (hash & (m - 1));
        long w = hash >>> p;
        int rho;
        if (w == 0) {
            rho = 64 - p + 1;
        } else {
            rho = Long.numberOfLeadingZeros(w) - p + 1;
        }
        if (rho > registers[index]) {
            registers[index] = (byte) rho;
        }
    }

    public long estimate() {
        double sum = 0.0;
        int zeroCount = 0;
        for (int i = 0; i < m; i++) {
            sum += 1.0 / (1L << registers[i]);
            if (registers[i] == 0) {
                zeroCount++;
            }
        }
        double rawEstimate = alphaM * m * m / sum;

        // Small-range correction using Linear Counting
        if (zeroCount > 0 && rawEstimate <= 2.5 * m) {
            return Math.round(m * Math.log((double) m / zeroCount));
        }

        return Math.round(rawEstimate);
    }

    public void merge(HyperLogLog other) {
        if (other == null || other.p != this.p) {
            throw new IllegalArgumentException("Cannot merge HyperLogLog with different precision");
        }
        for (int i = 0; i < m; i++) {
            if (other.registers[i] > this.registers[i]) {
                this.registers[i] = other.registers[i];
            }
        }
    }

    public int getPrecision() {
        return p;
    }

    public int getRegisterCount() {
        return m;
    }

    public byte[] getRegisters() {
        return registers.clone();
    }

    public byte[] toByteArray() {
        byte[] out = new byte[1 + m];
        out[0] = (byte) p;
        System.arraycopy(registers, 0, out, 1, m);
        return out;
    }

    public static HyperLogLog fromByteArray(byte[] data) {
        if (data == null || data.length < 2) {
            throw new IllegalArgumentException("Invalid HLL byte array");
        }
        int precision = data[0] & 0xff;
        byte[] regs = Arrays.copyOfRange(data, 1, data.length);
        return new HyperLogLog(precision, regs);
    }

    public static long hash64(Object value) {
        if (value == null) return 0L;
        if (value instanceof Number n) {
            return mix64(n.longValue() ^ Double.doubleToLongBits(n.doubleValue()));
        } else if (value instanceof String s) {
            return murmurHash3_64(s.getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof byte[] b) {
            return murmurHash3_64(b);
        } else if (value instanceof Boolean b) {
            return mix64(b ? 1L : 0L);
        } else {
            return murmurHash3_64(value.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    public static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    public static long murmurHash3_64(byte[] data) {
        long h = 0x123456789abcdef0L ^ (data.length * 0x517cc1b727220a95L);
        int len = data.length;
        int nblocks = len / 8;
        for (int i = 0; i < nblocks; i++) {
            long k = ByteBuffer.wrap(data, i * 8, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
            k *= 0xff51afd7ed558ccdL;
            k = Long.rotateLeft(k, 31);
            k *= 0xc4ceb9fe1a85ec53L;
            h ^= k;
            h = Long.rotateLeft(h, 27);
            h = h * 5 + 0x52dce729;
        }
        int tailOffset = nblocks * 8;
        long k1 = 0;
        int rem = len - tailOffset;
        for (int i = rem - 1; i >= 0; i--) {
            k1 = (k1 << 8) | (data[tailOffset + i] & 0xffL);
        }
        if (rem > 0) {
            k1 *= 0xff51afd7ed558ccdL;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= 0xc4ceb9fe1a85ec53L;
            h ^= k1;
        }
        h ^= len;
        return mix64(h);
    }
}
