package com.stratosdb.network.stdwire;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

public class StdBinaryDecoderTest {

    @Test
    public void testDecodeBinaryInt2() {
        byte[] bytes = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 42).array();
        Object val = StdBinaryDecoder.decode(bytes, 1, StdBinaryDecoder.OID_INT2, null);
        assertEquals((short) 42, val);

        // Inferred from column type
        Object val2 = StdBinaryDecoder.decode(bytes, 1, 0, "SMALLINT");
        assertEquals((short) 42, val2);

        // Inferred from length 2
        Object val3 = StdBinaryDecoder.decode(bytes, 1, 0, null);
        assertEquals((short) 42, val3);
    }

    @Test
    public void testDecodeBinaryInt4() {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(123456).array();
        Object val = StdBinaryDecoder.decode(bytes, 1, StdBinaryDecoder.OID_INT4, null);
        assertEquals(123456, val);

        Object val2 = StdBinaryDecoder.decode(bytes, 1, 0, "INTEGER");
        assertEquals(123456, val2);

        Object val3 = StdBinaryDecoder.decode(bytes, 1, 0, null);
        assertEquals(123456, val3);
    }

    @Test
    public void testDecodeBinaryInt8() {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(9876543210123L).array();
        Object val = StdBinaryDecoder.decode(bytes, 1, StdBinaryDecoder.OID_INT8, null);
        assertEquals(9876543210123L, val);

        Object val2 = StdBinaryDecoder.decode(bytes, 1, 0, "BIGINT");
        assertEquals(9876543210123L, val2);

        Object val3 = StdBinaryDecoder.decode(bytes, 1, 0, null);
        assertEquals(9876543210123L, val3);
    }

    @Test
    public void testDecodeBinaryFloat4() {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putFloat(3.1415f).array();
        Object val = StdBinaryDecoder.decode(bytes, 1, StdBinaryDecoder.OID_FLOAT4, null);
        assertEquals(3.1415f, (Float) val, 0.0001f);

        Object val2 = StdBinaryDecoder.decode(bytes, 1, 0, "FLOAT4");
        assertEquals(3.1415f, (Float) val2, 0.0001f);
    }

    @Test
    public void testDecodeBinaryFloat8() {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(2.718281828459).array();
        Object val = StdBinaryDecoder.decode(bytes, 1, StdBinaryDecoder.OID_FLOAT8, null);
        assertEquals(2.718281828459, (Double) val, 0.0000000001);

        Object val2 = StdBinaryDecoder.decode(bytes, 1, 0, "DOUBLE PRECISION");
        assertEquals(2.718281828459, (Double) val2, 0.0000000001);
    }

    @Test
    public void testDecodeBinaryTimestamp() {
        // Standard epoch is 2000-01-01 00:00:00 UTC.
        // Let's test an instant: 2026-11-01 12:00:00 UTC
        Instant instant = Instant.parse("2026-11-01T12:00:00Z");
        long unixMicros = instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000L);
        long wireMicros = unixMicros - StdBinaryDecoder.EPOCH_DIFF_MICROS;

        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(wireMicros).array();
        Object val = StdBinaryDecoder.decode(bytes, 1, StdBinaryDecoder.OID_TIMESTAMP, null);
        assertTrue(val instanceof Timestamp);
        assertEquals(Timestamp.from(instant), val);

        Object val2 = StdBinaryDecoder.decode(bytes, 1, 0, "TIMESTAMP");
        assertEquals(Timestamp.from(instant), val2);
    }

    @Test
    public void testDecodeBinaryBytea() {
        byte[] payload = new byte[]{0x00, 0x01, (byte) 0xFF, (byte) 0xFE, 0x42};
        Object val = StdBinaryDecoder.decode(payload, 1, StdBinaryDecoder.OID_BYTEA, null);
        assertTrue(val instanceof byte[]);
        assertArrayEquals(payload, (byte[]) val);

        Object val2 = StdBinaryDecoder.decode(payload, 1, 0, "BYTEA");
        assertArrayEquals(payload, (byte[]) val2);
    }

    @Test
    public void testDecodeBinaryText() {
        byte[] bytes = "Hello, StratosDB Binary!".getBytes(StandardCharsets.UTF_8);
        Object val = StdBinaryDecoder.decode(bytes, 1, StdBinaryDecoder.OID_TEXT, null);
        assertEquals("Hello, StratosDB Binary!", val);

        Object val2 = StdBinaryDecoder.decode(bytes, 1, StdBinaryDecoder.OID_VARCHAR, null);
        assertEquals("Hello, StratosDB Binary!", val2);
    }

    @Test
    public void testDecodeBinaryBool() {
        byte[] t = new byte[]{1};
        byte[] f = new byte[]{0};
        assertEquals(Boolean.TRUE, StdBinaryDecoder.decode(t, 1, StdBinaryDecoder.OID_BOOL, null));
        assertEquals(Boolean.FALSE, StdBinaryDecoder.decode(f, 1, StdBinaryDecoder.OID_BOOL, null));
    }

    @Test
    public void testDecodeTextByteaHex() {
        byte[] expected = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
        byte[] raw = "\\xdeadbeef".getBytes(StandardCharsets.UTF_8);
        Object val = StdBinaryDecoder.decode(raw, 0, StdBinaryDecoder.OID_BYTEA, null);
        assertTrue(val instanceof byte[]);
        assertArrayEquals(expected, (byte[]) val);

        Object val2 = StdBinaryDecoder.decode(raw, 0, 0, "BYTEA");
        assertArrayEquals(expected, (byte[]) val2);
    }

    @Test
    public void testDecodeNull() {
        assertNull(StdBinaryDecoder.decode(null, 1, StdBinaryDecoder.OID_INT4, "INTEGER"));
        assertNull(StdBinaryDecoder.decode(null, 0, StdBinaryDecoder.OID_TEXT, "TEXT"));
    }
}
