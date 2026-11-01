package com.stratosdb.network.stdwire;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class PgBinaryDecoderTest {

    @Test
    public void testDecodeBinaryInt2() {
        byte[] bytes = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 42).array();
        Object val = PgBinaryDecoder.decode(bytes, 1, PgBinaryDecoder.OID_INT2, null);
        assertEquals((short) 42, val);

        // Inferred from column type
        Object val2 = PgBinaryDecoder.decode(bytes, 1, 0, "SMALLINT");
        assertEquals((short) 42, val2);

        // Inferred from length 2
        Object val3 = PgBinaryDecoder.decode(bytes, 1, 0, null);
        assertEquals((short) 42, val3);
    }

    @Test
    public void testDecodeBinaryInt4() {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(123456).array();
        Object val = PgBinaryDecoder.decode(bytes, 1, PgBinaryDecoder.OID_INT4, null);
        assertEquals(123456, val);

        Object val2 = PgBinaryDecoder.decode(bytes, 1, 0, "INTEGER");
        assertEquals(123456, val2);

        Object val3 = PgBinaryDecoder.decode(bytes, 1, 0, null);
        assertEquals(123456, val3);
    }

    @Test
    public void testDecodeBinaryInt8() {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(9876543210123L).array();
        Object val = PgBinaryDecoder.decode(bytes, 1, PgBinaryDecoder.OID_INT8, null);
        assertEquals(9876543210123L, val);

        Object val2 = PgBinaryDecoder.decode(bytes, 1, 0, "BIGINT");
        assertEquals(9876543210123L, val2);

        Object val3 = PgBinaryDecoder.decode(bytes, 1, 0, null);
        assertEquals(9876543210123L, val3);
    }

    @Test
    public void testDecodeBinaryFloat4() {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putFloat(3.1415f).array();
        Object val = PgBinaryDecoder.decode(bytes, 1, PgBinaryDecoder.OID_FLOAT4, null);
        assertEquals(3.1415f, (Float) val, 0.0001f);

        Object val2 = PgBinaryDecoder.decode(bytes, 1, 0, "FLOAT4");
        assertEquals(3.1415f, (Float) val2, 0.0001f);
    }

    @Test
    public void testDecodeBinaryFloat8() {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(2.718281828459).array();
        Object val = PgBinaryDecoder.decode(bytes, 1, PgBinaryDecoder.OID_FLOAT8, null);
        assertEquals(2.718281828459, (Double) val, 0.0000000001);

        Object val2 = PgBinaryDecoder.decode(bytes, 1, 0, "DOUBLE PRECISION");
        assertEquals(2.718281828459, (Double) val2, 0.0000000001);
    }

    @Test
    public void testDecodeBinaryTimestamp() {
        // PG epoch is 2000-01-01 00:00:00 UTC.
        // Let's test an instant: 2026-11-01 12:00:00 UTC
        Instant instant = Instant.parse("2026-11-01T12:00:00Z");
        long unixMicros = instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000L);
        long pgMicros = unixMicros - PgBinaryDecoder.PG_EPOCH_MICROS;

        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(pgMicros).array();
        Object val = PgBinaryDecoder.decode(bytes, 1, PgBinaryDecoder.OID_TIMESTAMP, null);
        assertTrue(val instanceof Timestamp);
        assertEquals(Timestamp.from(instant), val);

        Object val2 = PgBinaryDecoder.decode(bytes, 1, 0, "TIMESTAMP");
        assertEquals(Timestamp.from(instant), val2);
    }

    @Test
    public void testDecodeBinaryBytea() {
        byte[] payload = new byte[]{0x00, 0x01, (byte) 0xFF, (byte) 0xFE, 0x42};
        Object val = PgBinaryDecoder.decode(payload, 1, PgBinaryDecoder.OID_BYTEA, null);
        assertTrue(val instanceof byte[]);
        assertArrayEquals(payload, (byte[]) val);

        Object val2 = PgBinaryDecoder.decode(payload, 1, 0, "BYTEA");
        assertArrayEquals(payload, (byte[]) val2);
    }

    @Test
    public void testDecodeBinaryText() {
        byte[] bytes = "Hello, StratosDB Binary!".getBytes(StandardCharsets.UTF_8);
        Object val = PgBinaryDecoder.decode(bytes, 1, PgBinaryDecoder.OID_TEXT, null);
        assertEquals("Hello, StratosDB Binary!", val);

        Object val2 = PgBinaryDecoder.decode(bytes, 1, PgBinaryDecoder.OID_VARCHAR, null);
        assertEquals("Hello, StratosDB Binary!", val2);
    }

    @Test
    public void testDecodeBinaryBool() {
        byte[] t = new byte[]{1};
        byte[] f = new byte[]{0};
        assertEquals(Boolean.TRUE, PgBinaryDecoder.decode(t, 1, PgBinaryDecoder.OID_BOOL, null));
        assertEquals(Boolean.FALSE, PgBinaryDecoder.decode(f, 1, PgBinaryDecoder.OID_BOOL, null));
    }

    @Test
    public void testDecodeTextByteaHex() {
        byte[] expected = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
        byte[] raw = "\\xdeadbeef".getBytes(StandardCharsets.UTF_8);
        Object val = PgBinaryDecoder.decode(raw, 0, PgBinaryDecoder.OID_BYTEA, null);
        assertTrue(val instanceof byte[]);
        assertArrayEquals(expected, (byte[]) val);

        Object val2 = PgBinaryDecoder.decode(raw, 0, 0, "BYTEA");
        assertArrayEquals(expected, (byte[]) val2);
    }

    @Test
    public void testDecodeNull() {
        assertNull(PgBinaryDecoder.decode(null, 1, PgBinaryDecoder.OID_INT4, "INTEGER"));
        assertNull(PgBinaryDecoder.decode(null, 0, PgBinaryDecoder.OID_TEXT, "TEXT"));
    }
}
