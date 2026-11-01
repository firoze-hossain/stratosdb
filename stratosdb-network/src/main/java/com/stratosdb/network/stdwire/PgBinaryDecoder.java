package com.stratosdb.network.stdwire;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Decodes PostgreSQL wire protocol parameters (both binary format code 1 and text format code 0)
 * directly into strongly-typed Java values.
 *
 * Supports native pgwire binary formats:
 * - int2 (short, 2 bytes big-endian)
 * - int4 (int, 4 bytes big-endian)
 * - int8 (long, 8 bytes big-endian)
 * - float4 (float, 4 bytes big-endian IEEE 754)
 * - float8 (double, 8 bytes big-endian IEEE 754)
 * - timestamp / timestamptz (8 bytes big-endian, microseconds since PostgreSQL epoch 2000-01-01 00:00:00 UTC)
 * - bytea (raw bytes as-is)
 * - text / varchar (UTF-8 bytes)
 * - bool (1 byte, 0 = false, non-zero = true)
 */
public final class PgBinaryDecoder {

    /** PostgreSQL epoch is 2000-01-01 00:00:00 UTC. Unix epoch difference is 946_684_800_000_000 microseconds (10,957 days). */
    public static final long PG_EPOCH_MICROS = 946_684_800_000_000L;

    public static final int OID_BOOL = 16;
    public static final int OID_BYTEA = 17;
    public static final int OID_INT8 = 20;
    public static final int OID_INT2 = 21;
    public static final int OID_INT4 = 23;
    public static final int OID_TEXT = 25;
    public static final int OID_FLOAT4 = 700;
    public static final int OID_FLOAT8 = 701;
    public static final int OID_VARCHAR = 1043;
    public static final int OID_DATE = 1082;
    public static final int OID_TIMESTAMP = 1114;
    public static final int OID_TIMESTAMPTZ = 1184;

    private PgBinaryDecoder() {}

    /**
     * Decodes raw parameter bytes sent in pgwire Bind message into a typed Object.
     *
     * @param rawValue raw parameter bytes (null means SQL NULL)
     * @param formatCode 0 for text, 1 for binary
     * @param typeOid PostgreSQL type OID (0 if unspecified by client)
     * @param targetColumnType declared schema column type name (e.g. "INT4", "BYTEA", etc.), or null if unknown
     * @return strongly-typed Java object (Integer, Long, Short, Float, Double, Boolean, byte[], Timestamp, String, or null)
     */
    public static Object decode(byte[] rawValue, int formatCode, int typeOid, String targetColumnType) {
        if (rawValue == null) {
            return null;
        }

        int effectiveOid = typeOid;
        if (effectiveOid == 0 && targetColumnType != null) {
            effectiveOid = oidFromColumnType(targetColumnType);
        }

        if (formatCode == 1) {
            return decodeBinary(rawValue, effectiveOid, targetColumnType);
        } else {
            return decodeText(rawValue, effectiveOid, targetColumnType);
        }
    }

    public static Object decodeBinary(byte[] raw, int typeOid, String targetColumnType) {
        ByteBuffer buf = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);

        switch (typeOid) {
            case OID_BOOL -> {
                return raw.length > 0 && raw[0] != 0;
            }
            case OID_BYTEA -> {
                return raw;
            }
            case OID_INT2 -> {
                return buf.getShort();
            }
            case OID_INT4 -> {
                return buf.getInt();
            }
            case OID_INT8 -> {
                return buf.getLong();
            }
            case OID_FLOAT4 -> {
                return buf.getFloat();
            }
            case OID_FLOAT8 -> {
                return buf.getDouble();
            }
            case OID_TIMESTAMP, OID_TIMESTAMPTZ -> {
                long pgMicros = buf.getLong();
                long unixMicros = pgMicros + PG_EPOCH_MICROS;
                long unixMillis = Math.floorDiv(unixMicros, 1000L);
                int nanoAdjustment = (int) (Math.floorMod(unixMicros, 1000L) * 1000);
                Instant instant = Instant.ofEpochMilli(unixMillis).plusNanos(nanoAdjustment);
                return Timestamp.from(instant);
            }
            case OID_TEXT, OID_VARCHAR -> {
                return new String(raw, StandardCharsets.UTF_8);
            }
            default -> {
                // If type OID was 0 / unspecified, infer from byte length or column type
                if (targetColumnType != null) {
                    String norm = targetColumnType.trim().toUpperCase();
                    if (norm.equals("BYTEA") || norm.equals("BLOB") || norm.equals("BYTES")) return raw;
                    if (norm.equals("SMALLINT") || norm.equals("INT2")) return buf.getShort();
                    if (norm.equals("INT") || norm.equals("INTEGER") || norm.equals("INT4") || norm.equals("SERIAL")) return buf.getInt();
                    if (norm.equals("BIGINT") || norm.equals("INT8") || norm.equals("BIGSERIAL")) return buf.getLong();
                    if (norm.equals("FLOAT") || norm.equals("FLOAT4") || norm.equals("REAL")) return buf.getFloat();
                    if (norm.equals("DOUBLE") || norm.equals("FLOAT8") || norm.equals("NUMERIC") || norm.equals("DECIMAL") || norm.startsWith("DOUBLE")) return buf.getDouble();
                    if (norm.equals("BOOLEAN") || norm.equals("BOOL")) return raw.length > 0 && raw[0] != 0;
                    if (norm.equals("TIMESTAMP") || norm.equals("TIMESTAMPTZ")) {
                        long pgMicros = buf.getLong();
                        long unixMicros = pgMicros + PG_EPOCH_MICROS;
                        long unixMillis = Math.floorDiv(unixMicros, 1000L);
                        int nanoAdjustment = (int) (Math.floorMod(unixMicros, 1000L) * 1000);
                        return Timestamp.from(Instant.ofEpochMilli(unixMillis).plusNanos(nanoAdjustment));
                    }
                }
                // Fallback by length:
                return switch (raw.length) {
                    case 1 -> raw[0] != 0;
                    case 2 -> buf.getShort();
                    case 4 -> buf.getInt();
                    case 8 -> buf.getLong();
                    default -> raw;
                };
            }
        }
    }

    public static Object decodeText(byte[] raw, int typeOid, String targetColumnType) {
        String text = new String(raw, StandardCharsets.UTF_8);

        switch (typeOid) {
            case OID_BOOL -> {
                return text.equalsIgnoreCase("true") || text.equalsIgnoreCase("t") || text.equals("1");
            }
            case OID_BYTEA -> {
                return decodeHexOrRaw(text, raw);
            }
            case OID_INT2 -> {
                try { return Short.parseShort(text.trim()); } catch (NumberFormatException ignored) { return text; }
            }
            case OID_INT4 -> {
                try { return Integer.parseInt(text.trim()); } catch (NumberFormatException ignored) { return text; }
            }
            case OID_INT8 -> {
                try { return Long.parseLong(text.trim()); } catch (NumberFormatException ignored) { return text; }
            }
            case OID_FLOAT4 -> {
                try { return Float.parseFloat(text.trim()); } catch (NumberFormatException ignored) { return text; }
            }
            case OID_FLOAT8 -> {
                try { return Double.parseDouble(text.trim()); } catch (NumberFormatException ignored) { return text; }
            }
            case OID_TIMESTAMP, OID_TIMESTAMPTZ -> {
                try {
                    return Timestamp.valueOf(text.trim());
                } catch (IllegalArgumentException e) {
                    try {
                        return Timestamp.from(Instant.parse(text.trim()));
                    } catch (Exception ignored) {
                        return text;
                    }
                }
            }
            case OID_TEXT, OID_VARCHAR -> {
                return text;
            }
            default -> {
                if (targetColumnType != null) {
                    String norm = targetColumnType.trim().toUpperCase();
                    if (norm.equals("BYTEA") || norm.equals("BLOB") || norm.equals("BYTES")) {
                        return decodeHexOrRaw(text, raw);
                    }
                    if (norm.equals("SMALLINT") || norm.equals("INT2")) {
                        try { return Short.parseShort(text.trim()); } catch (NumberFormatException ignored) {}
                    }
                    if (norm.equals("INT") || norm.equals("INTEGER") || norm.equals("INT4") || norm.equals("SERIAL")) {
                        try { return Integer.parseInt(text.trim()); } catch (NumberFormatException ignored) {}
                    }
                    if (norm.equals("BIGINT") || norm.equals("INT8") || norm.equals("BIGSERIAL")) {
                        try { return Long.parseLong(text.trim()); } catch (NumberFormatException ignored) {}
                    }
                    if (norm.equals("FLOAT") || norm.equals("FLOAT4") || norm.equals("REAL")) {
                        try { return Float.parseFloat(text.trim()); } catch (NumberFormatException ignored) {}
                    }
                    if (norm.equals("DOUBLE") || norm.equals("FLOAT8") || norm.equals("NUMERIC") || norm.equals("DECIMAL")) {
                        try { return Double.parseDouble(text.trim()); } catch (NumberFormatException ignored) {}
                    }
                    if (norm.equals("BOOLEAN") || norm.equals("BOOL")) {
                        return text.equalsIgnoreCase("true") || text.equalsIgnoreCase("t") || text.equals("1");
                    }
                }
                // Default literal parsing
                if (text.equalsIgnoreCase("true") || text.equalsIgnoreCase("false")) {
                    return Boolean.parseBoolean(text);
                }
                try {
                    if (text.contains(".")) {
                        return Double.parseDouble(text);
                    }
                    return Long.parseLong(text);
                } catch (NumberFormatException ignored) {}
                return text;
            }
        }
    }

    private static byte[] decodeHexOrRaw(String text, byte[] fallback) {
        String s = text.trim();
        if (s.startsWith("\\x") || s.startsWith("\\X")) {
            return decodeHex(s.substring(2));
        } else if (s.startsWith("x'") || s.startsWith("X'")) {
            return decodeHex(s.substring(2, s.length() - 1));
        }
        return fallback;
    }

    public static byte[] decodeHex(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                 + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    public static int oidFromColumnType(String colType) {
        if (colType == null) return 0;
        String norm = colType.trim().toUpperCase();
        if (norm.equals("SMALLINT") || norm.equals("INT2")) return OID_INT2;
        if (norm.equals("INT") || norm.equals("INTEGER") || norm.equals("INT4") || norm.equals("SERIAL")) return OID_INT4;
        if (norm.equals("BIGINT") || norm.equals("INT8") || norm.equals("BIGSERIAL")) return OID_INT8;
        if (norm.equals("FLOAT") || norm.equals("FLOAT4") || norm.equals("REAL")) return OID_FLOAT4;
        if (norm.equals("DOUBLE") || norm.equals("FLOAT8") || norm.equals("NUMERIC") || norm.equals("DECIMAL") || norm.startsWith("DOUBLE")) return OID_FLOAT8;
        if (norm.equals("BOOLEAN") || norm.equals("BOOL")) return OID_BOOL;
        if (norm.equals("BYTEA") || norm.equals("BLOB") || norm.equals("BYTES")) return OID_BYTEA;
        if (norm.equals("TIMESTAMP") || norm.equals("TIMESTAMPTZ")) return OID_TIMESTAMP;
        if (norm.equals("DATE")) return OID_DATE;
        if (norm.equals("TEXT") || norm.equals("VARCHAR") || norm.equals("CHAR")) return OID_TEXT;
        return 0;
    }
}
