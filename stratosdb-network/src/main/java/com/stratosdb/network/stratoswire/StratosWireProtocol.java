package com.stratosdb.network.stratoswire;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Stratos Native Binary Wire Protocol (SWP v1.0).
 *
 * 100% independent, bespoke binary protocol designed specifically for StratosDB.
 * Completely distinct from any other database protocol (such as PostgreSQL or MySQL).
 *
 * Wire Frame Specification:
 * +--------------------+--------------------+--------------------+
 * | Magic: "STDB" (4B) | Message Type (1B)  | Length (4B, BE)    |
 * +--------------------+--------------------+--------------------+
 * | Payload Bytes (N bytes)                                       |
 * +---------------------------------------------------------------+
 */
public final class StratosWireProtocol {

    /** 4-byte Magic header identifying StratosDB protocol stream: ASCII "STDB" */
    public static final int MAGIC = 0x53544442;
    public static final int PROTOCOL_VERSION = 1;

    // --- Message Types (0x01 - 0xFF) ---
    public static final byte MSG_CONNECT_REQ     = 0x01; // Client -> Server: Handshake & credentials
    public static final byte MSG_CONNECT_OK      = 0x02; // Server -> Client: Handshake acknowledged
    public static final byte MSG_AUTH_CHALLENGE  = 0x03; // Server -> Client: Auth challenge (SCRAM/salt)
    public static final byte MSG_AUTH_RESPONSE   = 0x04; // Client -> Server: Auth response

    public static final byte MSG_QUERY           = 0x10; // Client -> Server: Execute SQL query
    public static final byte MSG_QUERY_HEADER    = 0x11; // Server -> Client: Query column schema
    public static final byte MSG_ROW             = 0x12; // Server -> Client: Data row
    public static final byte MSG_QUERY_END       = 0x13; // Server -> Client: Query completed with row count

    public static final byte MSG_PREPARE         = 0x20; // Client -> Server: Prepare SQL statement
    public static final byte MSG_PREPARE_OK      = 0x21; // Server -> Client: Statement prepared
    public static final byte MSG_EXECUTE         = 0x22; // Client -> Server: Execute prepared statement

    public static final byte MSG_PING            = 0x30; // Client -> Server: Heartbeat ping
    public static final byte MSG_PONG            = 0x31; // Server -> Client: Heartbeat pong

    public static final byte MSG_ERROR           = (byte) 0xFE; // Server -> Client: Error notification
    public static final byte MSG_DISCONNECT      = (byte) 0xFF; // Either -> Other: Graceful session disconnect

    // --- Value Type Tags (Binary serialization) ---
    public static final byte TYPE_NULL           = 0x00;
    public static final byte TYPE_INT32          = 0x01;
    public static final byte TYPE_INT64          = 0x02;
    public static final byte TYPE_DOUBLE         = 0x03;
    public static final byte TYPE_STRING         = 0x04;
    public static final byte TYPE_BOOLEAN        = 0x05;
    public static final byte TYPE_BYTES          = 0x06;
    public static final byte TYPE_JSON           = 0x07;
    public static final byte TYPE_ARRAY          = 0x08;

    // --- Error Codes ---
    public static final int ERR_GENERIC          = 1000;
    public static final int ERR_AUTH_FAILED      = 1001;
    public static final int ERR_SYNTAX           = 1002;
    public static final int ERR_TABLE_NOT_FOUND  = 1003;
    public static final int ERR_DUPLICATE_KEY    = 1004;
    public static final int ERR_TRANSACTION      = 1005;
    public static final int ERR_UNSUPPORTED      = 1006;

    private StratosWireProtocol() {}

    /**
     * Reads the next message frame from the input stream.
     * Validates the 4-byte "STDB" magic header.
     */
    public static Frame readFrame(DataInputStream in) throws IOException {
        int magic = in.readInt();
        if (magic != MAGIC) {
            throw new IOException(String.format("Invalid StratosDB protocol magic: 0x%08X (expected 0x%08X 'STDB')", magic, MAGIC));
        }
        byte type = in.readByte();
        int length = in.readInt();
        if (length < 0 || length > 64 * 1024 * 1024) { // 64MB frame sanity limit
            throw new IOException("Frame length out of valid bounds: " + length);
        }
        byte[] payload = new byte[length];
        in.readFully(payload);
        return new Frame(type, payload);
    }

    /**
     * Writes a complete message frame to the output stream.
     */
    public static void writeFrame(DataOutputStream out, byte type, byte[] payload) throws IOException {
        out.writeInt(MAGIC);
        out.writeByte(type);
        out.writeInt(payload != null ? payload.length : 0);
        if (payload != null && payload.length > 0) {
            out.write(payload);
        }
        out.flush();
    }

    // --- Strongly Typed Value Serialization ---

    public static void writeTypedValue(DataOutputStream out, Object value) throws IOException {
        if (value == null) {
            out.writeByte(TYPE_NULL);
        } else if (value instanceof Integer i) {
            out.writeByte(TYPE_INT32);
            out.writeInt(i);
        } else if (value instanceof Long l) {
            out.writeByte(TYPE_INT64);
            out.writeLong(l);
        } else if (value instanceof Double d) {
            out.writeByte(TYPE_DOUBLE);
            out.writeDouble(d);
        } else if (value instanceof Float f) {
            out.writeByte(TYPE_DOUBLE);
            out.writeDouble(f.doubleValue());
        } else if (value instanceof Boolean b) {
            out.writeByte(TYPE_BOOLEAN);
            out.writeByte(b ? 1 : 0);
        } else if (value instanceof byte[] bytes) {
            out.writeByte(TYPE_BYTES);
            out.writeInt(bytes.length);
            out.write(bytes);
        } else if (value instanceof java.util.Map<?, ?> map) {
            out.writeByte(TYPE_JSON);
            byte[] jsonBytes = com.stratosdb.sql.executor.JsonParser.toJsonText(map).getBytes(StandardCharsets.UTF_8);
            out.writeInt(jsonBytes.length);
            out.write(jsonBytes);
        } else if (value instanceof List<?> list) {
            out.writeByte(TYPE_ARRAY);
            out.writeInt(list.size());
            for (Object item : list) {
                writeTypedValue(out, item);
            }
        } else {
            // String / Fallback
            out.writeByte(TYPE_STRING);
            byte[] strBytes = value.toString().getBytes(StandardCharsets.UTF_8);
            out.writeInt(strBytes.length);
            out.write(strBytes);
        }
    }

    public static Object readTypedValue(DataInputStream in) throws IOException {
        byte tag = in.readByte();
        return switch (tag) {
            case TYPE_NULL -> null;
            case TYPE_INT32 -> in.readInt();
            case TYPE_INT64 -> in.readLong();
            case TYPE_DOUBLE -> in.readDouble();
            case TYPE_BOOLEAN -> in.readByte() != 0;
            case TYPE_BYTES -> {
                int len = in.readInt();
                byte[] b = new byte[len];
                in.readFully(b);
                yield b;
            }
            case TYPE_JSON -> {
                int len = in.readInt();
                byte[] b = new byte[len];
                in.readFully(b);
                yield new String(b, StandardCharsets.UTF_8);
            }
            case TYPE_ARRAY -> {
                int count = in.readInt();
                List<Object> items = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    items.add(readTypedValue(in));
                }
                yield items;
            }
            case TYPE_STRING -> {
                int len = in.readInt();
                byte[] b = new byte[len];
                in.readFully(b);
                yield new String(b, StandardCharsets.UTF_8);
            }
            default -> throw new IOException("Unknown StratosWire type tag: " + tag);
        };
    }

    /** Single protocol frame containing message type and binary payload */
    public record Frame(byte type, byte[] payload) {
        public DataInputStream dataStream() {
            return new DataInputStream(new ByteArrayInputStream(payload));
        }
    }

    /** Column metadata returned in MSG_QUERY_HEADER */
    public record ColumnMeta(String name, byte typeTag) {}
}
