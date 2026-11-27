package com.stratosdb.network.mysql;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Low-level MySQL Client/Server Protocol v10 packet framing and encoding helpers.
 */
public final class MySqlPacketCodec {
    public static final int PROTOCOL_VERSION = 10;
    public static final String SERVER_VERSION = "8.0.36-StratosDB";

    // Capability flags
    public static final int CLIENT_LONG_PASSWORD = 0x00000001;
    public static final int CLIENT_FOUND_ROWS = 0x00000002;
    public static final int CLIENT_LONG_FLAG = 0x00000004;
    public static final int CLIENT_CONNECT_WITH_DB = 0x00000008;
    public static final int CLIENT_PROTOCOL_41 = 0x00000200;
    public static final int CLIENT_TRANSACTIONS = 0x00002000;
    public static final int CLIENT_SECURE_CONNECTION = 0x00008000;
    public static final int CLIENT_MULTI_RESULTS = 0x00020000;
    public static final int CLIENT_PLUGIN_AUTH = 0x00080000;
    public static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
    public static final int CLIENT_DEPRECATE_EOF = 0x01000000;

    public static final int SERVER_STATUS_AUTOCOMMIT = 0x0002;
    public static final int DEFAULT_CHARSET_UTF8MB4 = 45;

    // MySQL COM_* commands
    public static final int COM_QUIT = 0x01;
    public static final int COM_INIT_DB = 0x02;
    public static final int COM_QUERY = 0x03;
    public static final int COM_PING = 0x0E;

    // MySQL column types
    public static final int TYPE_LONG = 0x03;
    public static final int TYPE_DOUBLE = 0x05;
    public static final int TYPE_LONGLONG = 0x08;
    public static final int TYPE_VAR_STRING = 0xFD;

    private MySqlPacketCodec() {}

    public record Packet(int sequenceId, byte[] payload) {}

    public static Packet readPacket(DataInputStream in) throws IOException {
        int b0 = in.read();
        if (b0 < 0) throw new EOFException("Connection closed");
        int b1 = in.readUnsignedByte();
        int b2 = in.readUnsignedByte();
        int len = b0 | (b1 << 8) | (b2 << 16);
        int seq = in.readUnsignedByte();
        byte[] payload = new byte[len];
        in.readFully(payload);
        return new Packet(seq, payload);
    }

    public static void writePacket(OutputStream out, int sequenceId, byte[] payload) throws IOException {
        int len = payload.length;
        out.write(len & 0xFF);
        out.write((len >>> 8) & 0xFF);
        out.write((len >>> 16) & 0xFF);
        out.write(sequenceId & 0xFF);
        out.write(payload);
    }

    public static byte[] buildHandshakeV10(int connectionId, byte[] scramble20, String authPluginName) throws IOException {
        int caps = CLIENT_LONG_PASSWORD | CLIENT_FOUND_ROWS | CLIENT_LONG_FLAG
            | CLIENT_CONNECT_WITH_DB | CLIENT_PROTOCOL_41 | CLIENT_TRANSACTIONS
            | CLIENT_SECURE_CONNECTION | CLIENT_MULTI_RESULTS | CLIENT_PLUGIN_AUTH
            | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_DEPRECATE_EOF;

        ByteArrayOutputStream out = new ByteArrayOutputStream(128);
        out.write(PROTOCOL_VERSION);
        out.write(SERVER_VERSION.getBytes(StandardCharsets.UTF_8));
        out.write(0);
        writeInt4LE(out, connectionId);
        // auth-plugin-data-part-1 (8 bytes)
        out.write(scramble20, 0, 8);
        out.write(0); // filler
        writeInt2LE(out, caps & 0xFFFF);
        out.write(DEFAULT_CHARSET_UTF8MB4);
        writeInt2LE(out, SERVER_STATUS_AUTOCOMMIT);
        writeInt2LE(out, (caps >>> 16) & 0xFFFF);
        out.write(21); // auth_plugin_data_len (8 + 12 + 1)
        out.write(new byte[10]); // reserved
        // auth-plugin-data-part-2 (12 bytes + NUL)
        out.write(scramble20, 8, 12);
        out.write(0);
        out.write(authPluginName.getBytes(StandardCharsets.UTF_8));
        out.write(0);
        return out.toByteArray();
    }

    public static byte[] buildOkPacket(long affectedRows, long lastInsertId, boolean deprecateEof) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(32);
        out.write(0x00); // OK header
        writeLenEncInt(out, affectedRows);
        writeLenEncInt(out, lastInsertId);
        writeInt2LE(out, SERVER_STATUS_AUTOCOMMIT);
        writeInt2LE(out, 0); // warnings
        return out.toByteArray();
    }

    public static byte[] buildEofPacket() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(8);
        out.write(0xFE);
        writeInt2LE(out, 0); // warnings
        writeInt2LE(out, SERVER_STATUS_AUTOCOMMIT);
        return out.toByteArray();
    }

    public static byte[] buildErrPacket(int errorCode, String sqlState, String message) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(64);
        out.write(0xFF);
        writeInt2LE(out, errorCode);
        out.write('#');
        String state = (sqlState != null && sqlState.length() == 5) ? sqlState : "HY000";
        out.write(state.getBytes(StandardCharsets.US_ASCII));
        out.write((message != null ? message : "Unknown error").getBytes(StandardCharsets.UTF_8));
        return out.toByteArray();
    }

    public static byte[] buildColumnCountPacket(int count) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(4);
        writeLenEncInt(out, count);
        return out.toByteArray();
    }

    public static byte[] buildColumnDef41(String schema, String table, String columnName, int mysqlType) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(64);
        writeLenEncString(out, "def");
        writeLenEncString(out, schema != null ? schema : "stratos");
        writeLenEncString(out, table != null ? table : "");
        writeLenEncString(out, table != null ? table : "");
        writeLenEncString(out, columnName);
        writeLenEncString(out, columnName);
        out.write(0x0C); // length of fixed-length fields
        writeInt2LE(out, DEFAULT_CHARSET_UTF8MB4);
        writeInt4LE(out, 255); // column length
        out.write(mysqlType & 0xFF);
        writeInt2LE(out, 0); // flags
        out.write(0); // decimals
        writeInt2LE(out, 0); // filler
        return out.toByteArray();
    }

    public static byte[] buildTextRowPacket(List<String> values) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(128);
        for (String val : values) {
            if (val == null) {
                out.write(0xFB); // NULL
            } else {
                writeLenEncString(out, val);
            }
        }
        return out.toByteArray();
    }

    public static void writeInt2LE(ByteArrayOutputStream out, int val) {
        out.write(val & 0xFF);
        out.write((val >>> 8) & 0xFF);
    }

    public static void writeInt4LE(ByteArrayOutputStream out, int val) {
        out.write(val & 0xFF);
        out.write((val >>> 8) & 0xFF);
        out.write((val >>> 16) & 0xFF);
        out.write((val >>> 24) & 0xFF);
    }

    public static void writeLenEncInt(ByteArrayOutputStream out, long val) {
        if (val < 251) {
            out.write((int) val);
        } else if (val < 65536) {
            out.write(0xFC);
            writeInt2LE(out, (int) val);
        } else if (val < 16777216) {
            out.write(0xFD);
            out.write((int) (val & 0xFF));
            out.write((int) ((val >>> 8) & 0xFF));
            out.write((int) ((val >>> 16) & 0xFF));
        } else {
            out.write(0xFE);
            for (int i = 0; i < 8; i++) {
                out.write((int) ((val >>> (i * 8)) & 0xFF));
            }
        }
    }

    public static void writeLenEncString(ByteArrayOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeLenEncInt(out, bytes.length);
        out.write(bytes);
    }

    public static int readInt4LE(byte[] buf, int pos) {
        return (buf[pos] & 0xFF)
            | ((buf[pos + 1] & 0xFF) << 8)
            | ((buf[pos + 2] & 0xFF) << 16)
            | ((buf[pos + 3] & 0xFF) << 24);
    }
}
