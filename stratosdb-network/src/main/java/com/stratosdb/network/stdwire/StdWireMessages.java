package com.stratosdb.network.stdwire;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Encodes/decodes PostgreSQL wire protocol v3 messages - the actual byte
 * format every real Postgres client (psql, pgAdmin, JDBC/ODBC/psycopg
 * drivers, BI tools) already speaks, captured and verified against a real
 * `psql` client (see PROGRESS.md), not implemented from memory of the spec
 * alone.
 *
 * Scope, stated plainly: this implements the startup handshake (including
 * declining SSL, since psql tries SSL first by default) and the *simple*
 * query protocol (single 'Q' message in, a stream of result messages out) -
 * what every basic client uses for plain SQL statements. It does not
 * implement the *extended* query protocol (Parse/Bind/Execute, used for
 * real server-side prepared statements and binary parameter binding) - a
 * real, separate, further piece of work, not silently missing.
 *
 * Authentication is "trust" only right now (AuthenticationOk immediately,
 * no password required) - a deliberate, minimal first step matching this
 * project's own plan, which lists SCRAM-SHA-256 (the actual mechanism real
 * pg-wire clients negotiate) as a distinct, later item, not bundled in here.
 */
public final class StdWireMessages {
    private StdWireMessages() {}

    public static final int SSL_REQUEST_CODE = 80877103;
    public static final int PROTOCOL_VERSION_3 = 0x00030000;

    // --- reading from the client ---

    /** Reads a length-prefixed startup-phase packet (SSLRequest or StartupMessage) - these have no leading type byte, unlike every other message. */
    public static byte[] readUntypedPacket(DataInputStream in) throws IOException {
        int length = in.readInt();
        byte[] body = new byte[length - 4];
        in.readFully(body);
        return body;
    }

    /** Reads a normal, typed message (type byte + length + body) - everything after the startup phase. */
    public static TypedMessage readTypedMessage(DataInputStream in) throws IOException {
        int type = in.readUnsignedByte();
        int length = in.readInt();
        byte[] body = new byte[length - 4];
        in.readFully(body);
        return new TypedMessage((char) type, body);
    }

    public record TypedMessage(char type, byte[] body) {
        public String readCString(int offset) {
            int end = offset;
            while (body[end] != 0) end++;
            return new String(body, offset, end - offset, StandardCharsets.UTF_8);
        }
    }

    /** Parses a StartupMessage body (after the 4-byte length and 4-byte protocol version already consumed by the caller) into its key/value parameters. */
    public static java.util.Map<String, String> parseStartupParams(byte[] body, int offset) {
        java.util.Map<String, String> params = new java.util.LinkedHashMap<>();
        int i = offset;
        while (i < body.length && body[i] != 0) {
            int keyStart = i;
            while (body[i] != 0) i++;
            String key = new String(body, keyStart, i - keyStart, StandardCharsets.UTF_8);
            i++; // skip null
            int valStart = i;
            while (body[i] != 0) i++;
            String value = new String(body, valStart, i - valStart, StandardCharsets.UTF_8);
            i++; // skip null
            params.put(key, value);
        }
        return params;
    }

    // --- writing to the client ---

    public static void writeSslDecline(DataOutputStream out) throws IOException {
        out.writeByte('N');
        out.flush();
    }

    public static void writeAuthenticationOk(DataOutputStream out) throws IOException {
        writeMessage(out, 'R', buf -> buf.putInt(0));
    }

    public static void writeParameterStatus(DataOutputStream out, String name, String value) throws IOException {
        writeMessage(out, 'S', buf -> {
            putCString(buf, name);
            putCString(buf, value);
        });
    }

    public static void writeBackendKeyData(DataOutputStream out, int pid, int secretKey) throws IOException {
        writeMessage(out, 'K', buf -> {
            buf.putInt(pid);
            buf.putInt(secretKey);
        });
    }

    /** status: 'I' idle, 'T' in a transaction block, 'E' in a failed transaction block. */
    public static void writeReadyForQuery(DataOutputStream out, char status) throws IOException {
        writeMessage(out, 'Z', buf -> buf.put((byte) status));
    }

    public record Column(String name, int typeOid, short typeSize) {}

    public static void writeRowDescription(DataOutputStream out, java.util.List<Column> columns) throws IOException {
        writeMessage(out, 'T', buf -> {
            buf.putShort((short) columns.size());
            for (Column c : columns) {
                putCString(buf, c.name());
                buf.putInt(0);           // table OID - none, this isn't a real catalog-backed column
                buf.putShort((short) 0); // column attribute number - unknown/unused
                buf.putInt(c.typeOid());
                buf.putShort(c.typeSize());
                buf.putInt(-1);          // type modifier - none
                buf.putShort((short) 0); // format code - 0 = text, the only format this implementation sends
            }
        });
    }

    /** values: null entries become SQL NULL (-1 length, no bytes) - text format throughout, so every non-null value is just its String form's UTF-8 bytes. */
    public static void writeDataRow(DataOutputStream out, java.util.List<String> values) throws IOException {
        writeMessage(out, 'D', buf -> {
            buf.putShort((short) values.size());
            for (String v : values) {
                if (v == null) {
                    buf.putInt(-1);
                } else {
                    byte[] b = v.getBytes(StandardCharsets.UTF_8);
                    buf.putInt(b.length);
                    buf.put(b);
                }
            }
        });
    }

    public static void writeCommandComplete(DataOutputStream out, String tag) throws IOException {
        writeMessage(out, 'C', buf -> putCString(buf, tag));
    }

    public static void writeEmptyQueryResponse(DataOutputStream out) throws IOException {
        writeMessage(out, 'I', buf -> {});
    }

    /** A minimal but valid ErrorResponse - severity, SQLSTATE (always the generic "internal error" code, since this engine doesn't classify errors into real SQLSTATEs yet), and the message text. */
    public static void writeErrorResponse(DataOutputStream out, String message) throws IOException {
        writeMessage(out, 'E', buf -> {
            buf.put((byte) 'S'); putCString(buf, "ERROR");
            buf.put((byte) 'C'); putCString(buf, "XX000");
            buf.put((byte) 'M'); putCString(buf, message == null ? "unknown error" : message);
            buf.put((byte) 0);
        });
    }

    // --- shared plumbing ---

    private interface BodyWriter {
        void write(java.nio.ByteBuffer buf);
    }

    /** Every non-startup server message shares this shape: 1-byte type, 4-byte length (including itself, excluding the type byte), then the body. Bodies here are built into a generously-sized scratch buffer first since the length has to be known before it can be written. */
    private static void writeMessage(DataOutputStream out, char type, BodyWriter writer) throws IOException {
        java.nio.ByteBuffer scratch = java.nio.ByteBuffer.allocate(65536);
        writer.write(scratch);
        int bodyLen = scratch.position();

        out.writeByte(type);
        out.writeInt(bodyLen + 4);
        out.write(scratch.array(), 0, bodyLen);
    }

    private static void putCString(java.nio.ByteBuffer buf, String s) {
        buf.put(s.getBytes(StandardCharsets.UTF_8));
        buf.put((byte) 0);
    }
}
