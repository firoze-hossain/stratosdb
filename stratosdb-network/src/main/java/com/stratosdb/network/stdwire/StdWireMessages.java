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
 * Scope: the startup handshake (including declining SSL, since psql tries
 * SSL first by default), the simple query protocol (single 'Q' message in,
 * a stream of result messages out), and now the extended query protocol
 * (Parse/Bind/Describe/Execute/Sync/Close) - real server-side prepared
 * statements and portals, verified against this project's own native
 * `stdsql` client (see stratosdb-cli's StdSql.java) rather than psql, since
 * psql's own extended-protocol usage isn't easily driven from its
 * interactive prompt the way a purpose-built test client can be.
 *
 * A real, named simplification for the extended protocol: parameter
 * substitution happens by interpolating each bound value as a properly
 * quoted/escaped SQL literal into the query text before handing it to the
 * same executor every simple-query statement already goes through - not
 * via a native parameterized-query path inside ExecutorEngine itself
 * (which has no such concept). This is a real implementation of the wire
 * PROTOCOL (real Parse/Bind/Describe/Execute/Sync/Close message handling,
 * real prepared-statement and portal lifecycle, real parameter type
 * inference from $1/$2 placeholders) - not a relabeled simple query - but
 * it does not give a reusable, pre-planned query the way a real
 * server-side prepared statement in Postgres itself does. Values are still
 * escaped correctly before substitution, so this remains SQL-injection-safe
 * despite not being a native parameterized path.
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

    // --- extended query protocol: client-to-server message parsing ---

    /** Parse ('P'): a prepared-statement name (empty = unnamed), the query text (with $1/$2/... placeholders), and the parameter type OIDs the client chose to specify up front (any may be 0 = "let the server infer it", which this implementation always effectively does anyway, since it has no static type-checking phase separate from execution). */
    public record ParseMessage(String statementName, String query, int[] paramTypeOids) {}

    public static ParseMessage readParseMessage(TypedMessage msg) {
        byte[] body = msg.body();
        int pos = 0;
        String statementName = msg.readCString(pos);
        pos += statementName.length() + 1;
        String query = msg.readCString(pos);
        pos += query.length() + 1;
        int paramCount = readShortAt(body, pos);
        pos += 2;
        int[] paramTypeOids = new int[paramCount];
        for (int i = 0; i < paramCount; i++) {
            paramTypeOids[i] = readIntAt(body, pos);
            pos += 4;
        }
        return new ParseMessage(statementName, query, paramTypeOids);
    }

    /** Bind ('B'): binds a portal (destinationPortal, empty = unnamed) to a previously-Parsed statement, with concrete parameter values - paramValues entries are raw bytes as sent (null entry = SQL NULL), always interpreted as text format here (see the class javadoc: format codes are read and stored but this implementation only ever produces/consumes text). */
    /** paramFormatCodes: one entry per parameter in paramValues, resolved to its real, effective value (0=text, 1=binary) - the PG wire protocol's own three shorthand cases (zero codes sent meaning "all text", one code sent meaning "applies to every parameter", or exactly one code per parameter) are already normalized here, so a caller never needs to re-derive which case applies. */
    public record BindMessage(String portalName, String statementName, byte[][] paramValues, int[] paramFormatCodes) {}

    public static BindMessage readBindMessage(TypedMessage msg) {
        byte[] body = msg.body();
        int pos = 0;
        String portalName = msg.readCString(pos);
        pos += portalName.length() + 1;
        String statementName = msg.readCString(pos);
        pos += statementName.length() + 1;

        int paramFormatCodeCount = readShortAt(body, pos);
        pos += 2;
        int[] rawFormatCodes = new int[paramFormatCodeCount];
        for (int i = 0; i < paramFormatCodeCount; i++) {
            rawFormatCodes[i] = readShortAt(body, pos);
            pos += 2;
        }

        int paramValueCount = readShortAt(body, pos);
        pos += 2;
        byte[][] paramValues = new byte[paramValueCount][];
        for (int i = 0; i < paramValueCount; i++) {
            int len = readIntAt(body, pos);
            pos += 4;
            if (len == -1) {
                paramValues[i] = null; // SQL NULL
            } else {
                paramValues[i] = new byte[len];
                System.arraycopy(body, pos, paramValues[i], 0, len);
                pos += len;
            }
        }

        // Normalize the three real, valid shorthand cases the wire protocol itself
        // defines - see this record's own javadoc - into one format code per real
        // parameter, so formatParamAsSqlLiteral never needs to re-derive this.
        int[] paramFormatCodes = new int[paramValueCount];
        if (paramFormatCodeCount == 0) {
            // paramFormatCodes already all-zero (text) by Java's own array default.
        } else if (paramFormatCodeCount == 1) {
            java.util.Arrays.fill(paramFormatCodes, rawFormatCodes[0]);
        } else {
            System.arraycopy(rawFormatCodes, 0, paramFormatCodes, 0, Math.min(paramFormatCodeCount, paramValueCount));
        }

        // result format codes (trailing Int16 count + Int16[] codes) are intentionally
        // not parsed - this implementation always responds in text format regardless.
        return new BindMessage(portalName, statementName, paramValues, paramFormatCodes);
    }

    /** Describe ('D'): 'S' for a prepared statement (client wants ParameterDescription + RowDescription/NoData) or 'P' for a portal (client wants RowDescription/NoData only). */
    public record DescribeMessage(char targetType, String name) {}

    public static DescribeMessage readDescribeMessage(TypedMessage msg) {
        char targetType = (char) msg.body()[0];
        String name = msg.readCString(1);
        return new DescribeMessage(targetType, name);
    }

    /** Execute ('E'): runs a previously-Bound portal. maxRows (0 = unlimited) is read but not honored - see class javadoc: this engine has no partial/cursor-based execution to limit against. */
    public record ExecuteMessage(String portalName, int maxRows) {}

    public static ExecuteMessage readExecuteMessage(TypedMessage msg) {
        String portalName = msg.readCString(0);
        int maxRows = readIntAt(msg.body(), portalName.length() + 1);
        return new ExecuteMessage(portalName, maxRows);
    }

    /** Close ('C'): 'S' or 'P', same shape as Describe. */
    public record CloseMessage(char targetType, String name) {}

    public static CloseMessage readCloseMessage(TypedMessage msg) {
        char targetType = (char) msg.body()[0];
        String name = msg.readCString(1);
        return new CloseMessage(targetType, name);
    }

    private static int readShortAt(byte[] body, int offset) {
        return ((body[offset] & 0xFF) << 8) | (body[offset + 1] & 0xFF);
    }

    private static int readIntAt(byte[] body, int offset) {
        return ((body[offset] & 0xFF) << 24) | ((body[offset + 1] & 0xFF) << 16)
            | ((body[offset + 2] & 0xFF) << 8) | (body[offset + 3] & 0xFF);
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

    // --- SCRAM-SHA-256 authentication (AuthenticationSASL family) ---

    /** AuthenticationSASL (auth code 10): lists the mechanism names the server supports, each null-terminated, ending with one additional empty string - a real client picks one and responds with SASLInitialResponse. */
    public static void writeAuthenticationSasl(DataOutputStream out, String... mechanisms) throws IOException {
        writeMessage(out, 'R', buf -> {
            buf.putInt(10);
            for (String mechanism : mechanisms) {
                putCString(buf, mechanism);
            }
            buf.put((byte) 0); // final empty string terminates the mechanism list
        });
    }

    /** SASLInitialResponse ('p'): the client's chosen mechanism name plus its initial response data (client-first-message for SCRAM) - the ONE 'p' message shaped differently from a plain PasswordMessage, since it's prefixed with the mechanism name and an explicit data length rather than being just a bare, null-terminated string. */
    public record SaslInitialResponse(String mechanism, String initialResponseData) {}

    public static SaslInitialResponse readSaslInitialResponse(TypedMessage msg) {
        byte[] body = msg.body();
        String mechanism = msg.readCString(0);
        int pos = mechanism.length() + 1;
        int dataLen = readIntAt(body, pos);
        pos += 4;
        String data = dataLen < 0 ? "" : new String(body, pos, dataLen, StandardCharsets.UTF_8);
        return new SaslInitialResponse(mechanism, data);
    }

    /** AuthenticationSASLContinue (auth code 11): carries the server-first-message - unlike AuthenticationSASL's list of C-strings, this is raw bytes filling the rest of the message (no length prefix or terminator of its own; the outer message length IS the boundary). */
    public static void writeAuthenticationSaslContinue(DataOutputStream out, String data) throws IOException {
        writeMessage(out, 'R', buf -> {
            buf.putInt(11);
            buf.put(data.getBytes(StandardCharsets.UTF_8));
        });
    }

    /** SASLResponse ('p'): raw SASL data (client-final-message for SCRAM) filling the whole message body - no mechanism name or length prefix this time, unlike SASLInitialResponse, since the server already knows which mechanism and handshake step this belongs to from context. */
    public static String readSaslResponse(TypedMessage msg) {
        return new String(msg.body(), StandardCharsets.UTF_8);
    }

    /** AuthenticationSASLFinal (auth code 12): carries the server-final-message - sent once verification succeeds, immediately followed by a normal AuthenticationOk. */
    public static void writeAuthenticationSaslFinal(DataOutputStream out, String data) throws IOException {
        writeMessage(out, 'R', buf -> {
            buf.putInt(12);
            buf.put(data.getBytes(StandardCharsets.UTF_8));
        });
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

    /**
     * CopyInResponse/CopyOutResponse - sent once, right after a COPY
     * statement is recognized, before any CopyData at all: tells the
     * client "switch into COPY sub-protocol mode now." format 0 = text
     * (covers both this engine's own TEXT and CSV COPY formats - real
     * Postgres uses the same format byte for both, distinguishing them
     * only via the earlier SQL text itself, not a different wire
     * format code); this engine has no BINARY COPY format, so format 1
     * is never sent. numColumns/columnFormats are both required fields
     * of the real message even though this engine doesn't use
     * per-column binary/text formatting - columnFormats is always all
     * zeros (text), matching format itself.
     */
    public static void writeCopyInResponse(DataOutputStream out, int columnCount) throws IOException {
        writeMessage(out, 'G', buf -> {
            buf.put((byte) 0);
            buf.putShort((short) columnCount);
            for (int i = 0; i < columnCount; i++) {
                buf.putShort((short) 0);
            }
        });
    }

    public static void writeCopyOutResponse(DataOutputStream out, int columnCount) throws IOException {
        writeMessage(out, 'H', buf -> {
            buf.put((byte) 0);
            buf.putShort((short) columnCount);
            for (int i = 0; i < columnCount; i++) {
                buf.putShort((short) 0);
            }
        });
    }

    /** One CopyData message per line of COPY output - not the only valid chunking real Postgres allows (a CopyData message can hold any byte range, not necessarily whole lines), but simple, correct, and what every real client already handles regardless of how the server chooses to chunk it. */
    public static void writeCopyData(DataOutputStream out, String line) throws IOException {
        byte[] bytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
        writeMessage(out, 'd', buf -> buf.put(bytes));
    }

    /** Sent by the server once, after every row for a COPY TO STDOUT - or by the client, once, after every line for a COPY FROM STDIN (this engine writes the client-bound version; the client-bound version arriving on a real connection is read as an ordinary TypedMessage of type 'c', not through this method). */
    public static void writeCopyDone(DataOutputStream out) throws IOException {
        writeMessage(out, 'c', buf -> {});
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

    // --- extended query protocol: server-to-client responses ---

    public static void writeParseComplete(DataOutputStream out) throws IOException {
        writeMessage(out, '1', buf -> {});
    }

    public static void writeBindComplete(DataOutputStream out) throws IOException {
        writeMessage(out, '2', buf -> {});
    }

    public static void writeCloseComplete(DataOutputStream out) throws IOException {
        writeMessage(out, '3', buf -> {});
    }

    /** typeOids.size() must match the statement's actual parameter count - sent in response to Describe('S', ...), always ahead of that same statement's RowDescription/NoData. */
    public static void writeParameterDescription(DataOutputStream out, java.util.List<Integer> typeOids) throws IOException {
        writeMessage(out, 't', buf -> {
            buf.putShort((short) typeOids.size());
            for (int oid : typeOids) {
                buf.putInt(oid);
            }
        });
    }

    /** Sent instead of RowDescription when a statement/portal produces no result columns at all (e.g. INSERT/UPDATE/DELETE/DDL) - a real, separate message type from an empty RowDescription (zero columns would be a different, wrong signal: "this returns rows, there just happen to be none described"). */
    public static void writeNoData(DataOutputStream out) throws IOException {
        writeMessage(out, 'n', buf -> {});
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

    /**
     * Writes an already-read TypedMessage's own exact bytes back out
     * verbatim - the real, generic passthrough primitive StratosPooler
     * needs to forward a message between a client and a real backend
     * connection without needing to understand that message's own
     * specific type/format at all (RowDescription, DataRow, CopyData,
     * whatever it is - a pooler proxying at the message-boundary level
     * doesn't need to parse SQL results, only recognize message
     * boundaries and, for ReadyForQuery specifically, its own single
     * transaction-status byte - see StratosPooler's own javadoc).
     */
    public static void writeRawMessage(DataOutputStream out, TypedMessage msg) throws IOException {
        out.writeByte(msg.type());
        out.writeInt(msg.body().length + 4);
        out.write(msg.body());
    }

    private static void putCString(java.nio.ByteBuffer buf, String s) {
        buf.put(s.getBytes(StandardCharsets.UTF_8));
        buf.put((byte) 0);
    }

    // --- client-side message writers (used by stdsql, a real native stdwire client) ---

    /** The StartupMessage: untyped (no leading type byte, like SSLRequest), just protocol version + key/value params, ending with a final empty string. */
    public static void writeStartupMessage(DataOutputStream out, String user, String database) throws IOException {
        java.nio.ByteBuffer scratch = java.nio.ByteBuffer.allocate(65536);
        scratch.putInt(PROTOCOL_VERSION_3);
        putCString(scratch, "user");
        putCString(scratch, user);
        if (database != null) {
            putCString(scratch, "database");
            putCString(scratch, database);
        }
        scratch.put((byte) 0); // terminating empty string
        int bodyLen = scratch.position();
        out.writeInt(bodyLen + 4);
        out.write(scratch.array(), 0, bodyLen);
        out.flush();
    }

    /** Simple query protocol: one 'Q' message containing the whole SQL text. */
    public static void writeQuery(DataOutputStream out, String sql) throws IOException {
        writeMessage(out, 'Q', buf -> putCString(buf, sql));
        out.flush();
    }

    public static void writeTerminate(DataOutputStream out) throws IOException {
        writeMessage(out, 'X', buf -> {});
        out.flush();
    }

    /** Parse: statementName empty = unnamed. paramTypeOids may be empty - 0 means "let the server infer it," which this project's own server always does anyway. */
    public static void writeParse(DataOutputStream out, String statementName, String query, int[] paramTypeOids) throws IOException {
        writeMessage(out, 'P', buf -> {
            putCString(buf, statementName);
            putCString(buf, query);
            buf.putShort((short) paramTypeOids.length);
            for (int oid : paramTypeOids) {
                buf.putInt(oid);
            }
        });
    }

    /** Bind: always text format for both parameters and results (this project's own server, and this client, never produce or consume binary format) - paramValues entries are the parameter's literal text, UTF-8 encoded; a null entry means SQL NULL. */
    public static void writeBind(DataOutputStream out, String portalName, String statementName, String[] paramValues) throws IOException {
        writeMessage(out, 'B', buf -> {
            putCString(buf, portalName);
            putCString(buf, statementName);
            buf.putShort((short) 0); // 0 parameter format codes = every parameter is text format, the protocol's own default
            buf.putShort((short) paramValues.length);
            for (String v : paramValues) {
                if (v == null) {
                    buf.putInt(-1);
                } else {
                    byte[] b = v.getBytes(StandardCharsets.UTF_8);
                    buf.putInt(b.length);
                    buf.put(b);
                }
            }
            buf.putShort((short) 0); // 0 result format codes = text format for every result column
        });
    }

    public static void writeDescribe(DataOutputStream out, char targetType, String name) throws IOException {
        writeMessage(out, 'D', buf -> {
            buf.put((byte) targetType);
            putCString(buf, name);
        });
    }

    public static void writeExecute(DataOutputStream out, String portalName, int maxRows) throws IOException {
        writeMessage(out, 'E', buf -> {
            putCString(buf, portalName);
            buf.putInt(maxRows);
        });
    }

    public static void writeClose(DataOutputStream out, char targetType, String name) throws IOException {
        writeMessage(out, 'C', buf -> {
            buf.put((byte) targetType);
            putCString(buf, name);
        });
    }

    public static void writeSync(DataOutputStream out) throws IOException {
        writeMessage(out, 'S', buf -> {});
        out.flush();
    }
}
