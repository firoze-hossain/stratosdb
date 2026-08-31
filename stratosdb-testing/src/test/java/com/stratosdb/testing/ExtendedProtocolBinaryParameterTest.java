package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireMessages;
import com.stratosdb.network.stdwire.StdWireServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, end-to-end proof that a real, binary-format bound parameter (the
 * exact wire encoding the real, official org.postgresql JDBC driver sends
 * for setInt/setLong/setBoolean from its very first execution - found via
 * direct diagnostic capture during a real, broad driver/ORM verification
 * pass, not assumed from documentation) is decoded correctly rather than
 * silently corrupted. This is the single most serious bug that pass
 * found: before the fix, a binary parameter's own raw bytes were read as
 * if they were UTF-8 text and stored as a garbled string - genuine,
 * silent data corruption in the actual stored row, not just a display
 * glitch. writeBind's own shared helper only ever sends text-format
 * parameters (see its own comment), so this test builds a real, raw Bind
 * message by hand instead, exactly matching the real wire bytes a real
 * client sends.
 */
public class ExtendedProtocolBinaryParameterTest {

    private StratosDB db;
    private StdWireServer server;

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void binaryInt4ParameterIsDecodedCorrectlyNotGarbled(@TempDir Path tempDir) throws Exception {
        int port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        server = new StdWireServer(port, db);
        server.start();
        Thread.sleep(200);

        db.execute("CREATE TABLE t (id INT, name VARCHAR)");

        try (Socket socket = new Socket("localhost", port)) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            StdWireMessages.writeStartupMessage(out, "anyuser", "anydb");
            out.flush();
            while (StdWireMessages.readTypedMessage(in).type() != 'Z') { /* drain startup */ }

            StdWireMessages.writeParse(out, "", "insert into t (id, name) values ($1, $2)", new int[0]);

            // A real, raw Bind message, built by hand: parameter 1 (id) is real,
            // genuine BINARY format (format code 1) carrying the real 4-byte,
            // big-endian encoding of the integer 42 - exactly what
            // PreparedStatement.setInt(1, 42) sends over the real wire protocol,
            // not a simulation of it. Parameter 2 (name) stays real TEXT format,
            // confirming the fix doesn't disturb the pre-existing, already-working
            // text path when the two are mixed in the same statement.
            byte[] binaryId = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(42).array();
            byte[] textName = "Alice".getBytes(StandardCharsets.UTF_8);
            writeRawBindWithMixedFormats(out, "", "", new byte[][]{binaryId, textName}, new int[]{1, 0});

            StdWireMessages.writeDescribe(out, 'P', "");
            StdWireMessages.writeExecute(out, "", 0);
            StdWireMessages.writeSync(out);
            out.flush();

            String errorMessage = null;
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                if (msg.type() == 'E') {
                    errorMessage = extractErrorMessage(msg);
                } else if (msg.type() == 'Z') {
                    break;
                }
            }
            String finalErrorMessage = errorMessage;
            assertNull(errorMessage, () -> "the real, binary-encoded parameter must be accepted, not rejected: " + finalErrorMessage);
        }

        // The real, decisive check: not just "no error was thrown", but that the
        // real, stored value is genuinely 42 - a real int - not a garbled string
        // built from the raw, misinterpreted binary bytes.
        var result = db.execute("SELECT * FROM t");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size());
        Object storedId = result.getRows().get(0).getValue("id");
        assertEquals(42, storedId, () -> "the real, stored id must be the genuine integer 42, not a garbled binary-as-text string; got: " + storedId + " (" + (storedId == null ? "null" : storedId.getClass()) + ")");
        assertEquals("Alice", result.getRows().get(0).getValue("name"));
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static String extractErrorMessage(StdWireMessages.TypedMessage msg) {
        byte[] b = msg.body();
        int pos = 0;
        while (pos < b.length && b[pos] != 0) {
            char field = (char) b[pos]; pos++;
            int start = pos;
            while (b[pos] != 0) pos++;
            String value = new String(b, start, pos - start, StandardCharsets.UTF_8);
            pos++;
            if (field == 'M') return value;
        }
        return "unknown";
    }

    /** A real, raw Bind ('B') message, hand-built to send a genuine, per-parameter mix of binary (1) and text (0) format codes - the exact real shape writeBind's own shared helper (always all-text) cannot produce. */
    private static void writeRawBindWithMixedFormats(DataOutputStream out, String portalName, String statementName, byte[][] paramValues, int[] formatCodes) throws java.io.IOException {
        java.io.ByteArrayOutputStream bodyBytes = new java.io.ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bodyBytes);
        writeCString(body, portalName);
        writeCString(body, statementName);
        body.writeShort(formatCodes.length);
        for (int code : formatCodes) {
            body.writeShort(code);
        }
        body.writeShort(paramValues.length);
        for (byte[] v : paramValues) {
            if (v == null) {
                body.writeInt(-1);
            } else {
                body.writeInt(v.length);
                body.write(v);
            }
        }
        body.writeShort(0); // 0 result format codes = text format for every result column
        byte[] built = bodyBytes.toByteArray();
        out.writeByte('B');
        out.writeInt(built.length + 4);
        out.write(built);
    }

    private static void writeCString(DataOutputStream out, String s) throws java.io.IOException {
        out.write(s.getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);
    }
}
