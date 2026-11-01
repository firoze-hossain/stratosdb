package com.stratosdb.network.stdwire;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class StdBinaryParameterDecodingTest {

    @TempDir
    Path tempDir;

    private int port;
    private StratosDB db;
    private StdWireServer server;

    @BeforeEach
    void setUp() throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        server = new StdWireServer(port, db);
        server.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    @Test
    public void testBinaryParametersAllTypes() throws Exception {
        try (RawConn conn = new RawConn(port)) {
            QueryOutcome createRes = conn.query("CREATE TABLE all_types (id INT, s SMALLINT, b BIGINT, r REAL, d DOUBLE PRECISION, flag BOOLEAN, bin BYTEA, ts TIMESTAMP, txt TEXT);");
            assertNull(createRes.error, "CREATE TABLE failed: " + createRes.error);

            // Encode binary parameters
            byte[] idBytes = StdWireMessages.encodeInt4(101);
            byte[] sBytes = StdWireMessages.encodeInt2((short) 42);
            byte[] bBytes = StdWireMessages.encodeInt8(9876543210L);
            byte[] rBytes = StdWireMessages.encodeFloat4(3.14f);
            byte[] dBytes = StdWireMessages.encodeFloat8(2.71828);
            byte[] flagBytes = new byte[]{1}; // true
            byte[] binBytes = StdWireMessages.encodeBytea(new byte[]{(byte) 0xAA, (byte) 0xBB, (byte) 0xCC, (byte) 0xDD});
            Instant testInstant = Instant.parse("2026-11-01T15:30:00Z");
            byte[] tsBytes = StdWireMessages.encodeTimestamp(testInstant);
            byte[] txtBytes = StdWireMessages.encodeText("Binary Wire Protocol 2.0");

            byte[][] paramValues = new byte[][]{
                idBytes, sBytes, bBytes, rBytes, dBytes, flagBytes, binBytes, tsBytes, txtBytes
            };
            int[] formatCodes = new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1}; // all binary

            QueryOutcome insertOutcome = conn.extendedQueryBinary(
                "INSERT INTO all_types VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                paramValues,
                formatCodes
            );
            assertNull(insertOutcome.error, "Insert failed: " + insertOutcome.error);
            assertEquals("INSERT 0 1", insertOutcome.commandTag);

            // Now query with binary parameter WHERE id = $1
            QueryOutcome selectOutcome = conn.extendedQueryBinary(
                "SELECT id, s, b, bin, txt FROM all_types WHERE id = $1",
                new byte[][]{idBytes},
                new int[]{1}
            );
            assertNull(selectOutcome.error, "Select failed: " + selectOutcome.error);
            assertEquals(1, selectOutcome.rows.size());
            List<String> row = selectOutcome.rows.get(0);
            assertEquals("101", row.get(0));
            assertEquals("42", row.get(1));
            assertEquals("9876543210", row.get(2));
            assertEquals("\\xaabbccdd", row.get(3).toLowerCase());
            assertEquals("Binary Wire Protocol 2.0", row.get(4));
        }
    }

    @Test
    public void testSqlInjectionImmunity() throws Exception {
        try (RawConn conn = new RawConn(port)) {
            conn.query("CREATE TABLE accounts (id INT, username TEXT, balance INT);");
            conn.query("INSERT INTO accounts VALUES (1, 'alice', 1000);");
            conn.query("INSERT INTO accounts VALUES (2, 'bob', 500);");

            // Attempt injection: parameter containing SQL injection payload
            String injectionPayload = "' OR '1'='1";
            byte[] payloadBytes = StdWireMessages.encodeText(injectionPayload);

            // Extended query with injection payload
            QueryOutcome outcome = conn.extendedQueryBinary(
                "SELECT * FROM accounts WHERE username = $1",
                new byte[][]{payloadBytes},
                new int[]{0} // text format parameter
            );
            assertNull(outcome.error);
            // Should return ZERO rows because no account has the literal username "' OR '1'='1"
            assertEquals(0, outcome.rows.size(), "SQL injection succeeded: returned rows when it shouldn't have!");

            // Attempt destructive injection payload: "bob'; DROP TABLE accounts; --"
            String destructivePayload = "bob'; DROP TABLE accounts; --";
            QueryOutcome updateOutcome = conn.extendedQueryBinary(
                "UPDATE accounts SET balance = 0 WHERE username = $1",
                new byte[][]{StdWireMessages.encodeText(destructivePayload)},
                new int[]{0}
            );
            assertNull(updateOutcome.error);

            // Verify table still exists and balances were NOT wiped out
            QueryOutcome verifyOutcome = conn.query("SELECT COUNT(*) FROM accounts;");
            assertNull(verifyOutcome.error);
            assertEquals("2", verifyOutcome.rows.get(0).get(0));

            QueryOutcome aliceOutcome = conn.query("SELECT balance FROM accounts WHERE username = 'alice';");
            assertEquals("1000", aliceOutcome.rows.get(0).get(0));
        }
    }

    @Test
    public void testBinaryParameterizedUpdateAndDelete() throws Exception {
        try (RawConn conn = new RawConn(port)) {
            conn.query("CREATE TABLE items (id INT, name TEXT, qty INT);");
            conn.query("INSERT INTO items VALUES (1, 'Widget', 10);");
            conn.query("INSERT INTO items VALUES (2, 'Gadget', 20);");

            // Binary UPDATE: update qty for item 1
            byte[] newQty = StdWireMessages.encodeInt4(99);
            byte[] targetId = StdWireMessages.encodeInt4(1);
            QueryOutcome updateRes = conn.extendedQueryBinary(
                "UPDATE items SET qty = $1 WHERE id = $2",
                new byte[][]{newQty, targetId},
                new int[]{1, 1}
            );
            assertNull(updateRes.error);

            QueryOutcome checkUpd = conn.query("SELECT qty FROM items WHERE id = 1;");
            assertEquals("99", checkUpd.rows.get(0).get(0));

            // Binary DELETE: delete item 2
            byte[] deleteId = StdWireMessages.encodeInt4(2);
            QueryOutcome delRes = conn.extendedQueryBinary(
                "DELETE FROM items WHERE id = $1",
                new byte[][]{deleteId},
                new int[]{1}
            );
            assertNull(delRes.error);

            QueryOutcome checkDel = conn.query("SELECT COUNT(*) FROM items;");
            assertEquals("1", checkDel.rows.get(0).get(0));
        }
    }

    @Test
    public void testDescribeStatementInferredOids() throws Exception {
        try (RawConn conn = new RawConn(port)) {
            QueryOutcome createRes = conn.query("CREATE TABLE sensors (id INT, reading DOUBLE PRECISION, raw_data BYTEA);");
            assertNull(createRes.error, "CREATE TABLE failed: " + createRes.error);

            // Parse statement with 3 placeholders and 0 specified type OIDs
            conn.sendParse("stmt1", "SELECT * FROM sensors WHERE id = $1 AND reading > $2 AND raw_data = $3", new int[0]);

            // Describe statement 'S'
            List<Integer> oids = conn.describeStatement("stmt1");
            assertEquals(3, oids.size());
            assertEquals(StdBinaryDecoder.OID_INT4, oids.get(0));     // id is INT -> 23
            assertEquals(StdBinaryDecoder.OID_FLOAT8, oids.get(1));   // reading is DOUBLE PRECISION -> 701
            assertEquals(StdBinaryDecoder.OID_BYTEA, oids.get(2));    // raw_data is BYTEA -> 17
        }
    }

    // --- Raw test connection helper supporting binary standard wire extended protocol ---

    private static final class QueryOutcome {
        final List<String> columnNames;
        final List<List<String>> rows;
        final String commandTag;
        final String error;

        QueryOutcome(List<String> columnNames, List<List<String>> rows, String commandTag, String error) {
            this.columnNames = columnNames;
            this.rows = rows;
            this.commandTag = commandTag;
            this.error = error;
        }
    }

    private static final class RawConn implements AutoCloseable {
        private final Socket socket;
        private final DataOutputStream out;
        private final DataInputStream in;

        RawConn(int port) throws IOException {
            socket = new Socket("localhost", port);
            socket.setSoTimeout(8000);
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            StdWireMessages.writeStartupMessage(out, "testuser", "testdb");
            readUntilReady();
        }

        private void readUntilReady() throws IOException {
            while (true) {
                int type = in.readUnsignedByte();
                int len = in.readInt();
                byte[] body = new byte[len - 4];
                in.readFully(body);
                if (type == 'Z') break;
            }
        }

        QueryOutcome query(String sql) throws IOException {
            StdWireMessages.writeQuery(out, sql);
            return readQueryOutcome();
        }

        QueryOutcome extendedQueryBinary(String sql, byte[][] paramValues, int[] formatCodes) throws IOException {
            StdWireMessages.writeParse(out, "", sql, new int[0]);
            StdWireMessages.writeBind(out, "", "", paramValues, formatCodes);
            StdWireMessages.writeDescribe(out, 'P', "");
            StdWireMessages.writeExecute(out, "", 0);
            StdWireMessages.writeSync(out);

            return readQueryOutcome();
        }

        void sendParse(String statementName, String sql, int[] typeOids) throws IOException {
            StdWireMessages.writeParse(out, statementName, sql, typeOids);
            StdWireMessages.writeSync(out);
            readUntilReady();
        }

        List<Integer> describeStatement(String statementName) throws IOException {
            StdWireMessages.writeDescribe(out, 'S', statementName);
            StdWireMessages.writeSync(out);

            List<Integer> paramOids = new ArrayList<>();
            while (true) {
                int type = in.readUnsignedByte();
                int len = in.readInt();
                byte[] body = new byte[len - 4];
                in.readFully(body);

                if (type == 't') { // ParameterDescription
                    ByteBuffer buf = ByteBuffer.wrap(body);
                    int count = buf.getShort();
                    for (int i = 0; i < count; i++) {
                        paramOids.add(buf.getInt());
                    }
                }
                if (type == 'Z') break;
            }
            return paramOids;
        }

        private QueryOutcome readQueryOutcome() throws IOException {
            List<String> columnNames = new ArrayList<>();
            List<List<String>> rows = new ArrayList<>();
            String commandTag = null;
            String error = null;

            while (true) {
                int type = in.readUnsignedByte();
                int len = in.readInt();
                byte[] body = new byte[len - 4];
                in.readFully(body);

                if (type == 'Z') break;
                if (type == 'T') columnNames.addAll(parseRowDescription(body));
                if (type == 'D') rows.add(parseDataRow(body));
                if (type == 'C') commandTag = new String(body, 0, body.length - 1, StandardCharsets.UTF_8);
                if (type == 'E') error = extractErrorMessage(body);
            }
            return new QueryOutcome(columnNames, rows, commandTag, error);
        }

        private static List<String> parseRowDescription(byte[] body) {
            List<String> cols = new ArrayList<>();
            ByteBuffer buf = ByteBuffer.wrap(body);
            int count = buf.getShort();
            for (int i = 0; i < count; i++) {
                ByteArrayOutputStream nameBytes = new ByteArrayOutputStream();
                byte b;
                while ((b = buf.get()) != 0) {
                    nameBytes.write(b);
                }
                cols.add(new String(nameBytes.toByteArray(), StandardCharsets.UTF_8));
                buf.getInt();   // table oid
                buf.getShort(); // col attr
                buf.getInt();   // type oid
                buf.getShort(); // type size
                buf.getInt();   // type mod
                buf.getShort(); // format code
            }
            return cols;
        }

        private static List<String> parseDataRow(byte[] body) {
            List<String> vals = new ArrayList<>();
            ByteBuffer buf = ByteBuffer.wrap(body);
            int count = buf.getShort();
            for (int i = 0; i < count; i++) {
                int colLen = buf.getInt();
                if (colLen == -1) {
                    vals.add(null);
                } else {
                    byte[] b = new byte[colLen];
                    buf.get(b);
                    vals.add(new String(b, StandardCharsets.UTF_8));
                }
            }
            return vals;
        }

        private static String extractErrorMessage(byte[] body) {
            int pos = 0;
            while (pos < body.length) {
                byte fieldType = body[pos++];
                if (fieldType == 0) break;
                ByteArrayOutputStream str = new ByteArrayOutputStream();
                while (pos < body.length && body[pos] != 0) {
                    str.write(body[pos++]);
                }
                pos++; // skip null
                if (fieldType == 'M') {
                    return new String(str.toByteArray(), StandardCharsets.UTF_8);
                }
            }
            return "unknown error";
        }

        @Override
        public void close() throws IOException {
            try {
                StdWireMessages.writeTerminate(out);
            } catch (Exception ignored) {}
            socket.close();
        }
    }
}
