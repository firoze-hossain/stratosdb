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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, end-to-end proof that row-level security actually restricts what
 * different, real, separately-authenticated (trust-auth) users can see
 * and write over a real connection - this project's own previously
 * entirely-missing gap. A real, raw wire-protocol client is used
 * (trust-auth, a different real username per connection - the exact
 * same real setup already proven manually) rather than this project's
 * own JDBC driver, since RLS's own real behavior depends only on which
 * real, authenticated username set session.get().currentUser (see
 * ExecutorEngine's own setCurrentUser javadoc) - trust-auth already sets
 * this correctly per real connection, with no need for a real password
 * or SCRAM handshake to prove RLS's own real row-filtering behavior.
 */
public class RowLevelSecurityEndToEndTest {

    private StratosDB db;
    private StdWireServer server;
    private int port;

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    private void startServer(Path tempDir) throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        server = new StdWireServer(port, db);
        server.start();
        Thread.sleep(200);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void policyRestrictsRowsToTheirOwnRealOwnerOverARealConnection(@TempDir Path tempDir) throws Exception {
        startServer(tempDir);

        try (RawClient admin = new RawClient("admin")) {
            assertNull(admin.query("CREATE ROLE admin WITH LOGIN"));
            assertNull(admin.query("CREATE ROLE alice WITH LOGIN"));
            assertNull(admin.query("CREATE ROLE bob WITH LOGIN"));
            assertNull(admin.query("CREATE TABLE documents (id INT, owner VARCHAR, content VARCHAR)"));
            assertNull(admin.query("ALTER TABLE documents ENABLE ROW LEVEL SECURITY"));
            assertNull(admin.query("CREATE POLICY owner_only ON documents FOR ALL USING (owner = current_user()) WITH CHECK (owner = current_user())"));
            assertNull(admin.query("INSERT INTO documents VALUES (1, 'alice', 'Alice secret doc')"));
            assertNull(admin.query("INSERT INTO documents VALUES (2, 'bob', 'Bob secret doc')"));
            assertNull(admin.query("INSERT INTO documents VALUES (3, 'alice', 'Another alice doc')"));
            assertNull(admin.query("GRANT SELECT, INSERT, UPDATE, DELETE ON documents TO alice"));
            assertNull(admin.query("GRANT SELECT, INSERT, UPDATE, DELETE ON documents TO bob"));

            // The real table owner bypasses RLS by default and sees every real row.
            QueryOutcome all = admin.select("SELECT id FROM documents");
            assertEquals(Set.of("1", "2", "3"), idColumn(all));
        }

        try (RawClient alice = new RawClient("alice")) {
            // alice's own real policy match: only her own two rows are visible.
            assertEquals(Set.of("1", "3"), idColumn(alice.select("SELECT id FROM documents")));

            // A real, legitimate insert of her own row succeeds.
            assertNull(alice.query("INSERT INTO documents VALUES (4, 'alice', 'Alice new doc')"));
            assertEquals(Set.of("1", "3", "4"), idColumn(alice.select("SELECT id FROM documents")));

            // A real, adversarial attempt to insert a row claiming to be bob's own
            // must be genuinely rejected by WITH CHECK, not silently allowed.
            String insertError = alice.query("INSERT INTO documents VALUES (5, 'bob', 'Alice impersonating bob')");
            assertNotNull(insertError, "an INSERT violating row-level security must fail, not silently succeed");
            assertTrue(insertError.toLowerCase().contains("row-level security"),
                () -> "expected a real row-level security error: " + insertError);

            // A real, adversarial attempt to modify bob's own row must silently
            // affect zero rows - the row is genuinely invisible to alice's own
            // UPDATE, not merely permission-denied.
            String updateError = alice.query("UPDATE documents SET content = 'hacked' WHERE id = 2");
            assertNull(updateError, "the UPDATE statement itself must succeed - it just affects zero rows");
        }

        try (RawClient bob = new RawClient("bob")) {
            // bob's own real, symmetric isolation - and his row must be untouched
            // by alice's own, correctly-blocked UPDATE attempt above.
            QueryOutcome bobRows = bob.select("SELECT id, content FROM documents");
            assertEquals(Set.of("2"), idColumn(bobRows));
            assertEquals("Bob secret doc", bobRows.rows.get(0).get(bobRows.columnIndex("content")),
                "bob's own row must be untouched by alice's own blocked attack");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void forceRowLevelSecurityBindsTheOwnerTooAndDefaultDenyWithNoPolicy(@TempDir Path tempDir) throws Exception {
        startServer(tempDir);

        try (RawClient admin = new RawClient("admin")) {
            assertNull(admin.query("CREATE ROLE admin WITH LOGIN"));
            assertNull(admin.query("CREATE TABLE secrets (id INT, owner VARCHAR)"));
            assertNull(admin.query("ALTER TABLE secrets ENABLE ROW LEVEL SECURITY"));
            assertNull(admin.query("CREATE POLICY owner_only ON secrets FOR ALL USING (owner = current_user()) WITH CHECK (owner = current_user())"));
            assertNull(admin.query("INSERT INTO secrets VALUES (1, 'someone_else')"));

            // Merely ENABLEing RLS still exempts the real table owner by default.
            assertEquals(Set.of("1"), idColumn(admin.select("SELECT id FROM secrets")));

            // FORCE genuinely binds the owner to the same real policy everyone
            // else is subject to - admin's own current_user() ('admin') doesn't
            // match the row's own owner ('someone_else'), so it must vanish too.
            assertNull(admin.query("ALTER TABLE secrets FORCE ROW LEVEL SECURITY"));
            assertEquals(Set.of(), idColumn(admin.select("SELECT id FROM secrets")));

            // Real default-deny: RLS enabled (and now forced), but with the one
            // and only policy dropped, genuinely zero rows are visible at all -
            // not an error, and not silently showing every row either.
            assertNull(admin.query("DROP POLICY owner_only ON secrets"));
            assertEquals(Set.of(), idColumn(admin.select("SELECT id FROM secrets")));

            // DISABLE restores full, real visibility regardless of policies.
            assertNull(admin.query("ALTER TABLE secrets DISABLE ROW LEVEL SECURITY"));
            assertEquals(Set.of("1"), idColumn(admin.select("SELECT id FROM secrets")));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void rlsStateAndPoliciesSurviveARealRestart(@TempDir Path tempDir) throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        server = new StdWireServer(port, db);
        server.start();
        Thread.sleep(200);

        try (RawClient admin = new RawClient("admin")) {
            assertNull(admin.query("CREATE ROLE admin WITH LOGIN"));
            assertNull(admin.query("CREATE ROLE alice WITH LOGIN"));
            assertNull(admin.query("CREATE TABLE notes (id INT, owner VARCHAR)"));
            assertNull(admin.query("ALTER TABLE notes ENABLE ROW LEVEL SECURITY"));
            assertNull(admin.query("CREATE POLICY owner_only ON notes FOR ALL USING (owner = current_user()) WITH CHECK (owner = current_user())"));
            assertNull(admin.query("INSERT INTO notes VALUES (1, 'alice')"));
            assertNull(admin.query("GRANT SELECT ON notes TO alice"));
            // FORCE only after the data already exists - the real table owner
            // still bypasses RLS for this insert, matching real Postgres's own
            // real "merely ENABLE" behavior; FORCE is what this test is actually
            // trying to verify survives a restart, applied last.
            assertNull(admin.query("ALTER TABLE notes FORCE ROW LEVEL SECURITY"));
        }

        server.stop();
        db.shutdown();
        db = new StratosDB(config); // fresh instance, same directory - a real restart
        server = new StdWireServer(port, db);
        server.start();
        Thread.sleep(200);

        try (RawClient admin = new RawClient("admin")) {
            // FORCE survived: admin's own current_user() doesn't match owner='alice'.
            assertEquals(Set.of(), idColumn(admin.select("SELECT id FROM notes")));
        }
        try (RawClient alice = new RawClient("alice")) {
            // The real policy itself survived, not just a blanket deny.
            assertEquals(Set.of("1"), idColumn(alice.select("SELECT id FROM notes")));
        }
    }

    private static Set<String> idColumn(QueryOutcome outcome) {
        // A real, zero-row result has no way to know its own column names at
        // all in this engine's own simple query protocol (see
        // StdWireServer.describeColumns' own javadoc: it can only introspect
        // an actual returned row, and there isn't one) - a real, pre-existing,
        // already-documented limitation unrelated to row-level security
        // itself, so a genuinely empty result is handled directly here rather
        // than needing a real column index lookup that has nothing to find.
        if (outcome.rows.isEmpty()) {
            return Set.of();
        }
        int idIdx = outcome.columnIndex("id");
        Set<String> ids = new TreeSet<>();
        for (List<String> row : outcome.rows) {
            ids.add(row.get(idIdx));
        }
        return ids;
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private record QueryOutcome(List<String> columnNames, List<List<String>> rows) {
        int columnIndex(String name) {
            int idx = columnNames.indexOf(name);
            if (idx < 0) throw new IllegalArgumentException("no such column: " + name + " in " + columnNames);
            return idx;
        }
    }

    /** A minimal, real, trust-authenticated wire-protocol client - see PlpgsqlEndToEndTest's own RawClient for the same, established pattern, extended here to also parse back real RowDescription/DataRow messages for a real SELECT's own row data. */
    private class RawClient implements AutoCloseable {
        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;

        RawClient(String user) throws Exception {
            socket = new Socket("localhost", port);
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            StdWireMessages.writeStartupMessage(out, user, "anydb");
            out.flush();
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                if (msg.type() == 'Z') break;
            }
        }

        /** Runs a non-SELECT statement, returning null on success or the real error message on failure. */
        String query(String sql) throws Exception {
            StdWireMessages.writeQuery(out, sql);
            out.flush();
            String error = null;
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                if (msg.type() == 'E') {
                    error = extractError(msg);
                } else if (msg.type() == 'Z') {
                    return error;
                }
            }
        }

        /** Runs a SELECT, parsing back the real RowDescription/DataRow messages into column names and row values. */
        QueryOutcome select(String sql) throws Exception {
            StdWireMessages.writeQuery(out, sql);
            out.flush();
            List<String> columnNames = new ArrayList<>();
            List<List<String>> rows = new ArrayList<>();
            String error = null;
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                if (msg.type() == 'T') {
                    columnNames.addAll(parseRowDescription(msg.body()));
                } else if (msg.type() == 'D') {
                    rows.add(parseDataRow(msg.body()));
                } else if (msg.type() == 'E') {
                    error = extractError(msg);
                } else if (msg.type() == 'Z') {
                    break;
                }
            }
            if (error != null) {
                throw new AssertionError("SELECT failed: " + error);
            }
            return new QueryOutcome(columnNames, rows);
        }

        /** Mirrors writeRowDescription's own exact wire format (see StdWireMessages): Int16 count, then per column a null-terminated name, Int32 table OID, Int16 attr number, Int32 type OID, Int16 type size, Int32 type modifier, Int16 format code. */
        private List<String> parseRowDescription(byte[] b) {
            List<String> names = new ArrayList<>();
            int pos = 0;
            int count = ((b[pos] & 0xFF) << 8) | (b[pos + 1] & 0xFF);
            pos += 2;
            for (int i = 0; i < count; i++) {
                int start = pos;
                while (b[pos] != 0) pos++;
                names.add(new String(b, start, pos - start, StandardCharsets.UTF_8));
                pos++; // null terminator
                pos += 4 + 2 + 4 + 2 + 4 + 2; // OID, attr#, type OID, type size, type modifier, format code
            }
            return names;
        }

        /** Mirrors writeDataRow's own exact wire format: Int16 count, then per value an Int32 length (-1 = NULL) followed by that many UTF-8 bytes. */
        private List<String> parseDataRow(byte[] b) {
            List<String> values = new ArrayList<>();
            int pos = 0;
            int count = ((b[pos] & 0xFF) << 8) | (b[pos + 1] & 0xFF);
            pos += 2;
            for (int i = 0; i < count; i++) {
                int len = ((b[pos] & 0xFF) << 24) | ((b[pos + 1] & 0xFF) << 16) | ((b[pos + 2] & 0xFF) << 8) | (b[pos + 3] & 0xFF);
                pos += 4;
                if (len == -1) {
                    values.add(null);
                } else {
                    values.add(new String(b, pos, len, StandardCharsets.UTF_8));
                    pos += len;
                }
            }
            return values;
        }

        private String extractError(StdWireMessages.TypedMessage msg) {
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

        @Override
        public void close() throws Exception {
            socket.close();
        }
    }
}
