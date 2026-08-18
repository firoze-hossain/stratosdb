package com.stratosdb.network.stdwire;

import com.stratosdb.core.StratosDB;
import com.stratosdb.network.auth.ScramSha256;
import com.stratosdb.network.auth.UserStore;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A real PostgreSQL wire-protocol-v3 server: `psql -h host -p port -U user
 * dbname` connects to this exactly as it would to real Postgres, with no
 * client-side changes. Verified against an actual `psql` client, not a
 * hand-rolled simulation - see PROGRESS.md for the specifics.
 *
 * This is a genuinely separate server from StratosServer (this project's
 * own custom wire protocol) - both can run simultaneously against the same
 * underlying StratosDB instance, on different ports, since neither owns
 * the database, they just speak different protocols to reach it.
 *
 * Scope: the startup handshake, the simple query protocol, the extended
 * query protocol, and now real SCRAM-SHA-256 authentication when a
 * UserStore is supplied (see StdWireMessages' javadoc for exactly what
 * else this does and doesn't cover).
 */
public class StdWireServer {
    private static final Logger LOG = LoggerFactory.getLogger(StdWireServer.class);

    private final int port;
    private final StratosDB db;
    private final UserStore userStore; // null = trust auth, matching StratosServer's own established convention for this project
    private volatile boolean running = false;
    private ServerSocket serverSocket;
    private ExecutorService connectionExecutor;
    private final AtomicInteger nextPid = new AtomicInteger(1000);

    public StdWireServer(int port, StratosDB db) {
        this(port, db, null);
    }

    public StdWireServer(int port, StratosDB db, UserStore userStore) {
        this.port = port;
        this.db = db;
        this.userStore = userStore;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        connectionExecutor = Executors.newVirtualThreadPerTaskExecutor();
        LOG.info("StdWireServer listening on port {}", port);

        Thread acceptThread = new Thread(() -> {
            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    connectionExecutor.submit(() -> handleConnection(client));
                } catch (IOException e) {
                    if (running) LOG.error("Accept failed", e);
                }
            }
        }, "stdwire-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {
        }
        if (connectionExecutor != null) connectionExecutor.shutdownNow();
    }

    private void handleConnection(Socket socket) {
        String remote = socket.getRemoteSocketAddress().toString();
        LOG.debug("pg-wire client connected: {}", remote);
        try (Socket s = socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()))) {

            if (!performStartup(in, out)) {
                return;
            }

            db.closeSession(); // defensive: this thread is new per connection, but be explicit about starting clean

            // Extended query protocol state - per connection, matching the protocol's own
            // scoping (a prepared statement/portal only ever means something to the
            // connection that created it). See ExtendedProtocolHandler's own javadoc for
            // the real, named simplification in how these are executed.
            ExtendedProtocolHandler extended = new ExtendedProtocolHandler(db, this);

            boolean inTransaction = false;
            while (running) {
                StdWireMessages.TypedMessage msg;
                try {
                    msg = StdWireMessages.readTypedMessage(in);
                } catch (EOFException e) {
                    break;
                }

                if (msg.type() == 'X') {
                    break; // Terminate
                }
                if (msg.type() == 'P' || msg.type() == 'B' || msg.type() == 'D'
                    || msg.type() == 'E' || msg.type() == 'C' || msg.type() == 'S') {
                    inTransaction = extended.handle(msg, out, inTransaction);
                    continue;
                }
                if (msg.type() != 'Q') {
                    StdWireMessages.writeErrorResponse(out, "Unsupported message type: " + msg.type());
                    out.flush();
                    continue;
                }

                String sql = msg.readCString(0);
                // libpq sometimes sends a trailing semicolon plus whitespace, or multiple
                // statements separated by semicolons in one Query message (psql's default
                // per-line behavior sends one at a time, but scripts/tools may not) - handle
                // each one in order, same as a real server would.
                for (String statement : splitStatements(sql)) {
                    if (statement.isBlank()) {
                        StdWireMessages.writeEmptyQueryResponse(out);
                        continue;
                    }
                    inTransaction = executeAndRespond(statement, out, inTransaction);
                }
                StdWireMessages.writeReadyForQuery(out, inTransaction ? 'T' : 'I');
                out.flush();
            }
        } catch (IOException e) {
            LOG.debug("pg-wire connection {} closed: {}", remote, e.getMessage());
        } finally {
            db.closeSession();
            LOG.debug("pg-wire client disconnected: {}", remote);
        }
    }

    /** Returns false if the connection should be closed (SSL request handled, then the client is expected to reconnect in plaintext, or the startup was malformed). */
    private boolean performStartup(DataInputStream in, DataOutputStream out) throws IOException {
        byte[] body = StdWireMessages.readUntypedPacket(in);
        int code = readInt(body, 0);

        if (code == StdWireMessages.SSL_REQUEST_CODE) {
            StdWireMessages.writeSslDecline(out);
            out.flush();
            // A real client falls back to a plaintext StartupMessage next on the SAME connection.
            body = StdWireMessages.readUntypedPacket(in);
            code = readInt(body, 0);
        }

        if (code != StdWireMessages.PROTOCOL_VERSION_3) {
            StdWireMessages.writeErrorResponse(out, "Only protocol version 3.0 is supported");
            out.flush();
            return false;
        }

        Map<String, String> params = StdWireMessages.parseStartupParams(body, 4);
        LOG.info("pg-wire startup: user={} database={} application_name={}",
            params.get("user"), params.get("database"), params.get("application_name"));

        if (userStore != null) {
            if (!performScramAuthentication(params.get("user"), in, out)) {
                return false; // performScramAuthentication already sent the ErrorResponse
            }
        } else {
            // Trust auth: no password required - the unchanged default when no
            // UserStore is supplied, matching StratosServer's own established
            // convention for this project.
            StdWireMessages.writeAuthenticationOk(out);
        }
        StdWireMessages.writeParameterStatus(out, "server_version", "16.0 (StratosDB pg-wire compatibility layer)");
        StdWireMessages.writeParameterStatus(out, "client_encoding", "UTF8");
        StdWireMessages.writeParameterStatus(out, "server_encoding", "UTF8");
        StdWireMessages.writeParameterStatus(out, "DateStyle", "ISO, MDY");
        StdWireMessages.writeParameterStatus(out, "integer_datetimes", "on");
        StdWireMessages.writeBackendKeyData(out, nextPid.getAndIncrement(), 12345);
        StdWireMessages.writeReadyForQuery(out, 'I');
        out.flush();
        return true;
    }

    /**
     * The real SCRAM-SHA-256 handshake (RFC 5802) - see ScramSha256's own
     * javadoc for the cryptographic details. Only ever reached when a
     * UserStore was supplied to this server; trust auth (the unchanged
     * default) never calls this at all.
     */
    private boolean performScramAuthentication(String username, DataInputStream in, DataOutputStream out) throws IOException {
        StdWireMessages.writeAuthenticationSasl(out, ScramSha256.MECHANISM_NAME);
        out.flush();

        StdWireMessages.TypedMessage initialMsg = StdWireMessages.readTypedMessage(in);
        if (initialMsg.type() != 'p') {
            StdWireMessages.writeErrorResponse(out, "Expected SASLInitialResponse");
            out.flush();
            return false;
        }
        StdWireMessages.SaslInitialResponse initial = StdWireMessages.readSaslInitialResponse(initialMsg);
        if (!ScramSha256.MECHANISM_NAME.equals(initial.mechanism())) {
            StdWireMessages.writeErrorResponse(out, "Unsupported SASL mechanism: " + initial.mechanism());
            out.flush();
            return false;
        }

        UserStore.ScramCredential credential = userStore.getScramCredential(username);
        ScramSha256.Handshake handshake = new ScramSha256.Handshake(username, credential);

        String serverFirstMessage;
        try {
            serverFirstMessage = handshake.clientFirst(initial.initialResponseData());
        } catch (ScramSha256.ScramAuthenticationException e) {
            StdWireMessages.writeErrorResponse(out, "Authentication failed: " + e.getMessage());
            out.flush();
            return false;
        }
        StdWireMessages.writeAuthenticationSaslContinue(out, serverFirstMessage);
        out.flush();

        StdWireMessages.TypedMessage finalMsg = StdWireMessages.readTypedMessage(in);
        if (finalMsg.type() != 'p') {
            StdWireMessages.writeErrorResponse(out, "Expected SASLResponse");
            out.flush();
            return false;
        }
        String clientFinalMessage = StdWireMessages.readSaslResponse(finalMsg);

        String serverFinalMessage;
        try {
            serverFinalMessage = handshake.clientFinal(clientFinalMessage);
        } catch (ScramSha256.ScramAuthenticationException e) {
            LOG.warn("SCRAM authentication failed for user {}: {}", username, e.getMessage());
            StdWireMessages.writeErrorResponse(out, "password authentication failed for user \"" + username + "\"");
            out.flush();
            return false;
        }
        StdWireMessages.writeAuthenticationSaslFinal(out, serverFinalMessage);
        StdWireMessages.writeAuthenticationOk(out);
        return true;
    }

    /** Returns the new inTransaction state after executing one statement. */
    private boolean executeAndRespond(String sql, DataOutputStream out, boolean inTransaction) throws IOException {
        if (tryHandleTableListingQuery(sql, out)) {
            return inTransaction; // a catalog-introspection query is never itself transaction control
        }

        QueryResult result;
        try {
            result = db.execute(sql);
        } catch (Exception e) {
            LOG.warn("Statement failed unexpectedly: {}", sql, e);
            StdWireMessages.writeErrorResponse(out, e.getMessage());
            return inTransaction;
        }

        if (!result.isSuccess()) {
            StdWireMessages.writeErrorResponse(out, result.getError());
            // A failed statement inside an explicit transaction leaves it open
            // but poisoned - ExecutorEngine already tracks that internally;
            // from the wire client's point of view it's still "in a transaction".
            return inTransaction || isTransactionControl(sql);
        }

        List<Tuple> rows = result.getRows();
        if (rows != null) {
            // A SELECT (or similar row-returning statement) - describe the
            // columns from the first row (every row in one result shares the
            // same shape) and stream each row, even if there are zero rows
            // (RowDescription must still go out so the client knows the shape).
            List<StdWireMessages.Column> columns = describeColumns(rows);
            StdWireMessages.writeRowDescription(out, columns);
            for (Tuple row : rows) {
                List<String> values = new ArrayList<>(row.size());
                for (int i = 0; i < row.size(); i++) {
                    Object v = row.getValue(i);
                    values.add(v == null ? null : formatValueForWire(v));
                }
                StdWireMessages.writeDataRow(out, values);
            }
            StdWireMessages.writeCommandComplete(out, buildCommandTag(sql, rows.size()));
        } else {
            StdWireMessages.writeCommandComplete(out, buildCommandTag(sql, extractAffectedCount(result.getMessage())));
        }

        return updateTransactionState(sql, inTransaction);
    }

    /**
     * Detects the specific query psql's `\dt` meta-command sends (captured
     * from a real psql client - see PROGRESS.md) and, if matched,
     * synthesizes a correct response directly from StratosDB's own real
     * table metadata (ExecutorEngine.getTableNames()) - bypassing the
     * normal SQL execution path entirely, since StratosDB's parser has no
     * concept of `pg_catalog.pg_class`, `pg_namespace`, or any of the
     * other real Postgres system-catalog machinery this query references.
     *
     * Scope, stated plainly: this recognizes the `\dt` pattern specifically
     * (matching on the pg_class + pg_namespace signature any Postgres
     * version's `\dt` uses), not general pg_catalog SQL. `\d TABLENAME`
     * and `\l` are NOT handled here - `\d` needs a multi-query,
     * OID-based lookup sequence (captured and confirmed materially more
     * complex - see PROGRESS.md), and `\l` needs a "multiple databases"
     * concept StratosDB doesn't have at all (this engine is one database
     * per data directory). Implementing those properly is real further
     * work, not attempted here rather than half-done and left to surprise
     * someone later.
     */
    private boolean tryHandleTableListingQuery(String sql, DataOutputStream out) throws IOException {
        String normalized = sql.toLowerCase();
        if (!normalized.contains("pg_catalog.pg_class") || !normalized.contains("pg_namespace")) {
            return false;
        }

        List<StdWireMessages.Column> columns = List.of(
            new StdWireMessages.Column("Schema", 25, (short) -1),
            new StdWireMessages.Column("Name", 25, (short) -1),
            new StdWireMessages.Column("Type", 25, (short) -1),
            new StdWireMessages.Column("Owner", 25, (short) -1)
        );
        StdWireMessages.writeRowDescription(out, columns);

        java.util.List<String> tableNames = new java.util.ArrayList<>(db.getExecutor().getTableNames());
        java.util.Collections.sort(tableNames);
        for (String tableName : tableNames) {
            StdWireMessages.writeDataRow(out, List.of("public", tableName, "table", "stratosdb"));
        }

        StdWireMessages.writeCommandComplete(out, "SELECT " + tableNames.size());
        return true;
    }

    /**
     * Formats one column's value for the wire protocol - almost always
     * just Object.toString(), with one real exception: a JSON/JSONB
     * column's value is stored internally as a parsed Map/List/scalar
     * structure (see JsonParser and Tuple's own Map serialization
     * support), and Java's default Map.toString() format ({@code
     * {status=active}}) isn't valid JSON at all (no quotes around keys or
     * string values, "=" instead of ":") - actively misleading output for
     * a real client to receive from what's supposed to be a JSON column.
     * Converted back to real JSON text here instead.
     */
    String formatValueForWire(Object value) {
        if (value instanceof java.util.Map) {
            return com.stratosdb.sql.executor.JsonParser.toJsonText(value);
        }
        return value.toString();
    }

    List<StdWireMessages.Column> describeColumns(List<Tuple> rows) {
        List<StdWireMessages.Column> columns = new ArrayList<>();
        if (rows.isEmpty()) {
            return columns; // no rows at all means no way to know column names from this result alone - a real, minor limitation of the simple query protocol without a schema catalog behind it
        }
        Tuple first = rows.get(0);
        for (int i = 0; i < first.size(); i++) {
            String name = first.getColumnNames().get(i);
            Object sampleValue = first.getValue(i);
            columns.add(new StdWireMessages.Column(name, inferTypeOid(sampleValue), inferTypeSize(sampleValue)));
        }
        return columns;
    }

    /** Infers a PostgreSQL type OID from the runtime value, since results here aren't backed by a declared-type catalog at the wire layer - good enough for clients to render results correctly, since everything is sent in text format regardless of the declared OID. */
    private int inferTypeOid(Object value) {
        if (value instanceof Integer || value instanceof Long) return 23;   // int4
        if (value instanceof Double || value instanceof Float) return 701;  // float8
        if (value instanceof Boolean) return 16;                            // bool
        return 25; // text - the safe default; text format means any value round-trips correctly as a string regardless of OID
    }

    private short inferTypeSize(Object value) {
        if (value instanceof Integer) return 4;
        if (value instanceof Long) return 8;
        if (value instanceof Double) return 8;
        if (value instanceof Boolean) return 1;
        return -1; // variable-length
    }

    private static final Pattern UPDATED_PATTERN = Pattern.compile("Updated (\\d+) row");
    private static final Pattern DELETED_PATTERN = Pattern.compile("Deleted (\\d+) row");

    int extractAffectedCount(String message) {
        if (message == null) return 0;
        Matcher u = UPDATED_PATTERN.matcher(message);
        if (u.find()) return Integer.parseInt(u.group(1));
        Matcher d = DELETED_PATTERN.matcher(message);
        if (d.find()) return Integer.parseInt(d.group(1));
        if (message.startsWith("Inserted")) return 1;
        return 0;
    }

    /**
     * The command tag libpq/psql parses to report "SELECT 3", "INSERT 0 1",
     * etc. - real Postgres clients depend on this exact shape for some
     * interactive behavior (row-count reporting), not just cosmetics.
     */
    String buildCommandTag(String sql, int count) {
        String upper = sql.trim().toUpperCase();
        if (upper.startsWith("SELECT") || upper.startsWith("EXPLAIN")) return "SELECT " + count;
        if (upper.startsWith("INSERT")) return "INSERT 0 " + count;
        if (upper.startsWith("UPDATE")) return "UPDATE " + count;
        if (upper.startsWith("DELETE")) return "DELETE " + count;
        if (upper.startsWith("CREATE TABLE")) return "CREATE TABLE";
        if (upper.startsWith("CREATE INDEX")) return "CREATE INDEX";
        if (upper.startsWith("CREATE VIEW")) return "CREATE VIEW";
        if (upper.startsWith("DROP TABLE")) return "DROP TABLE";
        if (upper.startsWith("DROP VIEW")) return "DROP VIEW";
        if (upper.startsWith("BEGIN") || upper.startsWith("START")) return "BEGIN";
        if (upper.startsWith("COMMIT")) return "COMMIT";
        if (upper.startsWith("ROLLBACK")) return "ROLLBACK";
        if (upper.startsWith("VACUUM")) return "VACUUM";
        if (upper.startsWith("ANALYZE")) return "ANALYZE";
        return "OK";
    }

    boolean isTransactionControl(String sql) {
        String upper = sql.trim().toUpperCase();
        return upper.startsWith("BEGIN") || upper.startsWith("START");
    }

    boolean updateTransactionState(String sql, boolean currentlyInTransaction) {
        String upper = sql.trim().toUpperCase();
        if (upper.startsWith("BEGIN") || upper.startsWith("START")) return true;
        if (upper.startsWith("COMMIT") || upper.startsWith("ROLLBACK")) return false;
        return currentlyInTransaction;
    }

    /** Splits a semicolon-separated batch of statements the way libpq's simple query protocol allows in one Query message - naive on purpose (no string-literal-aware splitting), matching this engine's existing SQL surface, which doesn't support semicolons inside string literals in a way this would break. */
    private List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        for (String part : sql.split(";")) {
            if (!part.isBlank()) statements.add(part.trim());
        }
        if (statements.isEmpty()) statements.add(""); // an all-whitespace/empty query still gets one EmptyQueryResponse
        return statements;
    }

    private static int readInt(byte[] b, int offset) {
        return ((b[offset] & 0xFF) << 24) | ((b[offset + 1] & 0xFF) << 16) | ((b[offset + 2] & 0xFF) << 8) | (b[offset + 3] & 0xFF);
    }
}
