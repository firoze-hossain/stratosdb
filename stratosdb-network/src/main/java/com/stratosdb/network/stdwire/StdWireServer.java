package com.stratosdb.network.stdwire;

import com.stratosdb.core.StratosDB;
import com.stratosdb.network.auth.ScramSha256;
import com.stratosdb.network.auth.UserStore;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.sql.parser.SqlParser;
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
    /** Stateless - safe to keep one, reused only for isEffectivelyEmpty's own real lexer check below (real SQL parsing itself still goes through StratosDB.execute's own internal parser, not this instance). */
    private final SqlParser sqlParser = new SqlParser();
    private volatile boolean running = false;
    private ServerSocket serverSocket;
    private ExecutorService connectionExecutor;
    private final AtomicInteger nextPid = new AtomicInteger(1000);
    /** Non-null only when this server's own StratosDB instance is currently following a primary as a replica - see setReplicationClient's own javadoc and the new PROMOTE command below. */
    private volatile com.stratosdb.network.replication.ReplicationClient replicationClient;

    /**
     * Configures this server's own instance as a replica currently
     * following client - wired in by whatever process starts both a
     * StratosDB instance and a ReplicationClient together (see
     * ReplicationClient's own javadoc), so that the new PROMOTE command
     * below has something real to stop when an operator or an HA
     * orchestrator (see StratosHa) decides this replica should become
     * the new primary. Pass null (the default) for an instance that was
     * never a replica at all, or one that has already been promoted -
     * PROMOTE then correctly reports there's nothing to promote.
     */
    public void setReplicationClient(com.stratosdb.network.replication.ReplicationClient replicationClient) {
        this.replicationClient = replicationClient;
    }

    public StdWireServer(int port, StratosDB db) {
        this(port, db, null);
    }

    public StdWireServer(int port, StratosDB db, UserStore userStore) {
        this.port = port;
        this.db = db;
        this.userStore = userStore;
        if (userStore != null) {
            // The real bridge - see ExecutorEngine.RoleCredentialSink's own javadoc for
            // why this can't be a direct dependency instead: CREATE ROLE ... LOGIN
            // PASSWORD 'x' becomes a genuine, SCRAM-authenticatable credential in this
            // server's own UserStore, not just privilege bookkeeping.
            db.setRoleCredentialSink(new com.stratosdb.sql.executor.ExecutorEngine.RoleCredentialSink() {
                @Override
                public void onRoleCredential(String username, String plaintextPassword) {
                    userStore.addUser(username, plaintextPassword);
                }

                @Override
                public void onRoleDropped(String username) {
                    userStore.removeUser(username);
                }
            });
        }
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
        com.stratosdb.sql.executor.SessionActivity activity = null;
        try (Socket s = socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()))) {

            // Defensive: this thread is new per connection from the pool's own point of
            // view, but a pooled thread may be reused across connections - MUST run
            // before performStartup(), not after: performStartup() now calls
            // db.setCurrentUser() as part of authentication (see its own comment below),
            // and closeSession()'s own session.remove() would otherwise immediately wipe
            // that back out again on the very next line, silently leaving every
            // connection's own session with no current user at all - a real bug found
            // and fixed while testing this real, end to end: a role's own GRANTed
            // privileges appeared to do nothing at all over a real connection, because
            // by the time a query actually ran, currentUser had already been reset to
            // null right after being correctly set.
            db.closeSession();

            String username = performStartup(in, out);
            if (username == null) {
                return;
            }
            // The real pg_stat_activity equivalent - registered only once startup
            // genuinely succeeds (an authentication failure is never itself "activity"
            // worth showing an operator), unregistered in the finally block below
            // regardless of how this connection eventually ends.
            activity = db.getExecutor().getSessionActivityRegistry().register(username, remote);

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
                    if (statement.isBlank() || sqlParser.isEffectivelyEmpty(statement)) {
                        StdWireMessages.writeEmptyQueryResponse(out);
                        continue;
                    }
                    activity.state = "active";
                    activity.query = statement;
                    activity.queryStart = System.currentTimeMillis();
                    inTransaction = executeAndRespond(statement, in, out, inTransaction);
                }
                activity.state = inTransaction ? "idle in transaction" : "idle";
                StdWireMessages.writeReadyForQuery(out, inTransaction ? 'T' : 'I');
                out.flush();
            }
        } catch (IOException e) {
            LOG.debug("pg-wire connection {} closed: {}", remote, e.getMessage());
        } finally {
            if (activity != null) {
                db.getExecutor().getSessionActivityRegistry().unregister(activity);
            }
            db.closeSession();
            LOG.debug("pg-wire client disconnected: {}", remote);
        }
    }

    /** Returns false if the connection should be closed (SSL request handled, then the client is expected to reconnect in plaintext, or the startup was malformed). */
    /** Returns the authenticated username on success, or null on failure (an ErrorResponse has already been sent in that case). A String return, not a boolean, specifically so the caller can register this connection's own SessionActivity (see SessionActivityRegistry) without needing any shared, cross-connection state to recover the username afterward - a plain instance field here would race across concurrent connections on this same, shared StdWireServer. */
    private String performStartup(DataInputStream in, DataOutputStream out) throws IOException {
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
            return null;
        }

        Map<String, String> params = StdWireMessages.parseStartupParams(body, 4);
        LOG.info("pg-wire startup: user={} database={} application_name={}",
            params.get("user"), params.get("database"), params.get("application_name"));

        if (userStore != null) {
            if (!performScramAuthentication(params.get("user"), in, out)) {
                return null; // performScramAuthentication already sent the ErrorResponse
            }
        } else {
            // Trust auth: no password required - the unchanged default when no
            // UserStore is supplied, matching StratosServer's own established
            // convention for this project.
            StdWireMessages.writeAuthenticationOk(out);
        }
        // Real permission enforcement begins here, regardless of auth mode - see
        // ExecutorEngine.hasPrivilege's own javadoc for why an unknown username (one
        // never explicitly CREATE ROLE'd) stays fully unrestricted either way: trust
        // auth already has no real identity guarantee, so this call is what actually
        // makes GRANT/REVOKE mean anything once a role IS created, not a promise that
        // every connection is now locked down by default.
        db.setCurrentUser(params.get("user"));
        StdWireMessages.writeParameterStatus(out, "server_version", "16.0 (StratosDB pg-wire compatibility layer)");
        StdWireMessages.writeParameterStatus(out, "client_encoding", "UTF8");
        StdWireMessages.writeParameterStatus(out, "server_encoding", "UTF8");
        StdWireMessages.writeParameterStatus(out, "DateStyle", "ISO, MDY");
        StdWireMessages.writeParameterStatus(out, "integer_datetimes", "on");
        StdWireMessages.writeBackendKeyData(out, nextPid.getAndIncrement(), 12345);
        StdWireMessages.writeReadyForQuery(out, 'I');
        out.flush();
        return params.get("user");
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
    private boolean executeAndRespond(String sql, DataInputStream in, DataOutputStream out, boolean inTransaction) throws IOException {
        if (tryHandleTableListingQuery(sql, out)) {
            return inTransaction; // a catalog-introspection query is never itself transaction control
        }
        if (tryHandleCopyStatement(sql, in, out)) {
            return inTransaction; // COPY is never itself transaction control either
        }
        if (tryHandlePromoteStatement(sql, out)) {
            return inTransaction; // PROMOTE is never itself transaction control either
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
     * Intercepts a real COPY ... FROM/TO STDIN/STDOUT before it ever
     * reaches the normal execute() path - the same real reason
     * tryHandleTableListingQuery intercepts `\dt`'s own query above:
     * ExecutorEngine.execute(), reached with no socket access at all,
     * cannot stream the real CopyIn/CopyOut wire sub-protocol a real
     * client needs. A file-based COPY (a real, quoted path, not
     * STDIN/STDOUT) is NOT intercepted here - tryParseStdioCopy returns
     * null for it, so it correctly falls through to the normal
     * execute() path, which already handles it entirely on its own.
     */
    private boolean tryHandleCopyStatement(String sql, DataInputStream in, DataOutputStream out) throws IOException {
        com.stratosdb.sql.ast.CopyStatement copyStmt = db.getExecutor().tryParseStdioCopy(sql);
        if (copyStmt == null) {
            return false;
        }

        String prepareError = db.getExecutor().prepareCopy(copyStmt);
        if (prepareError != null) {
            StdWireMessages.writeErrorResponse(out, prepareError);
            return true;
        }

        return copyStmt.isFrom() ? handleCopyFromStdin(copyStmt, in, out) : handleCopyToStdout(copyStmt, out);
    }

    /**
     * PROMOTE - the real, remote-triggerable operation an operator or
     * an HA orchestrator (see StratosHa) uses to turn this replica into
     * a new primary: stops the real ReplicationClient this server was
     * configured with (see setReplicationClient's own javadoc), then
     * flips off real, enforced read-only mode (see ExecutorEngine's own
     * READ_ONLY_SAFE_STATEMENTS javadoc) so this instance starts
     * accepting writes directly. Intercepted here, before ever reaching
     * ExecutorEngine's own execute(), for the same real reason COPY's
     * STDIN/STDOUT sub-protocol is - the actual work needed
     * (stopping a ReplicationClient object) lives entirely outside
     * ExecutorEngine's own knowledge, in this class instead.
     *
     * Idempotent and honest about it either way: calling PROMOTE a
     * second time (or on an instance that was never a replica at all)
     * correctly reports there's nothing to promote, rather than
     * silently succeeding or throwing.
     */
    private boolean tryHandlePromoteStatement(String sql, DataOutputStream out) throws IOException {
        com.stratosdb.sql.ast.Statement parsed;
        try {
            parsed = sqlParser.parse(sql);
        } catch (Exception e) {
            return false; // not parseable as PROMOTE at all - let the normal execute() path report the real syntax error
        }
        if (!(parsed instanceof com.stratosdb.sql.ast.PromoteStatement)) {
            return false;
        }

        com.stratosdb.network.replication.ReplicationClient client = replicationClient;
        if (client == null) {
            StdWireMessages.writeErrorResponse(out, "not a replica - nothing to promote (either this instance was never configured as one, or it has already been promoted)");
            return true;
        }

        client.stop();
        db.setReadOnly(false);
        replicationClient = null; // a second PROMOTE call must correctly report "nothing to promote" above, not silently stop an already-stopped client again
        LOG.info("Replica promoted to primary - replication stopped, read-only mode disabled");
        StdWireMessages.writeCommandComplete(out, "PROMOTE");
        return true;
    }

    /**
     * COPY ... FROM STDIN: sends CopyInResponse, then reads real
     * CopyData messages from the client until CopyDone (success) or
     * CopyFail (the client itself aborted) - a CopyData message's own
     * byte chunk is NOT guaranteed to align with line boundaries (real
     * Postgres allows a client to chunk however it likes), so chunks
     * are buffered and only complete, newline-terminated lines are
     * ever handed to copyFromStdinLine. Every row runs inside one real
     * transaction spanning the whole COPY (see
     * ExecutorEngine.beginCopyTransaction's own javadoc) - a single bad
     * row aborts that whole transaction, not just that one row, the
     * same real, honest guarantee this engine's own DML already gives.
     */
    private boolean handleCopyFromStdin(com.stratosdb.sql.ast.CopyStatement copyStmt, DataInputStream in, DataOutputStream out) throws IOException {
        int columnCount = db.getExecutor().getCopyColumnCount(copyStmt);
        StdWireMessages.writeCopyInResponse(out, columnCount);
        out.flush();

        com.stratosdb.transaction.Transaction txn = db.getExecutor().beginCopyTransaction();
        long rowCount = 0;
        String firstError = null;
        StringBuilder partial = new StringBuilder();

        readLoop:
        while (true) {
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
            switch (msg.type()) {
                case 'd' -> {
                    partial.append(new String(msg.body(), java.nio.charset.StandardCharsets.UTF_8));
                    int newlineIdx;
                    while ((newlineIdx = partial.indexOf("\n")) >= 0) {
                        String line = partial.substring(0, newlineIdx);
                        partial.delete(0, newlineIdx + 1);
                        if (line.isEmpty()) continue;
                        if (firstError == null) {
                            String err = db.getExecutor().copyFromStdinLine(copyStmt, line, txn);
                            if (err != null) {
                                firstError = "COPY: row " + (rowCount + 1) + " failed: " + err;
                            } else {
                                rowCount++;
                            }
                        }
                    }
                }
                case 'c' -> { break readLoop; } // CopyDone
                case 'f' -> {
                    firstError = "COPY aborted by client";
                    break readLoop;
                }
                default -> {
                    firstError = "Unexpected message during COPY: " + msg.type();
                    break readLoop;
                }
            }
        }

        if (firstError != null) {
            db.getExecutor().abortCopyTransaction(txn);
            StdWireMessages.writeErrorResponse(out, firstError);
        } else {
            db.getExecutor().commitCopyTransaction(txn);
            StdWireMessages.writeCommandComplete(out, "COPY " + rowCount);
        }
        return true;
    }

    /**
     * COPY ... TO STDOUT: sends CopyOutResponse, then streams every row
     * as a real CopyData message, one row at a time (via
     * ExecutorEngine.copyToStdoutStream's own callback, never the whole
     * table buffered in memory first), then CopyDone.
     */
    private boolean handleCopyToStdout(com.stratosdb.sql.ast.CopyStatement copyStmt, DataOutputStream out) throws IOException {
        int columnCount = db.getExecutor().getCopyColumnCount(copyStmt);
        StdWireMessages.writeCopyOutResponse(out, columnCount);

        com.stratosdb.transaction.Transaction txn = db.getExecutor().beginCopyTransaction();
        long[] rowCount = {0};
        try {
            db.getExecutor().copyToStdoutStream(copyStmt, txn, line -> {
                try {
                    StdWireMessages.writeCopyData(out, line);
                    rowCount[0]++;
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
        } catch (java.io.UncheckedIOException e) {
            db.getExecutor().abortCopyTransaction(txn);
            throw e.getCause();
        }
        db.getExecutor().commitCopyTransaction(txn);
        StdWireMessages.writeCopyDone(out);
        StdWireMessages.writeCommandComplete(out, "COPY " + rowCount[0]);
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
        if (upper.startsWith("CREATE OR REPLACE FUNCTION") || upper.startsWith("CREATE FUNCTION")) return "CREATE FUNCTION";
        if (upper.startsWith("DROP FUNCTION")) return "DROP FUNCTION";
        if (upper.startsWith("CREATE SEQUENCE")) return "CREATE SEQUENCE";
        if (upper.startsWith("DROP SEQUENCE")) return "DROP SEQUENCE";
        if (upper.startsWith("CREATE OR REPLACE PROCEDURE") || upper.startsWith("CREATE PROCEDURE")) return "CREATE PROCEDURE";
        if (upper.startsWith("DROP PROCEDURE")) return "DROP PROCEDURE";
        if (upper.startsWith("CALL")) return "CALL";
        if (upper.startsWith("CREATE TRIGGER")) return "CREATE TRIGGER";
        if (upper.startsWith("DROP TRIGGER")) return "DROP TRIGGER";
        if (upper.startsWith("CREATE EXTENSION")) return "CREATE EXTENSION";
        if (upper.startsWith("DROP EXTENSION")) return "DROP EXTENSION";
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
