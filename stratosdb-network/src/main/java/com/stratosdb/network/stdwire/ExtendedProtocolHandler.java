package com.stratosdb.network.stdwire;

import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Real handling for the extended query protocol's message types - Parse,
 * Bind, Describe, Execute, Sync, Close - one instance per connection,
 * exactly matching the protocol's own scoping (a prepared statement or
 * portal only ever means something to the connection that created it).
 *
 * The one real, honestly-stated simplification: this engine has no
 * native parameterized-query execution path and no query-planning phase
 * separate from running the statement, so Bind substitutes each bound
 * parameter as a properly quoted/escaped SQL literal directly into the
 * query text (see substituteParams), then EAGERLY executes it immediately
 * - not lazily at Execute time. That means Describe('P', ...) and
 * Execute can describe/return the statement's REAL, actual shape (this
 * engine has no partial/incremental execution to preserve anyway, so
 * nothing is lost by executing early), while Describe('S', ...) - before
 * any values are bound - has no real row shape to report yet and
 * honestly reports NoData rather than guessing. Every message here is a
 * real, distinct piece of the wire protocol, not a relabeled simple
 * query - only the "how is a parameter actually threaded into execution"
 * detail is simplified, and values remain properly escaped throughout,
 * so this stays SQL-injection-safe despite not being a native
 * parameterized path.
 */
final class ExtendedProtocolHandler {
    private static final Pattern PARAM_PLACEHOLDER = Pattern.compile("\\$(\\d+)");

    private record PreparedStatement(String query, int paramCount) {}
    private record Portal(String substitutedSql, QueryResult result) {}

    private final StratosDB db;
    private final StdWireServer server;
    private final Map<String, PreparedStatement> preparedStatements = new HashMap<>();
    private final Map<String, Portal> portals = new HashMap<>();

    /**
     * Set the moment any extended-protocol message fails - references a
     * prepared statement/portal that doesn't exist, or a Bind whose
     * substituted query itself fails at execution. Real Postgres's own
     * behavior, and the reason this exists: once one message in a
     * pipelined Parse/Bind/.../Sync batch fails, every later message
     * before the next Sync is skipped rather than acted on, so a stale
     * portal or statement from an earlier, unrelated part of the session
     * can never be silently picked up and executed in its place just
     * because a later message happened to reuse the same name. A real,
     * previously-latent gap found by testing this exact sequence - Bind
     * fails, then Execute against a portal name that still had a valid,
     * unrelated entry from earlier in the session - and returned that
     * stale row instead of correctly doing nothing until Sync.
     */
    private boolean inErrorState = false;

    ExtendedProtocolHandler(StratosDB db, StdWireServer server) {
        this.db = db;
        this.server = server;
    }

    /** Dispatches one extended-protocol message and returns the (possibly updated) inTransaction state - the same state threaded through the simple query path, since a Bind/Execute can just as easily run inside an explicit transaction as a simple 'Q' statement can. */
    boolean handle(StdWireMessages.TypedMessage msg, DataOutputStream out, boolean inTransaction) throws IOException {
        if (inErrorState && msg.type() != 'S') {
            return inTransaction; // silently skipped - see inErrorState's own javadoc
        }
        switch (msg.type()) {
            case 'P' -> {
                handleParse(msg, out);
                return inTransaction;
            }
            case 'B' -> {
                return handleBind(msg, out, inTransaction);
            }
            case 'D' -> {
                handleDescribe(msg, out);
                return inTransaction;
            }
            case 'E' -> {
                return handleExecute(msg, out, inTransaction);
            }
            case 'C' -> {
                handleClose(msg, out);
                return inTransaction;
            }
            case 'S' -> {
                inErrorState = false; // Sync always clears it, whether or not an error actually happened this round
                StdWireMessages.writeReadyForQuery(out, inTransaction ? 'T' : 'I');
                out.flush();
                return inTransaction;
            }
            default -> throw new IllegalStateException("ExtendedProtocolHandler cannot handle message type: " + msg.type());
        }
    }

    private void handleParse(StdWireMessages.TypedMessage msg, DataOutputStream out) throws IOException {
        StdWireMessages.ParseMessage parsed = StdWireMessages.readParseMessage(msg);
        int paramCount = countDistinctPlaceholders(parsed.query());
        preparedStatements.put(parsed.statementName(), new PreparedStatement(parsed.query(), paramCount));
        StdWireMessages.writeParseComplete(out);
        out.flush();
    }

    private boolean handleBind(StdWireMessages.TypedMessage msg, DataOutputStream out, boolean inTransaction) throws IOException {
        StdWireMessages.BindMessage bind = StdWireMessages.readBindMessage(msg);
        PreparedStatement stmt = preparedStatements.get(bind.statementName());
        if (stmt == null) {
            inErrorState = true;
            StdWireMessages.writeErrorResponse(out, "Prepared statement does not exist: \"" + bind.statementName() + "\"");
            out.flush();
            return inTransaction;
        }

        String substituted = substituteParams(stmt.query(), bind.paramValues(), bind.paramFormatCodes());
        QueryResult result;
        try {
            result = db.execute(substituted);
        } catch (Exception e) {
            result = QueryResult.error(e.getMessage());
        }
        portals.put(bind.portalName(), new Portal(substituted, result));
        StdWireMessages.writeBindComplete(out);
        out.flush();

        // No inErrorState=true here even when result failed: BindComplete was
        // just sent, meaning Bind itself succeeded as a protocol operation - the
        // eagerly-executed query's failure is stored on the portal and gets
        // reported to the client later, via Describe (NoData) or Execute (the
        // real ErrorResponse) - see this class's own javadoc on why execution
        // happens this early. Marking the connection as errored before the
        // client has even been told there's a problem would cause the message
        // that's actually supposed to report it (Execute) to be silently
        // skipped instead - a real regression caught by testing this exact
        // "Bind succeeds, later Execute reports the real failure" sequence,
        // not by inspection.
        if (result.isSuccess()) {
            return server.updateTransactionState(substituted, inTransaction);
        }
        return inTransaction || server.isTransactionControl(substituted);
    }

    private void handleDescribe(StdWireMessages.TypedMessage msg, DataOutputStream out) throws IOException {
        StdWireMessages.DescribeMessage describe = StdWireMessages.readDescribeMessage(msg);
        if (describe.targetType() == 'S') {
            PreparedStatement stmt = preparedStatements.get(describe.name());
            if (stmt == null) {
                inErrorState = true;
                StdWireMessages.writeErrorResponse(out, "Prepared statement does not exist: \"" + describe.name() + "\"");
                out.flush();
                return;
            }
            List<Integer> paramOids = new ArrayList<>();
            for (int i = 0; i < stmt.paramCount(); i++) {
                paramOids.add(25); // Postgres OID 25 = text/unknown - see class javadoc, real types aren't tracked separately from execution
            }
            StdWireMessages.writeParameterDescription(out, paramOids);
            // No row shape to honestly report before any values are bound and the
            // statement actually runs - see class javadoc.
            StdWireMessages.writeNoData(out);
        } else {
            Portal portal = portals.get(describe.name());
            if (portal == null) {
                inErrorState = true;
                StdWireMessages.writeErrorResponse(out, "Portal does not exist: \"" + describe.name() + "\"");
                out.flush();
                return;
            }
            List<Tuple> rows = portal.result().isSuccess() ? portal.result().getRows() : null;
            if (rows != null) {
                StdWireMessages.writeRowDescription(out, server.describeColumns(rows));
            } else {
                StdWireMessages.writeNoData(out);
            }
        }
        out.flush();
    }

    private boolean handleExecute(StdWireMessages.TypedMessage msg, DataOutputStream out, boolean inTransaction) throws IOException {
        StdWireMessages.ExecuteMessage execute = StdWireMessages.readExecuteMessage(msg);
        // execute.maxRows() is intentionally not honored - see class javadoc:
        // this engine has no partial/cursor-based execution to limit against,
        // and the portal's result was already fully computed at Bind time.
        Portal portal = portals.get(execute.portalName());
        if (portal == null) {
            inErrorState = true;
            StdWireMessages.writeErrorResponse(out, "Portal does not exist: \"" + execute.portalName() + "\"");
            out.flush();
            return inTransaction;
        }

        QueryResult result = portal.result();
        if (!result.isSuccess()) {
            inErrorState = true;
            StdWireMessages.writeErrorResponse(out, result.getError());
            out.flush();
            return inTransaction;
        }

        List<Tuple> rows = result.getRows();
        if (rows != null) {
            for (Tuple row : rows) {
                List<String> values = new ArrayList<>(row.size());
                for (int i = 0; i < row.size(); i++) {
                    Object v = row.getValue(i);
                    values.add(v == null ? null : server.formatValueForWire(v));
                }
                StdWireMessages.writeDataRow(out, values);
            }
            StdWireMessages.writeCommandComplete(out, server.buildCommandTag(portal.substitutedSql(), rows.size()));
        } else {
            StdWireMessages.writeCommandComplete(out, server.buildCommandTag(portal.substitutedSql(), server.extractAffectedCount(result.getMessage())));
        }
        out.flush();
        return inTransaction;
    }

    private void handleClose(StdWireMessages.TypedMessage msg, DataOutputStream out) throws IOException {
        StdWireMessages.CloseMessage close = StdWireMessages.readCloseMessage(msg);
        if (close.targetType() == 'S') {
            preparedStatements.remove(close.name());
        } else {
            portals.remove(close.name());
        }
        StdWireMessages.writeCloseComplete(out);
        out.flush();
    }

    /** The number of distinct $N placeholders in a query - not just a count of occurrences, since a well-formed parameterized query may reference the same parameter more than once (e.g. "WHERE a = $1 OR b = $1"). */
    private int countDistinctPlaceholders(String query) {
        Matcher matcher = PARAM_PLACEHOLDER.matcher(query);
        int max = 0;
        while (matcher.find()) {
            max = Math.max(max, Integer.parseInt(matcher.group(1)));
        }
        return max;
    }

    /**
     * Substitutes every $N placeholder in query with its corresponding
     * bound parameter value from paramValues (1-indexed, matching the
     * protocol's own convention) - see class javadoc for why this is
     * text substitution into the query rather than a native parameterized
     * execution path, and why that remains injection-safe.
     */
    private String substituteParams(String query, byte[][] paramValues, int[] paramFormatCodes) {
        Matcher matcher = PARAM_PLACEHOLDER.matcher(query);
        StringBuilder result = new StringBuilder();
        int lastEnd = 0;
        while (matcher.find()) {
            result.append(query, lastEnd, matcher.start());
            int paramIndex = Integer.parseInt(matcher.group(1)) - 1;
            byte[] rawValue = (paramIndex >= 0 && paramIndex < paramValues.length) ? paramValues[paramIndex] : null;
            int formatCode = (paramIndex >= 0 && paramIndex < paramFormatCodes.length) ? paramFormatCodes[paramIndex] : 0;
            result.append(formatParamAsSqlLiteral(rawValue, formatCode));
            lastEnd = matcher.end();
        }
        result.append(query.substring(lastEnd));
        return result.toString();
    }

    /**
     * Renders one bound parameter's raw bytes as a SQL literal: NULL for
     * a SQL NULL, a bare token for something that parses as a number or
     * a boolean (matching the grammar's own bare BOOLEAN_LITERAL/
     * INTEGER_LITERAL/FLOAT_LITERAL tokens), and a properly quoted,
     * escaped string literal otherwise - doubling any embedded single
     * quote, the standard SQL escaping rule, so a value containing a
     * quote can never break out of the literal it's placed into.
     *
     * formatCode 1 (binary) is now real, not ignored - found missing
     * entirely during a real, broad driver/ORM verification pass: the
     * real, official org.postgresql JDBC driver sends every setInt/
     * setLong/setBoolean-bound parameter in BINARY format from its very
     * first execution (not just after some "prepareThreshold" is
     * reached, a real, wrong assumption corrected only by adding
     * temporary diagnostic logging and reading the actual bytes, not by
     * reasoning about the driver's own documented defaults). Before this
     * fix, a binary parameter's own raw bytes (e.g. the 4 real bytes
     * {0,0,0,1} for the integer 1) were read as if they were UTF-8 TEXT,
     * producing a garbled string that then got quoted and inserted into
     * the target column as-is - genuine, silent stored-data corruption,
     * not just a display glitch, since a real int column could end up
     * holding a string of near-unprintable bytes instead of the real
     * integer.
     *
     * Decoded by real, known Postgres binary wire-format byte length,
     * since the format code alone only says "binary," not which
     * specific type: 1 byte -> boolean, 4 bytes -> a signed, big-endian
     * int4, 8 bytes -> a signed, big-endian int8. A real, honestly-
     * named ambiguity: int8 and float8 are both 8 bytes in Postgres's
     * own binary wire format, and nothing at the Bind message level
     * distinguishes them - int8 is treated as the default here since a
     * bound integer id/count is a far more common case in real ORM-
     * generated SQL than a bound 8-byte binary double; a real, separate,
     * further piece of work would thread the target column's own
     * declared type through from Parse time to resolve this correctly
     * in every case, including binary float8. Any other byte length
     * (or any decode failure) falls back to the original, UTF-8-text
     * interpretation this method already used for every parameter
     * before this fix, so no previously-working, real text-format case
     * regresses.
     */
    private String formatParamAsSqlLiteral(byte[] rawValue, int formatCode) {
        if (rawValue == null) {
            return "NULL";
        }
        if (formatCode == 1) {
            String decoded = tryDecodeBinary(rawValue);
            if (decoded != null) {
                return decoded;
            }
        }
        String text = new String(rawValue, StandardCharsets.UTF_8);
        if (text.equalsIgnoreCase("true") || text.equalsIgnoreCase("false")) {
            return text.toLowerCase();
        }
        if (isNumericLiteral(text)) {
            return text;
        }
        return "'" + text.replace("'", "''") + "'";
    }

    /** Returns a real SQL literal for a real, known binary encoding by byte length, or null when the length isn't one this implementation recognizes at all - see formatParamAsSqlLiteral's own javadoc for the full, honest reasoning. */
    private String tryDecodeBinary(byte[] rawValue) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(rawValue).order(java.nio.ByteOrder.BIG_ENDIAN);
        if (rawValue.length == 1) {
            return rawValue[0] != 0 ? "true" : "false";
        }
        if (rawValue.length == 4) {
            return String.valueOf(buf.getInt());
        }
        if (rawValue.length == 8) {
            return String.valueOf(buf.getLong());
        }
        return null;
    }

    private boolean isNumericLiteral(String text) {
        if (text.isEmpty()) {
            return false;
        }
        try {
            Double.parseDouble(text);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
