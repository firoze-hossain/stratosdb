package com.stratosdb.network.stdwire;

import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;

import java.io.DataOutputStream;
import java.io.IOException;
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
 * Uses native parameterized query execution in ExecutorEngine without
 * text query interpolation: parameter placeholders ($1, $2, ...) are
 * evaluated directly against strongly typed values decoded from pgwire
 * binary (format 1) or text (format 0) formats. This completely
 * eliminates SQL injection surface.
 */
final class ExtendedProtocolHandler {
    private static final Pattern PARAM_PLACEHOLDER = Pattern.compile("\\$(\\d+)");

    private record PreparedStatement(String query, int paramCount, int[] paramTypeOids, List<String> targetColumnTypes) {}
    private record Portal(String query, List<Object> params, QueryResult result) {}

    private final StratosDB db;
    private final StdWireServer server;
    private final Map<String, PreparedStatement> preparedStatements = new HashMap<>();
    private final Map<String, Portal> portals = new HashMap<>();

    /**
     * Set the moment any extended-protocol message fails - references a
     * prepared statement/portal that doesn't exist, or a Bind whose
     * query itself fails at execution. Once one message in a
     * pipelined Parse/Bind/.../Sync batch fails, every later message
     * before the next Sync is skipped rather than acted on.
     */
    private boolean inErrorState = false;

    ExtendedProtocolHandler(StratosDB db, StdWireServer server) {
        this.db = db;
        this.server = server;
    }

    /** Dispatches one extended-protocol message and returns the (possibly updated) inTransaction state. */
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
        List<String> targetColTypes = inferTargetColumnTypes(parsed.query(), paramCount);
        preparedStatements.put(parsed.statementName(),
            new PreparedStatement(parsed.query(), paramCount, parsed.paramTypeOids(), targetColTypes));
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

        List<Object> typedParams = new ArrayList<>();
        byte[][] rawValues = bind.paramValues();
        int[] formatCodes = bind.paramFormatCodes();
        for (int i = 0; i < rawValues.length; i++) {
            byte[] rawVal = rawValues[i];
            int formatCode = (i < formatCodes.length) ? formatCodes[i] : 0;
            int typeOid = (stmt.paramTypeOids() != null && i < stmt.paramTypeOids().length) ? stmt.paramTypeOids()[i] : 0;
            String targetColType = (stmt.targetColumnTypes() != null && i < stmt.targetColumnTypes().size()) ? stmt.targetColumnTypes().get(i) : null;
            Object typed = StdBinaryDecoder.decode(rawVal, formatCode, typeOid, targetColType);
            typedParams.add(typed);
        }

        QueryResult result;
        try {
            result = db.execute(stmt.query(), typedParams);
        } catch (Exception e) {
            result = QueryResult.error(e.getMessage());
        }
        portals.put(bind.portalName(), new Portal(stmt.query(), typedParams, result));
        StdWireMessages.writeBindComplete(out);
        out.flush();

        if (result.isSuccess()) {
            return server.updateTransactionState(stmt.query(), inTransaction);
        }
        return inTransaction || server.isTransactionControl(stmt.query());
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
                int oid = 0;
                if (stmt.paramTypeOids() != null && i < stmt.paramTypeOids().length && stmt.paramTypeOids()[i] != 0) {
                    oid = stmt.paramTypeOids()[i];
                } else if (stmt.targetColumnTypes() != null && i < stmt.targetColumnTypes().size() && stmt.targetColumnTypes().get(i) != null) {
                    oid = StdBinaryDecoder.oidFromColumnType(stmt.targetColumnTypes().get(i));
                }
                paramOids.add(oid != 0 ? oid : 25);
            }
            StdWireMessages.writeParameterDescription(out, paramOids);
            // No row shape to honestly report before any values are bound and the statement actually runs
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
                StdWireMessages.writeRowDescription(out, server.describeColumns(rows, portal.result().getColumnNames()));
            } else {
                StdWireMessages.writeNoData(out);
            }
        }
        out.flush();
    }

    private boolean handleExecute(StdWireMessages.TypedMessage msg, DataOutputStream out, boolean inTransaction) throws IOException {
        StdWireMessages.ExecuteMessage execute = StdWireMessages.readExecuteMessage(msg);
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
            StdWireMessages.writeErrorResponse(out, result.getError(), result.getSqlState());
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
            StdWireMessages.writeCommandComplete(out, server.buildCommandTag(portal.query(), rows.size()));
        } else {
            StdWireMessages.writeCommandComplete(out, server.buildCommandTag(portal.query(), server.extractAffectedCount(result.getMessage())));
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

    /** The number of distinct $N placeholders in a query. */
    private int countDistinctPlaceholders(String query) {
        Matcher matcher = PARAM_PLACEHOLDER.matcher(query);
        int max = 0;
        while (matcher.find()) {
            max = Math.max(max, Integer.parseInt(matcher.group(1)));
        }
        return max;
    }

    private List<String> inferTargetColumnTypes(String query, int paramCount) {
        List<String> types = new ArrayList<>(paramCount);
        for (int i = 0; i < paramCount; i++) {
            types.add(null);
        }
        try {
            com.stratosdb.sql.parser.SqlParser parser = new com.stratosdb.sql.parser.SqlParser();
            com.stratosdb.sql.ast.Statement stmt = parser.parse(query);
            if (stmt instanceof com.stratosdb.sql.ast.InsertStatement ins) {
                String tbl = ins.tableName();
                Map<String, String> colTypes = db.getTableColumnTypes(tbl);
                List<String> cols = (ins.columns() != null && !ins.columns().isEmpty()) ? ins.columns() : db.getTableColumns(tbl);
                for (List<String> row : ins.rows()) {
                    for (int c = 0; c < row.size() && c < cols.size(); c++) {
                        String val = row.get(c);
                        if (val != null && val.startsWith("$") && val.length() > 1 && Character.isDigit(val.charAt(1))) {
                            int idx = Integer.parseInt(val.substring(1)) - 1;
                            if (idx >= 0 && idx < types.size()) {
                                types.set(idx, colTypes.get(cols.get(c)));
                            }
                        }
                    }
                }
            } else if (stmt instanceof com.stratosdb.sql.ast.UpdateStatement upd) {
                String tbl = upd.tableName();
                Map<String, String> colTypes = db.getTableColumnTypes(tbl);
                for (com.stratosdb.sql.ast.Assignment a : upd.assignments()) {
                    String val = a.value();
                    if (val != null && val.startsWith("$") && val.length() > 1 && Character.isDigit(val.charAt(1))) {
                        int idx = Integer.parseInt(val.substring(1)) - 1;
                        if (idx >= 0 && idx < types.size()) {
                            types.set(idx, colTypes.get(a.column()));
                        }
                    }
                }
                collectWhereParamTypes(upd.where(), colTypes, types);
            } else if (stmt instanceof com.stratosdb.sql.ast.SelectStatement sel) {
                if (sel.tableName() != null) {
                    Map<String, String> colTypes = db.getTableColumnTypes(sel.tableName());
                    collectWhereParamTypes(sel.where(), colTypes, types);
                }
            } else if (stmt instanceof com.stratosdb.sql.ast.DeleteStatement del) {
                Map<String, String> colTypes = db.getTableColumnTypes(del.tableName());
                collectWhereParamTypes(del.where(), colTypes, types);
            }
        } catch (Exception ignored) {}
        return types;
    }

    private void collectWhereParamTypes(com.stratosdb.sql.ast.WhereExpr where, Map<String, String> colTypes, List<String> types) {
        if (where == null || colTypes == null) return;
        if (where instanceof com.stratosdb.sql.ast.WhereExpr.Comparison cmp) {
            String lit = cmp.literal();
            if (lit != null && lit.startsWith("$") && lit.length() > 1 && Character.isDigit(lit.charAt(1))) {
                int idx = Integer.parseInt(lit.substring(1)) - 1;
                if (idx >= 0 && idx < types.size()) {
                    types.set(idx, colTypes.get(cmp.column()));
                }
            }
        } else if (where instanceof com.stratosdb.sql.ast.WhereExpr.And and) {
            collectWhereParamTypes(and.left(), colTypes, types);
            collectWhereParamTypes(and.right(), colTypes, types);
        } else if (where instanceof com.stratosdb.sql.ast.WhereExpr.Or or) {
            collectWhereParamTypes(or.left(), colTypes, types);
            collectWhereParamTypes(or.right(), colTypes, types);
        } else if (where instanceof com.stratosdb.sql.ast.WhereExpr.Not not) {
            collectWhereParamTypes(not.inner(), colTypes, types);
        }
    }
}
