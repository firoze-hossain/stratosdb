package com.stratosdb.network.protocol;

import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * StratosDB's wire protocol: a small, custom binary protocol, not a clone
 * of the PostgreSQL wire protocol (that's named as a possible future goal
 * in PROJECT_PLAN.md, not attempted here). Every message is self-framing
 * via DataOutputStream/DataInputStream's own primitives (writeInt/readInt,
 * writeUTF/readUTF, ...) - these already block internally until enough
 * bytes have arrived over the socket, so there is no separate outer
 * length-prefix frame layered on top; it would be redundant.
 *
 * Message shapes:
 *
 * QUERY (client -> server):
 *   byte    MSG_QUERY
 *   UTF     sql text
 *
 * RESULT (server -> client):
 *   byte    MSG_RESULT
 *   boolean success
 *   if success:
 *     boolean hasRows        - true iff the server's QueryResult carried a
 *                               row list (even an empty one) rather than a
 *                               plain message. This distinguishes "SELECT
 *                               matched zero rows" from "CREATE TABLE
 *                               succeeded" without changing QueryResult's
 *                               own API - both are inferred from whether
 *                               QueryResult.getRows() is null.
 *     if hasRows:
 *       int     row count
 *       for each row:
 *         int     column count
 *         for each column:
 *           UTF     column name
 *           value   (see writeValue/readValue)
 *     else:
 *       UTF     message
 *   else:
 *     UTF     error message
 *
 * Values are tagged so the reader doesn't need to guess a type:
 *   0 = null            (no bytes follow)
 *   1 = Integer          (int)
 *   2 = Long              (long)
 *   3 = Double            (double)
 *   4 = String/other      (UTF, via toString() for anything untagged)
 *   5 = Boolean           (boolean)
 */
public final class WireProtocol {

    public static final int MSG_QUERY = 1;
    public static final int MSG_RESULT = 2;

    private WireProtocol() {}

    public static int readMessageType(DataInputStream in) throws IOException {
        return in.readUnsignedByte();
    }

    // --- QUERY ---

    public static void writeQuery(DataOutputStream out, String sql) throws IOException {
        out.writeByte(MSG_QUERY);
        out.writeUTF(sql);
        out.flush();
    }

    /** Call after readMessageType() has already consumed the type byte and confirmed MSG_QUERY. */
    public static String readQueryBody(DataInputStream in) throws IOException {
        return in.readUTF();
    }

    // --- RESULT ---

    public static void writeResult(DataOutputStream out, QueryResult result) throws IOException {
        out.writeByte(MSG_RESULT);
        out.writeBoolean(result.isSuccess());
        if (result.isSuccess()) {
            List<Tuple> rows = result.getRows();
            out.writeBoolean(rows != null);
            if (rows != null) {
                out.writeInt(rows.size());
                for (Tuple row : rows) {
                    List<String> columnNames = row.getColumnNames();
                    out.writeInt(columnNames.size());
                    for (int i = 0; i < columnNames.size(); i++) {
                        out.writeUTF(columnNames.get(i));
                        writeValue(out, row.getValue(i));
                    }
                }
            } else {
                String message = result.getMessage();
                out.writeUTF(message == null ? "" : message);
            }
        } else {
            String error = result.getError();
            out.writeUTF(error == null ? "" : error);
        }
        out.flush();
    }

    /** Call after readMessageType() has already consumed the type byte and confirmed MSG_RESULT. */
    public static QueryResult readResultBody(DataInputStream in) throws IOException {
        boolean success = in.readBoolean();
        if (!success) {
            return QueryResult.error(in.readUTF());
        }

        boolean hasRows = in.readBoolean();
        if (!hasRows) {
            return QueryResult.success(in.readUTF());
        }

        int rowCount = in.readInt();
        List<Tuple> rows = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            int colCount = in.readInt();
            Tuple tuple = new Tuple();
            for (int c = 0; c < colCount; c++) {
                String name = in.readUTF();
                tuple.addValue(name, readValue(in));
            }
            rows.add(tuple);
        }
        return QueryResult.success(rows);
    }

    private static void writeValue(DataOutputStream out, Object value) throws IOException {
        if (value == null) {
            out.writeByte(0);
        } else if (value instanceof Integer i) {
            out.writeByte(1);
            out.writeInt(i);
        } else if (value instanceof Long l) {
            out.writeByte(2);
            out.writeLong(l);
        } else if (value instanceof Double d) {
            out.writeByte(3);
            out.writeDouble(d);
        } else if (value instanceof Boolean b) {
            out.writeByte(5);
            out.writeBoolean(b);
        } else {
            out.writeByte(4);
            out.writeUTF(value.toString());
        }
    }

    private static Object readValue(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        return switch (tag) {
            case 0 -> null;
            case 1 -> in.readInt();
            case 2 -> in.readLong();
            case 3 -> in.readDouble();
            case 4 -> in.readUTF();
            case 5 -> in.readBoolean();
            default -> throw new IOException("Unknown wire value tag: " + tag
                + " - client and server protocol versions may be mismatched");
        };
    }
}
