package com.stratosdb.cli;

import com.stratosdb.common.constants.ProtocolConstants;
import com.stratosdb.network.auth.ScramClient;
import com.stratosdb.network.stdwire.StdWireMessages;

import java.io.BufferedOutputStream;
import java.io.Console;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;

/**
 * StratosDB's own real backup/restore tool - the actual, concrete answer
 * to this project's own honestly-named "30 years of Postgres tooling,
 * no plan closes that gap at once" note: pick ONE thing Postgres has
 * always had and this engine didn't, and build it for real, rather than
 * a token gesture. Every real database needs a way to get its data back
 * out in a form that can rebuild it from nothing - without this, running
 * StratosDB anywhere that matters is a genuine risk, not a convenience
 * gap.
 *
 * Modeled directly on pg_dump's own plain-SQL output format (the
 * default, most portable one - not pg_dump's own binary custom/directory
 * formats, which need pg_restore specifically; plain SQL just needs any
 * SQL client, exactly matching how this engine's own stdsql already
 * works): a real client connecting over the real wire protocol (the
 * same protocol stdsql uses, not a shortcut reading the server's own
 * data files directly - this tool has no more access to a running
 * StratosDB instance than any other real client would), running
 * `SHOW CATALOG` to read back the exact, original CREATE statement text
 * this engine already persists for every schema object (see
 * ShowCatalogStatement's own javadoc), then `SELECT *` against every
 * table to serialize its data as real INSERT statements.
 *
 * Restoring a dump needs no separate tool at all - the output is
 * ordinary, valid SQL. Feed it back in with:
 *   stdsql -h host -p port -U user -d database < dump.sql
 * the same way `psql -f dump.sql` restores a real, plain-format pg_dump.
 *
 * Real, honestly-stated scope and limitations:
 *   - Every INSERT statement's own string-vs-numeric quoting decision is
 *     made by parsing each column's declared type out of the table's own
 *     CREATE TABLE text (see parseColumnTypes) - not from the wire
 *     protocol's own RowDescription type OIDs, which this engine does
 *     not populate with meaningful Postgres-compatible values today.
 *   - One INSERT statement per row, not pg_dump's own default COPY
 *     format or multi-row INSERT batching - slower to restore on a very
 *     large table, but simpler, and this engine has no COPY protocol
 *     support to target regardless.
 *   - No selective dump (a single table, a single schema) yet - always
 *     the whole, current database. No compression. Real, separate,
 *     further pieces of tooling work, not attempted in this first round.
 */
public class StratosDump {
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    public StratosDump(String host, int port, String user, String database, String password) throws IOException {
        this(host, port, user, database, password, 0);
    }

    /**
     * socketTimeoutMillis: 0 (the default constructor above) preserves
     * this class's own original, no-timeout behavior exactly - suitable
     * for a real, one-off, reliable operation like PitrBackup's own
     * CHECKPOINT call, where waiting as long as it takes is correct.
     * A positive value applies both a real connect timeout and a real
     * per-read timeout - added specifically for StratosHa's own repeated
     * health checks, where a hung connection (the exact real race a
     * primary genuinely crashing mid-response can create - found by
     * testing StratosHa's own real failover scenario repeatedly, not by
     * inspection: an intermittent, timing-dependent hang, not a
     * deterministic one) must fail fast rather than block the watchdog's
     * own health-check loop indefinitely.
     */
    public StratosDump(String host, int port, String user, String database, String password, int socketTimeoutMillis) throws IOException {
        this.socket = new Socket();
        if (socketTimeoutMillis > 0) {
            this.socket.connect(new java.net.InetSocketAddress(host, port), socketTimeoutMillis);
            this.socket.setSoTimeout(socketTimeoutMillis);
        } else {
            this.socket.connect(new java.net.InetSocketAddress(host, port));
        }
        this.in = new DataInputStream(new java.io.BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        StdWireMessages.writeStartupMessage(out, user, database);
        readStartupResponses(user, password);
    }

    // --- Connection + SCRAM auth: intentionally a separate, self-contained copy of
    // stdsql's own proven-working logic (see StdSql.java), not a shared refactor of
    // it - stdsql's own version is print-oriented (writes straight to System.out);
    // this tool needs the same bytes back as structured data instead, and touching
    // StdSql itself risked destabilizing an already-working, already-tested client
    // for no real benefit.

    private void readStartupResponses(String user, String password) throws IOException {
        while (true) {
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
            switch (msg.type()) {
                case 'R' -> {
                    int authCode = readAuthCode(msg);
                    if (authCode == 10) {
                        performScramHandshake(user, password);
                    }
                }
                case 'S', 'K' -> { /* ParameterStatus / BackendKeyData - not needed here */ }
                case 'Z' -> {
                    return;
                }
                case 'E' -> throw new IOException("Server rejected startup: " + extractErrorMessage(msg));
                default -> { /* ignore */ }
            }
        }
    }

    private void performScramHandshake(String username, String password) throws IOException {
        if (password == null) {
            password = promptForPassword(username);
        }
        ScramClient scram = new ScramClient(username, password);
        String clientFirstMessage = scram.buildClientFirstMessage();
        writeSaslInitialResponse(clientFirstMessage);

        StdWireMessages.TypedMessage continueMsg = StdWireMessages.readTypedMessage(in);
        if (continueMsg.type() != 'R' || readAuthCode(continueMsg) != 11) {
            throw new IOException("Expected AuthenticationSASLContinue during SCRAM handshake");
        }
        String serverFirstMessage = new String(continueMsg.body(), 4, continueMsg.body().length - 4, StandardCharsets.UTF_8);

        String clientFinalMessage = scram.buildClientFinalMessage(serverFirstMessage);
        byte[] cfBytes = clientFinalMessage.getBytes(StandardCharsets.UTF_8);
        out.writeByte('p');
        out.writeInt(cfBytes.length + 4);
        out.write(cfBytes);
        out.flush();

        StdWireMessages.TypedMessage finalMsg = StdWireMessages.readTypedMessage(in);
        if (finalMsg.type() == 'E') {
            throw new IOException("Authentication failed: " + extractErrorMessage(finalMsg));
        }
        if (finalMsg.type() != 'R' || readAuthCode(finalMsg) != 12) {
            throw new IOException("Expected AuthenticationSASLFinal during SCRAM handshake");
        }
        String serverFinalMessage = new String(finalMsg.body(), 4, finalMsg.body().length - 4, StandardCharsets.UTF_8);
        if (!scram.verifyServerFinalMessage(serverFinalMessage)) {
            throw new IOException("Server's SCRAM signature did not verify - possible impersonation, aborting");
        }
    }

    private void writeSaslInitialResponse(String clientFirstMessage) throws IOException {
        byte[] mechanismBytes = com.stratosdb.network.auth.ScramSha256.MECHANISM_NAME.getBytes(StandardCharsets.UTF_8);
        byte[] dataBytes = clientFirstMessage.getBytes(StandardCharsets.UTF_8);
        int bodyLen = mechanismBytes.length + 1 + 4 + dataBytes.length;
        out.writeByte('p');
        out.writeInt(bodyLen + 4);
        out.write(mechanismBytes);
        out.writeByte(0);
        out.writeInt(dataBytes.length);
        out.write(dataBytes);
        out.flush();
    }

    private String promptForPassword(String username) throws IOException {
        Console console = System.console();
        String promptText = "Password for user " + username + ": ";
        if (console != null) {
            char[] chars = console.readPassword(promptText);
            return chars == null ? "" : new String(chars);
        }
        System.out.print(promptText);
        System.out.flush();
        Scanner scanner = new Scanner(System.in);
        return scanner.hasNextLine() ? scanner.nextLine() : "";
    }

    private static int readAuthCode(StdWireMessages.TypedMessage msg) {
        byte[] body = msg.body();
        return ((body[0] & 0xFF) << 24) | ((body[1] & 0xFF) << 16) | ((body[2] & 0xFF) << 8) | (body[3] & 0xFF);
    }

    private String extractErrorMessage(StdWireMessages.TypedMessage msg) {
        byte[] body = msg.body();
        int pos = 0;
        while (pos < body.length && body[pos] != 0) {
            char field = (char) body[pos];
            pos++;
            int start = pos;
            while (body[pos] != 0) pos++;
            String value = new String(body, start, pos - start, StandardCharsets.UTF_8);
            pos++;
            if (field == 'M') return value;
        }
        return "unknown error";
    }

    /** A simple-query result, structured for programmatic use rather than printed - the actual, real difference from StdSql's own runSimpleQuery. */
    private record QueryResult(List<String> columnNames, List<List<String>> rows, String error) {
        boolean isSuccess() { return error == null; }
    }

    /**
     * A small, public wrapper around the private runQuery below - added so
     * this class's own already-working, real SCRAM-authenticated
     * connection logic can be reused directly by tests that need a real
     * wire-protocol client (see GrantPrivilegeEndToEndTest), rather than
     * a third, separate, duplicate SCRAM client implementation existing
     * purely for test purposes.
     */
    public String executeSql(String sql) throws IOException {
        QueryResult result = runQuery(sql);
        return result.isSuccess() ? null : result.error();
    }

    private QueryResult runQuery(String sql) throws IOException {
        StdWireMessages.writeQuery(out, sql);
        List<String> columnNames = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        String error = null;
        while (true) {
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
            switch (msg.type()) {
                case 'T' -> columnNames.addAll(parseRowDescriptionNames(msg));
                case 'D' -> rows.add(parseDataRowValues(msg));
                case 'E' -> error = extractErrorMessage(msg);
                case 'C', 'I' -> { /* CommandComplete / EmptyQueryResponse - nothing further needed */ }
                case 'Z' -> {
                    return new QueryResult(columnNames, rows, error);
                }
                default -> { /* ignore */ }
            }
        }
    }

    private List<String> parseRowDescriptionNames(StdWireMessages.TypedMessage msg) {
        byte[] body = msg.body();
        int columnCount = ((body[0] & 0xFF) << 8) | (body[1] & 0xFF);
        List<String> names = new ArrayList<>();
        int pos = 2;
        for (int i = 0; i < columnCount; i++) {
            int nameStart = pos;
            while (body[pos] != 0) pos++;
            names.add(new String(body, nameStart, pos - nameStart, StandardCharsets.UTF_8));
            pos++;
            pos += 4 + 2 + 4 + 2 + 4 + 2;
        }
        return names;
    }

    /** A NULL column comes back as a real null String element (not the literal text "NULL", which a real NULL-valued VARCHAR column could legitimately contain) - the wire protocol's own -1 length marker is the actual, unambiguous signal for this, and this tool preserves that distinction all the way through to how it writes SQL NULL vs a quoted 'NULL' string back out. */
    private List<String> parseDataRowValues(StdWireMessages.TypedMessage msg) {
        byte[] body = msg.body();
        int columnCount = ((body[0] & 0xFF) << 8) | (body[1] & 0xFF);
        List<String> values = new ArrayList<>();
        int pos = 2;
        for (int i = 0; i < columnCount; i++) {
            int len = ((body[pos] & 0xFF) << 24) | ((body[pos + 1] & 0xFF) << 16) | ((body[pos + 2] & 0xFF) << 8) | (body[pos + 3] & 0xFF);
            pos += 4;
            if (len == -1) {
                values.add(null);
            } else {
                values.add(new String(body, pos, len, StandardCharsets.UTF_8));
                pos += len;
            }
        }
        return values;
    }

    public void close() {
        try {
            StdWireMessages.writeTerminate(out);
        } catch (IOException ignored) {
        }
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    // --- The actual dump logic ---

    /**
     * The real dependency order a restore needs, worked out by hand
     * from this engine's own actual object relationships (not copied
     * from pg_dump's own, since this engine's own object set and their
     * dependencies differ): sequences before tables (a column default
     * can reference nextval('seq')), tables before their data, data
     * before indexes (so a bulk load isn't paying to maintain an index
     * row by row), functions/procedures before triggers (a trigger
     * names its own handler at CREATE time), and extensions before any
     * native function that references one.
     */
    private static final List<String> DEPENDENCY_ORDER = List.of(
        "EXTENSION", "SEQUENCE", "TABLE", "__DATA__", "INDEX", "FUNCTION", "NATIVEFUNCTION", "PROCEDURE", "VIEW", "TRIGGER"
    );

    public void dump(PrintWriter writer) throws IOException {
        QueryResult catalog = runQuery("SHOW CATALOG");
        if (!catalog.isSuccess()) {
            throw new IOException("SHOW CATALOG failed: " + catalog.error());
        }

        Map<String, List<String[]>> byType = new LinkedHashMap<>();
        List<String> tableNames = new ArrayList<>();
        Map<String, String> tableDdl = new LinkedHashMap<>();
        for (List<String> row : catalog.rows()) {
            String objectType = row.get(0);
            String objectName = row.get(1);
            String ddlSql = row.get(2);
            byType.computeIfAbsent(objectType, k -> new ArrayList<>()).add(new String[]{objectName, ddlSql});
            if (objectType.equals("TABLE")) {
                tableNames.add(objectName);
                tableDdl.put(objectName, ddlSql);
            }
        }

        writer.println("-- StratosDB dump - generated by stratosdump");
        writer.println("-- Restore with: stdsql -h host -p port -U user -d database < this_file.sql");
        writer.println();

        for (String type : DEPENDENCY_ORDER) {
            if (type.equals("__DATA__")) {
                for (String tableName : tableNames) {
                    dumpTableData(writer, tableName, tableDdl.get(tableName));
                }
                continue;
            }
            List<String[]> objects = byType.get(type);
            if (objects == null) continue;
            for (String[] object : objects) {
                writer.println(ensureTrailingSemicolon(object[1]));
            }
            if (!objects.isEmpty()) writer.println();
        }

        writer.flush();
    }

    private String ensureTrailingSemicolon(String sql) {
        String trimmed = sql.trim();
        return trimmed.endsWith(";") ? trimmed : trimmed + ";";
    }

    private void dumpTableData(PrintWriter writer, String tableName, String createTableDdl) throws IOException {
        List<Map.Entry<String, String>> columns = parseColumnTypes(createTableDdl);
        List<String> columnNames = columns.stream().map(Map.Entry::getKey).toList();

        QueryResult data = runQuery("SELECT " + String.join(", ", columnNames) + " FROM " + tableName);
        if (!data.isSuccess()) {
            writer.println("-- WARNING: could not dump data for " + tableName + ": " + data.error());
            return;
        }
        if (data.rows().isEmpty()) {
            return;
        }

        for (List<String> row : data.rows()) {
            StringBuilder line = new StringBuilder("INSERT INTO ").append(tableName)
                .append(" (").append(String.join(", ", columnNames)).append(") VALUES (");
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) line.append(", ");
                line.append(formatValue(row.get(i), columns.get(i).getValue()));
            }
            line.append(");");
            writer.println(line);
        }
        writer.println();
    }

    private static final java.util.Set<String> NUMERIC_TYPES = java.util.Set.of(
        "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "SERIAL", "BIGSERIAL", "DECIMAL", "DOUBLE", "FLOAT", "BOOLEAN", "BOOL"
    );

    /** Numeric/boolean types are written unquoted; everything else (VARCHAR, TEXT, CHAR, DATE/TIME/TIMESTAMP, UUID, JSON/JSONB, BYTEA/BLOB, and any array type) is quoted as a SQL string literal - the safe default, since quoting a number is still valid SQL but failing to quote real text corrupts the dump. A real, explicit NULL (see parseDataRowValues) is written as the bare SQL keyword, never a quoted 'NULL' string. */
    private String formatValue(String value, String declaredType) {
        if (value == null) {
            return "NULL";
        }
        String baseType = declaredType.split("\\(")[0].replace("[]", "").trim().toUpperCase(Locale.ROOT);
        if (NUMERIC_TYPES.contains(baseType)) {
            return value;
        }
        return "'" + value.replace("'", "''") + "'";
    }

    /**
     * Extracts (columnName -> declaredType) pairs, in order, from a real
     * CREATE TABLE statement's own column list - tracking paren depth
     * so a type like DECIMAL(10, 2)'s own internal comma is never
     * mistaken for a column separator.
     */
    static List<Map.Entry<String, String>> parseColumnTypes(String createTableSql) {
        int openParen = createTableSql.indexOf('(');
        int closeParen = createTableSql.lastIndexOf(')');
        String columnList = createTableSql.substring(openParen + 1, closeParen);

        List<String> rawColumns = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < columnList.length(); i++) {
            char c = columnList.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) {
                rawColumns.add(columnList.substring(start, i));
                start = i + 1;
            }
        }
        rawColumns.add(columnList.substring(start));

        List<Map.Entry<String, String>> result = new ArrayList<>();
        for (String raw : rawColumns) {
            String trimmed = raw.trim();
            int spaceIdx = trimmed.indexOf(' ');
            if (spaceIdx < 0) continue; // defensive - a malformed/unexpected column definition, skip rather than crash the whole dump
            String columnName = trimmed.substring(0, spaceIdx);
            String rest = trimmed.substring(spaceIdx + 1).trim();
            result.add(Map.entry(columnName, rest));
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = ProtocolConstants.DEFAULT_STDWIRE_PORT;
        String user = System.getProperty("user.name", "stratos");
        String database = null;
        String outputFile = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-h" -> host = requireArg(args, ++i, "-h");
                case "-p" -> port = Integer.parseInt(requireArg(args, ++i, "-p"));
                case "-U" -> user = requireArg(args, ++i, "-U");
                case "-d" -> database = requireArg(args, ++i, "-d");
                case "-f" -> outputFile = requireArg(args, ++i, "-f");
                default -> {
                    if (database == null && !args[i].startsWith("-")) {
                        database = args[i];
                    } else {
                        System.err.println("Unrecognized argument: " + args[i]);
                        System.err.println("Usage: stratosdump -h host -p port -U user -d database [-f outputfile]");
                        return;
                    }
                }
            }
        }
        if (database == null) {
            database = user;
        }
        String password = System.getenv("STDSQL_PASSWORD");

        try {
            StratosDump dump = new StratosDump(host, port, user, database, password);
            try (PrintWriter writer = outputFile != null
                ? new PrintWriter(new java.io.FileWriter(outputFile))
                : new PrintWriter(System.out)) {
                dump.dump(writer);
            }
            dump.close();
            if (outputFile != null) {
                System.err.println("Dump written to " + outputFile);
            }
        } catch (IOException e) {
            System.err.println("Could not connect to StratosDB stdwire server at " + host + ":" + port + " - " + e.getMessage());
        }
    }

    private static String requireArg(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException(flag + " requires a value");
        }
        return args[index];
    }
}
