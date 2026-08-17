package com.stratosdb.cli;

import com.stratosdb.common.constants.ProtocolConstants;
import com.stratosdb.network.stdwire.StdWireMessages;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * StratosDB's own command-line client for the stdwire protocol - a real,
 * native implementation of the same PostgreSQL wire protocol v3 the
 * server speaks, not a wrapper around the JDBC driver (that's what
 * StratosShell already is, and it exercises a different path entirely).
 *
 * This exists for two real reasons: first, as this project's own
 * independently-branded tool (matching stdwire's own naming - see
 * StdWireServer's javadoc - rather than asking anyone to reach for
 * `psql` to talk to StratosDB); second, as a test client whose extended
 * query protocol (Parse/Bind/Describe/Execute/Sync) usage can be driven
 * explicitly via the \bind command below, since psql's own interactive
 * prompt doesn't expose a simple way to trigger that path on demand.
 *
 * Two input forms:
 *   - A plain SQL statement -> sent as a single simple-query ('Q') message.
 *   - "\bind SQL_WITH_$N_PLACEHOLDERS | value1 | value2 | ..." -> drives
 *     the full extended protocol: Parse (unnamed statement) -> Bind
 *     (unnamed portal, the given values) -> Describe(portal) -> Execute
 *     -> Sync, printing every response message along the way.
 */
public class StdSql {
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final String connectionDescription;
    private boolean running = true;

    public StdSql(String host, int port, String user, String database) throws IOException {
        this.socket = new Socket(host, port);
        this.in = new DataInputStream(new java.io.BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        this.connectionDescription = host + ":" + port + " as " + user + (database != null ? "/" + database : "");

        StdWireMessages.writeStartupMessage(out, user, database);
        readStartupResponses();
    }

    private void readStartupResponses() throws IOException {
        while (true) {
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
            switch (msg.type()) {
                case 'R' -> { /* AuthenticationOk (or a real auth challenge, not yet sent by this server - see StdWireMessages' javadoc) */ }
                case 'S', 'K' -> { /* ParameterStatus / BackendKeyData - informational, not needed for this simple client */ }
                case 'Z' -> {
                    return; // ReadyForQuery - startup is complete
                }
                case 'E' -> throw new IOException("Server rejected startup: " + extractErrorMessage(msg));
                default -> { /* ignore anything else during startup */ }
            }
        }
    }

    public void start() {
        System.out.println("StratosDB stdsql - connected to " + connectionDescription);
        System.out.println("Type SQL statements ending in a semicolon, or \\bind SQL | val1 | val2 to test the extended query protocol. \\q to quit.");

        Scanner scanner = new Scanner(System.in);
        while (running) {
            System.out.print("stdsql> ");
            if (!scanner.hasNextLine()) {
                break;
            }
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) {
                continue;
            }
            if (line.equals("\\q") || line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
                break;
            }
            try {
                if (line.startsWith("\\bind ")) {
                    runExtendedProtocol(line.substring("\\bind ".length()));
                } else {
                    runSimpleQuery(line);
                }
            } catch (IOException e) {
                System.out.println("Connection error: " + e.getMessage());
                running = false;
            }
        }
        close();
    }

    private void runSimpleQuery(String sql) throws IOException {
        StdWireMessages.writeQuery(out, sql);
        while (true) {
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
            switch (msg.type()) {
                case 'T' -> printRowDescription(msg);
                case 'D' -> printDataRow(msg);
                case 'C' -> System.out.println(msg.readCString(0));
                case 'E' -> System.out.println("ERROR: " + extractErrorMessage(msg));
                case 'I' -> System.out.println("(empty query)");
                case 'Z' -> {
                    return; // ReadyForQuery - this statement's response is complete
                }
                default -> { /* ignore anything unrecognized rather than crash the shell */ }
            }
        }
    }

    /** SQL_WITH_$N_PLACEHOLDERS | value1 | value2 | ... - splits on the FIRST '|' to separate the query from its pipe-separated parameter values; an empty value (two adjacent '|'s, or trailing) is sent as SQL NULL. */
    private void runExtendedProtocol(String spec) throws IOException {
        String[] parts = spec.split("\\|", -1);
        String query = parts[0].trim();
        String[] paramValues = new String[parts.length - 1];
        for (int i = 1; i < parts.length; i++) {
            String v = parts[i].trim();
            paramValues[i - 1] = v.isEmpty() ? null : v;
        }

        StdWireMessages.writeParse(out, "", query, new int[0]);
        StdWireMessages.writeBind(out, "", "", paramValues);
        StdWireMessages.writeDescribe(out, 'P', "");
        StdWireMessages.writeExecute(out, "", 0);
        StdWireMessages.writeSync(out);

        while (true) {
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
            switch (msg.type()) {
                case '1' -> System.out.println("ParseComplete");
                case '2' -> System.out.println("BindComplete");
                case 't' -> System.out.println("ParameterDescription (" + (msg.body().length / 4 - 1) + " parameter(s))");
                case 'n' -> System.out.println("NoData");
                case 'T' -> printRowDescription(msg);
                case 'D' -> printDataRow(msg);
                case 'C' -> System.out.println(msg.readCString(0));
                case 'E' -> System.out.println("ERROR: " + extractErrorMessage(msg));
                case 'Z' -> {
                    return; // ReadyForQuery, sent in response to Sync - this exchange is complete
                }
                default -> { /* ignore anything unrecognized rather than crash the shell */ }
            }
        }
    }

    private void printRowDescription(StdWireMessages.TypedMessage msg) {
        byte[] body = msg.body();
        int columnCount = ((body[0] & 0xFF) << 8) | (body[1] & 0xFF);
        List<String> names = new ArrayList<>();
        int pos = 2;
        for (int i = 0; i < columnCount; i++) {
            int nameStart = pos;
            while (body[pos] != 0) pos++;
            names.add(new String(body, nameStart, pos - nameStart, java.nio.charset.StandardCharsets.UTF_8));
            pos++; // skip the name's null terminator
            pos += 4 + 2 + 4 + 2 + 4 + 2; // table OID, column number, type OID, type size, type modifier, format code
        }
        System.out.println(String.join(" | ", names));
    }

    private void printDataRow(StdWireMessages.TypedMessage msg) {
        byte[] body = msg.body();
        int columnCount = ((body[0] & 0xFF) << 8) | (body[1] & 0xFF);
        List<String> values = new ArrayList<>();
        int pos = 2;
        for (int i = 0; i < columnCount; i++) {
            int len = ((body[pos] & 0xFF) << 24) | ((body[pos + 1] & 0xFF) << 16) | ((body[pos + 2] & 0xFF) << 8) | (body[pos + 3] & 0xFF);
            pos += 4;
            if (len == -1) {
                values.add("NULL");
            } else {
                values.add(new String(body, pos, len, java.nio.charset.StandardCharsets.UTF_8));
                pos += len;
            }
        }
        System.out.println(String.join(" | ", values));
    }

    private String extractErrorMessage(StdWireMessages.TypedMessage msg) {
        byte[] body = msg.body();
        int pos = 0;
        while (pos < body.length && body[pos] != 0) {
            char field = (char) body[pos];
            pos++;
            int start = pos;
            while (body[pos] != 0) pos++;
            String value = new String(body, start, pos - start, java.nio.charset.StandardCharsets.UTF_8);
            pos++;
            if (field == 'M') {
                return value;
            }
        }
        return "unknown error";
    }

    private void close() {
        try {
            StdWireMessages.writeTerminate(out);
        } catch (IOException ignored) {
        }
        try {
            socket.close();
        } catch (IOException ignored) {
        }
        System.out.println("Connection closed.");
    }

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : ProtocolConstants.DEFAULT_STDWIRE_PORT;
        String user = args.length > 2 ? args[2] : "stratos";
        String database = args.length > 3 ? args[3] : "stratosdb";

        try {
            StdSql client = new StdSql(host, port, user, database);
            client.start();
        } catch (IOException e) {
            System.out.println("Could not connect to StratosDB stdwire server at " + host + ":" + port + " - " + e.getMessage());
            System.out.println("Is the server running? Start it with stratosdb-network's StdWireServerMain first.");
        }
    }
}
