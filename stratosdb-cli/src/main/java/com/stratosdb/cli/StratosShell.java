package com.stratosdb.cli;

import java.sql.*;
import java.util.Properties;
import java.util.Scanner;

/**
 * A real network client now, not an embedded engine: connects to a running
 * StratosDB server (started separately via stratosdb-network's
 * StratosServerMain) through the JDBC driver, exactly like any other JDBC
 * client would. This replaced an earlier version that linked StratosDB
 * in-process directly - that meant the shell was never actually exercising
 * the wire protocol or the JDBC driver, which is exactly backwards for a
 * client meant to prove the network layer works.
 *
 * One real, honest consequence of this move worth stating plainly: the
 * old in-process shell could show StratosDB's own descriptive messages
 * ("Table created: users", "Inserted row at 0/0") because it printed
 * QueryResult.toString() directly. Standard JDBC's Statement interface has
 * no equivalent - execute()/executeUpdate() only expose a boolean and a row
 * count, not an arbitrary message string. So DDL and DML here now print a
 * generic "OK" or a row count, the same as any other JDBC-based SQL client
 * (psql shows "CREATE TABLE" / "INSERT 0 1" for the same reason - this
 * isn't a StratosDB limitation, it's how JDBC's contract works). SHOW
 * TABLES was fixed to return real rows instead of a message specifically
 * so it keeps working meaningfully here - see ExecutorEngine.executeShowTables.
 */
public class StratosShell {
    private final Connection connection;
    private final Statement statement;
    private final Scanner scanner;
    private final String connectionDescription;
    private boolean running = true;

    public StratosShell(String host, int port, String username, String password, boolean useTls) throws SQLException {
        String url = "jdbc:stratos://" + host + ":" + port + "/";
        Properties props = new Properties();
        if (username != null) props.setProperty("user", username);
        if (password != null) props.setProperty("password", password);
        if (useTls) props.setProperty("ssl", "true");

        this.connection = DriverManager.getConnection(url, props);
        this.statement = connection.createStatement();
        this.scanner = new Scanner(System.in);
        this.connectionDescription = host + ":" + port
            + (username != null ? " as " + username : " (no authentication)")
            + (useTls ? " [TLS]" : "");
    }

    public void start() {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║                   🚀 STRATOSDB                        ║");
        System.out.println("║            PostgreSQL-inspired Database Engine         ║");
        System.out.println("║               Reach for the Clouds ☁️                  ║");
        System.out.println("║                                                      ║");
        System.out.println("║  Connected to: " + pad(connectionDescription, 41) + "║");
        System.out.println("║                                                      ║");
        System.out.println("║  Commands:                                           ║");
        System.out.println("║  - CREATE TABLE, INSERT, SELECT, UPDATE, DELETE     ║");
        System.out.println("║  - DROP TABLE, SHOW TABLES, CREATE INDEX, EXPLAIN   ║");
        System.out.println("║  - \\dt, \\l, \\help, \\exit, \\quit                  ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println();

        while (running) {
            System.out.print("stratos> ");
            String input = scanner.nextLine().trim();

            if (input.isEmpty()) continue;

            if (input.startsWith("\\")) {
                handleMetaCommand(input);
                continue;
            }

            if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                break;
            }

            runSql(input);
        }

        System.out.println("\n🌤️  Disconnecting...");
        try {
            statement.close();
            connection.close();
        } catch (SQLException e) {
            System.out.println("(error while closing connection: " + e.getMessage() + ")");
        }
        scanner.close();
        System.out.println("Goodbye!");
    }

    private void runSql(String sql) {
        long startTime = System.currentTimeMillis();
        try {
            boolean isResultSet = statement.execute(sql);
            long duration = System.currentTimeMillis() - startTime;

            if (isResultSet) {
                try (ResultSet rs = statement.getResultSet()) {
                    printResultSet(rs);
                }
            } else {
                int updateCount = statement.getUpdateCount();
                System.out.println(updateCount < 0 ? "OK" : updateCount + " row(s) affected");
            }
            System.out.println("Time: " + duration + "ms");
        } catch (SQLException e) {
            System.out.println("ERROR: " + e.getMessage());
        }
        System.out.println();
    }

    private void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();

        System.out.println("┌─────────────────────────────────────┐");
        int rowCount = 0;
        while (rs.next()) {
            StringBuilder row = new StringBuilder("│ ");
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) row.append(", ");
                row.append(md.getColumnName(i)).append("=").append(rs.getObject(i));
            }
            System.out.println(row);
            rowCount++;
        }
        System.out.println("└─────────────────────────────────────┘");
        System.out.println("(" + rowCount + " row(s))");
    }

    private void handleMetaCommand(String cmd) {
        switch (cmd.toLowerCase()) {
            case "\\dt":
            case "\\l":
                runSql("SHOW TABLES");
                return; // runSql already prints the trailing blank line
            case "\\help":
            case "\\h":
                printHelp();
                break;
            case "\\exit":
            case "\\quit":
                running = false;
                break;
            case "\\status":
                printStatus();
                break;
            default:
                System.out.println("Unknown command: " + cmd);
                System.out.println("Try \\help for available commands");
        }
        System.out.println();
    }

    private void printHelp() {
        System.out.println("\nSQL Commands:");
        System.out.println("  CREATE TABLE <name> (<col> <type>, ...)");
        System.out.println("  CREATE INDEX <name> ON <table> (<col>)");
        System.out.println("  INSERT INTO <table> VALUES (<values>)");
        System.out.println("  SELECT <columns> FROM <table> [JOIN <table> ON <a>=<b>] [WHERE <condition>]");
        System.out.println("  UPDATE <table> SET <col>=<value> [WHERE <condition>]");
        System.out.println("  DELETE FROM <table> [WHERE <condition>]");
        System.out.println("  DROP TABLE <name>");
        System.out.println("  SHOW TABLES");
        System.out.println("  EXPLAIN <select statement>");
        System.out.println();
        System.out.println("Meta-Commands:");
        System.out.println("  \\dt           - List all tables");
        System.out.println("  \\l            - List all tables");
        System.out.println("  \\status       - Show connection status");
        System.out.println("  \\help, \\h    - Show this help");
        System.out.println("  \\exit, \\quit - Exit StratosDB");
    }

    private void printStatus() {
        System.out.println("\n📊 Connection Status");
        System.out.println("─────────────────────────────────────");
        System.out.println("  Connected to: " + connectionDescription);
        try {
            System.out.println("  Connection open: " + !connection.isClosed());
        } catch (SQLException e) {
            System.out.println("  Connection open: unknown (" + e.getMessage() + ")");
        }
        System.out.println("─────────────────────────────────────");
    }

    private static String pad(String s, int width) {
        if (s.length() >= width) return s.substring(0, width);
        StringBuilder sb = new StringBuilder(s);
        while (sb.length() < width) sb.append(' ');
        return sb.toString();
    }

    /**
     * Args: [host] [port] [username] [password] [--ssl]
     * All positional args are optional; --ssl can appear anywhere.
     * Defaults: localhost, ProtocolConstants.DEFAULT_PORT, no credentials, no TLS.
     *
     * Requires a StratosDB server already running (stratosdb-network's
     * StratosServerMain) - this no longer starts one itself.
     */
    public static void main(String[] args) throws Exception {
        java.util.List<String> positional = new java.util.ArrayList<>();
        boolean useTls = false;
        for (String arg : args) {
            if (arg.equals("--ssl")) {
                useTls = true;
            } else {
                positional.add(arg);
            }
        }

        String host = positional.size() > 0 ? positional.get(0) : "localhost";
        int port = positional.size() > 1 ? Integer.parseInt(positional.get(1)) : com.stratosdb.common.constants.ProtocolConstants.DEFAULT_PORT;
        String username = positional.size() > 2 ? positional.get(2) : null;
        String password = positional.size() > 3 ? positional.get(3) : null;

        try {
            StratosShell shell = new StratosShell(host, port, username, password, useTls);
            shell.start();
        } catch (SQLException e) {
            System.out.println("Could not connect to StratosDB at " + host + ":" + port + " - " + e.getMessage());
            System.out.println("Is the server running? Start it with stratosdb-network's StratosServerMain first.");
        }
    }
}
