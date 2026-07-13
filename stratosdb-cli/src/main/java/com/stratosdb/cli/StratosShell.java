package com.stratosdb.cli;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;

import java.util.Scanner;

public class StratosShell {
    private final StratosDB database;
    private final Scanner scanner;
    private boolean running = true;
    
    public StratosShell(String dataDirectory) {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(dataDirectory);
        this.database = new StratosDB(config);
        this.scanner = new Scanner(System.in);
    }
    
    public void start() {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║                   🚀 STRATOSDB                        ║");
        System.out.println("║            PostgreSQL-inspired Database Engine         ║");
        System.out.println("║               Reach for the Clouds ☁️                  ║");
        System.out.println("║                                                      ║");
        System.out.println("║  Commands:                                           ║");
        System.out.println("║  - CREATE TABLE, INSERT, SELECT, UPDATE, DELETE     ║");
        System.out.println("║  - DROP TABLE, SHOW TABLES                          ║");
        System.out.println("║  - \\dt, \\l, \\help, \\exit, \\quit                  ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println();
        
        while (running) {
            System.out.print("stratos> ");
            String input = scanner.nextLine().trim();
            
            if (input.isEmpty()) continue;
            
            // Handle meta-commands
            if (input.startsWith("\\")) {
                handleMetaCommand(input);
                continue;
            }
            
            if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                break;
            }
            
            // Execute SQL
            try {
                long startTime = System.currentTimeMillis();
                QueryResult result = database.execute(input);
                long duration = System.currentTimeMillis() - startTime;
                
                System.out.println(result);
                System.out.println("Time: " + duration + "ms");
                System.out.println();
            } catch (Exception e) {
                System.out.println("ERROR: " + e.getMessage());
                System.out.println();
            }
        }
        
        System.out.println("\n🌤️  StratosDB shutting down...");
        database.shutdown();
        scanner.close();
        System.out.println("Goodbye!");
    }
    
    private void handleMetaCommand(String cmd) {
        switch (cmd.toLowerCase()) {
            case "\\dt":
            case "\\l":
                System.out.println("Showing tables...");
                QueryResult result = database.execute("SHOW TABLES");
                System.out.println(result);
                break;
                
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
        System.out.println("  INSERT INTO <table> VALUES (<values>)");
        System.out.println("  SELECT <columns> FROM <table> [WHERE <condition>]");
        System.out.println("  UPDATE <table> SET <col>=<value> [WHERE <condition>]");
        System.out.println("  DELETE FROM <table> [WHERE <condition>]");
        System.out.println("  DROP TABLE <name>");
        System.out.println("  SHOW TABLES");
        System.out.println();
        System.out.println("Meta-Commands:");
        System.out.println("  \\dt           - List all tables");
        System.out.println("  \\l            - List all tables");
        System.out.println("  \\status       - Show database status");
        System.out.println("  \\help, \\h    - Show this help");
        System.out.println("  \\exit, \\quit - Exit StratosDB");
    }
    
    private void printStatus() {
        System.out.println("\n📊 StratosDB Status");
        System.out.println("─────────────────────────────────────");
        System.out.println("  Cache Hit Ratio: " + 
                String.format("%.2f%%", database.getBufferPool().getCacheHitRatio() * 100));
        System.out.println("  Cache Size: " + database.getBufferPool().getCacheSize() + " pages");
        System.out.println("  WAL LSN: " + database.getWalManager().getCurrentLSN());
        System.out.println("─────────────────────────────────────");
    }
    
    public static void main(String[] args) {
        String dataDir = args.length > 0 ? args[0] : "./stratosdb_data";
        StratosShell shell = new StratosShell(dataDir);
        shell.start();
    }
}