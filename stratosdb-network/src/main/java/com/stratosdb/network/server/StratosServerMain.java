package com.stratosdb.network.server;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;

/**
 * Runnable entry point: starts StratosDB and a StratosServer listening for
 * network clients. Args: [dataDirectory] [port] - both optional, defaulting
 * to "./stratosdb_data" and 5432.
 */
public class StratosServerMain {
    public static void main(String[] args) throws Exception {
        String dataDir = args.length > 0 ? args[0] : "./stratosdb_data";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5432;

        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(dataDir);
        config.setPort(port);

        StratosDB db = new StratosDB(config);
        StratosServer server = new StratosServer(port, db);
        server.start();

        System.out.println("StratosDB listening on port " + port + " (data: " + dataDir + "). Ctrl+C to stop.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            server.stop();
            db.shutdown();
        }));

        // Keep the main thread alive; the accept loop runs on its own daemon thread.
        Thread.currentThread().join();
    }
}
