package com.stratosdb.network.pgwire;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;

/**
 * Runnable entry point: starts StratosDB and a PgWireServer listening for
 * real PostgreSQL wire-protocol clients (psql, pgAdmin, any pg driver).
 * Args: [dataDirectory] [port] - both optional, defaulting to
 * "./stratosdb_data" and 5433 (not 5432, so this can run alongside a real
 * Postgres or StratosDB's own custom-protocol server on the same machine
 * without a port clash).
 */
public class PgWireServerMain {
    public static void main(String[] args) throws Exception {
        String dataDir = args.length > 0 ? args[0] : "./stratosdb_data";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5433;

        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(dataDir);

        StratosDB db = new StratosDB(config);
        PgWireServer server = new PgWireServer(port, db);
        server.start();

        System.out.println("StratosDB pg-wire server listening on port " + port + " (data: " + dataDir + "). Ctrl+C to stop.");
        System.out.println("Connect with: psql -h localhost -p " + port + " -U anyuser -d anydb");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            server.stop();
            db.shutdown();
        }));

        Thread.currentThread().join();
    }
}
