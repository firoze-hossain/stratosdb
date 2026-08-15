package com.stratosdb.network.stdwire;

import com.stratosdb.common.constants.ProtocolConstants;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;

/**
 * Runnable entry point: starts StratosDB and a StdWireServer listening for
 * real PostgreSQL wire-protocol clients (psql, pgAdmin, any pg driver).
 * Args: [dataDirectory] [port] - both optional, defaulting to
 * "./stratosdb_data" and ProtocolConstants.DEFAULT_STDWIRE_PORT (one above
 * StratosDB's own custom-protocol default, so both servers can run
 * alongside each other without a port clash).
 */
public class StdWireServerMain {
    public static void main(String[] args) throws Exception {
        String dataDir = args.length > 0 ? args[0] : "./stratosdb_data";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : ProtocolConstants.DEFAULT_STDWIRE_PORT;

        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(dataDir);

        StratosDB db = new StratosDB(config);
        StdWireServer server = new StdWireServer(port, db);
        server.start();

        System.out.println("StratosDB standard-wire server listening on port " + port + " (data: " + dataDir + "). Ctrl+C to stop.");
        System.out.println("Connect with: psql -h localhost -p " + port + " -U anyuser -d anydb");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            server.stop();
            db.shutdown();
        }));

        Thread.currentThread().join();
    }
}
