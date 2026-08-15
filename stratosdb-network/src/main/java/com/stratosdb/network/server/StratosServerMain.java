package com.stratosdb.network.server;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.pgwire.PgWireServer;

/**
 * Runnable entry point: starts StratosDB and a StratosServer listening for
 * network clients. Args: [dataDirectory] [port] [--pgwire[=port]] - the
 * first two are optional and positional, defaulting to "./stratosdb_data"
 * and 5432; --pgwire is an optional flag (can appear anywhere in argv)
 * that also starts a real PostgreSQL wire-protocol server (see
 * com.stratosdb.network.pgwire) against the SAME underlying database, on
 * pgwirePort if given or (port + 1) otherwise - defaulting to a different
 * port than the custom protocol's own, so both can run without a clash.
 */
public class StratosServerMain {
    public static void main(String[] args) throws Exception {
        String dataDir = "./stratosdb_data";
        Integer port = null;
        boolean pgWireRequested = false;
        Integer pgWirePort = null;

        java.util.List<String> positional = new java.util.ArrayList<>();
        for (String arg : args) {
            if (arg.equals("--pgwire")) {
                pgWireRequested = true;
            } else if (arg.startsWith("--pgwire=")) {
                pgWireRequested = true;
                pgWirePort = Integer.parseInt(arg.substring("--pgwire=".length()));
            } else {
                positional.add(arg);
            }
        }
        if (positional.size() > 0) dataDir = positional.get(0);
        if (positional.size() > 1) port = Integer.parseInt(positional.get(1));
        if (port == null) port = 5432;
        if (pgWirePort == null) pgWirePort = port + 1;

        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(dataDir);
        config.setPort(port);

        StratosDB db = new StratosDB(config);
        StratosServer server = new StratosServer(port, db);
        server.start();
        System.out.println("StratosDB listening on port " + port + " (custom protocol, data: " + dataDir + "). Ctrl+C to stop.");

        PgWireServer pgWireServer = null;
        if (pgWireRequested) {
            pgWireServer = new PgWireServer(pgWirePort, db);
            pgWireServer.start();
            System.out.println("StratosDB also listening on port " + pgWirePort + " (PostgreSQL wire protocol - connect with psql -h localhost -p " + pgWirePort + " -U anyuser -d anydb)");
        }

        PgWireServer finalPgWireServer = pgWireServer;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            server.stop();
            if (finalPgWireServer != null) finalPgWireServer.stop();
            db.shutdown();
        }));

        // Keep the main thread alive; the accept loops run on their own daemon threads.
        Thread.currentThread().join();
    }
}
