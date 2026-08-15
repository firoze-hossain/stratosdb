package com.stratosdb.network.server;

import com.stratosdb.common.constants.ProtocolConstants;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireServer;

/**
 * Runnable entry point: starts StratosDB and a StratosServer listening for
 * network clients. Args: [dataDirectory] [port] [--stdwire[=port]] - the
 * first two are optional and positional, defaulting to "./stratosdb_data"
 * and ProtocolConstants.DEFAULT_PORT; --stdwire is an optional flag (can
 * appear anywhere in argv) that also starts a real PostgreSQL
 * wire-protocol-v3-compatible server (see com.stratosdb.network.stdwire)
 * against the SAME underlying database, on the given port if provided
 * (--stdwire=PORT) or (port + 1) otherwise, so both can run without a clash.
 */
public class StratosServerMain {
    public static void main(String[] args) throws Exception {
        String dataDir = "./stratosdb_data";
        Integer port = null;
        boolean stdWireRequested = false;
        Integer stdWirePort = null;

        java.util.List<String> positional = new java.util.ArrayList<>();
        for (String arg : args) {
            if (arg.equals("--stdwire")) {
                stdWireRequested = true;
            } else if (arg.startsWith("--stdwire=")) {
                stdWireRequested = true;
                stdWirePort = Integer.parseInt(arg.substring("--stdwire=".length()));
            } else {
                positional.add(arg);
            }
        }
        if (positional.size() > 0) dataDir = positional.get(0);
        if (positional.size() > 1) port = Integer.parseInt(positional.get(1));
        if (port == null) port = ProtocolConstants.DEFAULT_PORT;
        if (stdWirePort == null) stdWirePort = port + 1;

        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(dataDir);
        config.setPort(port);

        StratosDB db = new StratosDB(config);
        StratosServer server = new StratosServer(port, db);
        server.start();
        System.out.println("StratosDB listening on port " + port + " (custom protocol, data: " + dataDir + "). Ctrl+C to stop.");

        StdWireServer stdWireServer = null;
        if (stdWireRequested) {
            stdWireServer = new StdWireServer(stdWirePort, db);
            stdWireServer.start();
            System.out.println("StratosDB also listening on port " + stdWirePort + " (PostgreSQL wire protocol - connect with psql -h localhost -p " + stdWirePort + " -U anyuser -d anydb)");
        }

        StdWireServer finalStdWireServer = stdWireServer;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            server.stop();
            if (finalStdWireServer != null) finalStdWireServer.stop();
            db.shutdown();
        }));

        // Keep the main thread alive; the accept loops run on their own daemon threads.
        Thread.currentThread().join();
    }
}
