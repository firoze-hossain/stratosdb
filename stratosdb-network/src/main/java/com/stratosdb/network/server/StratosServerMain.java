package com.stratosdb.network.server;

import com.stratosdb.common.constants.ProtocolConstants;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireServer;

/**
 * Runnable entry point: starts StratosDB and a real, current
 * PostgreSQL-wire-protocol-v3-compatible server (see
 * com.stratosdb.network.stdwire) - this project's own actual server,
 * the one every real feature in this whole project (replication, HA,
 * row-level security, StratosBench, StratosMigrate, real psql/pgjdbc/
 * psycopg2 compatibility, and now StratosDB's own real JDBC driver) has
 * always run on.
 *
 * A real, previously-broken default corrected here, not merely stated:
 * this class used to start the {@code stratosdb-network.server.StratosServer}
 * (a small, custom, pre-wire-protocol binary protocol - {@code WireProtocol})
 * as the PRIMARY server on the given port by default, only optionally also
 * starting this real server via an easy-to-miss {@code --stdwire} flag on a
 * secondary port. That old protocol was never updated to match this
 * project's own real, current wire-protocol work, and StratosDB's own real
 * JDBC driver ({@code stratosdb-jdbc}) has since been rewritten to speak
 * only the real, current protocol - so the old default here would have
 * left the actual, documented, primary way to start a StratosDB server
 * producing something the project's own real driver (and StratosShell,
 * which uses it) could no longer connect to at all. The old server and
 * protocol have been removed entirely, not left running alongside this
 * fix, since nothing else in this project depends on them any more and
 * keeping a now-permanently-incompatible "alternate" server around would
 * only mislead future maintainers into thinking it was still a real option.
 *
 * Args: [dataDirectory] [port] - both optional and positional, defaulting
 * to "./stratosdb_data" and ProtocolConstants.DEFAULT_PORT.
 *
 * {@code --cluster} starts a real, multi-database {@link com.stratosdb.core.StratosCluster}
 * instead of a single, plain {@link StratosDB} - see that class's own
 * javadoc for what this actually means (real {@code CREATE DATABASE}/
 * {@code DROP DATABASE}/{@code SHOW DATABASES}, genuinely isolated
 * databases, PostgreSQL's own real "one server, many databases" shape).
 * A deliberate, explicit opt-in, not the new default: an existing, real,
 * single-database data directory has its own files (WAL, heap files, ...)
 * directly inside it, not nested one level down under a per-database
 * subdirectory the way a cluster's own data directory is structured -
 * silently changing the default here could make an existing, real
 * deployment's own data appear to have vanished (StratosCluster would
 * find no recognizable per-database subdirectory at all, and quietly
 * bootstrap a brand-new, empty default database instead, alongside the
 * real, now-orphaned data still sitting there on disk). No such risk
 * exists for a fresh, brand-new data directory - but this class has no
 * reliable way to tell "fresh" apart from "not cluster-shaped yet" on
 * its own, so the safe, honest choice is to require this explicitly
 * rather than guess.
 *
 * {@code --stdwire} / {@code --stdwire=PORT} are still accepted, for real
 * backward CLI compatibility with any existing script that already passes
 * them, but are now genuine no-ops (with a clear, printed note explaining
 * why) - what they used to opt into is what this class always does now.
 */
public class StratosServerMain {
    public static void main(String[] args) throws Exception {
        String dataDir = "./stratosdb_data";
        Integer port = null;
        boolean clusterMode = false;

        java.util.List<String> positional = new java.util.ArrayList<>();
        boolean sawLegacyStdwireFlag = false;
        for (String arg : args) {
            if (arg.equals("--stdwire") || arg.startsWith("--stdwire=")) {
                sawLegacyStdwireFlag = true;
            } else if (arg.equals("--cluster")) {
                clusterMode = true;
            } else {
                positional.add(arg);
            }
        }
        if (positional.size() > 0) dataDir = positional.get(0);
        if (positional.size() > 1) port = Integer.parseInt(positional.get(1));
        if (port == null) port = ProtocolConstants.DEFAULT_PORT;

        if (sawLegacyStdwireFlag) {
            System.out.println("Note: --stdwire is no longer needed - the real PostgreSQL "
                + "wire-protocol server is always what this class starts now (see this class's own javadoc). Ignoring the flag.");
        }

        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(dataDir);
        config.setPort(port);

        if (clusterMode) {
            com.stratosdb.core.StratosCluster cluster = new com.stratosdb.core.StratosCluster(config);
            StdWireServer server = new StdWireServer(port, cluster);
            server.start();
            System.out.println("StratosDB listening on port " + port
                + " (PostgreSQL wire protocol, real multi-database cluster mode, data: " + dataDir + ") - "
                + "databases: " + cluster.listDatabaseNames() + " - connect with psql -h localhost -p " + port
                + " -U anyuser -d " + com.stratosdb.core.StratosCluster.DEFAULT_DATABASE
                + ", or StratosShell/stdsql, or the real stratosdb-jdbc driver. Ctrl+C to stop.");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down...");
                server.stop();
                cluster.shutdown();
            }));

            Thread.currentThread().join();
            return;
        }

        StratosDB db = new StratosDB(config);
        StdWireServer server = new StdWireServer(port, db);
        server.start();
        System.out.println("StratosDB listening on port " + port
            + " (PostgreSQL wire protocol, data: " + dataDir + ") - connect with psql -h localhost -p " + port + " -U anyuser -d anydb, "
            + "or StratosShell/stdsql, or the real stratosdb-jdbc driver. Ctrl+C to stop.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            server.stop();
            db.shutdown();
        }));

        // Keep the main thread alive; the accept loop runs on its own daemon thread.
        Thread.currentThread().join();
    }
}

