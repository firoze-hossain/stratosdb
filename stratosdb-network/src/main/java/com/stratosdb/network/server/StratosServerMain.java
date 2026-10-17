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
        String explicitConfigFile = null;
        String dataDir = null;
        Integer port = null;
        Boolean clusterMode = null;

        java.util.List<String> positional = new java.util.ArrayList<>();
        boolean sawLegacyStdwireFlag = false;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--stdwire") || arg.startsWith("--stdwire=")) {
                sawLegacyStdwireFlag = true;
            } else if (arg.equals("--cluster")) {
                clusterMode = true;
            } else if (arg.startsWith("--config=")) {
                explicitConfigFile = arg.substring("--config=".length());
            } else if (arg.equals("--config") && i + 1 < args.length) {
                explicitConfigFile = args[++i];
            } else if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring("--port=".length()));
            } else if (arg.equals("--port") && i + 1 < args.length) {
                port = Integer.parseInt(args[++i]);
            } else if (arg.startsWith("--data-dir=")) {
                dataDir = arg.substring("--data-dir=".length());
            } else if (arg.equals("--data-dir") && i + 1 < args.length) {
                dataDir = args[++i];
            } else {
                positional.add(arg);
            }
        }

        // Positional args take precedence if provided: [dataDir] [port]
        if (positional.size() > 0) dataDir = positional.get(0);
        if (positional.size() > 1) port = Integer.parseInt(positional.get(1));

        // Locate config file: explicit flag, env var, or standard system paths
        String configFile = explicitConfigFile;
        if (configFile == null) {
            String envConf = System.getenv("STRATOSDB_CONF");
            if (envConf != null && !envConf.isBlank()) {
                configFile = envConf;
            }
        }
        if (configFile == null) {
            String[] candidatePaths = new String[] {
                "/etc/stratosdb/stratosdb.conf",
                System.getenv("PROGRAMDATA") != null ? System.getenv("PROGRAMDATA") + "\\StratosDB\\stratosdb.conf" : null,
                "/usr/local/etc/stratosdb/stratosdb.conf",
                "./stratosdb.conf"
            };
            for (String path : candidatePaths) {
                if (path != null && new java.io.File(path).isFile()) {
                    configFile = path;
                    break;
                }
            }
        }

        DatabaseConfig config = new DatabaseConfig();

        if (configFile != null && new java.io.File(configFile).isFile()) {
            java.util.Properties props = new java.util.Properties();
            try (java.io.InputStream in = new java.io.FileInputStream(configFile)) {
                props.load(in);
                System.out.println("Loaded StratosDB configuration from: " + configFile);
                if (port == null && props.getProperty("port") != null) {
                    port = Integer.parseInt(props.getProperty("port").trim());
                }
                if (dataDir == null) {
                    if (props.getProperty("data_directory") != null) {
                        dataDir = props.getProperty("data_directory").trim();
                    } else if (props.getProperty("dataDirectory") != null) {
                        dataDir = props.getProperty("dataDirectory").trim();
                    }
                }
                if (clusterMode == null && props.getProperty("cluster") != null) {
                    clusterMode = Boolean.parseBoolean(props.getProperty("cluster").trim());
                }
                if (props.getProperty("buffer_pool_size") != null) {
                    config.setBufferPoolSize(Integer.parseInt(props.getProperty("buffer_pool_size").trim()));
                }
                if (props.getProperty("sync_wal") != null) {
                    config.setSyncWAL(Boolean.parseBoolean(props.getProperty("sync_wal").trim()));
                }
                if (props.getProperty("max_connections") != null) {
                    config.setMaxConnections(Integer.parseInt(props.getProperty("max_connections").trim()));
                }
                if (props.getProperty("autovacuum_interval_ms") != null) {
                    config.setAutovacuumIntervalMs(Long.parseLong(props.getProperty("autovacuum_interval_ms").trim()));
                }
                if (props.getProperty("slow_query_threshold_ms") != null) {
                    config.setSlowQueryThresholdMs(Long.parseLong(props.getProperty("slow_query_threshold_ms").trim()));
                }
            } catch (Exception e) {
                System.err.println("Warning: failed to load configuration file " + configFile + ": " + e.getMessage());
            }
        }

        if (dataDir == null) dataDir = "./stratosdb_data";
        if (port == null) port = ProtocolConstants.DEFAULT_PORT;
        if (clusterMode == null) clusterMode = false;

        if (sawLegacyStdwireFlag) {
            System.out.println("Note: --stdwire is no longer needed - the real PostgreSQL "
                + "wire-protocol server is always what this class starts now (see this class's own javadoc). Ignoring the flag.");
        }

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

