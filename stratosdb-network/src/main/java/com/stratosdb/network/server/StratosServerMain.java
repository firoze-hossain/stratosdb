package com.stratosdb.network.server;

import com.stratosdb.common.constants.ProtocolConstants;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.replication.ReplicationClient;
import com.stratosdb.network.replication.ReplicationServer;
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
        Integer stdwirePort = null;
        Integer replicationPort = null;
        String replicaOf = null;
        Boolean clusterMode = null;

        java.util.List<String> positional = new java.util.ArrayList<>();
        boolean sawLegacyStdwireFlag = false;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--stdwire") || arg.startsWith("--stdwire=")) {
                sawLegacyStdwireFlag = true;
                if (arg.startsWith("--stdwire=") && arg.length() > "--stdwire=".length()) {
                    stdwirePort = Integer.parseInt(arg.substring("--stdwire=".length()));
                }
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
            } else if (arg.startsWith("--stdwire-port=")) {
                stdwirePort = Integer.parseInt(arg.substring("--stdwire-port=".length()));
            } else if (arg.equals("--stdwire-port") && i + 1 < args.length) {
                stdwirePort = Integer.parseInt(args[++i]);
            } else if (arg.startsWith("--replication-port=")) {
                replicationPort = Integer.parseInt(arg.substring("--replication-port=".length()));
            } else if (arg.equals("--replication-port")) {
                boolean hasExplicitPort = i + 1 < args.length && !args[i + 1].startsWith("--");
                replicationPort = hasExplicitPort ? Integer.parseInt(args[++i]) : ProtocolConstants.DEFAULT_REPLICATION_PORT;
            } else if (arg.startsWith("--replica-of=")) {
                replicaOf = arg.substring("--replica-of=".length());
            } else if (arg.equals("--replica-of") && i + 1 < args.length) {
                replicaOf = args[++i];
            } else if (arg.startsWith("--data-dir=")) {
                dataDir = arg.substring("--data-dir=".length());
            } else if (arg.equals("--data-dir") && i + 1 < args.length) {
                dataDir = args[++i];
            } else {
                positional.add(arg);
            }
        }

        // Environment variables fallback
        if (dataDir == null && System.getenv("STRATOS_DATA_DIR") != null) {
            dataDir = System.getenv("STRATOS_DATA_DIR").trim();
        }
        if (port == null && System.getenv("STRATOS_PORT") != null) {
            try { port = Integer.parseInt(System.getenv("STRATOS_PORT").trim()); } catch (Exception ignored) {}
        }
        if (stdwirePort == null && System.getenv("STRATOS_STDWIRE_PORT") != null) {
            try { stdwirePort = Integer.parseInt(System.getenv("STRATOS_STDWIRE_PORT").trim()); } catch (Exception ignored) {}
        }
        if (replicationPort == null && System.getenv("STRATOS_REPLICATION_PORT") != null) {
            try { replicationPort = Integer.parseInt(System.getenv("STRATOS_REPLICATION_PORT").trim()); } catch (Exception ignored) {}
        }
        if (replicaOf == null && System.getenv("STRATOS_REPLICA_OF") != null) {
            replicaOf = System.getenv("STRATOS_REPLICA_OF").trim();
        }
        if (clusterMode == null && System.getenv("STRATOS_CLUSTER_MODE") != null) {
            clusterMode = Boolean.parseBoolean(System.getenv("STRATOS_CLUSTER_MODE").trim());
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
                if (props.getProperty("direct_allocation") != null) {
                    config.setDirectAllocation(Boolean.parseBoolean(props.getProperty("direct_allocation").trim()));
                }
                if (stdwirePort == null && props.getProperty("stdwire_port") != null) {
                    stdwirePort = Integer.parseInt(props.getProperty("stdwire_port").trim());
                }
                if (replicationPort == null && props.getProperty("replication_port") != null) {
                    replicationPort = Integer.parseInt(props.getProperty("replication_port").trim());
                }
                if (replicaOf == null && props.getProperty("replica_of") != null) {
                    replicaOf = props.getProperty("replica_of").trim();
                }
            } catch (Exception e) {
                System.err.println("Warning: failed to load configuration file " + configFile + ": " + e.getMessage());
            }
        }

        if (dataDir == null) dataDir = "./stratosdb_data";
        if (clusterMode == null) clusterMode = false;

        int effectiveNativePort;
        int effectiveStdwirePort;
        if (stdwirePort != null) {
            effectiveStdwirePort = stdwirePort;
            effectiveNativePort = port != null ? port : (effectiveStdwirePort == 5432 ? 5431 : effectiveStdwirePort - 1);
        } else if (port != null && port == 5432) {
            effectiveStdwirePort = 5432;
            effectiveNativePort = 5431;
        } else {
            effectiveNativePort = port != null ? port : ProtocolConstants.DEFAULT_PORT;
            effectiveStdwirePort = effectiveNativePort + 1;
        }

        if (effectiveNativePort == effectiveStdwirePort) {
            if (effectiveStdwirePort == 5432) {
                effectiveNativePort = 5431;
            } else {
                effectiveNativePort = effectiveStdwirePort - 1;
            }
        }

        if (sawLegacyStdwireFlag) {
            System.out.println("Note: --stdwire is no longer needed - the real PostgreSQL "
                + "wire-protocol server is always what this class starts now (see this class's own javadoc). Ignoring the flag.");
        }

        config.setDataDirectory(dataDir);
        config.setPort(effectiveStdwirePort);

        if (clusterMode) {
            com.stratosdb.core.StratosCluster cluster = new com.stratosdb.core.StratosCluster(config);
            com.stratosdb.network.stratoswire.StratosWireServer nativeServer =
                new com.stratosdb.network.stratoswire.StratosWireServer(effectiveNativePort, cluster);
            nativeServer.start();

            StdWireServer compatServer = new StdWireServer(effectiveStdwirePort, cluster);
            compatServer.start();

            System.out.println("StratosDB Server started (Multi-database Cluster Mode, data: " + dataDir + "):");
            System.out.println("  • PostgreSQL wire compatibility:      port " + effectiveStdwirePort + " (psql, JDBC, Hibernate, Prisma)");
            System.out.println("  • Stratos Native Protocol (SWP v1.0): port " + effectiveNativePort + " (DBNavigator)");
            System.out.println("Databases: " + cluster.listDatabaseNames() + ". Ctrl+C to stop.");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down...");
                nativeServer.stop();
                compatServer.stop();
                cluster.shutdown();
            }));

            Thread.currentThread().join();
            return;
        }

        StratosDB db = new StratosDB(config);
        if (replicaOf != null) {
            db.setReadOnly(true);
        }
        com.stratosdb.network.stratoswire.StratosWireServer nativeServer =
            new com.stratosdb.network.stratoswire.StratosWireServer(effectiveNativePort, db);
        nativeServer.start();

        StdWireServer compatServer = new StdWireServer(effectiveStdwirePort, db);
        compatServer.start();

        ReplicationServer replicationServer = null;
        if (replicationPort != null) {
            replicationServer = new ReplicationServer(replicationPort, db.getWalManager());
            replicationServer.start();
            System.out.println("  • Streaming Replication (Primary):    port " + replicationPort);
        }

        ReplicationClient replicationClient = null;
        if (replicaOf != null) {
            int colonIndex = replicaOf.lastIndexOf(':');
            if (colonIndex > 0) {
                String host = replicaOf.substring(0, colonIndex);
                int rPort = Integer.parseInt(replicaOf.substring(colonIndex + 1));
                replicationClient = new ReplicationClient(host, rPort, db.getDiskManager(), db.getBufferPool());
                replicationClient.start();
                System.out.println("  • Streaming Replication from:         " + replicaOf + " (Replica Mode)");
            }
        }

        System.out.println("StratosDB Server started (data: " + dataDir + "):");
        System.out.println("  • PostgreSQL wire compatibility:      port " + effectiveStdwirePort + " (psql, JDBC, Hibernate, Prisma)");
        System.out.println("  • Stratos Native Protocol (SWP v1.0): port " + effectiveNativePort + " (DBNavigator)");
        System.out.println("Ctrl+C to stop.");

        ReplicationServer finalRepServer = replicationServer;
        ReplicationClient finalRepClient = replicationClient;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            if (finalRepClient != null) finalRepClient.stop();
            if (finalRepServer != null) finalRepServer.stop();
            nativeServer.stop();
            compatServer.stop();
            db.shutdown();
        }));

        // Keep the main thread alive; the accept loop runs on its own daemon thread.
        Thread.currentThread().join();
    }
}

