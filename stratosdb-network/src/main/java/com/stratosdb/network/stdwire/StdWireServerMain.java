package com.stratosdb.network.stdwire;

import com.stratosdb.common.constants.ProtocolConstants;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.replication.ReplicationClient;
import com.stratosdb.network.replication.ReplicationServer;

import java.util.ArrayList;
import java.util.List;

/**
 * Runnable entry point: starts StratosDB and a StdWireServer listening for
 * real PostgreSQL wire-protocol clients (psql, pgAdmin, any pg driver).
 * Args: [dataDirectory] [port] - both optional, defaulting to
 * "./stratosdb_data" and ProtocolConstants.DEFAULT_STDWIRE_PORT (one above
 * StratosDB's own custom-protocol default, so both servers can run
 * alongside each other without a port clash).
 *
 * Two further, optional, named flags enable real physical (WAL-shipping)
 * replication - see ReplicationServer/ReplicationClient's own javadoc for
 * what this actually does and its real, honestly-stated limitations:
 *
 *   --replication-port [port]   Starts this instance as a PRIMARY, also
 *                                accepting replica connections on the
 *                                given port (default:
 *                                ProtocolConstants.DEFAULT_REPLICATION_PORT
 *                                if no port follows the flag).
 *   --replica-of host:port      Starts this instance as a REPLICA,
 *                                streaming and applying WAL from the
 *                                primary at host:port. Mutually exclusive
 *                                with --replication-port - an instance is
 *                                either a primary or a replica, not both.
 *
 * Example: a primary and a replica on the same machine -
 *   StdWireServerMain ./primary_data 6583 --replication-port 6584
 *   StdWireServerMain ./replica_data 6585 --replica-of localhost:6584
 */
public class StdWireServerMain {
    public static void main(String[] args) throws Exception {
        List<String> positional = new ArrayList<>();
        Integer replicationPort = null;
        String replicaOf = null;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--replication-port")) {
                boolean hasExplicitPort = i + 1 < args.length && !args[i + 1].startsWith("--");
                replicationPort = hasExplicitPort ? Integer.parseInt(args[++i]) : ProtocolConstants.DEFAULT_REPLICATION_PORT;
            } else if (args[i].equals("--replica-of")) {
                replicaOf = args[++i];
            } else {
                positional.add(args[i]);
            }
        }
        if (replicationPort != null && replicaOf != null) {
            System.err.println("--replication-port and --replica-of are mutually exclusive: an instance is either a primary or a replica, not both.");
            System.exit(1);
        }

        String dataDir = !positional.isEmpty() ? positional.get(0) : "./stratosdb_data";
        int port = positional.size() > 1 ? Integer.parseInt(positional.get(1)) : ProtocolConstants.DEFAULT_STDWIRE_PORT;

        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(dataDir);

        StratosDB db = new StratosDB(config);
        StdWireServer server = new StdWireServer(port, db);
        server.start();

        System.out.println("StratosDB standard-wire server listening on port " + port + " (data: " + dataDir + "). Ctrl+C to stop.");
        System.out.println("Connect with: psql -h localhost -p " + port + " -U anyuser -d anydb");

        ReplicationServer replicationServer = null;
        if (replicationPort != null) {
            replicationServer = new ReplicationServer(replicationPort, db.getWalManager());
            replicationServer.start();
            System.out.println("Replication server listening on port " + replicationPort + " (primary mode) - a replica can connect with --replica-of localhost:" + replicationPort);
        }

        ReplicationClient replicationClient = null;
        if (replicaOf != null) {
            int colonIndex = replicaOf.lastIndexOf(':');
            if (colonIndex < 0) {
                System.err.println("--replica-of must be in host:port form, e.g. --replica-of localhost:6584");
                System.exit(1);
            }
            String primaryHost = replicaOf.substring(0, colonIndex);
            int primaryPort = Integer.parseInt(replicaOf.substring(colonIndex + 1));
            replicationClient = new ReplicationClient(primaryHost, primaryPort, db.getDiskManager(), db.getBufferPool());
            replicationClient.start();
            System.out.println("Replicating from primary at " + replicaOf + " (replica mode)");
        }

        ReplicationServer finalReplicationServer = replicationServer;
        ReplicationClient finalReplicationClient = replicationClient;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            if (finalReplicationClient != null) finalReplicationClient.stop();
            if (finalReplicationServer != null) finalReplicationServer.stop();
            server.stop();
            db.shutdown();
        }));

        Thread.currentThread().join();
    }
}
