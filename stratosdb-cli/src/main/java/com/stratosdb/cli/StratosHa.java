package com.stratosdb.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * StratosDB's own real, but honestly-scoped, automatic failover / HA
 * orchestrator - this project's own honestly-named "replication exists,
 * but there's no promotion command and no automatic failover at all"
 * gap (see PROGRESS.md). Real Postgres's own answer to this is Patroni
 * or repmgr, and this class deliberately does NOT claim to be either of
 * those in full.
 *
 * The real, honestly-stated architectural limit, stated plainly rather
 * than glossed over: Patroni's own real safety against split-brain (two
 * nodes both believing they're primary at once) comes from a genuine
 * distributed consensus store (etcd/Consul/ZooKeeper) - multiple
 * independent nodes agreeing on "who is primary now" even if some of
 * them can't reach each other. Building real distributed consensus
 * (Raft/Paxos) from scratch is its own, separate, large body of work,
 * not attempted here. This class is instead a single, standalone
 * watchdog process: ONE process's own view of "is the primary
 * reachable" decides whether failover happens - a real, working
 * automatic failover mechanism for the common case (the primary
 * process/machine genuinely goes down), but with a real, named
 * weakness real Patroni does not have: if the primary is still actually
 * running but this watchdog process itself loses network connectivity
 * to it (a partition, not a real primary failure), this watchdog would
 * incorrectly trigger failover, and there is no second, independent
 * voice to disagree - the same single-point-of-decision risk StratosDB's
 * own StdWireServer connection accounting already accepts elsewhere.
 * Real, further work (a second, independent watchdog with a quorum
 * requirement, or a real DCS integration) is a real, separate, further
 * piece of work, not attempted here given the scope of what's already
 * covered this round.
 *
 * What this class DOES do, for real: periodically health-checks a real
 * primary over a real connection (not just a TCP port probe - a real
 * SELECT 1 round trip, since a port accepting connections doesn't prove
 * the engine behind it is actually responsive), and on N consecutive
 * real failures, sends a real PROMOTE to the first reachable configured
 * replica candidate, in priority order.
 *
 * What this class does NOT do, named honestly: reconfigure surviving
 * OTHER replicas to follow the newly-promoted primary instead of the
 * old one - this engine's own ReplicationClient has no "change primary
 * at runtime" mechanism, only a fixed host/port set at construction, so
 * every surviving replica's own process must be restarted, manually,
 * pointed at the new primary - a real, separate, further piece of work.
 * Nor does it perform automatic re-integration of the old primary once
 * it comes back (a real, separate, harder problem - the old primary's
 * own WAL, written after the last replicated point but before it went
 * down, needs to be reconciled or discarded, not silently merged).
 */
public class StratosHa {
    private static final Logger LOG = LoggerFactory.getLogger(StratosHa.class);

    /** One candidate node this watchdog can reach - the primary itself, or a replica eligible for promotion. */
    public record NodeConfig(String host, int port, String user, String password, String database) {}

    private final NodeConfig primary;
    private final List<NodeConfig> replicaCandidates;
    private final long healthCheckIntervalMillis;
    private final int failureThreshold;
    private volatile boolean running = false;
    private volatile boolean failoverTriggered = false;
    private Thread watchdogThread;

    public StratosHa(NodeConfig primary, List<NodeConfig> replicaCandidates, long healthCheckIntervalMillis, int failureThreshold) {
        this.primary = primary;
        this.replicaCandidates = replicaCandidates;
        this.healthCheckIntervalMillis = healthCheckIntervalMillis;
        this.failureThreshold = failureThreshold;
    }

    public void start() {
        running = true;
        watchdogThread = new Thread(this::watchLoop, "stratos-ha-watchdog");
        watchdogThread.setDaemon(true);
        watchdogThread.start();
        LOG.info("StratosHa watchdog started: monitoring primary {}:{} every {}ms, failover after {} consecutive failures",
            primary.host(), primary.port(), healthCheckIntervalMillis, failureThreshold);
    }

    public void stop() {
        running = false;
        if (watchdogThread != null) {
            watchdogThread.interrupt();
        }
    }

    public boolean hasTriggeredFailover() {
        return failoverTriggered;
    }

    private void watchLoop() {
        int consecutiveFailures = 0;
        while (running && !failoverTriggered) {
            boolean healthy = isHealthy(primary);
            if (healthy) {
                if (consecutiveFailures > 0) {
                    LOG.info("Primary {}:{} healthy again after {} consecutive failure(s) - resetting failure count", primary.host(), primary.port(), consecutiveFailures);
                }
                consecutiveFailures = 0;
            } else {
                consecutiveFailures++;
                LOG.warn("Primary {}:{} health check failed ({} of {} before failover)", primary.host(), primary.port(), consecutiveFailures, failureThreshold);
                if (consecutiveFailures >= failureThreshold) {
                    triggerFailover();
                    return;
                }
            }
            try {
                Thread.sleep(healthCheckIntervalMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * A real health check - not just "does the TCP port accept a
     * connection" (a port can accept connections from a hung, deadlocked,
     * or partially-crashed process that will never actually respond),
     * but a real SCRAM-authenticated connection followed by a real,
     * minimal SQL round trip. SHOW TABLES, not a bare SELECT 1 - this
     * dialect's own grammar requires a real FROM-less SELECT to still
     * name a real, callable expression (a function, not a bare integer
     * literal), so SELECT 1 is a genuine syntax error here, not a
     * portable no-op the way it is against real Postgres - found by
     * testing this real health check against a real, live replica, not
     * by inspection. SHOW TABLES is real, always valid regardless of
     * whether any tables exist, and already allowed under real,
     * enforced read-only mode (see ExecutorEngine's own
     * READ_ONLY_SAFE_STATEMENTS), so it works identically against a
     * primary or a not-yet-promoted replica candidate. Both connecting
     * and the query itself must succeed within this attempt for the
     * node to count as healthy.
     */
    /** A real connect+read timeout for every real connection this watchdog itself makes - see StratosDump's own new overload's javadoc for the real, intermittent hang this prevents. Deliberately shorter than any reasonable health-check interval, so a hung attempt fails fast and the watchdog's own loop keeps making real, timely progress. */
    private static final int CONNECTION_TIMEOUT_MILLIS = 3000;

    private boolean isHealthy(NodeConfig node) {
        try {
            StratosDump probe = new StratosDump(node.host(), node.port(), node.user(), node.database(), node.password(), CONNECTION_TIMEOUT_MILLIS);
            try {
                String error = probe.executeSql("SHOW TABLES");
                return error == null;
            } finally {
                probe.close();
            }
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Picks the first configured replica candidate that is itself
     * currently reachable (in the same real sense isHealthy checks -
     * a real connection, not just a port probe), and sends it a real
     * PROMOTE. Real, honestly-stated scope: no attempt is made to pick
     * the MOST caught-up replica (this engine has no SQL-level way to
     * query a replica's own last-applied WAL position - ReplicationClient
     * tracks it in-memory, but that's not exposed over the wire protocol
     * itself) - candidates are tried strictly in the order configured,
     * the same real, simple priority-list model repmgr itself supports
     * as one of its own real modes.
     */
    private void triggerFailover() {
        LOG.error("Primary {}:{} declared unreachable after {} consecutive failures - beginning failover", primary.host(), primary.port(), failureThreshold);
        for (NodeConfig candidate : replicaCandidates) {
            if (!isHealthy(candidate)) {
                LOG.warn("Failover candidate {}:{} is itself unreachable - trying the next one", candidate.host(), candidate.port());
                continue;
            }
            try {
                StratosDump promoter = new StratosDump(candidate.host(), candidate.port(), candidate.user(), candidate.database(), candidate.password(), CONNECTION_TIMEOUT_MILLIS);
                try {
                    String error = promoter.executeSql("PROMOTE");
                    if (error == null) {
                        LOG.error("Failover complete: {}:{} promoted to primary. Every OTHER surviving replica must be manually restarted, "
                            + "pointed at this new primary - this watchdog does not reconfigure them automatically (see this class's own javadoc).",
                            candidate.host(), candidate.port());
                        failoverTriggered = true;
                        return;
                    } else {
                        LOG.error("PROMOTE failed on candidate {}:{}: {} - trying the next one", candidate.host(), candidate.port(), error);
                    }
                } finally {
                    promoter.close();
                }
            } catch (IOException e) {
                LOG.warn("Failed to connect to failover candidate {}:{} ({}) - trying the next one", candidate.host(), candidate.port(), e.getMessage());
            }
        }
        LOG.error("Failover FAILED: no configured replica candidate could be promoted. Manual intervention is required.");
    }

    public static void main(String[] args) throws Exception {
        String primaryHost = null;
        int primaryPort = -1;
        String user = System.getProperty("user.name", "stratos");
        String database = null;
        long intervalMillis = 5000;
        int threshold = 3;
        List<NodeConfig> replicas = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--primary" -> {
                    String[] parts = args[++i].split(":");
                    primaryHost = parts[0];
                    primaryPort = Integer.parseInt(parts[1]);
                }
                case "--replica" -> {
                    String[] parts = args[++i].split(":");
                    replicas.add(new NodeConfig(parts[0], Integer.parseInt(parts[1]), user, System.getenv("STRATOSDB_HA_PASSWORD"), database));
                }
                case "-U" -> user = args[++i];
                case "-d" -> database = args[++i];
                case "--interval-ms" -> intervalMillis = Long.parseLong(args[++i]);
                case "--failure-threshold" -> threshold = Integer.parseInt(args[++i]);
                default -> {
                    System.err.println("Unrecognized argument: " + args[i]);
                    printUsage();
                    return;
                }
            }
        }
        if (primaryHost == null || replicas.isEmpty()) {
            printUsage();
            return;
        }
        if (database == null) {
            database = user;
        }
        String password = System.getenv("STRATOSDB_HA_PASSWORD");
        NodeConfig primary = new NodeConfig(primaryHost, primaryPort, user, password, database);
        // Rebuild replica configs now that database/password are finalized (they were built with a possibly-null database above).
        List<NodeConfig> finalReplicas = new ArrayList<>();
        for (NodeConfig r : replicas) {
            finalReplicas.add(new NodeConfig(r.host(), r.port(), user, password, database));
        }

        StratosHa ha = new StratosHa(primary, finalReplicas, intervalMillis, threshold);
        ha.start();
        Thread.currentThread().join();
    }

    private static void printUsage() {
        System.err.println("Usage: StratosHa --primary host:port --replica host:port [--replica host:port ...] "
            + "-U user -d database [--interval-ms 5000] [--failure-threshold 3]");
        System.err.println("Password is read from the STRATOSDB_HA_PASSWORD environment variable.");
        System.err.println("Real, honestly-stated scope: a single watchdog process, not real distributed consensus - see this class's own javadoc.");
    }
}
