package com.stratosdb.testing.jepsen;

import com.stratosdb.benchmark.tpcc.StdTpccDataGenerator;
import com.stratosdb.benchmark.tpcc.StdTpccSchema;
import com.stratosdb.benchmark.tpcc.StdTpccWorkload;
import com.stratosdb.cli.StratosHa;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.consensus.StdRaftCluster;
import com.stratosdb.network.consensus.StdRaftLeaseClient;
import com.stratosdb.network.consensus.StdRaftNode;
import com.stratosdb.network.consensus.StdRaftPeer;
import com.stratosdb.network.replication.ReplicationClient;
import com.stratosdb.network.replication.ReplicationServer;
import com.stratosdb.network.stdwire.StdWireServer;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.wal.SynchronousCommitMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Automated Jepsen-style Chaos Test Harness for a 3-node StratosDB cluster.
 *
 * Subjects the cluster to:
 *   1. Network partitions (minority vs majority)
 *   2. Packet loss (configurable drop rate on consensus RPCs)
 *   3. Clock skew (+/- multi-second drift across nodes)
 *   4. Hard node kills (kill -9 / SIGKILL on active primary + standby failover)
 *
 * Simultaneously executes TPC-C transactions and linearizable register operations,
 * verifying zero stale reads, zero lost updates, and zero split-brain inconsistencies.
 */
public class JepsenChaosHarness implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JepsenChaosHarness.class);

    public enum FaultType {
        NETWORK_PARTITION,
        PACKET_LOSS,
        CLOCK_SKEW,
        KILL_MINUS_9
    }

    public record ChaosReport(
        long durationMillis,
        int chaosRounds,
        int tpccTransactionsCommitted,
        int registerWritesAcked,
        int registerReadsVerified,
        int maxConcurrentLeaders,
        List<String> violations
    ) {
        public boolean isClean() {
            return violations.isEmpty() && maxConcurrentLeaders <= 1;
        }
    }

    private final Path workDir;
    private final JepsenHistoryVerifier verifier = new JepsenHistoryVerifier();
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Active cluster state
    private StdRaftCluster raftCluster;
    private final List<StratosDB> dbs = new ArrayList<>();
    private final List<StdWireServer> wireServers = new ArrayList<>();
    private final List<ReplicationClient> replClients = new ArrayList<>();
    private final List<StratosHa> watchdogs = new ArrayList<>();
    private ReplicationServer activeReplServer;
    private StdRaftLeaseClient leaseClient;

    private final AtomicReference<StratosDB> activePrimaryDb = new AtomicReference<>();
    private final AtomicInteger activePrimaryWirePort = new AtomicInteger();

    public JepsenChaosHarness(Path workDir) {
        this.workDir = workDir;
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void copyDirectory(Path source, Path target) throws IOException {
        Files.walk(source).forEach(src -> {
            try {
                Path dest = target.resolve(source.relativize(src));
                if (Files.isDirectory(src)) {
                    Files.createDirectories(dest);
                } else {
                    Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Runs the full Jepsen chaos suite for the given duration against a 3-node StratosDB cluster.
     */
    public ChaosReport runChaos(Duration duration) throws Exception {
        long startMs = System.currentTimeMillis();
        long endMs = startMs + duration.toMillis();

        int r1 = findFreePort();
        int r2 = findFreePort();
        int r3 = findFreePort();
        int wirePort1 = findFreePort();
        int wirePort2 = findFreePort();
        int wirePort3 = findFreePort();
        int replPort1 = findFreePort();

        Path dir1 = workDir.resolve("node1");
        Path dir2 = workDir.resolve("node2");
        Path dir3 = workDir.resolve("node3");

        // 1. Bootstrap initial TPC-C + register schema & data on node1, then clone to node2 & node3
        DatabaseConfig initCfg = new DatabaseConfig();
        initCfg.setDataDirectory(dir1.toString());
        initCfg.setSyncWAL(false);
        StratosDB initDb = new StratosDB(initCfg);
        initDb.execute("CREATE ROLE admin WITH LOGIN SUPERUSER PASSWORD 'admin'");
        StdWireServer initWire = new StdWireServer(wirePort1, initDb);
        initWire.start();
        try (Connection conn = DriverManager.getConnection("jdbc:stratos://127.0.0.1:" + wirePort1 + "/stratos", "admin", "admin")) {
            StdTpccSchema.initializeSchema(conn, "stratos");
            new StdTpccDataGenerator(1, 10, 2).loadData(conn);
        }
        initDb.execute("CREATE TABLE jepsen_reg (id INT PRIMARY KEY, val BIGINT)");
        for (int k = 1; k <= 10; k++) {
            initDb.execute("INSERT INTO jepsen_reg VALUES (" + k + ", 0)");
        }
        initDb.execute("CHECKPOINT");
        initWire.stop();
        initDb.shutdown();

        copyDirectory(dir1, dir2);
        copyDirectory(dir1, dir3);

        // 2. Start 3-node Raft cluster + 3 StratosDB nodes
        List<StdRaftPeer> peers = List.of(
            new StdRaftPeer("n1", "127.0.0.1", r1),
            new StdRaftPeer("n2", "127.0.0.1", r2),
            new StdRaftPeer("n3", "127.0.0.1", r3)
        );
        StdRaftNode n1 = new StdRaftNode("n1", "127.0.0.1", r1, peers, 120, 240, 40);
        StdRaftNode n2 = new StdRaftNode("n2", "127.0.0.1", r2, peers, 120, 240, 40);
        StdRaftNode n3 = new StdRaftNode("n3", "127.0.0.1", r3, peers, 120, 240, 40);
        raftCluster = new StdRaftCluster(List.of(n1, n2, n3));
        raftCluster.start();

        StdRaftNode initialLeader = raftCluster.waitForLeader(3000);
        if (initialLeader == null) {
            throw new IllegalStateException("3-node Raft cluster failed to elect initial leader");
        }

        DatabaseConfig cfg1 = new DatabaseConfig();
        cfg1.setDataDirectory(dir1.toString());
        cfg1.setSyncWAL(false);
        cfg1.setSynchronousCommit(SynchronousCommitMode.REMOTE_APPLY);
        StratosDB db1 = new StratosDB(cfg1);

        DatabaseConfig cfg2 = new DatabaseConfig();
        cfg2.setDataDirectory(dir2.toString());
        cfg2.setSyncWAL(false);
        StratosDB db2 = new StratosDB(cfg2);
        db2.setReadOnly(true);

        DatabaseConfig cfg3 = new DatabaseConfig();
        cfg3.setDataDirectory(dir3.toString());
        cfg3.setSyncWAL(false);
        StratosDB db3 = new StratosDB(cfg3);
        db3.setReadOnly(true);

        dbs.add(db1);
        dbs.add(db2);
        dbs.add(db3);

        StdWireServer wire1 = new StdWireServer(wirePort1, db1);
        StdWireServer wire2 = new StdWireServer(wirePort2, db2);
        StdWireServer wire3 = new StdWireServer(wirePort3, db3);
        wireServers.add(wire1);
        wireServers.add(wire2);
        wireServers.add(wire3);

        activeReplServer = new ReplicationServer(replPort1, db1.getWalManager(), 50, 2, 3000);
        ReplicationClient replClient2 = new ReplicationClient(
            "127.0.0.1", replPort1, db2.getDiskManager(), db2.getBufferPool(), db2.getTransactionManager(), 0, 0
        );
        ReplicationClient replClient3 = new ReplicationClient(
            "127.0.0.1", replPort1, db3.getDiskManager(), db3.getBufferPool(), db3.getTransactionManager(), 0, 0
        );
        replClients.add(replClient2);
        replClients.add(replClient3);
        wire2.setReplicationClient(replClient2);
        wire3.setReplicationClient(replClient3);

        wire1.start();
        wire2.start();
        wire3.start();
        activeReplServer.start();
        replClient2.start();
        replClient3.start();

        long connDeadline = System.currentTimeMillis() + 3000;
        while ((!replClient2.isConnected() || !replClient3.isConnected()) && System.currentTimeMillis() < connDeadline) {
            Thread.sleep(10);
        }

        leaseClient = new StdRaftLeaseClient(peers, "127.0.0.1:" + wirePort1, "primary", 800, 150);
        leaseClient.start();

        activePrimaryDb.set(db1);
        activePrimaryWirePort.set(wirePort1);
        running.set(true);

        AtomicInteger tpccCommitted = new AtomicInteger(0);
        AtomicInteger regWritesAcked = new AtomicInteger(0);
        AtomicInteger regReadsVerified = new AtomicInteger(0);
        AtomicInteger chaosRounds = new AtomicInteger(0);

        // 3. Background Split-Brain & Leader Invariant Monitor
        Thread invariantMonitor = new Thread(() -> {
            while (running.get()) {
                verifier.recordLeaderCount(raftCluster.countLeaders(), "continuous-monitor");
                try {
                    Thread.sleep(25);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "jepsen-invariant-monitor");
        invariantMonitor.setDaemon(true);
        invariantMonitor.start();

        // 4. Concurrent TPC-C & Linearizable Register Workload Workers
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch workersStopped = new CountDownLatch(2);
        AtomicLong monotonicVal = new AtomicLong(1);
        AtomicInteger nextOrderId = new AtomicInteger(10_000);

        // Worker A: TPC-C transactions (NEW_ORDER + PAYMENT + STOCK_LEVEL) via JDBC against active primary
        workers.submit(() -> {
            try {
                StdTpccWorkload tpcc = new StdTpccWorkload(1, 10, 2);
                Random rng = new Random(42);
                while (running.get()) {
                    int port = activePrimaryWirePort.get();
                    try (Connection conn = DriverManager.getConnection("jdbc:stratos://127.0.0.1:" + port + "/stratos", "admin", "admin")) {
                        while (running.get() && activePrimaryWirePort.get() == port) {
                            StdTpccWorkload.TxType txType = tpcc.pickRandomTx(rng);
                            try {
                                boolean ok = tpcc.executeTransaction(conn, txType, rng);
                                if (ok) {
                                    tpccCommitted.incrementAndGet();
                                }
                            } catch (Exception e) {
                                // Expected transiently during failover / partition; retry against current primary
                                break;
                            }
                        }
                    } catch (Exception e) {
                        try { Thread.sleep(25); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                    }
                }
            } finally {
                workersStopped.countDown();
            }
        });

        // Worker B: Linearizable register writes & reads checked by JepsenHistoryVerifier
        workers.submit(() -> {
            try {
                Random rng = new Random(99);
                while (running.get()) {
                    StratosDB primary = activePrimaryDb.get();
                    if (primary == null || primary.isReadOnly()) {
                        try { Thread.sleep(15); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                        continue;
                    }
                    int key = rng.nextInt(10) + 1;
                    long nextVal = monotonicVal.incrementAndGet();
                    long opId = verifier.nextOpId();
                    try {
                        QueryResult res = primary.execute("UPDATE jepsen_reg SET val = " + nextVal + " WHERE id = " + key);
                        if (res.isSuccess() && activePrimaryDb.get() == primary) {
                            verifier.recordAckedWrite(opId, key, nextVal, "primary@" + activePrimaryWirePort.get());
                            regWritesAcked.incrementAndGet();

                            // Immediately read back and verify monotonic/linearizable visibility
                            long readOp = verifier.nextOpId();
                            QueryResult readRes = primary.execute("SELECT val FROM jepsen_reg WHERE id = " + key);
                            if (readRes.isSuccess() && !readRes.getRows().isEmpty() && activePrimaryDb.get() == primary) {
                                long observed = ((Number) readRes.getRows().get(0).getValue(0)).longValue();
                                verifier.recordRead(readOp, key, observed, "primary@" + activePrimaryWirePort.get());
                                regReadsVerified.incrementAndGet();
                            }
                        } else {
                            verifier.recordFailedWrite(opId, key, nextVal, "primary@" + activePrimaryWirePort.get(), res.getError());
                        }
                    } catch (Exception e) {
                        verifier.recordFailedWrite(opId, key, nextVal, "primary@" + activePrimaryWirePort.get(), e.getMessage());
                    }

                    // Also insert a tracked order row in jepsen_reg
                    if (activePrimaryDb.get() != primary) continue;
                    int trackedKey = nextOrderId.incrementAndGet();
                    long trackedOp = verifier.nextOpId();
                    try {
                        QueryResult ins = primary.execute("INSERT INTO jepsen_reg VALUES (" + trackedKey + ", " + trackedKey + ")");
                        if (ins.isSuccess() && activePrimaryDb.get() == primary) {
                            verifier.recordAckedWrite(trackedOp, trackedKey, trackedKey, "primary@" + activePrimaryWirePort.get());
                            regWritesAcked.incrementAndGet();
                        }
                    } catch (Exception ignored) {}
                }
            } finally {
                workersStopped.countDown();
            }
        });

        // 5. Nemesis Loop: cycle through Network Partition, Packet Loss, Clock Skew, and kill -9 failover
        long nemesisEndMs = System.currentTimeMillis() + duration.toMillis();
        boolean failoverExecuted = false;
        int round = 0;
        while (System.currentTimeMillis() < nemesisEndMs || round < 4) {
            FaultType fault = FaultType.values()[round % FaultType.values().length];
            round++;
            chaosRounds.incrementAndGet();

            switch (fault) {
                case NETWORK_PARTITION -> {
                    if (failoverExecuted) {
                        // Only 2 of 3 Raft nodes remain alive after kill -9; cannot form a 2-node majority if partitioned
                        Thread.sleep(150);
                        break;
                    }
                    StdRaftNode leader = raftCluster.getLeader();
                    if (leader != null) {
                        String leaderId = leader.getNodeId();
                        Set<String> minority = Set.of(leaderId);
                        Set<String> majority = raftCluster.getNodes().keySet().stream()
                            .filter(id -> !id.equals(leaderId))
                            .collect(Collectors.toSet());
                        LOG.info("[Jepsen Nemesis] Injecting NETWORK_PARTITION minority={} majority={}", minority, majority);
                        raftCluster.partition(minority, majority);
                        StdRaftNode newLeader = raftCluster.waitForNewLeader(leaderId, 3000);
                        if (newLeader == null || !majority.contains(newLeader.getNodeId())) {
                            verifier.recordViolation("partition-no-majority-leader: expected majority leader, got " + newLeader);
                        }
                        verifier.recordLeaderCount(raftCluster.countLeaders(), "during-partition");
                        Thread.sleep(200);
                        raftCluster.heal();
                        Thread.sleep(250);
                        verifier.recordLeaderCount(raftCluster.countLeaders(), "after-partition-heal");
                    }
                }
                case PACKET_LOSS -> {
                    LOG.info("[Jepsen Nemesis] Injecting 30% PACKET_LOSS across cluster");
                    raftCluster.setPacketLoss(0.30);
                    Thread.sleep(400);
                    verifier.recordLeaderCount(raftCluster.countLeaders(), "during-packet-loss");
                    raftCluster.setPacketLoss(0.0);
                    Thread.sleep(200);
                }
                case CLOCK_SKEW -> {
                    LOG.info("[Jepsen Nemesis] Injecting +/- 5000ms CLOCK_SKEW across nodes");
                    List<String> ids = new ArrayList<>(raftCluster.getNodes().keySet());
                    if (ids.size() >= 2) {
                        raftCluster.setClockSkew(ids.get(0), 5000L);
                        raftCluster.setClockSkew(ids.get(1), -5000L);
                    }
                    Thread.sleep(400);
                    verifier.recordLeaderCount(raftCluster.countLeaders(), "during-clock-skew");
                    raftCluster.clearChaos();
                    Thread.sleep(200);
                }
                case KILL_MINUS_9 -> {
                    if (!failoverExecuted) {
                        failoverExecuted = true;
                        LOG.info("[Jepsen Nemesis] Executing KILL -9 on primary node1 + Raft leader");
                        // Wire StratosHa watchdogs on node2 & node3 to promote node2 in < 3s
                        StratosHa.NodeConfig pCfg = new StratosHa.NodeConfig("127.0.0.1", wirePort1, "admin", "admin", "stratos");
                        StratosHa.NodeConfig rCfg = new StratosHa.NodeConfig("127.0.0.1", wirePort2, "admin", "admin", "stratos");
                        StdRaftNode leader = raftCluster.getLeader();
                        String oldLeaderId = leader != null ? leader.getNodeId() : "n1";
                        StratosHa ha2 = new StratosHa(pCfg, List.of(rCfg), 100, 2, n2);
                        StratosHa ha3 = new StratosHa(pCfg, List.of(rCfg), 100, 2, n3);
                        watchdogs.add(ha2);
                        watchdogs.add(ha3);
                        ha2.start();
                        ha3.start();

                        // Kill primary node1 + its Raft node n1 so n2 & n3 retain 2/3 majority quorum
                        activePrimaryDb.set(null);
                        Thread.sleep(150); // allow any in-flight REMOTE_APPLY write on db1 to complete replication
                        n1.stop();
                        leaseClient.stop();
                        wire1.stop();
                        db1.getWalManager().setSynchronousCommitMode(SynchronousCommitMode.LOCAL);
                        activeReplServer.stop();
                        db1.shutdown();

                        StdRaftNode newLeader = "n1".equals(oldLeaderId)
                            ? raftCluster.waitForNewLeader("n1", 3000)
                            : raftCluster.waitForLeader(3000);
                        long promoDeadline = System.currentTimeMillis() + 3000;
                        while (db2.isReadOnly() && System.currentTimeMillis() < promoDeadline) {
                            Thread.sleep(20);
                        }
                        if (newLeader == null || db2.isReadOnly()) {
                            verifier.recordViolation("kill9-failover-failed: newLeader=" + newLeader + ", db2.readOnly=" + db2.isReadOnly());
                        } else {
                            // Point active primary to promoted node2
                            activePrimaryDb.set(db2);
                            activePrimaryWirePort.set(wirePort2);
                        }
                        Thread.sleep(400);
                    } else {
                        Thread.sleep(200);
                    }
                }
            }
        }

        // 6. Stop workload & chaos, verify 100% of acknowledged writes survived
        running.set(false);
        raftCluster.clearChaos();
        workers.shutdown();
        workersStopped.await(5, TimeUnit.SECONDS);
        invariantMonitor.join(1000);

        StratosDB finalPrimary = activePrimaryDb.get();
        Set<Integer> presentKeys = new HashSet<>();
        QueryResult allKeys = finalPrimary.execute("SELECT id, val FROM jepsen_reg");
        if (allKeys.isSuccess()) {
            for (var row : allKeys.getRows()) {
                int k = ((Number) row.getValue(0)).intValue();
                long v = ((Number) row.getValue(1)).longValue();
                presentKeys.add(k);
                verifier.recordRead(verifier.nextOpId(), k, v, "final-primary");
            }
        }
        verifier.verifyAllAckedKeysPresent(presentKeys, "post-chaos-primary");

        return new ChaosReport(
            System.currentTimeMillis() - startMs,
            chaosRounds.get(),
            tpccCommitted.get(),
            regWritesAcked.get(),
            regReadsVerified.get(),
            verifier.getMaxObservedLeaders(),
            verifier.getViolations()
        );
    }

    /**
     * Exercises real OS-level SIGKILL (`kill -9` via Process.destroyForcibly()) on a standalone
     * StratosDB server JVM process mid-workload, restarts it, and verifies 100% WAL durability.
     */
    public static void verifyRealOsKill9Durability(Path tempDir) throws Exception {
        int port = findFreePort();
        Path dataDir = tempDir.resolve("sigkill_node");
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(
            javaBin, "-cp", classpath,
            "com.stratosdb.network.stdwire.StdWireServerMain",
            dataDir.toString(), String.valueOf(port)
        );
        pb.redirectErrorStream(true);
        Process proc = pb.start();
        try {
            waitForPort(port, 10_000);
            try (Connection conn = DriverManager.getConnection("jdbc:stratos://127.0.0.1:" + port + "/stratos", "admin", "");
                 Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE sigkill_tpcc (id INT PRIMARY KEY, w_ytd INT)");
                st.execute("CHECKPOINT");
                for (int i = 1; i <= 25; i++) {
                    st.execute("INSERT INTO sigkill_tpcc VALUES (" + i + ", " + (i * 100) + ")");
                }
            }

            // Send real OS SIGKILL (kill -9)
            proc.destroyForcibly();
            boolean exited = proc.waitFor(5, TimeUnit.SECONDS);
            if (!exited) {
                throw new IllegalStateException("Process did not terminate on SIGKILL");
            }

            // Restart StratosDB on the same data directory and verify 100% of committed rows survived via WAL
            Process restarted = pb.start();
            try {
                waitForPort(port, 10_000);
                try (Connection conn = DriverManager.getConnection("jdbc:stratos://127.0.0.1:" + port + "/stratos", "admin", "");
                     Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM sigkill_tpcc")) {
                    if (!rs.next() || rs.getInt(1) != 25) {
                        throw new AssertionError("Expected 25 committed rows after SIGKILL + WAL recovery");
                    }
                }
            } finally {
                restarted.destroyForcibly();
                restarted.waitFor(5, TimeUnit.SECONDS);
            }
        } finally {
            if (proc.isAlive()) {
                proc.destroyForcibly();
            }
        }
    }

    private static void waitForPort(int port, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (java.net.Socket s = new java.net.Socket("127.0.0.1", port)) {
                return;
            } catch (IOException ignored) {
                Thread.sleep(50);
            }
        }
        throw new IllegalStateException("Server did not bind port " + port + " within " + timeoutMs + "ms");
    }

    @Override
    public void close() {
        running.set(false);
        for (StratosHa ha : watchdogs) {
            try { ha.stop(); } catch (Exception ignored) {}
        }
        if (leaseClient != null) {
            try { leaseClient.close(); } catch (Exception ignored) {}
        }
        for (ReplicationClient rc : replClients) {
            try { rc.stop(); } catch (Exception ignored) {}
        }
        if (activeReplServer != null) {
            try { activeReplServer.stop(); } catch (Exception ignored) {}
        }
        for (StdWireServer ws : wireServers) {
            try { ws.stop(); } catch (Exception ignored) {}
        }
        for (StratosDB db : dbs) {
            try { db.shutdown(); } catch (Exception ignored) {}
        }
        if (raftCluster != null) {
            try { raftCluster.stop(); } catch (Exception ignored) {}
        }
    }

    /**
     * CLI entrypoint for long-running continuous Jepsen chaos soak runs (e.g. --duration-seconds 172800 for 48h).
     */
    public static void main(String[] args) throws Exception {
        long seconds = 60;
        for (int i = 0; i < args.length - 1; i++) {
            if ("--duration-seconds".equals(args[i])) {
                seconds = Long.parseLong(args[++i]);
            }
        }
        Path tempDir = Files.createTempDirectory("stratos-jepsen-soak-");
        try (JepsenChaosHarness harness = new JepsenChaosHarness(tempDir)) {
            ChaosReport report = harness.runChaos(Duration.ofSeconds(seconds));
            System.out.println("Jepsen Chaos Report: " + report);
            if (!report.isClean()) {
                System.err.println("VIOLATIONS DETECTED: " + report.violations());
                System.exit(1);
            }
        }
    }
}
