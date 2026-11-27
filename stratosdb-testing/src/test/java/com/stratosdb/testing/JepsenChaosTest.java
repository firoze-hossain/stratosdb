package com.stratosdb.testing;

import com.stratosdb.testing.jepsen.JepsenChaosHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Sprint 12 Week 24: Jepsen Partition Testing & Chaos.
 * Subjects a 3-node StratosDB cluster to network partitions, packet loss,
 * clock skew, and kill -9 during TPC-C & linearizable register workloads.
 */
class JepsenChaosTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testThreeNodeClusterContinuousChaosDuringTpccWorkload(@TempDir Path tempDir) throws Exception {
        long durationSecs = Long.getLong("jepsen.duration.seconds", 6L);
        try (JepsenChaosHarness harness = new JepsenChaosHarness(tempDir)) {
            JepsenChaosHarness.ChaosReport report = harness.runChaos(Duration.ofSeconds(durationSecs));

            assertTrue(report.chaosRounds() >= 4, "Must exercise all 4 chaos fault types (partition, packet loss, clock skew, kill -9)");
            assertTrue(report.tpccTransactionsCommitted() > 0, "TPC-C workload must commit transactions during chaos");
            assertTrue(report.registerWritesAcked() > 0, "Linearizable register writes must succeed");
            assertTrue(report.registerReadsVerified() > 0, "Linearizable register reads must be verified");
            assertTrue(report.maxConcurrentLeaders() <= 1, "Zero split-brain: never > 1 concurrent leader");
            assertTrue(report.violations().isEmpty(), () -> "Jepsen consistency violations: " + report.violations());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testRealOsSigkillDuringWritesRecoversAllCommittedRows(@TempDir Path tempDir) throws Exception {
        JepsenChaosHarness.verifyRealOsKill9Durability(tempDir);
    }
}
