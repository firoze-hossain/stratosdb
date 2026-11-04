package com.stratosdb.benchmark.tpcc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-threaded TPC-C Benchmark Harness.
 * Runs standard transaction mix against StratosDB, PostgreSQL 16, or MySQL 8.0.
 */
public class StdTpccBench {

    private static final Logger LOG = LoggerFactory.getLogger(StdTpccBench.class);

    public record StdTpccResult(
        String engine,
        int warehouses,
        int clients,
        double durationSeconds,
        long totalTransactions,
        long successfulTransactions,
        long failedTransactions,
        long newOrderCount,
        double tps,
        double tpmC,
        double p50Ms,
        double p90Ms,
        double p99Ms,
        double p999Ms,
        double maxMs
    ) {}

    @FunctionalInterface
    public interface ConnectionSupplier {
        Connection getConnection() throws Exception;
    }

    public static StdTpccResult runBenchmark(
            String engine,
            int warehouses,
            int clients,
            int durationSeconds,
            int warmupSeconds,
            ConnectionSupplier supplier) throws Exception {

        StdTpccWorkload workload = new StdTpccWorkload(warehouses);
        ExecutorService pool = Executors.newFixedThreadPool(clients);

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean measuring = new AtomicBoolean(false);

        AtomicLong totalCount = new AtomicLong(0);
        AtomicLong successCount = new AtomicLong(0);
        AtomicLong failureCount = new AtomicLong(0);
        AtomicLong newOrderCount = new AtomicLong(0);

        List<List<Long>> threadLatencies = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < clients; i++) {
            threadLatencies.add(new ArrayList<>());
        }

        CountDownLatch startLatch = new CountDownLatch(clients);
        CountDownLatch doneLatch = new CountDownLatch(clients);

        for (int threadIdx = 0; threadIdx < clients; threadIdx++) {
            final int tId = threadIdx;
            final List<Long> latencies = threadLatencies.get(tId);

            pool.submit(() -> {
                Random rng = new Random(1000L + tId);
                try (Connection conn = supplier.getConnection()) {
                    startLatch.countDown();
                    startLatch.await();

                    while (running.get()) {
                        StdTpccWorkload.TxType tx = workload.pickRandomTx(rng);
                        long start = System.nanoTime();
                        boolean ok = false;
                        try {
                            ok = workload.executeTransaction(conn, tx, rng);
                        } catch (Exception e) {
                            ok = false;
                        }
                        long duration = System.nanoTime() - start;

                        if (measuring.get()) {
                            totalCount.incrementAndGet();
                            if (ok) {
                                successCount.incrementAndGet();
                                if (tx == StdTpccWorkload.TxType.NEW_ORDER) {
                                    newOrderCount.incrementAndGet();
                                }
                                latencies.add(duration);
                            } else {
                                failureCount.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.error("TPC-C worker error in thread {}", tId, e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.await();
        if (warmupSeconds > 0) {
            Thread.sleep(warmupSeconds * 1000L);
        }

        measuring.set(true);
        long measureStart = System.nanoTime();
        Thread.sleep(durationSeconds * 1000L);
        long measureDurationNanos = System.nanoTime() - measureStart;
        running.set(false);
        doneLatch.await();
        pool.shutdown();

        double elapsedSec = measureDurationNanos / 1_000_000_000.0;
        double tps = successCount.get() / Math.max(1e-9, elapsedSec);
        double tpmC = (newOrderCount.get() * 60.0) / Math.max(1e-9, elapsedSec);

        List<Long> allLatencies = new ArrayList<>();
        for (List<Long> l : threadLatencies) {
            allLatencies.addAll(l);
        }
        long[] sorted = allLatencies.stream().mapToLong(Long::longValue).toArray();
        Arrays.sort(sorted);

        double p50 = sorted.length > 0 ? percentile(sorted, 0.50) / 1_000_000.0 : 0.0;
        double p90 = sorted.length > 0 ? percentile(sorted, 0.90) / 1_000_000.0 : 0.0;
        double p99 = sorted.length > 0 ? percentile(sorted, 0.99) / 1_000_000.0 : 0.0;
        double p999 = sorted.length > 0 ? percentile(sorted, 0.999) / 1_000_000.0 : 0.0;
        double max = sorted.length > 0 ? sorted[sorted.length - 1] / 1_000_000.0 : 0.0;

        return new StdTpccResult(
            engine,
            warehouses,
            clients,
            elapsedSec,
            totalCount.get(),
            successCount.get(),
            failureCount.get(),
            newOrderCount.get(),
            tps,
            tpmC,
            p50,
            p90,
            p99,
            p999,
            max
        );
    }

    private static long percentile(long[] sorted, double p) {
        int idx = Math.min(sorted.length - 1, (int) (sorted.length * p));
        return sorted[idx];
    }
}
