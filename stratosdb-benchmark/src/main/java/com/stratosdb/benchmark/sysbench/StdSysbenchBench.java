package com.stratosdb.benchmark.sysbench;

import java.sql.Connection;
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
 * Multi-threaded Sysbench Benchmark Driver for StratosDB, PostgreSQL, and MySQL.
 */
public class StdSysbenchBench {

    private static final Logger LOG = LoggerFactory.getLogger(StdSysbenchBench.class);

    public record StdSysbenchResult(
        String engine,
        String workload,
        int tables,
        int tableSize,
        int clients,
        double durationSeconds,
        long totalTransactions,
        long totalQueries,
        long failedTransactions,
        double tps,
        double qps,
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

    public static StdSysbenchResult runBenchmark(
            String engine,
            StdSysbenchWorkload.WorkloadType workloadType,
            int tables,
            int tableSize,
            int clients,
            int durationSeconds,
            int warmupSeconds,
            ConnectionSupplier supplier) throws Exception {

        StdSysbenchWorkload workload = new StdSysbenchWorkload(tables, tableSize);
        ExecutorService pool = Executors.newFixedThreadPool(clients);

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean measuring = new AtomicBoolean(false);

        AtomicLong txSuccessCount = new AtomicLong(0);
        AtomicLong txFailureCount = new AtomicLong(0);
        AtomicLong queryCount = new AtomicLong(0);

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
                Random rng = new Random(2000L + tId);
                try (Connection conn = supplier.getConnection()) {
                    startLatch.countDown();
                    startLatch.await();

                    while (running.get()) {
                        long start = System.nanoTime();
                        int q = 0;
                        boolean ok = false;
                        try {
                            q = workload.executeTransaction(conn, workloadType, rng);
                            ok = true;
                        } catch (Exception e) {
                            ok = false;
                        }
                        long duration = System.nanoTime() - start;

                        if (measuring.get()) {
                            if (ok) {
                                txSuccessCount.incrementAndGet();
                                queryCount.addAndGet(q);
                                latencies.add(duration);
                            } else {
                                txFailureCount.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Sysbench worker error in thread {}", tId, e);
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
        double tps = txSuccessCount.get() / Math.max(1e-9, elapsedSec);
        double qps = queryCount.get() / Math.max(1e-9, elapsedSec);

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

        return new StdSysbenchResult(
            engine,
            workloadType.name().toLowerCase(),
            tables,
            tableSize,
            clients,
            elapsedSec,
            txSuccessCount.get() + txFailureCount.get(),
            queryCount.get(),
            txFailureCount.get(),
            tps,
            qps,
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
