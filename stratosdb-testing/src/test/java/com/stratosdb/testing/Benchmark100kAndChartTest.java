package com.stratosdb.testing;

import com.stratosdb.benchmark.comparison.Benchmark100kSuite;
import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.Page;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Sprint 13 Week 25:
 *   - 64-byte cache-line aligned off-heap page frame allocation
 *   - Lock-free L1 hot-cache page lookup throughput under multi-threaded load
 *   - Automated TPC-C & Sysbench benchmark execution + SVG/HTML/MD comparison charts vs PG 17
 */
class Benchmark100kAndChartTest {

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testCacheLineAlignmentAndLockFreeHotCacheLookup(@TempDir Path tempDir) throws Exception {
        DiskManager dm = new DiskManager(tempDir.toString());
        BufferPoolManager bpm = new BufferPoolManager(512, dm);
        try {
            // 1. Verify 64-byte CPU cache-line aligned native memory segment allocation
            MemorySegment seg = bpm.acquireMemorySegment();
            assertEquals(0L, seg.address() % 64L, "Native page frame must be 64-byte cache-line aligned");

            // 2. Populate 16 pages across BufferPoolPartitions
            for (long p = 0; p < 16; p++) {
                Page page = bpm.getPage("perf_tbl", p);
                bpm.markDirty("perf_tbl", page.getPageId());
                bpm.unpinPage("perf_tbl", page.getPageId());
            }

            // 3. Measure 8-thread lock-free L1 hotCache lookup throughput
            int threads = 8;
            int opsPerThread = 25_000;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            AtomicLong totalLookups = new AtomicLong(0);

            for (int t = 0; t < threads; t++) {
                final int tid = t;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < opsPerThread; i++) {
                            long pageId = (i + tid) % 16;
                            Page p = bpm.getPage("perf_tbl", pageId);
                            assertNotNull(p);
                            bpm.unpinPage("perf_tbl", pageId);
                            totalLookups.incrementAndGet();
                        }
                    } catch (Exception e) {
                        fail(e);
                    } finally {
                        done.countDown();
                    }
                });
            }

            long t0 = System.nanoTime();
            start.countDown();
            assertTrue(done.await(20, TimeUnit.SECONDS));
            long elapsedNs = System.nanoTime() - t0;
            pool.shutdownNow();

            double lookupsPerSec = totalLookups.get() / (elapsedNs / 1_000_000_000.0);
            System.out.printf("Lock-free BufferPool L1 hotCache 8-thread lookup throughput: %,.0f lookups/sec%n", lookupsPerSec);
            assertEquals(threads * opsPerThread, totalLookups.get());
            assertTrue(lookupsPerSec > 100_000.0, "Lock-free page lookup must exceed 100k ops/sec");
        } finally {
            bpm.close();
            dm.close();
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testBenchmark100kSuiteGeneratesComparisonChartsVsPg17(@TempDir Path tempDir) throws Exception {
        Path chartDir = Path.of("../benchmarks");
        if (!Files.exists(chartDir.getParent())) {
            chartDir = Path.of("benchmarks");
        }

        Benchmark100kSuite.SuiteSummary summary = Benchmark100kSuite.runSuite(
            tempDir.resolve("bench_data"),
            1,          // 1 warehouse for fast CI verification (CLI defaults to 100)
            1,          // 1 sysbench table
            100,        // 100 rows/table for fast CI verification (CLI defaults to 10M)
            new int[]{1, 2, 4},
            1,          // 1s per step
            null, null, null,
            chartDir
        );

        assertTrue(summary.peakSysbenchTps() > 0, "Sysbench TPS must be positive");
        assertTrue(summary.peakTpccTpmC() > 0, "TPC-C tpmC must be positive");
        assertEquals(3, summary.sysbenchSeries().size());
        assertEquals(3, summary.tpccSeries().size());

        assertTrue(Files.exists(chartDir.resolve("sysbench_vs_pg17.svg")));
        assertTrue(Files.exists(chartDir.resolve("tpcc_vs_pg17.svg")));
        assertTrue(Files.exists(chartDir.resolve("stratosdb_vs_pg17_charts.html")));
        assertTrue(Files.exists(chartDir.resolve("BENCHMARK_100K_REPORT.md")));

        String svg = Files.readString(chartDir.resolve("sysbench_vs_pg17.svg"));
        assertTrue(svg.contains("<svg") && svg.contains("PostgreSQL 17"));
    }
}
