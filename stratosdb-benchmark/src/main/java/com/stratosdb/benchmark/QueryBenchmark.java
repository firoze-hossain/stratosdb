package com.stratosdb.benchmark;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;

import java.io.File;
import java.util.Arrays;
import java.util.Random;

/**
 * Real, runnable benchmark: indexed point lookup vs. full scan, through the
 * actual SQL interface (parse -> transaction begin/commit -> MVCC scan or
 * B+Tree lookup -> commit), not an idealized "just the storage layer"
 * number. This is what a real caller of StratosDB.execute() actually
 * experiences today, including costs that don't exist yet as separate,
 * cheaper paths - there is no prepared-statement API, so every query here
 * pays a full ANTLR parse and a full transaction begin/commit, every time.
 * That's an honest number for the system as it exists, not a flattering one.
 *
 * Run with: java -cp <classpath> com.stratosdb.benchmark.QueryBenchmark [rowCount] [queryCount]
 * Defaults: 100,000 rows, 300 queries per scenario.
 */
public class QueryBenchmark {

    public static void main(String[] args) throws Exception {
        // Must be set before any Logger is created - keeps benchmark output readable.
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

        int rowCount = args.length > 0 ? Integer.parseInt(args[0]) : 100_000;
        int queryCount = args.length > 1 ? Integer.parseInt(args[1]) : 300;

        String dataDir = "./benchmark_data_" + System.currentTimeMillis();
        File dataDirFile = new File(dataDir);

        System.out.println("=================================================");
        System.out.println(" StratosDB Query Benchmark");
        System.out.println(" Rows: " + rowCount + "   Queries per scenario: " + queryCount);
        System.out.println("=================================================\n");

        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(dataDir);
        StratosDB db = new StratosDB(config);

        try {
            run(db, rowCount, queryCount);
        } finally {
            db.shutdown();
            deleteRecursively(dataDirFile);
        }
    }

    private static void run(StratosDB db, int rowCount, int queryCount) {
        require(db.execute("CREATE TABLE bench (id INT, indexed_col INT, plain_col INT)"),
            "CREATE TABLE");

        System.out.println("Inserting " + rowCount + " rows...");
        long insertStart = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            require(db.execute("INSERT INTO bench VALUES (" + i + ", " + i + ", " + i + ")"),
                "INSERT row " + i);
            if (i > 0 && i % 20_000 == 0) {
                System.out.println("  ... " + i + " rows inserted");
            }
        }
        long insertMs = System.currentTimeMillis() - insertStart;
        System.out.printf("Inserted %,d rows in %,d ms (%.0f rows/sec)%n%n",
            rowCount, insertMs, rowCount * 1000.0 / Math.max(1, insertMs));

        System.out.println("Creating index on indexed_col (plain_col deliberately left unindexed)...");
        long indexStart = System.currentTimeMillis();
        QueryResult indexResult = db.execute("CREATE INDEX idx_bench_indexed ON bench (indexed_col)");
        require(indexResult, "CREATE INDEX");
        System.out.printf("%s (%,d ms)%n%n", indexResult.getMessage(), System.currentTimeMillis() - indexStart);

        // Prove the planner is actually doing what this benchmark assumes, rather than asserting it blind.
        System.out.println("Confirming the planner's choice for each column:");
        System.out.println("  indexed_col: " + db.execute("EXPLAIN SELECT * FROM bench WHERE indexed_col=1").getMessage());
        System.out.println("  plain_col:   " + db.execute("EXPLAIN SELECT * FROM bench WHERE plain_col=1").getMessage());
        System.out.println();

        Random rand = new Random(42);

        System.out.println("Warming up (JIT, page cache)...");
        for (int i = 0; i < Math.min(50, queryCount); i++) {
            int key = rand.nextInt(rowCount);
            db.execute("SELECT * FROM bench WHERE indexed_col=" + key);
            db.execute("SELECT * FROM bench WHERE plain_col=" + key);
        }
        System.out.println();

        System.out.println("Running " + queryCount + " indexed point lookups...");
        BenchResult indexed = benchmarkPointLookups(db, "indexed_col", rowCount, queryCount, rand);

        System.out.println("Running " + queryCount + " full-scan point lookups (unindexed column)...");
        BenchResult seqScan = benchmarkPointLookups(db, "plain_col", rowCount, queryCount, rand);

        printReport(rowCount, queryCount, indexed, seqScan);
    }

    private record BenchResult(double avgMs, double p50Ms, double p95Ms, double p99Ms, double opsPerSec) {}

    private static BenchResult benchmarkPointLookups(StratosDB db, String column, int rowCount, int queryCount, Random rand) {
        long[] latenciesNanos = new long[queryCount];
        for (int i = 0; i < queryCount; i++) {
            int key = rand.nextInt(rowCount);
            long start = System.nanoTime();
            QueryResult result = db.execute("SELECT * FROM bench WHERE " + column + "=" + key);
            latenciesNanos[i] = System.nanoTime() - start;
            require(result, "SELECT WHERE " + column + "=" + key);
            if (result.getRows().size() != 1) {
                throw new IllegalStateException("Expected exactly 1 row for " + column + "=" + key
                    + ", got " + result.getRows().size() + " - benchmark data is inconsistent");
            }
        }

        Arrays.sort(latenciesNanos);
        double avgMs = Arrays.stream(latenciesNanos).average().orElse(0) / 1_000_000.0;
        double p50Ms = percentile(latenciesNanos, 0.50) / 1_000_000.0;
        double p95Ms = percentile(latenciesNanos, 0.95) / 1_000_000.0;
        double p99Ms = percentile(latenciesNanos, 0.99) / 1_000_000.0;
        double opsPerSec = avgMs > 0 ? 1000.0 / avgMs : 0;
        return new BenchResult(avgMs, p50Ms, p95Ms, p99Ms, opsPerSec);
    }

    private static long percentile(long[] sortedNanos, double p) {
        int idx = Math.min(sortedNanos.length - 1, (int) (sortedNanos.length * p));
        return sortedNanos[idx];
    }

    private static void printReport(int rowCount, int queryCount, BenchResult indexed, BenchResult seqScan) {
        System.out.println("\n=================================================");
        System.out.println(" Results (" + rowCount + " rows, " + queryCount + " queries/scenario)");
        System.out.println("=================================================");
        System.out.printf("%-22s %12s %12s %12s %12s %14s%n",
            "Scenario", "avg (ms)", "p50 (ms)", "p95 (ms)", "p99 (ms)", "ops/sec");
        System.out.printf("%-22s %12.3f %12.3f %12.3f %12.3f %14.1f%n",
            "Index Scan", indexed.avgMs(), indexed.p50Ms(), indexed.p95Ms(), indexed.p99Ms(), indexed.opsPerSec());
        System.out.printf("%-22s %12.3f %12.3f %12.3f %12.3f %14.1f%n",
            "Seq Scan", seqScan.avgMs(), seqScan.p50Ms(), seqScan.p95Ms(), seqScan.p99Ms(), seqScan.opsPerSec());

        double speedup = seqScan.avgMs() / Math.max(0.0001, indexed.avgMs());
        System.out.printf("%nIndex scan was %.1fx faster than a full scan for a point lookup on %,d rows.%n", speedup, rowCount);
        System.out.println("\nHonest caveats, not smoothed over:");
        System.out.println("  - Every query here pays a full ANTLR parse and a full transaction");
        System.out.println("    begin/commit - there is no prepared-statement API yet, so this number");
        System.out.println("    includes overhead a production system would let you avoid.");
        System.out.println("  - Full-scan cost is O(rowCount); at " + rowCount + " rows this already favors");
        System.out.println("    the index heavily. The gap will only widen as the table grows.");
        System.out.println("  - This is one machine, one run. It is not a substitute for reproducing");
        System.out.println("    numbers against another database on identical hardware.");
    }

    private static void require(QueryResult result, String what) {
        if (!result.isSuccess()) {
            throw new IllegalStateException(what + " failed: " + result.getError());
        }
    }

    private static void deleteRecursively(File file) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }
}
