package com.stratosdb.sql.optimizer;

import com.stratosdb.sql.statistics.ColumnStatistics;
import com.stratosdb.sql.statistics.TableStatistics;

/**
 * PostgreSQL / System-R style cost model for query optimization.
 *
 * Models physical I/O and CPU parameters:
 * - seq_page_cost = 1.0 (cost to read one disk page sequentially)
 * - random_page_cost = 4.0 (cost to read one disk page randomly, e.g. B-Tree descent or unclustered heap fetch)
 * - cpu_tuple_cost = 0.01 (CPU cost to process or emit one tuple)
 * - cpu_index_tuple_cost = 0.005 (CPU cost to evaluate an index entry)
 * - cpu_operator_cost = 0.0025 (CPU cost to evaluate a WHERE comparison or hash bucket lookup)
 */
public class CostModel {
    public static final double DEFAULT_SEQ_PAGE_COST = 1.0;
    public static final double DEFAULT_RANDOM_PAGE_COST = 4.0;
    public static final double DEFAULT_CPU_TUPLE_COST = 0.01;
    public static final double DEFAULT_CPU_INDEX_TUPLE_COST = 0.005;
    public static final double DEFAULT_CPU_OPERATOR_COST = 0.0025;

    private final double seqPageCost;
    private final double randomPageCost;
    private final double cpuTupleCost;
    private final double cpuIndexTupleCost;
    private final double cpuOperatorCost;

    public CostModel() {
        this(DEFAULT_SEQ_PAGE_COST, DEFAULT_RANDOM_PAGE_COST, DEFAULT_CPU_TUPLE_COST,
             DEFAULT_CPU_INDEX_TUPLE_COST, DEFAULT_CPU_OPERATOR_COST);
    }

    public CostModel(double seqPageCost, double randomPageCost, double cpuTupleCost,
                     double cpuIndexTupleCost, double cpuOperatorCost) {
        this.seqPageCost = seqPageCost;
        this.randomPageCost = randomPageCost;
        this.cpuTupleCost = cpuTupleCost;
        this.cpuIndexTupleCost = cpuIndexTupleCost;
        this.cpuOperatorCost = cpuOperatorCost;
    }

    public double getSeqPageCost() { return seqPageCost; }
    public double getRandomPageCost() { return randomPageCost; }
    public double getCpuTupleCost() { return cpuTupleCost; }
    public double getCpuIndexTupleCost() { return cpuIndexTupleCost; }
    public double getCpuOperatorCost() { return cpuOperatorCost; }

    /**
     * Estimates the cost of a sequential scan over a table.
     * Cost = (pages * seq_page_cost) + (rows * cpu_tuple_cost)
     */
    public double costSeqScan(TableStatistics stats) {
        if (stats == null) return 0.0;
        long pages = Math.max(1, stats.pageCount());
        long rows = Math.max(0, stats.rowCount());
        return costSeqScan(pages, rows);
    }

    public double costSeqScan(long pages, long rows) {
        pages = Math.max(1, pages);
        rows = Math.max(0, rows);
        return (pages * seqPageCost) + (rows * cpuTupleCost);
    }

    /**
     * Estimates the cost of an index scan using a B+Tree or Hash index.
     * Cost = startup_cost + (data_pages * random_page_cost) + (est_rows * (cpu_tuple_cost + cpu_index_tuple_cost))
     */
    public double costIndexScan(TableStatistics stats, String column, String operator, long lo, long hi, ColumnStatistics colStats) {
        if (stats == null) return 0.0;
        long rows = Math.max(0, stats.rowCount());
        if (rows == 0) return randomPageCost;

        double selectivity = colStats != null ? colStats.estimateSelectivity(operator, lo, hi) : 1.0;
        long estRows = Math.max(1, Math.round(rows * selectivity));
        long pages = Math.max(1, stats.pageCount());
        return costIndexScan(pages, rows, estRows);
    }

    public double costIndexScan(long pages, long totalRows, long estRows) {
        pages = Math.max(1, pages);
        totalRows = Math.max(0, totalRows);
        estRows = Math.max(1, Math.min(totalRows, estRows));

        // Startup cost: B+Tree root-to-leaf traversal (1 random page read)
        double startupCost = randomPageCost;

        // In an unclustered heap table, each matching tuple may require a random page read,
        // bounded by the total number of table pages
        double dataPagesAccessed = Math.min((double) pages, (double) estRows);
        double dataPageIoCost = dataPagesAccessed * randomPageCost;

        // CPU evaluation for index entries + heap tuples
        double cpuCost = estRows * (cpuTupleCost + cpuIndexTupleCost);

        return startupCost + dataPageIoCost + cpuCost;
    }

    /**
     * Estimates the cost of a Hash Join.
     * Cost = left_cost + right_cost + (build_rows * cpu_operator_cost) + (probe_rows * cpu_operator_cost) + (out_rows * cpu_tuple_cost)
     */
    public double costHashJoin(double leftCost, double leftRows, double rightCost, double rightRows, double outRows, boolean buildOnRight) {
        double buildRows = buildOnRight ? rightRows : leftRows;
        double probeRows = buildOnRight ? leftRows : rightRows;
        // Building hash table requires hashing + bucket insertion (higher per-tuple CPU cost than probing)
        double buildCpu = buildRows * (cpuOperatorCost * 2.0);
        double probeCpu = probeRows * cpuOperatorCost;
        double outCpu = outRows * cpuTupleCost;
        return leftCost + rightCost + buildCpu + probeCpu + outCpu;
    }

    /**
     * Estimates the cost of a Nested Loop Join.
     * Cost = outer_cost + (outer_rows * inner_cost) + (outer_rows * inner_rows * cpu_operator_cost) + (out_rows * cpu_tuple_cost)
     */
    public double costNestedLoopJoin(double outerCost, double outerRows, double innerCost, double innerRows, double outRows) {
        double evalCpu = (outerRows * innerRows) * cpuOperatorCost;
        double outCpu = outRows * cpuTupleCost;
        return outerCost + (outerRows * innerCost) + evalCpu + outCpu;
    }

    /**
     * Estimates the cost of a Merge Join.
     */
    public double costMergeJoin(double leftCost, double leftRows, double rightCost, double rightRows, double outRows, boolean leftSorted, boolean rightSorted) {
        double sortLeft = leftSorted ? 0.0 : 2.0 * leftRows * log2(Math.max(2, leftRows)) * cpuOperatorCost;
        double sortRight = rightSorted ? 0.0 : 2.0 * rightRows * log2(Math.max(2, rightRows)) * cpuOperatorCost;
        double mergeCpu = (leftRows + rightRows) * cpuOperatorCost + outRows * cpuTupleCost;
        return leftCost + rightCost + sortLeft + sortRight + mergeCpu;
    }

    private static double log2(double n) {
        return Math.log(n) / Math.log(2);
    }
}
