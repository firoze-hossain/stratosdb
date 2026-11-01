package com.stratosdb.sql.optimizer;

import com.stratosdb.sql.statistics.ColumnStatistics;
import com.stratosdb.sql.statistics.EquiDepthHistogram;
import com.stratosdb.sql.statistics.MostCommonValues;
import com.stratosdb.sql.statistics.TableStatistics;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CostModelTest {

    private final CostModel costModel = new CostModel();

    @Test
    public void testDefaultCostParameters() {
        assertEquals(1.0, costModel.getSeqPageCost(), 1e-6);
        assertEquals(4.0, costModel.getRandomPageCost(), 1e-6);
        assertEquals(0.01, costModel.getCpuTupleCost(), 1e-6);
        assertEquals(0.005, costModel.getCpuIndexTupleCost(), 1e-6);
        assertEquals(0.0025, costModel.getCpuOperatorCost(), 1e-6);
    }

    @Test
    public void testCostSeqScanFormula() {
        // Table with 100 pages and 10,000 rows
        // Cost = 100 * 1.0 + 10,000 * 0.01 = 100.0 + 100.0 = 200.0
        double cost = costModel.costSeqScan(100, 10000);
        assertEquals(200.0, cost, 1e-6);

        // From TableStatistics
        TableStatistics stats = new TableStatistics(10000, 100, Map.of());
        assertEquals(200.0, costModel.costSeqScan(stats), 1e-6);
    }

    @Test
    public void testCostIndexScanHighSelectivity() {
        // Table with 50 pages and 10,000 rows
        // Predicate matches 1 row (point lookup, selectivity 0.0001)
        // Startup = 4.0
        // Data pages accessed = min(50, 1) = 1 -> 1 * 4.0 = 4.0
        // CPU = 1 * (0.01 + 0.005) = 0.015
        // Total = 4.0 + 4.0 + 0.015 = 8.015
        double idxCost = costModel.costIndexScan(50, 10000, 1);
        assertEquals(8.015, idxCost, 1e-6);

        // SeqScan cost = 50 * 1.0 + 10,000 * 0.01 = 150.0
        double seqCost = costModel.costSeqScan(50, 10000);
        assertTrue(idxCost < seqCost, "IndexScan should be vastly cheaper for 1 row lookup");
    }

    @Test
    public void testCostIndexScanLowSelectivityPrefersSeqScan() {
        // Table with 50 pages and 10,000 rows
        // Predicate matches 5,000 rows (selectivity 0.50)
        // Startup = 4.0
        // Data pages accessed = min(50, 5000) = 50 -> 50 * 4.0 = 200.0
        // CPU = 5000 * (0.01 + 0.005) = 75.0
        // Total IndexScan = 4.0 + 200.0 + 75.0 = 279.0
        double idxCost = costModel.costIndexScan(50, 10000, 5000);
        assertEquals(279.0, idxCost, 1e-6);

        // SeqScan cost = 50 * 1.0 + 10,000 * 0.01 = 150.0
        double seqCost = costModel.costSeqScan(50, 10000);
        assertTrue(seqCost < idxCost, "SeqScan should be preferred when matching 50% of the table");
    }

    @Test
    public void testCostHashJoinSmallerTableAsBuildSideIsCheaper() {
        // Outer table: 10,000 rows (cost 150.0)
        // Inner table: 100 rows (cost 5.0)
        // Output rows: 100
        // Build on inner (100 rows) vs build on outer (10,000 rows)
        double costBuildSmall = costModel.costHashJoin(150.0, 10000, 5.0, 100, 100, true);
        double costBuildLarge = costModel.costHashJoin(150.0, 10000, 5.0, 100, 100, false);

        assertTrue(costBuildSmall < costBuildLarge,
            "Building hash table on smaller relation (100 rows) must be cheaper than on 10,000 rows");
    }

    @Test
    public void testCostNestedLoopJoinVersusHashJoin() {
        // 1,000 rows x 1,000 rows
        double nlCost = costModel.costNestedLoopJoin(20.0, 1000, 20.0, 1000, 1000);
        double hashCost = costModel.costHashJoin(20.0, 1000, 20.0, 1000, 1000, true);

        // NL does 1000 * 20 (inner scans) + 1,000,000 * 0.0025 = 20,000 + 2,500 = 22,500+
        // Hash join does 20 + 20 + 2000 * 0.0025 + 1000 * 0.01 = ~55
        assertTrue(hashCost < nlCost, "Hash join must be vastly cheaper than nested loop for large unindexed joins");
    }
}
