package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.sql.ast.SelectStatement;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.sql.optimizer.CostModel;
import com.stratosdb.sql.optimizer.JoinPlanNode;
import com.stratosdb.sql.optimizer.PlanNode;
import com.stratosdb.sql.optimizer.SystemROptimizer;
import com.stratosdb.sql.statistics.ColumnStatistics;
import com.stratosdb.sql.statistics.TableStatistics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Sprint 06 Week 11:
 * System-R Dynamic Programming Join Reordering (up to 10 tables, bushy/left-deep trees,
 * and >= 10x cost reduction on complex 6-table joins).
 */
public class DpJoinReorderingTest {

    private final CostModel costModel = new CostModel();
    private final SystemROptimizer optimizer = new SystemROptimizer(costModel);

    @Test
    void testSixTableJoinAchievesOver10xCostReduction() {
        // 6-table analytical join where naive FROM order joins large unselective tables first
        // (causing large intermediate results), while System-R DP starts from selective
        // filtered dimensions and keeps intermediates small.
        // Base scan cost per 10,000-row table (100 pages) is only 200.0.
        Map<String, TableStatistics> stats = Map.of(
            "t1", new TableStatistics(10_000, 100, Map.of(
                "k2", colStats("t1", "k2", 10_000, 100, 50.0),
                "k5", colStats("t1", "k5", 10_000, 100, 1_000.0)
            )),
            "t2", new TableStatistics(10_000, 100, Map.of(
                "id", colStats("t2", "id", 10_000, 100, 50.0),
                "k3", colStats("t2", "k3", 10_000, 100, 50.0)
            )),
            "t3", new TableStatistics(10_000, 100, Map.of(
                "id", colStats("t3", "id", 10_000, 100, 50.0),
                "k4", colStats("t3", "k4", 10_000, 100, 50.0)
            )),
            "t4", new TableStatistics(10_000, 100, Map.of(
                "id", colStats("t4", "id", 10_000, 100, 50.0),
                "k6", colStats("t4", "k6", 10_000, 100, 1_000.0)
            )),
            "t5", new TableStatistics(1_000, 10, Map.of(
                "id", colStats("t5", "id", 1_000, 10, 1_000.0)
            )),
            "t6", new TableStatistics(1_000, 10, Map.of(
                "id", colStats("t6", "id", 1_000, 10, 1_000.0)
            ))
        );

        Map<String, List<String>> cols = Map.of(
            "t1", List.of("id", "k2", "k5"),
            "t2", List.of("id", "k3"),
            "t3", List.of("id", "k4"),
            "t4", List.of("id", "k6"),
            "t5", List.of("id", "name"),
            "t6", List.of("id", "name")
        );

        com.stratosdb.sql.ast.WhereExpr where = new com.stratosdb.sql.ast.WhereExpr.And(
            new com.stratosdb.sql.ast.WhereExpr.Comparison("t5.id", "=", "42"),
            new com.stratosdb.sql.ast.WhereExpr.Comparison("t6.id", "=", "7")
        );

        SelectStatement stmt = new SelectStatement(
            "t1", List.of("*"), where, null, null, null,
            List.of(
                new JoinClause("t2", "t1.k2", "t2.id", JoinClause.JoinType.INNER),
                new JoinClause("t3", "t2.k3", "t3.id", JoinClause.JoinType.INNER),
                new JoinClause("t4", "t3.k4", "t4.id", JoinClause.JoinType.INNER),
                new JoinClause("t5", "t1.k5", "t5.id", JoinClause.JoinType.INNER),
                new JoinClause("t6", "t4.k6", "t6.id", JoinClause.JoinType.INNER)
            ),
            List.of(), List.of(), null, List.of(), List.of(), List.of()
        );

        PlanNode naivePlan = optimizer.buildNaiveLeftDeepPlan(stmt, stats, cols, (t, c, eq) -> null);
        PlanNode optimalPlan = optimizer.optimize(stmt, stats, cols, (t, c, eq) -> null, SystemROptimizer.JoinTreeShape.BUSHY);

        assertNotNull(naivePlan);
        assertNotNull(optimalPlan);
        assertEquals(6, optimalPlan.getCoveredTables().size());

        double naiveCost = naivePlan.getCost();
        double optimalCost = optimalPlan.getCost();
        double reduction = naiveCost / optimalCost;

        System.out.printf(
            "=== Sprint 06 Week 11 DoD (6-table join) ===%n"
            + "Naive left-deep cost: %.2f%n"
            + "System-R DP optimal:  %.2f%n"
            + "Cost reduction:       %.2fx (Goal: >= 10.0x)%n",
            naiveCost, optimalCost, reduction);

        assertTrue(reduction >= 10.0,
            "Expected >= 10x cost reduction on 6-table join, got " + reduction + "x");
    }

    @Test
    void testBushyJoinTreeBeatsLeftDeepTree() {
        // Two independent selective pairs:
        // Pair 1: t1 (50,000 rows) JOIN t2 (10 rows) -> intermediate ~50 rows
        // Pair 2: t3 (50,000 rows) JOIN t4 (10 rows) -> intermediate ~50 rows
        // Bridge: t1.b = t3.b (distinct 10,000)
        // Optimal bushy plan: (t1 JOIN t2) JOIN (t3 JOIN t4)
        Map<String, TableStatistics> stats = Map.of(
            "t1", new TableStatistics(50_000, 500, Map.of(
                "k2", colStats("t1", "k2", 50_000, 500, 10_000.0),
                "b", colStats("t1", "b", 50_000, 500, 10_000.0)
            )),
            "t2", new TableStatistics(10, 1, Map.of(
                "id", colStats("t2", "id", 10, 1, 10.0)
            )),
            "t3", new TableStatistics(50_000, 500, Map.of(
                "k4", colStats("t3", "k4", 50_000, 500, 10_000.0),
                "b", colStats("t3", "b", 50_000, 500, 10_000.0)
            )),
            "t4", new TableStatistics(10, 1, Map.of(
                "id", colStats("t4", "id", 10, 1, 10.0)
            ))
        );
        Map<String, List<String>> cols = Map.of(
            "t1", List.of("id", "k2", "b"),
            "t2", List.of("id"),
            "t3", List.of("id", "k4", "b"),
            "t4", List.of("id")
        );

        SelectStatement stmt = new SelectStatement(
            "t1", List.of("*"), null, null, null, null,
            List.of(
                new JoinClause("t3", "t1.b", "t3.b", JoinClause.JoinType.INNER),
                new JoinClause("t2", "t1.k2", "t2.id", JoinClause.JoinType.INNER),
                new JoinClause("t4", "t3.k4", "t4.id", JoinClause.JoinType.INNER)
            ),
            List.of(), List.of(), null, List.of(), List.of(), List.of()
        );

        PlanNode leftDeep = optimizer.optimize(stmt, stats, cols, (t, c, eq) -> null, SystemROptimizer.JoinTreeShape.LEFT_DEEP);
        PlanNode bushy = optimizer.optimize(stmt, stats, cols, (t, c, eq) -> null, SystemROptimizer.JoinTreeShape.BUSHY);

        assertNotNull(leftDeep);
        assertNotNull(bushy);
        assertTrue(bushy instanceof JoinPlanNode);
        JoinPlanNode root = (JoinPlanNode) bushy;

        // Verify genuinely bushy: both children are JoinPlanNode
        assertTrue(root.getLeft() instanceof JoinPlanNode && root.getRight() instanceof JoinPlanNode,
            "Expected bushy join tree with JoinPlanNode on both sides, got:\n" + bushy.explain(""));
        assertTrue(bushy.getCost() < leftDeep.getCost(),
            "Bushy plan cost (" + bushy.getCost() + ") should be strictly less than left-deep (" + leftDeep.getCost() + ")");
    }

    @Test
    void testTenTableDynamicProgrammingEnumeration() {
        Map<String, TableStatistics> stats = new HashMap<>();
        Map<String, List<String>> cols = new HashMap<>();
        List<JoinClause> joins = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            String name = "t" + i;
            long rows = (i == 0) ? 100_000 : Math.max(10, 100_000L >> i);
            long pages = Math.max(1, rows / 100);
            stats.put(name, new TableStatistics(rows, pages, Map.of(
                "id", colStats(name, "id", rows, pages, rows),
                "next_id", colStats(name, "next_id", rows, pages, Math.max(10, rows / 2))
            )));
            cols.put(name, List.of("id", "next_id"));
            if (i > 0) {
                joins.add(new JoinClause(name, "t" + (i - 1) + ".next_id", name + ".id", JoinClause.JoinType.INNER));
            }
        }

        SelectStatement stmt10 = new SelectStatement(
            "t0", List.of("*"), null, null, null, null,
            joins, List.of(), List.of(), null, List.of(), List.of(), List.of()
        );
        assertTrue(SystemROptimizer.canOptimize(stmt10, stats));

        // Warmup JIT
        optimizer.optimize(stmt10, stats, cols, (t, c, eq) -> null);

        long startNs = System.nanoTime();
        PlanNode optimal = optimizer.optimize(stmt10, stats, cols, (t, c, eq) -> null);
        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;

        assertNotNull(optimal);
        assertEquals(10, optimal.getCoveredTables().size());
        assertTrue(elapsedMs < 500, "10-table DP enumeration should complete quickly, took " + elapsedMs + " ms");

        PlanNode naive = optimizer.buildNaiveLeftDeepPlan(stmt10, stats, cols, (t, c, eq) -> null);
        assertTrue(optimal.getCost() < naive.getCost());

        // Verify 11-table query safely respects MAX_DP_TABLES = 10 guard
        stats.put("t10", new TableStatistics(10, 1, Map.of()));
        cols.put("t10", List.of("id"));
        List<JoinClause> joins11 = new ArrayList<>(joins);
        joins11.add(new JoinClause("t10", "t9.next_id", "t10.id", JoinClause.JoinType.INNER));
        SelectStatement stmt11 = new SelectStatement(
            "t0", List.of("*"), null, null, null, null,
            joins11, List.of(), List.of(), null, List.of(), List.of(), List.of()
        );
        assertFalse(SystemROptimizer.canOptimize(stmt11, stats));
    }

    @Test
    void testEndToEndSixTableJoinExecutionWithAnalyze(@TempDir Path tempDir) {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setBufferPoolSize(128);
        StratosDB db = new StratosDB(config);
        try {
            db.execute("CREATE TABLE t1 (id INT, t2_id INT, v1 INT)");
            db.execute("CREATE TABLE t2 (id INT, t3_id INT, v2 INT)");
            db.execute("CREATE TABLE t3 (id INT, t4_id INT, v3 INT)");
            db.execute("CREATE TABLE t4 (id INT, t5_id INT, v4 INT)");
            db.execute("CREATE TABLE t5 (id INT, t6_id INT, v5 INT)");
            db.execute("CREATE TABLE t6 (id INT, label VARCHAR)");

            for (int i = 1; i <= 50; i++) {
                db.execute("INSERT INTO t1 VALUES (" + i + ", " + ((i % 10) + 1) + ", " + (i * 10) + ")");
            }
            for (int i = 1; i <= 10; i++) {
                db.execute("INSERT INTO t2 VALUES (" + i + ", " + ((i % 5) + 1) + ", " + (i * 2) + ")");
            }
            for (int i = 1; i <= 5; i++) {
                db.execute("INSERT INTO t3 VALUES (" + i + ", " + ((i % 3) + 1) + ", " + i + ")");
            }
            for (int i = 1; i <= 3; i++) {
                db.execute("INSERT INTO t4 VALUES (" + i + ", " + ((i % 2) + 1) + ", " + i + ")");
            }
            for (int i = 1; i <= 2; i++) {
                db.execute("INSERT INTO t5 VALUES (" + i + ", 1, " + i + ")");
            }
            db.execute("INSERT INTO t6 VALUES (1, 'root')");

            for (int i = 1; i <= 6; i++) {
                assertTrue(db.execute("ANALYZE t" + i).isSuccess());
            }

            String sql = "SELECT t1.id, t6.label FROM t1 "
                + "JOIN t2 ON t1.t2_id = t2.id "
                + "JOIN t3 ON t2.t3_id = t3.id "
                + "JOIN t4 ON t3.t4_id = t4.id "
                + "JOIN t5 ON t4.t5_id = t5.id "
                + "JOIN t6 ON t5.t6_id = t6.id";

            QueryResult explainRes = db.execute("EXPLAIN " + sql);
            assertTrue(explainRes.isSuccess());
            assertNotNull(explainRes.getMessage());

            QueryResult res = db.execute(sql);
            assertTrue(res.isSuccess());
            assertEquals(50, res.getRows().size());
        } finally {
            db.shutdown();
        }
    }

    private static ColumnStatistics colStats(String table, String col, long rows, long pages, double ndv) {
        return new ColumnStatistics(table, col, rows, pages, 0.0, 4, ndv, null, null, null, null, 0.0);
    }
}
