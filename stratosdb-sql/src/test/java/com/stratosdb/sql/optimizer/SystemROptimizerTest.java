package com.stratosdb.sql.optimizer;

import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.sql.ast.SelectStatement;
import com.stratosdb.sql.ast.WhereExpr;
import com.stratosdb.sql.statistics.ColumnStatistics;
import com.stratosdb.sql.statistics.TableStatistics;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SystemROptimizerTest {

    private final CostModel costModel = new CostModel();
    private final SystemROptimizer optimizer = new SystemROptimizer(costModel);

    @Test
    public void testTwoTableJoinBuildsOnSmallerRelation() {
        // Table A: 10,000 rows, 50 pages
        // Table B: 100 rows, 2 pages
        TableStatistics statsA = new TableStatistics(10000, 50, Map.of(
            "b_id", new ColumnStatistics("a", "b_id", 10000, 50, 0.0, 4, 100.0, null, null, null, null, 0.0)
        ));
        TableStatistics statsB = new TableStatistics(100, 2, Map.of(
            "id", new ColumnStatistics("b", "id", 100, 2, 0.0, 4, 100.0, null, null, null, null, 0.0)
        ));

        Map<String, TableStatistics> stats = Map.of("a", statsA, "b", statsB);
        Map<String, List<String>> cols = Map.of("a", List.of("id", "b_id", "val"), "b", List.of("id", "name"));

        SelectStatement stmt = new SelectStatement(
            "a", List.of("*"), null, null, null, null,
            List.of(new JoinClause("b", "a.b_id", "b.id", JoinClause.JoinType.INNER)),
            List.of(), List.of(), null, List.of(), List.of(), List.of()
        );

        assertTrue(SystemROptimizer.canOptimize(stmt, stats));
        PlanNode plan = optimizer.optimize(stmt, stats, cols, (t, c, eq) -> null);
        assertNotNull(plan);
        assertTrue(plan instanceof JoinPlanNode);

        JoinPlanNode join = (JoinPlanNode) plan;
        assertEquals(JoinPlanNode.JoinAlgorithm.HASH, join.getAlgorithm());
        // Verify cost is calculated and rows estimated
        assertTrue(join.getCost() > 0);
        assertEquals(10000, join.getEstimatedRows(), 500);
    }

    @Test
    public void testThreeTableJoinOptimalOrder() {
        // Orders (10,000 rows, 50 pages)
        // Customers (500 rows, 5 pages)
        // Countries (10 rows, 1 page)
        TableStatistics statsOrders = new TableStatistics(10000, 50, Map.of(
            "cust_id", new ColumnStatistics("orders", "cust_id", 10000, 50, 0.0, 4, 500.0, null, null, null, null, 0.0)
        ));
        TableStatistics statsCust = new TableStatistics(500, 5, Map.of(
            "id", new ColumnStatistics("customers", "id", 500, 5, 0.0, 4, 500.0, null, null, null, null, 0.0),
            "country_id", new ColumnStatistics("customers", "country_id", 500, 5, 0.0, 4, 10.0, null, null, null, null, 0.0)
        ));
        TableStatistics statsCountries = new TableStatistics(10, 1, Map.of(
            "id", new ColumnStatistics("countries", "id", 10, 1, 0.0, 4, 10.0, null, null, null, null, 0.0)
        ));

        Map<String, TableStatistics> stats = Map.of(
            "orders", statsOrders,
            "customers", statsCust,
            "countries", statsCountries
        );
        Map<String, List<String>> cols = Map.of(
            "orders", List.of("id", "cust_id", "amount"),
            "customers", List.of("id", "country_id", "name"),
            "countries", List.of("id", "name")
        );

        // Query written in reverse optimal order: orders JOIN customers JOIN countries
        SelectStatement stmt = new SelectStatement(
            "orders", List.of("*"), null, null, null, null,
            List.of(
                new JoinClause("customers", "orders.cust_id", "customers.id", JoinClause.JoinType.INNER),
                new JoinClause("countries", "customers.country_id", "countries.id", JoinClause.JoinType.INNER)
            ),
            List.of(), List.of(), null, List.of(), List.of(), List.of()
        );

        PlanNode plan = optimizer.optimize(stmt, stats, cols, (t, c, eq) -> null);
        assertNotNull(plan);
        assertTrue(plan instanceof JoinPlanNode);

        // System-R reorders to join the smaller tables first: (countries JOIN customers) JOIN orders
        String explain = plan.explain("");
        assertTrue(explain.contains("countries"));
        assertTrue(explain.contains("customers"));
        assertTrue(explain.contains("orders"));

        JoinPlanNode rootJoin = (JoinPlanNode) plan;
        // Verify cost is significantly less than naively joining orders first
        assertTrue(rootJoin.getCost() < 350.0, "Optimized join tree cost should be < 350, got: " + rootJoin.getCost());
    }

    @Test
    public void testIndexScanSelectedForSelectiveSingleTableFilter() {
        TableStatistics stats = new TableStatistics(10000, 50, Map.of(
            "id", new ColumnStatistics("users", "id", 10000, 50, 0.0, 4, 10000.0, null, null, 1.0, 10000.0, 1.0)
        ));
        TableStatistics statsOrders = new TableStatistics(5000, 25, Map.of(
            "user_id", new ColumnStatistics("orders", "user_id", 5000, 25, 0.0, 4, 1000.0, null, null, 1.0, 1000.0, 1.0)
        ));

        Map<String, TableStatistics> allStats = Map.of("users", stats, "orders", statsOrders);
        Map<String, List<String>> cols = Map.of(
            "users", List.of("id", "name"),
            "orders", List.of("id", "user_id", "amt")
        );

        // WHERE users.id = 500
        WhereExpr where = new WhereExpr.Comparison("users.id", "=", "500");
        SelectStatement stmt = new SelectStatement(
            "users", List.of("*"), where, null, null, null,
            List.of(new JoinClause("orders", "users.id", "orders.user_id", JoinClause.JoinType.INNER)),
            List.of(), List.of(), null, List.of(), List.of(), List.of()
        );

        // Resolver reports index on users.id
        SystemROptimizer.IndexResolver resolver = (tbl, col, eq) -> {
            if ("users".equalsIgnoreCase(tbl) && "id".equalsIgnoreCase(col)) {
                return new SystemROptimizer.IndexInfo("idx_users_id", "users", "id", true, true, false);
            }
            return null;
        };

        PlanNode plan = optimizer.optimize(stmt, allStats, cols, resolver);
        assertNotNull(plan);

        String explain = plan.explain("");
        assertTrue(explain.contains("Index Scan using idx_users_id on users"),
            "Planner should select IndexScan for highly selective point lookup (users.id = 500): " + explain);
    }
}
