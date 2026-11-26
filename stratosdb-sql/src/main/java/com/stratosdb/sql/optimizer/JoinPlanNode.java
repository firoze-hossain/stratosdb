package com.stratosdb.sql.optimizer;

import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.sql.ast.WhereExpr;
import com.stratosdb.sql.execution.*;
import com.stratosdb.sql.executor.ExecutorEngine;
import com.stratosdb.transaction.Transaction;

import java.util.*;

/**
 * Join plan node representing a 2-way join between two subplans.
 * Supports multi-column composite equi-keys and residual non-equi filters.
 */
public class JoinPlanNode implements PlanNode {
    public enum JoinAlgorithm {
        HASH,
        MERGE,
        NESTED_LOOP
    }

    private final PlanNode left;
    private final PlanNode right;
    private final JoinAlgorithm algorithm;
    private final JoinClause.JoinType joinType;
    private final List<String> leftColumns;
    private final List<String> rightColumns;
    private final WhereExpr nonEquiFilter;
    private final boolean buildOnRight;
    private final double cost;
    private final double estimatedRows;
    private final List<String> outputColumns;
    private final Set<String> coveredTables;

    public JoinPlanNode(PlanNode left,
                        PlanNode right,
                        JoinAlgorithm algorithm,
                        JoinClause.JoinType joinType,
                        List<String> leftColumns,
                        List<String> rightColumns,
                        WhereExpr nonEquiFilter,
                        boolean buildOnRight,
                        double cost,
                        double estimatedRows,
                        List<String> outputColumns) {
        this.left = left;
        this.right = right;
        this.algorithm = algorithm;
        this.joinType = joinType != null ? joinType : JoinClause.JoinType.INNER;
        this.leftColumns = leftColumns != null ? List.copyOf(leftColumns) : List.of();
        this.rightColumns = rightColumns != null ? List.copyOf(rightColumns) : List.of();
        this.nonEquiFilter = nonEquiFilter;
        this.buildOnRight = buildOnRight;
        this.cost = cost;
        this.estimatedRows = estimatedRows;
        this.outputColumns = outputColumns != null ? List.copyOf(outputColumns) : List.of();

        Set<String> tables = new HashSet<>();
        if (left != null) tables.addAll(left.getCoveredTables());
        if (right != null) tables.addAll(right.getCoveredTables());
        this.coveredTables = Collections.unmodifiableSet(tables);
    }

    public JoinPlanNode(PlanNode left,
                        PlanNode right,
                        JoinAlgorithm algorithm,
                        JoinClause.JoinType joinType,
                        String leftColumn,
                        String rightColumn,
                        boolean buildOnRight,
                        double cost,
                        double estimatedRows,
                        List<String> outputColumns) {
        this(left, right, algorithm, joinType,
             leftColumn != null ? List.of(leftColumn) : List.of(),
             rightColumn != null ? List.of(rightColumn) : List.of(),
             null, buildOnRight, cost, estimatedRows, outputColumns);
    }

    public PlanNode getLeft() { return left; }
    public PlanNode getRight() { return right; }
    public JoinAlgorithm getAlgorithm() { return algorithm; }
    public JoinClause.JoinType getJoinType() { return joinType; }
    public String getLeftColumn() { return !leftColumns.isEmpty() ? leftColumns.get(0) : null; }
    public String getRightColumn() { return !rightColumns.isEmpty() ? rightColumns.get(0) : null; }
    public List<String> getLeftColumns() { return leftColumns; }
    public List<String> getRightColumns() { return rightColumns; }
    public WhereExpr getNonEquiFilter() { return nonEquiFilter; }
    public boolean isBuildOnRight() { return buildOnRight; }

    @Override
    public double getCost() {
        return cost;
    }

    @Override
    public double getEstimatedRows() {
        return estimatedRows;
    }

    @Override
    public List<String> getOutputColumns() {
        return outputColumns;
    }

    @Override
    public Set<String> getCoveredTables() {
        return coveredTables;
    }

    @Override
    public String explain(String indent) {
        StringBuilder sb = new StringBuilder();
        String algoName = switch (algorithm) {
            case HASH -> "Hash Join";
            case MERGE -> "Merge Join";
            case NESTED_LOOP -> "Nested Loop";
        };
        sb.append(indent).append(algoName)
          .append(String.format(" (cost=%.1f rows=%.0f)", cost, estimatedRows));
        if (!leftColumns.isEmpty() && !rightColumns.isEmpty()) {
            sb.append(" ON ");
            for (int i = 0; i < leftColumns.size(); i++) {
                if (i > 0) sb.append(" AND ");
                sb.append(leftColumns.get(i)).append("=").append(rightColumns.get(i));
            }
            if (nonEquiFilter != null) {
                sb.append(" AND ").append(nonEquiFilter);
            }
        }
        sb.append("\n");
        sb.append(left.explain(indent + "  ")).append("\n");
        sb.append(right.explain(indent + "  "));
        return sb.toString();
    }

    public String toCompactString() {
        List<String> scanDescriptions = new ArrayList<>();
        collectScans(this, scanDescriptions);
        String algoName = switch (algorithm) {
            case HASH -> "Hash Join";
            case MERGE -> "Merge Join";
            case NESTED_LOOP -> "Nested Loop Join";
        };
        StringBuilder sb = new StringBuilder();
        sb.append(algoName).append(String.format(" (cost=%.1f rows=%.0f): ", cost, estimatedRows));
        sb.append(String.join(" -> ", scanDescriptions));
        if (!leftColumns.isEmpty() && !rightColumns.isEmpty()) {
            sb.append(" ON ");
            for (int i = 0; i < leftColumns.size(); i++) {
                if (i > 0) sb.append(" AND ");
                sb.append(leftColumns.get(i)).append("=").append(rightColumns.get(i));
            }
            if (nonEquiFilter != null) {
                sb.append(" AND ").append(nonEquiFilter);
            }
        }
        return sb.toString();
    }

    private static void collectScans(PlanNode node, List<String> list) {
        if (node instanceof ScanPlanNode scan) {
            String eff = scan.getEffectiveName();
            if (scan.isIndexScan()) {
                list.add("Index Scan using " + scan.getIndexName() + " on " + eff);
            } else {
                list.add("Seq Scan on " + eff);
            }
        } else if (node instanceof JoinPlanNode join) {
            collectScans(join.getLeft(), list);
            collectScans(join.getRight(), list);
        }
    }

    @Override
    public PhysicalOperator buildOperator(ExecutorEngine engine, Transaction txn) {
        PhysicalOperator leftOp = left.buildOperator(engine, txn);
        PhysicalOperator rightOp = right.buildOperator(engine, txn);

        if (algorithm == JoinAlgorithm.HASH) {
            if (buildOnRight) {
                return new HashJoinOperator(leftOp, rightOp, leftColumns, rightColumns, nonEquiFilter, joinType);
            } else {
                // Swap so build side is right internally
                return new HashJoinOperator(rightOp, leftOp, rightColumns, leftColumns, nonEquiFilter, joinType);
            }
        } else if (algorithm == JoinAlgorithm.MERGE) {
            return new MergeJoinOperator(leftOp, rightOp, leftColumns, rightColumns, nonEquiFilter, joinType);
        } else {
            return new NestedLoopJoinOperator(leftOp, rightOp, leftColumns, rightColumns, nonEquiFilter, joinType);
        }
    }

    @Override
    public String toString() {
        return explain("");
    }
}
