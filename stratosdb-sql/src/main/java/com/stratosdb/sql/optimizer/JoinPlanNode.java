package com.stratosdb.sql.optimizer;

import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.sql.execution.*;
import com.stratosdb.sql.executor.ExecutorEngine;
import com.stratosdb.transaction.Transaction;

import java.util.*;

/**
 * Join plan node representing a 2-way join between two subplans.
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
    private final String leftColumn;
    private final String rightColumn;
    private final boolean buildOnRight;
    private final double cost;
    private final double estimatedRows;
    private final List<String> outputColumns;
    private final Set<String> coveredTables;

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
        this.left = left;
        this.right = right;
        this.algorithm = algorithm;
        this.joinType = joinType != null ? joinType : JoinClause.JoinType.INNER;
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
        this.buildOnRight = buildOnRight;
        this.cost = cost;
        this.estimatedRows = estimatedRows;
        this.outputColumns = outputColumns != null ? List.copyOf(outputColumns) : List.of();

        Set<String> tables = new HashSet<>();
        if (left != null) tables.addAll(left.getCoveredTables());
        if (right != null) tables.addAll(right.getCoveredTables());
        this.coveredTables = Collections.unmodifiableSet(tables);
    }

    public PlanNode getLeft() { return left; }
    public PlanNode getRight() { return right; }
    public JoinAlgorithm getAlgorithm() { return algorithm; }
    public JoinClause.JoinType getJoinType() { return joinType; }
    public String getLeftColumn() { return leftColumn; }
    public String getRightColumn() { return rightColumn; }
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
        if (leftColumn != null && rightColumn != null) {
            sb.append(" ON ").append(leftColumn).append("=").append(rightColumn);
        }
        sb.append("\n");
        sb.append(left.explain(indent + "  ")).append("\n");
        sb.append(right.explain(indent + "  "));
        return sb.toString();
    }

    public String toCompactString() {
        // e.g. "Hash Join (cost=291.6 rows=10000): Seq Scan on countries -> Seq Scan on customers -> Seq Scan on orders ON ..."
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
        if (leftColumn != null && rightColumn != null) {
            sb.append(" ON ").append(leftColumn).append("=").append(rightColumn);
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
                return new HashJoinOperator(leftOp, rightOp, leftColumn, rightColumn, joinType);
            } else {
                // Swap so build side is right internally
                return new HashJoinOperator(rightOp, leftOp, rightColumn, leftColumn, joinType);
            }
        } else if (algorithm == JoinAlgorithm.MERGE) {
            return new MergeJoinOperator(leftOp, rightOp, leftColumn, rightColumn, joinType);
        } else {
            return new NestedLoopJoinOperator(leftOp, rightOp, leftColumn, rightColumn, joinType);
        }
    }

    @Override
    public String toString() {
        return explain("");
    }
}
