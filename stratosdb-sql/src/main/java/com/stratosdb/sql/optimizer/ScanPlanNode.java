package com.stratosdb.sql.optimizer;

import com.stratosdb.sql.ast.WhereExpr;
import com.stratosdb.sql.execution.PhysicalOperator;
import com.stratosdb.sql.executor.ExecutorEngine;
import com.stratosdb.transaction.Transaction;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Access path plan node for a single relation: SeqScan or IndexScan.
 */
public class ScanPlanNode implements PlanNode {
    private final String tableName;
    private final String tableAlias;
    private final boolean isIndexScan;
    private final String indexName;
    private final String indexColumn;
    private final String operator;
    private final Long loKey;
    private final Long hiKey;
    private final WhereExpr filterWhere;
    private final double cost;
    private final double estimatedRows;
    private final List<String> outputColumns;

    public ScanPlanNode(String tableName,
                        String tableAlias,
                        boolean isIndexScan,
                        String indexName,
                        String indexColumn,
                        String operator,
                        Long loKey,
                        Long hiKey,
                        WhereExpr filterWhere,
                        double cost,
                        double estimatedRows,
                        List<String> outputColumns) {
        this.tableName = tableName;
        this.tableAlias = tableAlias;
        this.isIndexScan = isIndexScan;
        this.indexName = indexName;
        this.indexColumn = indexColumn;
        this.operator = operator;
        this.loKey = loKey;
        this.hiKey = hiKey;
        this.filterWhere = filterWhere;
        this.cost = cost;
        this.estimatedRows = estimatedRows;
        this.outputColumns = outputColumns != null ? List.copyOf(outputColumns) : List.of();
    }

    public String getTableName() { return tableName; }
    public String getTableAlias() { return tableAlias; }
    public String getEffectiveName() { return tableAlias != null ? tableAlias : tableName; }
    public boolean isIndexScan() { return isIndexScan; }
    public String getIndexName() { return indexName; }
    public String getIndexColumn() { return indexColumn; }
    public String getOperator() { return operator; }
    public Long getLoKey() { return loKey; }
    public Long getHiKey() { return hiKey; }
    public WhereExpr getFilterWhere() { return filterWhere; }

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
        return Collections.singleton(getEffectiveName().toLowerCase(java.util.Locale.ROOT));
    }

    @Override
    public String explain(String indent) {
        String effName = getEffectiveName();
        String aliasSuffix = (tableAlias != null && !tableAlias.equalsIgnoreCase(tableName))
            ? " " + tableAlias
            : "";
        if (isIndexScan) {
            String range = (loKey != null && loKey.equals(hiKey))
                ? "=" + loKey
                : "[" + loKey + ", " + hiKey + "]";
            return indent + String.format("Index Scan using %s on %s%s (column=%s, range=%s) (cost=%.1f rows=%.0f)",
                indexName, tableName, aliasSuffix, indexColumn, range, cost, estimatedRows);
        } else {
            return indent + String.format("Seq Scan on %s%s (cost=%.1f rows=%.0f)",
                tableName, aliasSuffix, cost, estimatedRows);
        }
    }

    @Override
    public PhysicalOperator buildOperator(ExecutorEngine engine, Transaction txn) {
        return engine.buildScanOperator(tableName, tableAlias, isIndexScan, indexName, indexColumn, loKey, hiKey, filterWhere, txn);
    }

    @Override
    public String toString() {
        return explain("");
    }
}
