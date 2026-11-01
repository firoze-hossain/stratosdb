package com.stratosdb.sql.optimizer;

import com.stratosdb.sql.execution.PhysicalOperator;
import com.stratosdb.sql.executor.ExecutorEngine;
import com.stratosdb.transaction.Transaction;

import java.util.List;
import java.util.Set;

/**
 * Common interface for cost-based physical query plan nodes.
 */
public interface PlanNode {
    double getCost();
    double getEstimatedRows();
    List<String> getOutputColumns();
    Set<String> getCoveredTables();
    String explain(String indent);
    PhysicalOperator buildOperator(ExecutorEngine engine, Transaction txn);
}
