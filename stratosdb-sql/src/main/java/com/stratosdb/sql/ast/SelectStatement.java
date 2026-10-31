package com.stratosdb.sql.ast;

import java.util.List;

/**
 * columnAliases: parallel-indexed to columns - the alias given for that
 * position's own plain column reference (e.g. "person0_.id AS id0_"), or
 * null/empty string when none was given. A separate, additive field
 * rather than folding the alias into columns itself, since columns' own
 * raw text (e.g. "person0_.id") is still needed for real lookup against
 * the underlying tuple (see ExecutorEngine.findColumnValue) - the alias
 * only ever renames the OUTPUT column, after that real lookup already
 * happened (see ExecutorEngine.applyColumnAliases).
 *
 * fromSubquery and tableAlias: support derived tables in FROM clause
 * ("FROM (SELECT ...) AS dt") and table aliases ("FROM customers AS c").
 */
public record SelectStatement(String tableName, List<String> columns, WhereExpr where,
                              String orderBy, String limit, String offset, List<JoinClause> joins,
                              List<AggregateCall> aggregates, List<String> groupBy, String havingClause,
                              List<WindowFunctionCall> windowFunctions, List<FunctionCallItem> functionCalls,
                              List<String> columnAliases, List<OrderItem> orderItems,
                              SelectStatement fromSubquery, String tableAlias) implements Statement {

    public SelectStatement(String tableName, List<String> columns, WhereExpr where,
                           String orderBy, String limit, String offset, List<JoinClause> joins,
                           List<AggregateCall> aggregates, List<String> groupBy, String havingClause,
                           List<WindowFunctionCall> windowFunctions, List<FunctionCallItem> functionCalls,
                           List<String> columnAliases, List<OrderItem> orderItems) {
        this(tableName, columns, where, orderBy, limit, offset, joins, aggregates, groupBy, havingClause,
             windowFunctions, functionCalls, columnAliases, orderItems, null, null);
    }

    public SelectStatement(String tableName, List<String> columns, WhereExpr where,
                           String orderBy, String limit, String offset, List<JoinClause> joins,
                           List<AggregateCall> aggregates, List<String> groupBy, String havingClause,
                           List<WindowFunctionCall> windowFunctions, List<FunctionCallItem> functionCalls,
                           List<String> columnAliases) {
        this(tableName, columns, where, orderBy, limit, offset, joins, aggregates, groupBy, havingClause,
             windowFunctions, functionCalls, columnAliases, List.of(), null, null);
    }

    @Override
    public List<OrderItem> orderItems() {
        return orderItems != null ? orderItems : List.of();
    }

    public boolean isDerivedTable() {
        return fromSubquery != null;
    }

    public String effectiveTableName() {
        return tableAlias != null ? tableAlias : tableName;
    }

    public SelectStatement withWhere(WhereExpr newWhere) {
        return new SelectStatement(tableName, columns, newWhere, orderBy, limit, offset, joins,
                                   aggregates, groupBy, havingClause, windowFunctions, functionCalls,
                                   columnAliases, orderItems, fromSubquery, tableAlias);
    }

    public SelectStatement withJoins(List<JoinClause> newJoins) {
        return new SelectStatement(tableName, columns, where, orderBy, limit, offset, newJoins,
                                   aggregates, groupBy, havingClause, windowFunctions, functionCalls,
                                   columnAliases, orderItems, fromSubquery, tableAlias);
    }

    public SelectStatement withJoinsAndWhere(List<JoinClause> newJoins, WhereExpr newWhere) {
        return new SelectStatement(tableName, columns, newWhere, orderBy, limit, offset, newJoins,
                                   aggregates, groupBy, havingClause, windowFunctions, functionCalls,
                                   columnAliases, orderItems, fromSubquery, tableAlias);
    }
}
