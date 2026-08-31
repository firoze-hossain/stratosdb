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
 * happened (see ExecutorEngine.applyColumnAliases). Found missing
 * entirely during a real, broad driver/ORM verification pass: Hibernate's
 * own HQL-to-SQL translator always aliases every projected column
 * ("person0_.id AS id0_"), and the official JDBC driver then reads the
 * result set BACK by that exact alias name, not the original column.
 */
public record SelectStatement(String tableName, List<String> columns, WhereExpr where,
                              String orderBy, String limit, List<JoinClause> joins,
                              List<AggregateCall> aggregates, List<String> groupBy, String havingClause,
                              List<WindowFunctionCall> windowFunctions, List<FunctionCallItem> functionCalls,
                              List<String> columnAliases) implements Statement {}