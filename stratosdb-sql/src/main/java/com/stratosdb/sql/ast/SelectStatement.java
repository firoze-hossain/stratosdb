package com.stratosdb.sql.ast;

import java.util.List;

public record SelectStatement(String tableName, List<String> columns, WhereExpr where,
                              String orderBy, String limit, List<JoinClause> joins,
                              List<AggregateCall> aggregates, List<String> groupBy, String havingClause,
                              List<WindowFunctionCall> windowFunctions, List<FunctionCallItem> functionCalls) implements Statement {}