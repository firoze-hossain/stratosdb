package com.stratosdb.sql.ast;

public record CheckConstraintDefinition(
    String name,
    String expressionSql,
    WhereExpr expression
) {}
