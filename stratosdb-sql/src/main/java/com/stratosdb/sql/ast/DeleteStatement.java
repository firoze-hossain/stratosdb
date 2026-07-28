package com.stratosdb.sql.ast;

public record DeleteStatement(String tableName, WhereExpr where) implements Statement {}