package com.stratosdb.sql.ast;

public record DeleteStatement(String tableName, String whereClause) implements Statement {}