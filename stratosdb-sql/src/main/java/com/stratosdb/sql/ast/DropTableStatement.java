package com.stratosdb.sql.ast;

public record DropTableStatement(String tableName) implements Statement {}