package com.stratosdb.sql.ast;

public record VacuumStatement(String tableName) implements Statement {}
