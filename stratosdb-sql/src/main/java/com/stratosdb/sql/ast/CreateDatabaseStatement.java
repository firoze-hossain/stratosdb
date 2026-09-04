package com.stratosdb.sql.ast;

public record CreateDatabaseStatement(String databaseName) implements Statement {}
