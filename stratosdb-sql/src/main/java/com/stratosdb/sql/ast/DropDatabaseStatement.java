package com.stratosdb.sql.ast;

public record DropDatabaseStatement(String databaseName) implements Statement {}
