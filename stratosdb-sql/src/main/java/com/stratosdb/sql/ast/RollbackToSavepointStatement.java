package com.stratosdb.sql.ast;

public record RollbackToSavepointStatement(String name) implements Statement {}
