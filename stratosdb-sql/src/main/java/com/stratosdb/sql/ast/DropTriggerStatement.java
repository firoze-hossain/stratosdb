package com.stratosdb.sql.ast;

public record DropTriggerStatement(String name, String tableName) implements Statement {}
