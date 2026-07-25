package com.stratosdb.sql.ast;

public record CreateIndexStatement(String indexName, String tableName, String columnName) implements Statement {}
