package com.stratosdb.sql.ast;

public record AlterTableDropColumnStatement(String tableName, String columnName) implements Statement {}
