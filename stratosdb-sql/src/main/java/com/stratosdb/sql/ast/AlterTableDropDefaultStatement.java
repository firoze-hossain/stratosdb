package com.stratosdb.sql.ast;

public record AlterTableDropDefaultStatement(String tableName, String columnName) implements Statement {}
