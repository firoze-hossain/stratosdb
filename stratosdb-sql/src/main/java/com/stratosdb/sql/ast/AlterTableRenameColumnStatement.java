package com.stratosdb.sql.ast;

public record AlterTableRenameColumnStatement(String tableName, String oldColumnName, String newColumnName) implements Statement {}
