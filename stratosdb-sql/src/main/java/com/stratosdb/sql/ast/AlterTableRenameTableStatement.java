package com.stratosdb.sql.ast;

public record AlterTableRenameTableStatement(String oldTableName, String newTableName) implements Statement {}
