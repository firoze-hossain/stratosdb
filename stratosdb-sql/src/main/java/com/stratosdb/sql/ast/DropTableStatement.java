package com.stratosdb.sql.ast;

public record DropTableStatement(String tableName, boolean ifExists) implements Statement {
    public DropTableStatement(String tableName) {
        this(tableName, false);
    }
}
