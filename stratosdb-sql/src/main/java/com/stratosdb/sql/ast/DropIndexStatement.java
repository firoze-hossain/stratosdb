package com.stratosdb.sql.ast;

public record DropIndexStatement(String indexName, boolean ifExists) implements Statement {
    public DropIndexStatement(String indexName) {
        this(indexName, false);
    }
}
