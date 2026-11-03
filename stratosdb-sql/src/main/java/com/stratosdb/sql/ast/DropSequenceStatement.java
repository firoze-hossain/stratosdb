package com.stratosdb.sql.ast;

public record DropSequenceStatement(String name, boolean ifExists) implements Statement {
    public DropSequenceStatement(String name) {
        this(name, false);
    }
}
