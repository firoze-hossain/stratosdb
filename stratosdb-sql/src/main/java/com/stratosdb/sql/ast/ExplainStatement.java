package com.stratosdb.sql.ast;

public record ExplainStatement(SelectStatement select, boolean analyze) implements Statement {
    public ExplainStatement(SelectStatement select) {
        this(select, false);
    }
}
