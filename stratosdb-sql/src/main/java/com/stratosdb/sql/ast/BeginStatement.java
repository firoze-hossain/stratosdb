package com.stratosdb.sql.ast;

import com.stratosdb.transaction.IsolationLevel;

public record BeginStatement(IsolationLevel isolationLevel) implements Statement {
    public BeginStatement() {
        this(null);
    }
}
