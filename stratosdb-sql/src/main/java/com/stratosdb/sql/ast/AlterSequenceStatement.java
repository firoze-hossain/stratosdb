package com.stratosdb.sql.ast;

public record AlterSequenceStatement(String sequenceName, Long restartValue, Long incrementBy,
                                     Long minValue, Long maxValue, Long cache, Boolean cycle,
                                     boolean ifExists) implements Statement {
    public AlterSequenceStatement(String sequenceName, Long restartValue, Long incrementBy) {
        this(sequenceName, restartValue, incrementBy, null, null, null, null, false);
    }
}
