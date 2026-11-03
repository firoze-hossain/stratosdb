package com.stratosdb.sql.ast;

/**
 * CREATE SEQUENCE [IF NOT EXISTS] name [AS type] [START [WITH] n] [INCREMENT [BY] n]
 * [MINVALUE n | NO MINVALUE] [MAXVALUE n | NO MAXVALUE] [CACHE n] [CYCLE | NO CYCLE].
 * startValue/incrementBy default to 1 when not specified.
 */
public record CreateSequenceStatement(String name, long startValue, long incrementBy, boolean ifNotExists,
                                      Long minValue, Long maxValue, Long cache, boolean cycle) implements Statement {
    public CreateSequenceStatement(String name, long startValue, long incrementBy) {
        this(name, startValue, incrementBy, false, null, null, null, false);
    }
}
