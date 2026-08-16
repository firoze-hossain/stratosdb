package com.stratosdb.sql.ast;

/** CREATE SEQUENCE name [START [WITH] n] [INCREMENT [BY] n]. startValue/incrementBy default to 1 (matching real Postgres's own default sequence behavior) when not specified. */
public record CreateSequenceStatement(String name, long startValue, long incrementBy) implements Statement {}
