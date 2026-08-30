package com.stratosdb.sql.ast;

/** SHOW ACTIVITY - the real pg_stat_activity equivalent: one row per currently-registered connection, its state, and its current or last query. */
public record ShowActivityStatement() implements Statement {}
