package com.stratosdb.sql.ast;

/** SHOW STATEMENTS - the real pg_stat_statements equivalent: per-normalized-query call count, timing, and row totals, aggregated across every connection. */
public record ShowStatementsStatement() implements Statement {}
