package com.stratosdb.sql.ast;

/** SHOW STATS - real-time engine metrics as a queryable result, rather than only log lines. */
public record ShowStatsStatement() implements Statement {}
