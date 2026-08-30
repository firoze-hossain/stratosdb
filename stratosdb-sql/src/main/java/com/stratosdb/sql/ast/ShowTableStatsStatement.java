package com.stratosdb.sql.ast;

/** SHOW TABLE STATS - the real pg_stat_user_tables equivalent: one row per table with real activity, with seq-scan count, rows returned, and rows inserted/updated/deleted. */
public record ShowTableStatsStatement() implements Statement {}
