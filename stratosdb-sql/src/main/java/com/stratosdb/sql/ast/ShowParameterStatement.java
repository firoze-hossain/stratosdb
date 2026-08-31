package com.stratosdb.sql.ast;

/**
 * A generic SHOW <parameter_name> - the real Postgres mechanism for
 * reading a server GUC (Grand Unified Configuration) setting. Real
 * Postgres has dozens of these (standard_conforming_strings,
 * client_encoding, timezone, DateStyle, and many more), and virtually
 * every serious client/ORM queries at least a handful during its own
 * connection setup - a separate, named SQL command for each one
 * individually (the way SHOW STATS/SHOW TABLE STATS/etc. are each their
 * own grammar rule) would not scale. This one, general rule, ordered
 * after the more specific SHOW commands in the grammar (see
 * StratosSQL.g4's own sqlStatement rule), only ever matches when none
 * of those more specific keywords (STATS, TABLE, STATEMENTS, ACTIVITY,
 * TRANSACTION) are what follows SHOW - those are all their own,
 * separate, reserved tokens, never tokenized as a plain IDENTIFIER at
 * all, so there's no real ambiguity between this rule and them.
 *
 * See ExecutorEngine.executeShowParameter for the real, honest handling:
 * a small, explicit map of known parameter names to their own real,
 * honest values, and a clear error - not a fabricated value - for
 * anything not in it.
 */
public record ShowParameterStatement(String parameterName) implements Statement {}
