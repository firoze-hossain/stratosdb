package com.stratosdb.sql.ast;

/**
 * A generic SET <parameter_name> = <value> - the real Postgres mechanism
 * for setting a server GUC (Grand Unified Configuration) setting for the
 * current session. Found missing entirely during a real, broad driver/ORM
 * verification pass: the REAL, OFFICIAL org.postgresql JDBC driver -
 * not a StratosDB-specific or Hibernate-specific client at all - sends
 * `SET extra_float_digits = 3` as a completely standard part of its own
 * connection setup whenever the server reports itself as PostgreSQL 9.0
 * or newer (see this engine's own real "server_version" ParameterStatus -
 * StdWireServer already reports "16.0"). Without this, the official
 * driver cannot even complete its own connection handshake - a broad,
 * not niche, compatibility gap.
 *
 * See ExecutorEngine.executeSetParameter for the real, honestly-scoped
 * handling: this engine has no real per-session GUC settings store at
 * all (the same honest limitation set_config() already carries - see
 * ExecutorEngine.invokeFunction's own comment on it) - SET is accepted
 * and acknowledged, matching real Postgres's own "SET" success message,
 * without genuinely applying or remembering the new value anywhere.
 */
public record SetParameterStatement(String parameterName, String value) implements Statement {}
