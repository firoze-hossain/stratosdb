package com.stratosdb.sql.ast;

/**
 * SHOW TRANSACTION ISOLATION LEVEL - a real Postgres meta-command
 * virtually every serious client/ORM calls as part of its own
 * connection initialization (SQLAlchemy's own dialect does this
 * directly). Reports "read committed" - the real Postgres default, and
 * the honest answer given this engine's own actual MVCC snapshot
 * behavior hasn't been precisely characterized enough to claim the
 * stronger "repeatable read" instead (see ExecutorEngine's own
 * executeShowTransactionIsolationLevel for the full reasoning).
 */
public record ShowTransactionIsolationLevelStatement() implements Statement {}
