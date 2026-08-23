package com.stratosdb.sql.ast;

/**
 * SHOW CATALOG - a real, direct SQL surface onto ExecutorEngine's own
 * catalogLines registry: for every schema object (table, view, index,
 * sequence, function, procedure, trigger, extension, native function),
 * this engine already persists the object's own exact, original CREATE
 * statement text, verbatim, for restart survival. SHOW CATALOG exposes
 * that same, already-existing, exact text over SQL, so any real client
 * (not just this engine's own internals) can read it back - the actual
 * foundation stratosdump (see stratosdb-cli) is built on: rather than
 * separately re-serializing each object type's own AST back into SQL
 * text by hand (a real, separate source of subtle drift from what the
 * object actually is), a dump tool can replay the exact DDL this engine
 * itself already ran.
 */
public record ShowCatalogStatement() implements Statement {}
