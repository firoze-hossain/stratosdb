package com.stratosdb.sql.ast;

/**
 * WITH RECURSIVE cteName AS (baseQuery UNION ALL recursiveQuery) outerQuery.
 *
 * recursiveQuery must reference cteName in its own FROM clause (that
 * self-reference is what makes it "recursive" - the same detection real
 * SQL engines use). Evaluated by fixpoint iteration: run baseQuery once to
 * seed the result; then repeatedly run recursiveQuery with cteName
 * resolving to only the PREVIOUS iteration's newly-produced rows (not the
 * whole accumulated set - standard "working table" semantics, matching
 * real Postgres), adding whatever's newly produced each time, until an
 * iteration adds nothing more.
 */
public record RecursiveCteSelectStatement(String cteName, SelectStatement baseQuery, SelectStatement recursiveQuery, SelectStatement outerQuery) implements Statement {}
