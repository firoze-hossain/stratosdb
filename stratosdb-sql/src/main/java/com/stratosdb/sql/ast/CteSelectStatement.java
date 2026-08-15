package com.stratosdb.sql.ast;

/**
 * A single, non-recursive CTE: WITH cteName AS (cteQuery) outerQuery.
 * Scoped to this one statement only - unlike CREATE VIEW, nothing here
 * gets persisted; cteName is only resolvable while this statement is
 * executing. Multiple CTEs in one WITH clause and recursive CTEs
 * (WITH RECURSIVE) are real further work, not attempted here.
 */
public record CteSelectStatement(String cteName, SelectStatement cteQuery, SelectStatement outerQuery) implements Statement {}
