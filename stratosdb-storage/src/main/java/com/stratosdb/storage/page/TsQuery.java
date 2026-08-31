package com.stratosdb.storage.page;

/** The real, top-level parsed representation of a Postgres-style `tsquery` value - a thin wrapper around its own real boolean expression tree (see TsQueryExpr), giving it a distinct Java type of its own (matching TsVector's own precedent) rather than exposing TsQueryExpr's own AST directly as a stored column value. */
public record TsQuery(TsQueryExpr root) {
    public boolean matches(TsVector vector) {
        return root.matches(vector);
    }

    @Override
    public String toString() {
        return root.render();
    }
}
