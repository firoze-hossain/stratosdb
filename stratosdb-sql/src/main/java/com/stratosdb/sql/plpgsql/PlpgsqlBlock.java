package com.stratosdb.sql.plpgsql;

import java.util.List;

/**
 * The real, fully parsed representation of a procedural ("LANGUAGE
 * plpgsql") function or procedure body - real local variable
 * declarations, then the real statements to run. See PlpgsqlParser for
 * how this is built from a body's own raw dollar-quoted text, and
 * PlpgsqlInterpreter for how it's actually executed.
 */
public record PlpgsqlBlock(List<PlpgsqlVarDecl> declarations, List<PlpgsqlStmt> statements) {
    public record PlpgsqlVarDecl(String name, String dataType, PlpgsqlExpr initialValue) {}
}
