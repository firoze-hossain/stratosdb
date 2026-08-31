package com.stratosdb.sql.plpgsql;

import java.util.List;

/**
 * A real, deliberately non-exhaustive expression in the procedural
 * language's own body - literals, variables, arithmetic, comparison,
 * boolean logic, and function calls. See StratosSQL.g4's own plpgsqlExpr
 * rule for the real, standard operator precedence this AST already
 * reflects (built correctly at parse time, not re-derived here).
 */
public sealed interface PlpgsqlExpr {
    record Literal(Object value) implements PlpgsqlExpr {}
    record Variable(String name) implements PlpgsqlExpr {}
    /** op is one of: *, /, +, -, =, <, >, <=, >=, <>, AND, OR */
    record Binary(String op, PlpgsqlExpr left, PlpgsqlExpr right) implements PlpgsqlExpr {}
    /** op is one of: NOT, NEG (unary minus) */
    record Unary(String op, PlpgsqlExpr operand) implements PlpgsqlExpr {}
    record FunctionCall(String functionName, List<PlpgsqlExpr> args) implements PlpgsqlExpr {}
}
