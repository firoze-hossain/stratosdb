package com.stratosdb.storage.page;

/**
 * The real, parsed boolean expression tree for a Postgres-style
 * `tsquery` - a lexeme (normalized the same way TsVector's own lexemes
 * are), or a real AND/OR/NOT combination of sub-queries, matching real
 * Postgres's own `&`/`|`/`!` tsquery operators (see TextSearch's own
 * `parseTsQuery` for the real, small parser that builds this).
 */
public sealed interface TsQueryExpr {
    record Lexeme(String value) implements TsQueryExpr {}
    record And(TsQueryExpr left, TsQueryExpr right) implements TsQueryExpr {}
    record Or(TsQueryExpr left, TsQueryExpr right) implements TsQueryExpr {}
    record Not(TsQueryExpr operand) implements TsQueryExpr {}

    /** True if this query's own boolean expression is satisfied against the given tsvector - a lexeme node is true exactly when the vector contains it. */
    default boolean matches(TsVector vector) {
        return switch (this) {
            case Lexeme l -> vector.contains(l.value());
            case And a -> a.left().matches(vector) && a.right().matches(vector);
            case Or o -> o.left().matches(vector) || o.right().matches(vector);
            case Not n -> !n.operand().matches(vector);
        };
    }

    /** Real Postgres's own real tsquery display format: a bare lexeme is quoted; AND/OR/NOT are rendered with their own real `&`/`|`/`!` operators, with NOT and AND binding tighter than OR (real, standard boolean precedence) - parentheses are added only where real precedence would otherwise change the parsed meaning. */
    default String render() {
        return switch (this) {
            case Lexeme l -> "'" + l.value() + "'";
            case Not n -> "!" + parenthesizeIfNeeded(n.operand(), true);
            case And a -> parenthesizeIfNeeded(a.left(), true) + " & " + parenthesizeIfNeeded(a.right(), true);
            case Or o -> parenthesizeIfNeeded(o.left(), false) + " | " + parenthesizeIfNeeded(o.right(), false);
        };
    }

    private static String parenthesizeIfNeeded(TsQueryExpr child, boolean tightContext) {
        boolean needsParens = tightContext && child instanceof Or;
        String rendered = child.render();
        return needsParens ? "(" + rendered + ")" : rendered;
    }
}
