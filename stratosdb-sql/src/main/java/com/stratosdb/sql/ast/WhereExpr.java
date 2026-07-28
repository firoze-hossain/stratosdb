package com.stratosdb.sql.ast;

import java.util.List;

/**
 * A real expression tree for WHERE/HAVING conditions, replacing the
 * previous design where the whole clause was captured as raw text and
 * re-parsed with string splitting at evaluation time. That approach could
 * only ever handle a single flat "column op literal" predicate correctly -
 * AND/OR/NOT/LIKE/IN were accepted by the grammar but silently
 * misevaluated by the executor (see PROGRESS.md for the real bug this
 * uncovered: a compound AND condition returned wrong rows, not an error,
 * which is worse). This tree is built once at parse time by SqlParser and
 * evaluated recursively by ExecutorEngine - see evaluateWhere().
 */
public sealed interface WhereExpr {
    record Comparison(String column, String operator, String literal) implements WhereExpr {}

    /** column op column - needed for correlated subquery predicates like "orders.customer_id = customers.id", where neither side is a literal. */
    record ColumnComparison(String leftColumn, String operator, String rightColumn) implements WhereExpr {}

    record Like(String column, String pattern) implements WhereExpr {}

    record InList(String column, List<String> values, boolean negated) implements WhereExpr {}

    /** column IN (SELECT ...) / column NOT IN (SELECT ...) - the subquery must produce exactly one column. */
    record InSubquery(String column, SelectStatement subquery, boolean negated) implements WhereExpr {}

    /** column = (SELECT ...) and friends - the subquery must produce exactly one row and one column. */
    record ScalarSubqueryComparison(String column, String operator, SelectStatement subquery) implements WhereExpr {}

    /** EXISTS (SELECT ...) / NOT EXISTS (SELECT ...) - may be correlated (the subquery's own WHERE can reference the outer row). */
    record ExistsSubquery(SelectStatement subquery, boolean negated) implements WhereExpr {}

    record And(WhereExpr left, WhereExpr right) implements WhereExpr {}
    record Or(WhereExpr left, WhereExpr right) implements WhereExpr {}
    record Not(WhereExpr inner) implements WhereExpr {}
}
