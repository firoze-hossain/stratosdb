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

    /** column CONTAINS 'word' - GIN's real, primary use case (see GinIndex): a whole-word text-search predicate. Usable even without a GIN index on the column (falls back to a direct tokenize-and-check, same as LIKE working without any index), just faster with one. */
    record Contains(String column, String word) implements WhereExpr {}

    /** column @> 'value' - checks whether an array column contains a given scalar element. A deliberately scoped-down version of real Postgres's @> (which compares two full arrays for set containment) - this compares an array column against a single element, the more common, simpler case. */
    record ArrayContains(String column, String literalElement) implements WhereExpr {}
    /** column @@ literal - column is a real tsvector column; literalElement is a real, raw tsquery string literal (e.g. "'quick & fox'"), parsed via TextSearch.toTsQuery at evaluation time (see ExecutorEngine's own evaluateWhereExpr and tryGinOrBitmapIndexScan). */
    record TsMatch(String column, String tsqueryLiteral) implements WhereExpr {}

    /** column ->> 'key' = 'value' - extracts a top-level JSON key as text and compares it for equality. Deliberately scoped to top-level keys and equality only - real Postgres's ->> also supports array-index extraction and #>>'{path,to,key}' for nested paths, real further work not attempted here. */
    record JsonExtractTextEquals(String column, String key, String value) implements WhereExpr {}

    /** (startColumn, endColumn) OVERLAPS (queryStart, queryEnd) - real interval/range overlap, GiST's own classic real-world use case. Two intervals [a,b] and [c,d] overlap iff a <= d AND c <= b - the standard interval overlap test, used both here (for the no-index fallback) and by GistIntervalIndex's own search/pruning. */
    record RangeOverlaps(String startColumn, String endColumn, String queryStartLiteral, String queryEndLiteral) implements WhereExpr {}

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
