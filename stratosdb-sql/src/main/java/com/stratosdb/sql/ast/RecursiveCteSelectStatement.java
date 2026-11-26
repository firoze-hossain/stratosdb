package com.stratosdb.sql.ast;

import java.util.List;

/**
 * WITH RECURSIVE cteName AS (baseQuery UNION [ALL] recursiveQuery) [CYCLE ...] outerQuery.
 *
 * Evaluated by fixpoint iteration using a working-table queue:
 * 1. Evaluate baseQuery once to seed the accumulated result and worktable.
 * 2. Repeatedly evaluate recursiveQuery with cteName bound to the current worktable.
 * 3. For UNION (without ALL), deduplicate against previously seen rows so cyclic graphs
 *    reach a fixed point automatically.
 * 4. For CYCLE col1, ... SET cycleMarkCol USING pathCol, track ancestor keys per path,
 *    flag cycles, and prune cyclic branches from the worktable.
 */
public record RecursiveCteSelectStatement(
    String cteName,
    SelectStatement baseQuery,
    SelectStatement recursiveQuery,
    SelectStatement outerQuery,
    boolean unionAll,
    List<String> cycleColumns,
    String cycleMarkColumn,
    String cyclePathColumn
) implements Statement {
    public RecursiveCteSelectStatement(String cteName, SelectStatement baseQuery,
                                       SelectStatement recursiveQuery, SelectStatement outerQuery) {
        this(cteName, baseQuery, recursiveQuery, outerQuery, true, List.of(), null, null);
    }
}
