package com.stratosdb.sql.ast;

/**
 * Join clause representation containing target table, join columns, join type (INNER, LEFT, RIGHT, FULL, CROSS, SEMI, ANTI),
 * optional derived table/subquery target, and optional table alias.
 * left/right are columnName text as captured by the grammar - may be qualified as "table.column".
 */
public record JoinClause(String tableName, String leftColumn, String rightColumn, JoinType joinType,
                         SelectStatement subquery, String tableAlias) {

    public JoinClause(String tableName, String leftColumn, String rightColumn, JoinType joinType) {
        this(tableName, leftColumn, rightColumn, joinType, null, null);
    }

    public JoinClause(String tableName, String leftColumn, String rightColumn) {
        this(tableName, leftColumn, rightColumn, JoinType.INNER, null, null);
    }

    public JoinClause(SelectStatement subquery, String tableAlias, String leftColumn, String rightColumn, JoinType joinType) {
        this(null, leftColumn, rightColumn, joinType, subquery, tableAlias);
    }

    public boolean isSubquery() {
        return subquery != null;
    }

    public String effectiveTargetName() {
        return tableAlias != null ? tableAlias : (tableName != null ? tableName : "subquery");
    }

    public enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        FULL,
        CROSS,
        SEMI,
        ANTI
    }
}
