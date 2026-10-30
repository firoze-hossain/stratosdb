package com.stratosdb.sql.ast;

/**
 * Join clause representation containing target table, join columns, and join type (INNER, LEFT, RIGHT, FULL).
 * left/right are columnName text as captured by the grammar - may be qualified as "table.column".
 */
public record JoinClause(String tableName, String leftColumn, String rightColumn, JoinType joinType) {

    public JoinClause(String tableName, String leftColumn, String rightColumn) {
        this(tableName, leftColumn, rightColumn, JoinType.INNER);
    }

    public enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        FULL,
        CROSS
    }
}
