package com.stratosdb.sql.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Join clause representation containing target table, join columns, join type (INNER, LEFT, RIGHT, FULL, CROSS, SEMI, ANTI),
 * optional derived table/subquery target, optional table alias, full ON expression, and compound/non-equi predicates.
 */
public record JoinClause(
    String tableName,
    List<String> leftColumns,
    List<String> rightColumns,
    JoinType joinType,
    SelectStatement subquery,
    String tableAlias,
    WhereExpr onExpression,
    List<String> operators,
    WhereExpr residualFilter
) {

    public JoinClause(String tableName, String leftColumn, String rightColumn, JoinType joinType,
                      SelectStatement subquery, String tableAlias) {
        this(tableName,
             leftColumn != null ? List.of(leftColumn) : List.of(),
             rightColumn != null ? List.of(rightColumn) : List.of(),
             joinType != null ? joinType : JoinType.INNER,
             subquery,
             tableAlias,
             (leftColumn != null && rightColumn != null) ? new WhereExpr.ColumnComparison(leftColumn, "=", rightColumn) : null,
             (leftColumn != null && rightColumn != null) ? List.of("=") : List.of(),
             null);
    }

    public JoinClause(String tableName, String leftColumn, String rightColumn, JoinType joinType) {
        this(tableName, leftColumn, rightColumn, joinType, null, null);
    }

    public JoinClause(String tableName, String leftColumn, String rightColumn) {
        this(tableName, leftColumn, rightColumn, JoinType.INNER, null, null);
    }

    public JoinClause(SelectStatement subquery, String tableAlias, String leftColumn, String rightColumn, JoinType joinType) {
        this(null, leftColumn, rightColumn, joinType, subquery, tableAlias);
    }

    public JoinClause(String tableName,
                      List<String> leftColumns,
                      List<String> rightColumns,
                      JoinType joinType,
                      SelectStatement subquery,
                      String tableAlias,
                      WhereExpr onExpression,
                      List<String> operators) {
        this(tableName, leftColumns, rightColumns, joinType, subquery, tableAlias, onExpression, operators, null);
    }

    public JoinClause(String tableName,
                      List<String> leftColumns,
                      List<String> rightColumns,
                      WhereExpr onExpression,
                      JoinType joinType) {
        this(tableName, leftColumns, rightColumns, joinType, null, null, onExpression,
             (leftColumns != null ? leftColumns.stream().map(c -> "=").toList() : List.of()), null);
    }

    public String leftColumn() {
        return (leftColumns != null && !leftColumns.isEmpty()) ? leftColumns.get(0) : null;
    }

    public String rightColumn() {
        return (rightColumns != null && !rightColumns.isEmpty()) ? rightColumns.get(0) : null;
    }

    public boolean isSubquery() {
        return subquery != null;
    }

    public String effectiveTargetName() {
        return tableAlias != null ? tableAlias : (tableName != null ? tableName : "subquery");
    }

    public boolean hasCompoundPredicates() {
        return (leftColumns != null && leftColumns.size() > 1) || (residualFilter != null);
    }

    public boolean hasNonEquiPredicates() {
        if (residualFilter != null) return true;
        if (operators != null) {
            for (String op : operators) {
                if (!"=".equals(op) && !"==".equals(op)) return true;
            }
        }
        return false;
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
