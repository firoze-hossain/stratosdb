package com.stratosdb.sql.optimizer;

import com.stratosdb.sql.ast.JoinClause;

import java.util.Objects;

/**
 * Representation of an equi-join condition between two relations:
 * leftTable.leftColumn = rightTable.rightColumn
 */
public class JoinPredicate {
    private final String leftTable;
    private final String leftColumn;
    private final String rightTable;
    private final String rightColumn;
    private final JoinClause.JoinType joinType;

    public JoinPredicate(String leftTable, String leftColumn, String rightTable, String rightColumn, JoinClause.JoinType joinType) {
        this.leftTable = leftTable;
        this.leftColumn = leftColumn;
        this.rightTable = rightTable;
        this.rightColumn = rightColumn;
        this.joinType = joinType != null ? joinType : JoinClause.JoinType.INNER;
    }

    public String getLeftTable() { return leftTable; }
    public String getLeftColumn() { return leftColumn; }
    public String getRightTable() { return rightTable; }
    public String getRightColumn() { return rightColumn; }
    public JoinClause.JoinType getJoinType() { return joinType; }

    public boolean connects(String table1, String table2) {
        if (table1 == null || table2 == null) return false;
        boolean direct = table1.equalsIgnoreCase(leftTable) && table2.equalsIgnoreCase(rightTable);
        boolean reverse = table1.equalsIgnoreCase(rightTable) && table2.equalsIgnoreCase(leftTable);
        return direct || reverse;
    }

    public String getColumnForTable(String table) {
        if (table == null) return null;
        if (table.equalsIgnoreCase(leftTable)) return leftColumn;
        if (table.equalsIgnoreCase(rightTable)) return rightColumn;
        return null;
    }

    public String getQualifiedColumnForTable(String table) {
        String col = getColumnForTable(table);
        if (col == null) return null;
        return col.contains(".") ? col : table + "." + col;
    }

    @Override
    public String toString() {
        return leftTable + "." + leftColumn + " = " + rightTable + "." + rightColumn + " (" + joinType + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinPredicate that = (JoinPredicate) o;
        return connects(that.leftTable, that.rightTable) &&
            Objects.equals(leftColumn, that.leftColumn) &&
            Objects.equals(rightColumn, that.rightColumn) &&
            joinType == that.joinType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftTable, leftColumn, rightTable, rightColumn, joinType);
    }
}
