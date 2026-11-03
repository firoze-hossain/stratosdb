package com.stratosdb.sql.ast;

import java.util.List;

public record AlterTableAddConstraintStatement(
    String tableName,
    String constraintName,
    String constraintType,
    List<String> columns,
    String parentTable,
    List<String> parentColumns,
    String onDelete,
    String onUpdate,
    WhereExpr checkExpr
) implements Statement {
    public AlterTableAddConstraintStatement(String tableName, String constraintName, String constraintType, List<String> columns) {
        this(tableName, constraintName, constraintType, columns, null, List.of(), null, null, null);
    }
}
