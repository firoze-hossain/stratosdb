package com.stratosdb.sql.ast;

public record AlterTableDropConstraintStatement(String tableName, String constraintName, boolean ifExists) implements Statement {
    public AlterTableDropConstraintStatement(String tableName, String constraintName) {
        this(tableName, constraintName, false);
    }
}
