package com.stratosdb.sql.ast;

/** ALTER TABLE tableName DISABLE ROW LEVEL SECURITY. */
public record AlterTableDisableRlsStatement(String tableName) implements Statement {}
