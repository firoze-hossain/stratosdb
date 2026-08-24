package com.stratosdb.sql.ast;

/** ALTER TABLE name ALTER [COLUMN] col_name SET DEFAULT value - metadata-only, applies to future inserts, does not touch any existing row (matching real Postgres's own behavior for this specific sub-command). */
public record AlterTableSetDefaultStatement(String tableName, String columnName, String defaultValue) implements Statement {}
