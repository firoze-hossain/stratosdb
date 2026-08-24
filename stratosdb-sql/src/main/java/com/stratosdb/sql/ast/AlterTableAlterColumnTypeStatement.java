package com.stratosdb.sql.ast;

/**
 * ALTER TABLE name ALTER [COLUMN] col_name TYPE new_type.
 *
 * Real, honestly-stated scope: every existing value in the column is
 * converted via the same best-effort coerceForColumnType logic INSERT
 * already uses, not a full USING-expression conversion (real Postgres's
 * own ALTER COLUMN ... TYPE ... USING lets an arbitrary expression drive
 * the conversion) - a value that can't convert to the new type fails the
 * whole statement cleanly, with nothing on disk changed, rather than
 * silently corrupting or dropping data.
 */
public record AlterTableAlterColumnTypeStatement(String tableName, String columnName, String newDataType) implements Statement {}
