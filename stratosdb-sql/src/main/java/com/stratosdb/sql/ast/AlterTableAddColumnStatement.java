package com.stratosdb.sql.ast;

/**
 * ALTER TABLE name ADD [COLUMN] col_name data_type [DEFAULT value].
 *
 * defaultValue holds the raw, still-unparsed SQL text (or null), matching
 * this project's own established convention (e.g. ColumnDefinition's own
 * defaultValue field) - resolved to a real value by the executor via its
 * existing resolveValue/coerceForColumnType helpers, the exact same ones
 * CREATE TABLE and INSERT already use, not a separate, second
 * implementation of that logic.
 */
public record AlterTableAddColumnStatement(String tableName, String columnName, String dataType, String defaultValue) implements Statement {}
