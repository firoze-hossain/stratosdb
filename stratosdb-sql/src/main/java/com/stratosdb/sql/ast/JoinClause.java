package com.stratosdb.sql.ast;

/** left/right are columnName text as captured by the grammar - may be qualified as "table.column". */
public record JoinClause(String tableName, String leftColumn, String rightColumn) {}
