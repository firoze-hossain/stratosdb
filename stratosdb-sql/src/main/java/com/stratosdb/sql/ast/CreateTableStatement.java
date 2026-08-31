package com.stratosdb.sql.ast;

import java.util.List;

/**
 * primaryKeyColumns: the column(s) named by a real, standalone table-level
 * `PRIMARY KEY (col1, col2, ...)` constraint clause - see StratosSQL.g4's own
 * createTable rule. Empty (not null) when no such clause was present; a
 * column's own inline PRIMARY KEY (see ColumnDefinition's own field) is
 * tracked separately, not folded into this list, since ExecutorEngine
 * combines both real sources when actually recording a table's own primary
 * key (see executeCreateTable).
 */
public record CreateTableStatement(String tableName, List<ColumnDefinition> columns, List<String> primaryKeyColumns) implements Statement {}