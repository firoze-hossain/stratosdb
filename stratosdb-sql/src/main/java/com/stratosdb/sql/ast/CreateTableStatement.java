package com.stratosdb.sql.ast;

import java.util.List;

public record CreateTableStatement(String tableName, List<ColumnDefinition> columns) implements Statement {}