package com.stratosdb.sql.ast;

import java.util.List;

/** columns is empty when no explicit column list was given (INSERT INTO t VALUES (...)), meaning values map positionally to the table's own column order - not empty when given, meaning each value maps to its NAMED column, in whatever order the statement specified. */
public record InsertStatement(String tableName, List<String> columns, List<String> values) implements Statement {}