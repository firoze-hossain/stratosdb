package com.stratosdb.sql.ast;

import java.util.List;

public record UpdateStatement(String tableName, List<Assignment> assignments, WhereExpr where) implements Statement {}