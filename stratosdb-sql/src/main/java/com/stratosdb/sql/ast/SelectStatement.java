package com.stratosdb.sql.ast;

import java.util.List;

public record SelectStatement(String tableName, List<String> columns, String whereClause,
                              String orderBy, String limit) implements Statement {}