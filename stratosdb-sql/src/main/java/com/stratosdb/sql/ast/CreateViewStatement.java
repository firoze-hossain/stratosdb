package com.stratosdb.sql.ast;

public record CreateViewStatement(String viewName, SelectStatement query) implements Statement {}
