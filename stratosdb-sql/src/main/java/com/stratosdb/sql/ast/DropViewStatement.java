package com.stratosdb.sql.ast;

public record DropViewStatement(String viewName) implements Statement {}
