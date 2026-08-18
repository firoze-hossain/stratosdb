package com.stratosdb.sql.ast;

/** One parameter in a CREATE FUNCTION's parameter list: a name and its declared SQL type. */
public record FunctionParam(String name, String type) {}
