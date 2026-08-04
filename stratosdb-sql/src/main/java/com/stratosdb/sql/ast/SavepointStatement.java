package com.stratosdb.sql.ast;

public record SavepointStatement(String name) implements Statement {}
