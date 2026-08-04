package com.stratosdb.sql.ast;

public record ReleaseSavepointStatement(String name) implements Statement {}
