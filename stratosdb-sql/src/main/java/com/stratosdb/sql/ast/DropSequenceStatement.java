package com.stratosdb.sql.ast;

public record DropSequenceStatement(String name) implements Statement {}
