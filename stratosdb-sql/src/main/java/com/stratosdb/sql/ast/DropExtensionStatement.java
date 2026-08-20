package com.stratosdb.sql.ast;

public record DropExtensionStatement(String name) implements Statement {}
