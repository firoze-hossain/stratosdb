package com.stratosdb.sql.ast;

public record DropFunctionStatement(String name) implements Statement {}
