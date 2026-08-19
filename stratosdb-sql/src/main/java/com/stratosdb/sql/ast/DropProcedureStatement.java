package com.stratosdb.sql.ast;

public record DropProcedureStatement(String name) implements Statement {}
