package com.stratosdb.sql.ast;

public record ExplainStatement(SelectStatement select) implements Statement {}
