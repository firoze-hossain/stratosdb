package com.stratosdb.sql.ast;

/** DROP TYPE typeName - removes a previously-created enum type (see CreateTypeStatement). */
public record DropTypeStatement(String typeName) implements Statement {}
