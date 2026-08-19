package com.stratosdb.sql.ast;

import java.util.List;

/** CALL procedureName(arg1, arg2, ...) - each arg is a literal's raw text (a bare column name has no row context to resolve against here, unlike a function call inside a SELECT list). */
public record CallStatement(String procedureName, List<String> args) implements Statement {}
