package com.stratosdb.sql.ast;

public record ColumnDefinition(String name, String type, boolean notNull, String defaultValue) {}