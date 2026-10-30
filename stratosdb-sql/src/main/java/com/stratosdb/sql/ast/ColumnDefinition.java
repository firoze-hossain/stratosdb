package com.stratosdb.sql.ast;

public record ColumnDefinition(String name, String type, boolean notNull, String defaultValue, boolean primaryKey, boolean isUnique) {
    public ColumnDefinition(String name, String type, boolean notNull, String defaultValue, boolean primaryKey) {
        this(name, type, notNull, defaultValue, primaryKey, false);
    }
}
