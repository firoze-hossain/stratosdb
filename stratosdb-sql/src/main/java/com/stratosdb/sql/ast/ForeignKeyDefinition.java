package com.stratosdb.sql.ast;

import java.util.List;

public record ForeignKeyDefinition(
    String name,
    String childTable,
    List<String> childColumns,
    String parentTable,
    List<String> parentColumns,
    ActionType onDelete,
    ActionType onUpdate
) {
    public enum ActionType {
        NO_ACTION,
        RESTRICT,
        CASCADE,
        SET_NULL
    }

    public ForeignKeyDefinition(String name, String childTable, List<String> childColumns, String parentTable, List<String> parentColumns) {
        this(name, childTable, childColumns, parentTable, parentColumns, ActionType.RESTRICT, ActionType.RESTRICT);
    }
}
