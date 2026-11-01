package com.stratosdb.sql.ast;

public record ReindexStatement(TargetType targetType, String targetName) implements Statement {
    public enum TargetType {
        TABLE,
        INDEX
    }
}
