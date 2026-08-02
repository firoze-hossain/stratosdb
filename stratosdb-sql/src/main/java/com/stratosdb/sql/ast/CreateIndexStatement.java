package com.stratosdb.sql.ast;

public record CreateIndexStatement(String indexName, String tableName, String columnName, IndexType indexType) implements Statement {
    public enum IndexType { BTREE, HASH }
}
