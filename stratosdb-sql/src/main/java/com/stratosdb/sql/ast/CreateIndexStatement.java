package com.stratosdb.sql.ast;

/** columnName2 is null for every index type except GIST, which needs a (start, end) column pair to give an interval-overlap predicate any meaning. */
public record CreateIndexStatement(String indexName, String tableName, String columnName, String columnName2,
                                   IndexType indexType, boolean isUnique, boolean ifNotExists) implements Statement {
    public enum IndexType { BTREE, HASH, BRIN, GIN, BITMAP, GIST }

    public CreateIndexStatement(String indexName, String tableName, String columnName, String columnName2, IndexType indexType) {
        this(indexName, tableName, columnName, columnName2, indexType, false, false);
    }

    public CreateIndexStatement(String indexName, String tableName, String columnName, String columnName2, IndexType indexType, boolean isUnique) {
        this(indexName, tableName, columnName, columnName2, indexType, isUnique, false);
    }
}
