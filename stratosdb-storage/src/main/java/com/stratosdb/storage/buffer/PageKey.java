package com.stratosdb.storage.buffer;

import java.util.Objects;

/**
 * Immutable identifier for a page in a specific relation.
 */
public record PageKey(String tableName, long pageId) {

    public PageKey {
        Objects.requireNonNull(tableName, "tableName cannot be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PageKey other)) return false;
        return pageId == other.pageId && tableName.equals(other.tableName);
    }

    @Override
    public int hashCode() {
        int h = tableName.hashCode() * 31 + (int)(pageId ^ (pageId >>> 32));
        h ^= (h >>> 16);
        h *= 0x85ebca6b;
        h ^= (h >>> 13);
        h *= 0xc2b2ae35;
        h ^= (h >>> 16);
        return h;
    }
}
