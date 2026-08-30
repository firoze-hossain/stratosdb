package com.stratosdb.sql.executor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The real pg_stat_user_tables equivalent - a shared, server-wide
 * registry mapping each table name to its own accumulated TableStats.
 * One registry per ExecutorEngine, matching QueryStatsRegistry's and
 * SessionActivityRegistry's own real scope.
 */
public class TableStatsRegistry {
    private final ConcurrentHashMap<String, TableStats> statsByTable = new ConcurrentHashMap<>();

    public void recordSelect(String tableName, long rowsReturned) {
        statsByTable.computeIfAbsent(tableName, k -> new TableStats()).recordSelect(rowsReturned);
    }

    public void recordInsert(String tableName, long count) {
        statsByTable.computeIfAbsent(tableName, k -> new TableStats()).recordInsert(count);
    }

    public void recordUpdate(String tableName, long count) {
        statsByTable.computeIfAbsent(tableName, k -> new TableStats()).recordUpdate(count);
    }

    public void recordDelete(String tableName, long count) {
        statsByTable.computeIfAbsent(tableName, k -> new TableStats()).recordDelete(count);
    }

    public Map<String, TableStats> getAll() {
        return statsByTable;
    }

    /** Called when a table is dropped - a real, deliberate cleanup so a dropped-then-recreated table of the same name starts fresh, not silently inheriting the old table's own history. */
    public void remove(String tableName) {
        statsByTable.remove(tableName);
    }

    /** Called when a table is renamed - carries the existing counters forward under the new name, matching real Postgres's own pg_stat_user_tables behavior across a rename. */
    public void rename(String oldName, String newName) {
        TableStats existing = statsByTable.remove(oldName);
        if (existing != null) {
            statsByTable.put(newName, existing);
        }
    }
}
