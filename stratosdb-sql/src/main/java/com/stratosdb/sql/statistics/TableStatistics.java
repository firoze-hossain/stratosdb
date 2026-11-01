package com.stratosdb.sql.statistics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Accumulated table statistics: total row count, page count, and per-column statistics.
 */
public record TableStatistics(
    String tableName,
    long rowCount,
    long pageCount,
    Map<String, ColumnStatistics> columnStats
) {
    public TableStatistics {
        columnStats = columnStats != null ? Collections.unmodifiableMap(new HashMap<>(columnStats)) : Map.of();
    }

    public TableStatistics(long rowCount, Map<String, ColumnStatistics> columnStats) {
        this("", rowCount, 1L, columnStats);
    }
}
