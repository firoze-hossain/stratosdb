package com.stratosdb.sql.ast;

import java.util.List;

/**
 * AST representation of a SQL window function call:
 * {@code FUNC(arg [, offset [, defaultVal]]) OVER (PARTITION BY ... ORDER BY ...)}.
 *
 * Supports ranking ({@code ROW_NUMBER}, {@code RANK}, {@code DENSE_RANK}),
 * value ({@code LEAD}, {@code LAG}), and window aggregate ({@code SUM}, {@code COUNT},
 * {@code AVG}, {@code MIN}, {@code MAX}) functions.
 */
public record WindowFunctionCall(
    String functionName,
    String argument,
    int offset,
    Object defaultValue,
    List<String> partitionBy,
    List<WindowOrderItem> orderBy,
    String alias
) {
    public WindowFunctionCall(String functionName, List<String> partitionBy, List<WindowOrderItem> orderBy, String alias) {
        this(functionName, null, 1, null, partitionBy, orderBy, alias);
    }
}
