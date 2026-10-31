package com.stratosdb.sql.execution;

import com.stratosdb.storage.page.Tuple;

import java.util.List;
import java.util.Map;

/**
 * Shared utility operations for streaming sort and aggregation operators,
 * including type-safe comparisons (numbers, strings, booleans, nulls-last),
 * HAVING clause predicate evaluation, and column alias resolution.
 */
public final class SortAndAggregateUtils {

    private SortAndAggregateUtils() {}

    /**
     * Compares two scalar objects for ordering.
     * Nulls are considered larger than non-nulls (standard SQL ASC NULLS LAST).
     */
    public static int compareValues(Object a, Object b, boolean ascending) {
        if (a == null && b == null) return 0;
        // In SQL ASC, NULLs sort last; in DESC, NULLs sort first
        if (a == null) return ascending ? 1 : -1;
        if (b == null) return ascending ? -1 : 1;

        int cmp = compareNonNull(a, b);
        return ascending ? cmp : -cmp;
    }

    /**
     * Compares two non-null objects for MIN/MAX aggregations.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static int compareForMinMax(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        return compareNonNull(a, b);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareNonNull(Object a, Object b) {
        if (a instanceof Number na && b instanceof Number nb) {
            if ((na instanceof Long || na instanceof Integer || na instanceof Short || na instanceof Byte)
                && (nb instanceof Long || nb instanceof Integer || nb instanceof Short || nb instanceof Byte)) {
                return Long.compare(na.longValue(), nb.longValue());
            }
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        if (a instanceof Boolean ba && b instanceof Boolean bb) {
            return Boolean.compare(ba, bb);
        }
        if (a instanceof Comparable ca && a.getClass().isAssignableFrom(b.getClass())) {
            return ca.compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }

    /**
     * Resolves a column value from a tuple, checking the column name,
     * qualified/unqualified variations, and any associated column aliases.
     */
    public static Object resolveColumnValue(Tuple tuple, String col, List<String> columns, List<String> aliases) {
        if (tuple == null || col == null) return null;
        Object val = JoinUtils.findColumnValue(tuple, col);
        if (val != null) return val;

        if (columns != null && aliases != null) {
            for (int i = 0; i < aliases.size() && i < columns.size(); i++) {
                String alias = aliases.get(i);
                String orig = columns.get(i);
                if (alias != null && alias.equalsIgnoreCase(col)) {
                    val = JoinUtils.findColumnValue(tuple, orig);
                    if (val != null) return val;
                }
                if (orig != null && orig.equalsIgnoreCase(col) && alias != null) {
                    val = JoinUtils.findColumnValue(tuple, alias);
                    if (val != null) return val;
                }
            }
        }
        return null;
    }

    /**
     * Evaluates a HAVING clause against computed aggregate values.
     * HAVING references the aggregate's canonical "FUNC(arg)" form, e.g. "COUNT(*) > 2".
     */
    public static boolean matchesHaving(String havingClause, Map<String, Object> aggregateValues) {
        if (havingClause == null || havingClause.isBlank() || aggregateValues == null) {
            return true;
        }

        String[] operatorsLongestFirst = {">=", "<=", "!=", "=", ">", "<"};
        String operator = null;
        for (String op : operatorsLongestFirst) {
            if (havingClause.contains(op)) {
                operator = op;
                break;
            }
        }
        if (operator == null) {
            return true;
        }

        String[] parts = havingClause.split(operator, 2);
        if (parts.length != 2) {
            return true;
        }

        String targetCol = parts[0].trim();
        String targetVal = parts[1].trim();
        if (targetVal.startsWith("'") && targetVal.endsWith("'") && targetVal.length() >= 2) {
            targetVal = targetVal.substring(1, targetVal.length() - 1);
        }

        Object actualObj = aggregateValues.get(targetCol);
        if (actualObj == null) {
            // Check without whitespace inside targetCol if any (e.g. "COUNT (*)" vs "COUNT(*)")
            for (Map.Entry<String, Object> entry : aggregateValues.entrySet()) {
                if (entry.getKey().replaceAll("\\s+", "").equalsIgnoreCase(targetCol.replaceAll("\\s+", ""))) {
                    actualObj = entry.getValue();
                    break;
                }
            }
        }
        if (actualObj == null) {
            return false;
        }

        String actualStr = actualObj.toString();
        try {
            double actualNum = Double.parseDouble(actualStr);
            double targetNum = Double.parseDouble(targetVal);
            return switch (operator) {
                case "=" -> actualNum == targetNum;
                case "!=" -> actualNum != targetNum;
                case ">" -> actualNum > targetNum;
                case ">=" -> actualNum >= targetNum;
                case "<" -> actualNum < targetNum;
                case "<=" -> actualNum <= targetNum;
                default -> false;
            };
        } catch (NumberFormatException ignored) {
            return switch (operator) {
                case "=" -> actualStr.equals(targetVal);
                case "!=" -> !actualStr.equals(targetVal);
                default -> false;
            };
        }
    }
}
