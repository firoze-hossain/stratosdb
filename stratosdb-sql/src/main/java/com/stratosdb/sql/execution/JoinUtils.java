package com.stratosdb.sql.execution;

import com.stratosdb.storage.page.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared utility operations for streaming join operators (tuple merging,
 * key normalization, key comparison, column lookup, and outer-join null padding).
 */
public final class JoinUtils {

    private JoinUtils() {}

    public static Object findColumnValue(Tuple tuple, String columnName) {
        if (tuple == null || columnName == null) return null;
        List<String> columnNames = tuple.getColumnNames();

        // 1. Exact match (case-insensitive)
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(columnName)) {
                return tuple.getValue(i);
            }
        }

        // 2. Unqualified request (e.g. "amount" matching "orders.amount")
        if (!columnName.contains(".")) {
            String suffix = "." + columnName.toLowerCase();
            for (int i = 0; i < columnNames.size(); i++) {
                if (columnNames.get(i).toLowerCase().endsWith(suffix)) {
                    return tuple.getValue(i);
                }
            }
            return null;
        }

        // 3. Qualified request (e.g. "d.dept_id" or "orders.customer_id")
        int dotIdx = columnName.lastIndexOf('.');
        String prefix = columnName.substring(0, dotIdx).toLowerCase();
        String unqualified = columnName.substring(dotIdx + 1).toLowerCase();
        String suffix = "." + unqualified;

        // Pass 3a: Match table prefix/alias (e.g. "d" vs "departments")
        for (int i = 0; i < columnNames.size(); i++) {
            String c = columnNames.get(i).toLowerCase();
            if (c.endsWith(suffix) && c.contains(".")) {
                String cPrefix = c.substring(0, c.lastIndexOf('.'));
                if (cPrefix.equals(prefix) || cPrefix.startsWith(prefix) || prefix.startsWith(cPrefix)) {
                    return tuple.getValue(i);
                }
            }
        }

        // Pass 3b: Match plain/unqualified tuple column (e.g. "id" in a non-joined table)
        for (int i = 0; i < columnNames.size(); i++) {
            String c = columnNames.get(i);
            if (!c.contains(".") && c.equalsIgnoreCase(unqualified)) {
                return tuple.getValue(i);
            }
        }

        if (columnName.matches("-?\\d+(\\.\\d+)?")) {
            try {
                if (columnName.contains(".")) {
                    return Double.parseDouble(columnName);
                } else {
                    return Integer.parseInt(columnName);
                }
            } catch (NumberFormatException ignored) {}
        }
        if (columnName.startsWith("'") && columnName.endsWith("'") && columnName.length() >= 2) {
            return columnName.substring(1, columnName.length() - 1);
        }

        return null;
    }

    public static Object normalizeJoinKey(Object value) {
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        return value;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static int compareKeys(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        if (a instanceof Number na && b instanceof Number nb) {
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        return ((Comparable) a).compareTo(b);
    }

    public static Tuple qualify(Tuple tuple, String tableName) {
        if (tuple == null) return null;
        Tuple qualified = new Tuple();
        List<String> columnNames = tuple.getColumnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            String name = columnNames.get(i);
            String qualifiedName = name.startsWith(tableName + ".") ? name : (tableName + "." + name);
            qualified.addValue(qualifiedName, tuple.getValue(i));
        }
        return qualified;
    }

    public static List<String> qualifyColumns(List<String> columns, String tableName) {
        if (columns == null) return List.of();
        List<String> qualified = new ArrayList<>(columns.size());
        for (String col : columns) {
            qualified.add(col.startsWith(tableName + ".") ? col : (tableName + "." + col));
        }
        return qualified;
    }

    public static Tuple qualifyWithAlias(Tuple tuple, String alias) {
        if (tuple == null || alias == null) return tuple;
        Tuple qualified = new Tuple();
        List<String> columnNames = tuple.getColumnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            String name = columnNames.get(i);
            String bare = name.contains(".") ? name.substring(name.lastIndexOf('.') + 1) : name;
            qualified.addValue(alias + "." + bare, tuple.getValue(i));
        }
        return qualified;
    }

    public static List<String> qualifyColumnsWithAlias(List<String> columns, String alias) {
        if (columns == null || alias == null) return columns != null ? columns : List.of();
        List<String> qualified = new ArrayList<>(columns.size());
        for (String col : columns) {
            String bare = col.contains(".") ? col.substring(col.lastIndexOf('.') + 1) : col;
            qualified.add(alias + "." + bare);
        }
        return qualified;
    }

    public static Tuple unqualify(Tuple tuple) {
        if (tuple == null) return null;
        Tuple unqual = new Tuple();
        List<String> columnNames = tuple.getColumnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            String name = columnNames.get(i);
            String bare = name.contains(".") ? name.substring(name.lastIndexOf('.') + 1) : name;
            unqual.addValue(bare, tuple.getValue(i));
        }
        return unqual;
    }

    public static List<String> unqualifyColumns(List<String> columns) {
        if (columns == null) return List.of();
        List<String> unqual = new ArrayList<>(columns.size());
        for (String col : columns) {
            unqual.add(col.contains(".") ? col.substring(col.lastIndexOf('.') + 1) : col);
        }
        return unqual;
    }

    public static Tuple merge(Tuple left, Tuple right) {
        Tuple merged = new Tuple();
        if (left != null) {
            List<String> leftNames = left.getColumnNames();
            for (int i = 0; i < leftNames.size(); i++) {
                merged.addValue(leftNames.get(i), left.getValue(i));
            }
        }
        if (right != null) {
            List<String> rightNames = right.getColumnNames();
            for (int i = 0; i < rightNames.size(); i++) {
                merged.addValue(rightNames.get(i), right.getValue(i));
            }
        }
        return merged;
    }

    public static Tuple mergeWithNullRight(Tuple left, List<String> rightColumns) {
        Tuple merged = new Tuple();
        if (left != null) {
            List<String> leftNames = left.getColumnNames();
            for (int i = 0; i < leftNames.size(); i++) {
                merged.addValue(leftNames.get(i), left.getValue(i));
            }
        }
        if (rightColumns != null) {
            for (String col : rightColumns) {
                merged.addValue(col, null);
            }
        }
        return merged;
    }

    public static Tuple mergeWithNullLeft(List<String> leftColumns, Tuple right) {
        Tuple merged = new Tuple();
        if (leftColumns != null) {
            for (String col : leftColumns) {
                merged.addValue(col, null);
            }
        }
        if (right != null) {
            List<String> rightNames = right.getColumnNames();
            for (int i = 0; i < rightNames.size(); i++) {
                merged.addValue(rightNames.get(i), right.getValue(i));
            }
        }
        return merged;
    }

    public static List<String> combineColumns(List<String> left, List<String> right) {
        List<String> combined = new ArrayList<>();
        if (left != null) combined.addAll(left);
        if (right != null) combined.addAll(right);
        return combined;
    }
}
