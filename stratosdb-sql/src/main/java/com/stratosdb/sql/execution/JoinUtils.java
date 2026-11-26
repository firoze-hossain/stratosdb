package com.stratosdb.sql.execution;

import com.stratosdb.sql.ast.WhereExpr;
import com.stratosdb.storage.page.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Shared utility operations for streaming join operators (tuple merging,
 * key normalization, key comparison, column lookup, outer-join null padding,
 * composite key handling, and compound/non-equi join condition evaluation).
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

    /**
     * Composite join key holding normalized values for multi-column join predicates.
     */
    public static final class CompositeKey {
        private final Object[] values;
        private final int hashCode;
        private final boolean hasNull;

        public CompositeKey(Object[] values) {
            this.values = values != null ? values : new Object[0];
            int h = 1;
            boolean nullFound = false;
            for (Object v : this.values) {
                if (v == null) {
                    nullFound = true;
                    h = 31 * h;
                } else {
                    h = 31 * h + v.hashCode();
                }
            }
            this.hashCode = h;
            this.hasNull = nullFound;
        }

        public Object[] getValues() {
            return values;
        }

        public boolean hasNull() {
            return hasNull;
        }

        public int size() {
            return values.length;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CompositeKey other)) return false;
            if (values.length != other.values.length) return false;
            for (int i = 0; i < values.length; i++) {
                Object a = values[i];
                Object b = other.values[i];
                if (a == null || b == null) {
                    if (a != b) return false;
                } else if (!Objects.equals(a, b)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public String toString() {
            return Arrays.toString(values);
        }
    }

    public static CompositeKey extractCompositeKey(Tuple tuple, List<String> columns) {
        if (tuple == null || columns == null || columns.isEmpty()) {
            return new CompositeKey(new Object[0]);
        }
        Object[] vals = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Object raw = findColumnValue(tuple, columns.get(i));
            vals[i] = normalizeJoinKey(raw);
        }
        return new CompositeKey(vals);
    }

    public static int compareCompositeKeys(Tuple leftTuple, List<String> leftColumns,
                                           Tuple rightTuple, List<String> rightColumns) {
        if (leftColumns == null || rightColumns == null) return 0;
        int count = Math.min(leftColumns.size(), rightColumns.size());
        for (int i = 0; i < count; i++) {
            Object lVal = normalizeJoinKey(findColumnValue(leftTuple, leftColumns.get(i)));
            Object rVal = normalizeJoinKey(findColumnValue(rightTuple, rightColumns.get(i)));
            if (lVal == null && rVal == null) continue;
            if (lVal == null) return -1;
            if (rVal == null) return 1;
            int cmp = compareKeys(lVal, rVal);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(leftColumns.size(), rightColumns.size());
    }

    public static Object resolveTupleValue(Tuple left, Tuple right, String colName) {
        if (colName == null) return null;
        if (colName.contains(".")) {
            int dot = colName.lastIndexOf('.');
            String prefix = colName.substring(0, dot).toLowerCase();
            if (left != null && tupleMatchesPrefix(left, prefix)) {
                Object val = findColumnValue(left, colName);
                if (val != null) return val;
            }
            if (right != null && tupleMatchesPrefix(right, prefix)) {
                Object val = findColumnValue(right, colName);
                if (val != null) return val;
            }
            if (left != null) {
                Object val = findColumnValue(left, colName);
                if (val != null) return val;
            }
            if (right != null) {
                return findColumnValue(right, colName);
            }
            return null;
        }
        if (left != null) {
            Object val = findColumnValue(left, colName);
            if (val != null) return val;
        }
        if (right != null) {
            return findColumnValue(right, colName);
        }
        return null;
    }

    private static boolean tupleMatchesPrefix(Tuple tuple, String prefix) {
        if (tuple == null) return false;
        if (tuple.getTableName() != null && tuple.getTableName().equalsIgnoreCase(prefix)) {
            return true;
        }
        List<String> names = tuple.getColumnNames();
        if (names != null) {
            for (String n : names) {
                if (n.toLowerCase().startsWith(prefix + ".")) {
                    return true;
                }
            }
        }
        return false;
    }

    public static Object parseLiteralValue(String literalText) {
        if (literalText == null) return null;
        String s = literalText.trim();
        if (s.startsWith("'") && s.endsWith("'") && s.length() >= 2) {
            return s.substring(1, s.length() - 1).replace("''", "'");
        }
        if ("true".equalsIgnoreCase(s)) return Boolean.TRUE;
        if ("false".equalsIgnoreCase(s)) return Boolean.FALSE;
        if ("null".equalsIgnoreCase(s)) return null;
        try {
            if (s.contains(".")) {
                return Double.parseDouble(s);
            } else {
                return Long.parseLong(s);
            }
        } catch (NumberFormatException ignored) {}
        return s;
    }

    public static boolean compareValues(Object a, String operator, Object b) {
        if (a == null || b == null) return false;
        int cmp = compareKeys(normalizeJoinKey(a), normalizeJoinKey(b));
        return switch (operator) {
            case "=", "==" -> cmp == 0;
            case "!=", "<>" -> cmp != 0;
            case ">" -> cmp > 0;
            case ">=" -> cmp >= 0;
            case "<" -> cmp < 0;
            case "<=" -> cmp <= 0;
            default -> false;
        };
    }

    public static boolean evaluateJoinCondition(Tuple left, Tuple right, WhereExpr expr) {
        if (expr == null) return true;
        if (expr instanceof WhereExpr.BooleanLiteral bool) {
            return bool.value();
        }
        if (expr instanceof WhereExpr.And and) {
            return evaluateJoinCondition(left, right, and.left()) && evaluateJoinCondition(left, right, and.right());
        }
        if (expr instanceof WhereExpr.Or or) {
            return evaluateJoinCondition(left, right, or.left()) || evaluateJoinCondition(left, right, or.right());
        }
        if (expr instanceof WhereExpr.Not not) {
            return !evaluateJoinCondition(left, right, not.inner());
        }
        if (expr instanceof WhereExpr.ColumnComparison colCmp) {
            Object lVal = resolveTupleValue(left, right, colCmp.leftColumn());
            Object rVal = resolveTupleValue(left, right, colCmp.rightColumn());
            return lVal != null && rVal != null && compareValues(lVal, colCmp.operator(), rVal);
        }
        if (expr instanceof WhereExpr.Comparison cmp) {
            Object val = resolveTupleValue(left, right, cmp.column());
            Object lit = parseLiteralValue(cmp.literal());
            return val != null && lit != null && compareValues(val, cmp.operator(), lit);
        }
        return true;
    }

    public static Tuple qualify(Tuple tuple, String tableName) {
        if (tuple == null) return null;
        Tuple qualified = new Tuple();
        qualified.setPageId(tuple.getPageId());
        qualified.setSlot(tuple.getSlot());
        qualified.setTableName(tuple.getTableName() != null ? tuple.getTableName() : tableName);
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
        qualified.setPageId(tuple.getPageId());
        qualified.setSlot(tuple.getSlot());
        qualified.setTableName(tuple.getTableName());
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
        unqual.setPageId(tuple.getPageId());
        unqual.setSlot(tuple.getSlot());
        unqual.setTableName(tuple.getTableName());
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
        if (left != null && left.getPageId() >= 0) {
            merged.setPageId(left.getPageId());
            merged.setSlot(left.getSlot());
            merged.setTableName(left.getTableName());
        } else if (right != null && right.getPageId() >= 0) {
            merged.setPageId(right.getPageId());
            merged.setSlot(right.getSlot());
            merged.setTableName(right.getTableName());
        }
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
