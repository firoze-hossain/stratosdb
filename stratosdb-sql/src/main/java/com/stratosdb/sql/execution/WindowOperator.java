package com.stratosdb.sql.execution;

import com.stratosdb.sql.ast.WindowFunctionCall;
import com.stratosdb.sql.ast.WindowOrderItem;
import com.stratosdb.storage.page.Tuple;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Physical operator for SQL Window Functions:
 * {@code OVER (PARTITION BY ... ORDER BY ...)} supporting
 * {@code ROW_NUMBER()}, {@code RANK()}, {@code DENSE_RANK()},
 * {@code LEAD()}, {@code LAG()}, and window aggregates
 * ({@code SUM}, {@code COUNT}, {@code AVG}, {@code MIN}, {@code MAX}).
 *
 * Follows PostgreSQL window frame semantics:
 * - Without ORDER BY: full partition frame.
 * - With ORDER BY: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (peer-inclusive).
 */
public class WindowOperator implements PhysicalOperator {
    private final PhysicalOperator child;
    private final List<String> requestedColumns;
    private final List<WindowFunctionCall> windowFunctions;
    private final List<String> outputColumns;

    private List<Tuple> outputRows;
    private int cursor = 0;
    private final TupleSlot slot = new TupleSlot();
    private boolean open = false;

    public WindowOperator(PhysicalOperator child,
                          List<String> requestedColumns,
                          List<WindowFunctionCall> windowFunctions) {
        this.child = child;
        this.requestedColumns = requestedColumns != null ? new ArrayList<>(requestedColumns) : List.of("*");
        this.windowFunctions = windowFunctions != null ? new ArrayList<>(windowFunctions) : List.of();
        this.outputColumns = computeOutputColumns();
    }

    private List<String> computeOutputColumns() {
        List<String> cols = new ArrayList<>();
        List<String> childCols = child != null ? child.getOutputColumns() : List.of();
        if (requestedColumns.size() == 1 && "*".equals(requestedColumns.get(0))) {
            cols.addAll(childCols);
        } else {
            for (String c : requestedColumns) {
                if ("*".equals(c)) {
                    cols.addAll(childCols);
                } else {
                    cols.add(c);
                }
            }
        }
        for (WindowFunctionCall wf : windowFunctions) {
            cols.add(wf.alias());
        }
        return cols;
    }

    @Override
    public void open() {
        close();
        List<Tuple> inputRows = new ArrayList<>();
        if (child != null) {
            child.open();
            TupleSlot childSlot;
            while ((childSlot = child.next()) != null && !childSlot.isEmpty()) {
                inputRows.add(childSlot.getTuple());
            }
        }

        int n = inputRows.size();
        Map<String, Object[]> windowValuesByAlias = new LinkedHashMap<>();
        for (WindowFunctionCall call : windowFunctions) {
            windowValuesByAlias.put(call.alias(), computeWindowValues(inputRows, call));
        }

        List<Tuple> projectedRows = new ArrayList<>(n);
        List<String> childCols = child != null ? child.getOutputColumns() : List.of();
        boolean selectStar = requestedColumns.size() == 1 && "*".equals(requestedColumns.get(0));

        for (int i = 0; i < n; i++) {
            Tuple src = inputRows.get(i);
            Tuple out = new Tuple();
            if (selectStar) {
                for (int c = 0; c < childCols.size(); c++) {
                    out.addValue(childCols.get(c), src.getValue(c));
                }
            } else {
                for (String col : requestedColumns) {
                    if ("*".equals(col)) {
                        for (int c = 0; c < childCols.size(); c++) {
                            out.addValue(childCols.get(c), src.getValue(c));
                        }
                    } else {
                        out.addValue(col, JoinUtils.findColumnValue(src, col));
                    }
                }
            }
            for (Map.Entry<String, Object[]> entry : windowValuesByAlias.entrySet()) {
                out.addValue(entry.getKey(), entry.getValue()[i]);
            }
            projectedRows.add(out);
        }

        this.outputRows = projectedRows;
        this.cursor = 0;
        this.open = true;
    }

    @Override
    public TupleSlot next() {
        if (!open || outputRows == null || cursor >= outputRows.size()) {
            slot.clear();
            return slot;
        }
        slot.set(outputRows.get(cursor++));
        return slot;
    }

    @Override
    public void close() {
        if (child != null) {
            try {
                child.close();
            } catch (Exception ignored) {
            }
        }
        outputRows = null;
        cursor = 0;
        slot.clear();
        open = false;
    }

    @Override
    public List<String> getOutputColumns() {
        return outputColumns;
    }

    @Override
    public PhysicalOperator getChild() {
        return child;
    }

    private Object[] computeWindowValues(List<Tuple> rows, WindowFunctionCall call) {
        int n = rows.size();
        Object[] result = new Object[n];
        if (n == 0) {
            return result;
        }

        Map<List<Object>, List<Integer>> partitions = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            partitions.computeIfAbsent(buildCompositeKey(rows.get(i), call.partitionBy()), k -> new ArrayList<>()).add(i);
        }

        String func = call.functionName().toUpperCase();
        for (List<Integer> partitionIndices : partitions.values()) {
            if (call.orderBy() != null && !call.orderBy().isEmpty()) {
                partitionIndices.sort((a, b) -> compareForWindowOrder(rows.get(a), rows.get(b), call.orderBy()));
            }

            switch (func) {
                case "ROW_NUMBER", "RANK", "DENSE_RANK" -> evaluateRanking(rows, partitionIndices, call, func, result);
                case "LEAD", "LAG" -> evaluateLeadLag(rows, partitionIndices, call, "LEAD".equals(func), result);
                case "SUM", "COUNT", "AVG", "MIN", "MAX" -> evaluateWindowAggregate(rows, partitionIndices, call, func, result);
                default -> throw new IllegalArgumentException("Unsupported window function: " + func);
            }
        }
        return result;
    }

    private void evaluateRanking(List<Tuple> rows, List<Integer> partitionIndices,
                                 WindowFunctionCall call, String func, Object[] result) {
        long rowNumber = 0;
        long rank = 0;
        long denseRank = 0;
        List<Object> previousOrderKey = null;

        for (int idx : partitionIndices) {
            rowNumber++;
            List<Object> currentOrderKey = buildOrderKey(rows.get(idx), call.orderBy());
            boolean tiedWithPrevious = previousOrderKey != null && previousOrderKey.equals(currentOrderKey);
            if (!tiedWithPrevious) {
                rank = rowNumber;
                denseRank++;
            }
            result[idx] = switch (func) {
                case "ROW_NUMBER" -> rowNumber;
                case "RANK" -> rank;
                default -> denseRank;
            };
            previousOrderKey = currentOrderKey;
        }
    }

    private void evaluateLeadLag(List<Tuple> rows, List<Integer> partitionIndices,
                                 WindowFunctionCall call, boolean isLead, Object[] result) {
        int size = partitionIndices.size();
        int offset = call.offset();
        String col = call.argument();
        Object defaultVal = call.defaultValue();

        for (int pos = 0; pos < size; pos++) {
            int rowIdx = partitionIndices.get(pos);
            int targetPos = isLead ? (pos + offset) : (pos - offset);
            if (targetPos < 0 || targetPos >= size) {
                result[rowIdx] = defaultVal;
            } else {
                int targetRowIdx = partitionIndices.get(targetPos);
                result[rowIdx] = JoinUtils.findColumnValue(rows.get(targetRowIdx), col);
            }
        }
    }

    private void evaluateWindowAggregate(List<Tuple> rows, List<Integer> partitionIndices,
                                         WindowFunctionCall call, String func, Object[] result) {
        int size = partitionIndices.size();
        boolean hasOrderBy = call.orderBy() != null && !call.orderBy().isEmpty();
        String arg = call.argument();

        if (!hasOrderBy) {
            // Unordered OVER (PARTITION BY ...): full partition frame
            Object aggVal = computeFrameAggregate(rows, partitionIndices, 0, size, func, arg);
            for (int pos = 0; pos < size; pos++) {
                result[partitionIndices.get(pos)] = aggVal;
            }
            return;
        }

        // Ordered OVER (PARTITION BY ... ORDER BY ...):
        // PostgreSQL default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (peer-inclusive).
        double runningSum = 0.0;
        long runningCount = 0;
        boolean hasNumeric = false;
        boolean allIntegral = true;
        Object runningMinMax = null;

        int pos = 0;
        while (pos < size) {
            List<Object> peerKey = buildOrderKey(rows.get(partitionIndices.get(pos)), call.orderBy());
            int peerEnd = pos + 1;
            while (peerEnd < size && peerKey.equals(buildOrderKey(rows.get(partitionIndices.get(peerEnd)), call.orderBy()))) {
                peerEnd++;
            }

            // Advance running state across all peers [pos, peerEnd)
            for (int p = pos; p < peerEnd; p++) {
                Tuple row = rows.get(partitionIndices.get(p));
                if ("COUNT".equals(func) && "*".equals(arg)) {
                    runningCount++;
                } else {
                    Object val = JoinUtils.findColumnValue(row, arg);
                    if (val != null) {
                        runningCount++;
                        if ("SUM".equals(func) || "AVG".equals(func)) {
                            if (val instanceof Integer i) {
                                runningSum += i;
                                hasNumeric = true;
                            } else if (val instanceof Long l) {
                                runningSum += l;
                                hasNumeric = true;
                            } else if (val instanceof Number num) {
                                runningSum += num.doubleValue();
                                hasNumeric = true;
                                allIntegral = false;
                            }
                        } else if ("MIN".equals(func)) {
                            if (runningMinMax == null || SortAndAggregateUtils.compareForMinMax(val, runningMinMax) < 0) {
                                runningMinMax = val;
                            }
                        } else if ("MAX".equals(func)) {
                            if (runningMinMax == null || SortAndAggregateUtils.compareForMinMax(val, runningMinMax) > 0) {
                                runningMinMax = val;
                            }
                        }
                    }
                }
            }

            Object peerVal = switch (func) {
                case "COUNT" -> runningCount;
                case "SUM" -> !hasNumeric ? null : (allIntegral ? (Object) (long) runningSum : (Object) runningSum);
                case "AVG" -> runningCount > 0 ? (Double) (runningSum / runningCount) : null;
                case "MIN", "MAX" -> runningMinMax;
                default -> null;
            };

            for (int p = pos; p < peerEnd; p++) {
                result[partitionIndices.get(p)] = peerVal;
            }
            pos = peerEnd;
        }
    }

    private Object computeFrameAggregate(List<Tuple> rows, List<Integer> partitionIndices,
                                         int start, int end, String func, String arg) {
        double sum = 0.0;
        long count = 0;
        boolean hasNumeric = false;
        boolean allIntegral = true;
        Object minMax = null;

        for (int p = start; p < end; p++) {
            Tuple row = rows.get(partitionIndices.get(p));
            if ("COUNT".equals(func) && "*".equals(arg)) {
                count++;
                continue;
            }
            Object val = JoinUtils.findColumnValue(row, arg);
            if (val == null) continue;
            count++;
            if ("SUM".equals(func) || "AVG".equals(func)) {
                if (val instanceof Integer i) {
                    sum += i;
                    hasNumeric = true;
                } else if (val instanceof Long l) {
                    sum += l;
                    hasNumeric = true;
                } else if (val instanceof Number num) {
                    sum += num.doubleValue();
                    hasNumeric = true;
                    allIntegral = false;
                }
            } else if ("MIN".equals(func)) {
                if (minMax == null || SortAndAggregateUtils.compareForMinMax(val, minMax) < 0) {
                    minMax = val;
                }
            } else if ("MAX".equals(func)) {
                if (minMax == null || SortAndAggregateUtils.compareForMinMax(val, minMax) > 0) {
                    minMax = val;
                }
            }
        }

        return switch (func) {
            case "COUNT" -> count;
            case "SUM" -> !hasNumeric ? null : (allIntegral ? (Object) (long) sum : (Object) sum);
            case "AVG" -> count > 0 ? (Double) (sum / count) : null;
            case "MIN", "MAX" -> minMax;
            default -> null;
        };
    }

    private static List<Object> buildCompositeKey(Tuple row, List<String> columns) {
        List<Object> key = new ArrayList<>(columns.size());
        for (String col : columns) {
            key.add(JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(row, col)));
        }
        return key;
    }

    private static List<Object> buildOrderKey(Tuple row, List<WindowOrderItem> orderBy) {
        List<Object> key = new ArrayList<>(orderBy.size());
        for (WindowOrderItem item : orderBy) {
            key.add(JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(row, item.column())));
        }
        return key;
    }

    private static int compareForWindowOrder(Tuple a, Tuple b, List<WindowOrderItem> orderBy) {
        for (WindowOrderItem item : orderBy) {
            Object valA = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(a, item.column()));
            Object valB = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(b, item.column()));
            int cmp = compareNullsLast(valA, valB);
            if (cmp != 0) {
                return item.descending() ? -cmp : cmp;
            }
        }
        return 0;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareNullsLast(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return 1;
        if (b == null) return -1;
        return ((Comparable) a).compareTo(b);
    }
}
