package com.stratosdb.sql.execution;

import com.stratosdb.storage.page.Tuple;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Physical operator for Recursive CTEs ({@code WITH RECURSIVE}).
 *
 * Implements standard PostgreSQL working-table evaluation:
 * 1. Evaluates the non-recursive base term once to seed the accumulated output and worktable queue.
 * 2. Iteratively evaluates the recursive term using ONLY the current worktable rows.
 * 3. Supports {@code UNION} deduplication (automatic fixed-point termination on cyclic graphs)
 *    as well as {@code CYCLE col1, ... SET markCol USING pathCol} path-based cycle detection.
 * 4. Terminates cleanly at fixed point or throws if {@code maxIterations} is exceeded.
 */
public class RecursiveCteOperator implements PhysicalOperator {
    public static final int DEFAULT_MAX_ITERATIONS = 1000;

    private final String cteName;
    private final Supplier<List<Tuple>> baseEvaluator;
    private final Function<List<Tuple>, List<Tuple>> recursiveStepEvaluator;
    private final boolean unionAll;
    private final List<String> cycleColumns;
    private final String cycleMarkColumn;
    private final String cyclePathColumn;
    private final int maxIterations;

    private List<Tuple> accumulated;
    private List<String> outputColumns = List.of();
    private int cursor = 0;
    private final TupleSlot slot = new TupleSlot();
    private boolean open = false;

    public RecursiveCteOperator(String cteName,
                                Supplier<List<Tuple>> baseEvaluator,
                                Function<List<Tuple>, List<Tuple>> recursiveStepEvaluator,
                                boolean unionAll,
                                List<String> cycleColumns,
                                String cycleMarkColumn,
                                String cyclePathColumn,
                                int maxIterations) {
        this.cteName = cteName;
        this.baseEvaluator = baseEvaluator;
        this.recursiveStepEvaluator = recursiveStepEvaluator;
        this.unionAll = unionAll;
        this.cycleColumns = cycleColumns != null ? cycleColumns : List.of();
        this.cycleMarkColumn = cycleMarkColumn;
        this.cyclePathColumn = cyclePathColumn;
        this.maxIterations = maxIterations > 0 ? maxIterations : DEFAULT_MAX_ITERATIONS;
    }

    @Override
    public void open() {
        close();
        List<Tuple> baseRows = baseEvaluator.get();
        List<String> baseColumnNames = baseRows.isEmpty() ? List.of() : new ArrayList<>(baseRows.get(0).getColumnNames());

        boolean trackCycle = !cycleColumns.isEmpty() && cycleMarkColumn != null && cyclePathColumn != null;
        this.outputColumns = new ArrayList<>(baseColumnNames);
        if (trackCycle) {
            this.outputColumns.add(cycleMarkColumn);
            this.outputColumns.add(cyclePathColumn);
        }

        List<Tuple> alignedBase = alignColumnsToBaseSchema(baseRows, baseColumnNames);
        Set<List<Object>> seenRows = unionAll ? null : new HashSet<>();
        List<Tuple> allResults = new ArrayList<>();
        Deque<Tuple> worktableQueue = new ArrayDeque<>();

        for (Tuple row : alignedBase) {
            if (seenRows != null && !seenRows.add(buildTupleSignature(row))) {
                continue;
            }
            Tuple outRow = row;
            if (trackCycle) {
                List<Object> key = buildCycleKey(row);
                String pathStr = formatPath(List.of(key));
                outRow = cloneWithCycleCols(row, false, pathStr);
            }
            allResults.add(outRow);
            worktableQueue.addLast(outRow);
        }

        int iterations = 0;
        while (!worktableQueue.isEmpty()) {
            iterations++;
            if (iterations > maxIterations) {
                throw new IllegalStateException("Recursive CTE \"" + cteName + "\" exceeded "
                    + maxIterations + " iterations without reaching a fixpoint - "
                    + "likely a non-terminating recursion (does the recursive branch actually converge toward a base case, "
                    + "or could there be a cycle in the underlying data?)");
            }

            List<Tuple> currentWorktable = new ArrayList<>(worktableQueue.size());
            while (!worktableQueue.isEmpty()) {
                currentWorktable.add(worktableQueue.pollFirst());
            }

            if (trackCycle) {
                for (Tuple parentRow : currentWorktable) {
                    String parentPath = extractParentPath(parentRow, List.of(parentRow));
                    List<Tuple> stepRows = alignColumnsToBaseSchema(
                        recursiveStepEvaluator.apply(List.of(parentRow)), baseColumnNames);
                    for (Tuple stepRow : stepRows) {
                        if (seenRows != null && !seenRows.add(buildTupleSignature(stepRow))) {
                            continue;
                        }
                        List<Object> key = buildCycleKey(stepRow);
                        String keyToken = formatKeyToken(key);
                        boolean isCycle = parentPath != null && containsKeyToken(parentPath, keyToken);
                        String nextPath = (parentPath == null || parentPath.isEmpty())
                            ? keyToken
                            : parentPath + "->" + keyToken;
                        Tuple outRow = cloneWithCycleCols(stepRow, isCycle, nextPath);
                        allResults.add(outRow);
                        if (!isCycle) {
                            worktableQueue.addLast(outRow);
                        }
                    }
                }
            } else {
                List<Tuple> stepRows = alignColumnsToBaseSchema(
                    recursiveStepEvaluator.apply(currentWorktable), baseColumnNames);

                for (Tuple stepRow : stepRows) {
                    if (seenRows != null && !seenRows.add(buildTupleSignature(stepRow))) {
                        continue;
                    }
                    allResults.add(stepRow);
                    worktableQueue.addLast(stepRow);
                }
            }
        }

        this.accumulated = allResults;
        this.cursor = 0;
        this.open = true;
    }

    @Override
    public TupleSlot next() {
        if (!open || accumulated == null || cursor >= accumulated.size()) {
            slot.clear();
            return slot;
        }
        slot.set(accumulated.get(cursor++));
        return slot;
    }

    @Override
    public void close() {
        accumulated = null;
        cursor = 0;
        slot.clear();
        open = false;
    }

    @Override
    public List<String> getOutputColumns() {
        return outputColumns;
    }

    private static List<Tuple> alignColumnsToBaseSchema(List<Tuple> rows, List<String> baseColumnNames) {
        if (baseColumnNames.isEmpty()) {
            return rows;
        }
        List<Tuple> aligned = new ArrayList<>(rows.size());
        for (Tuple row : rows) {
            Tuple renamed = new Tuple();
            for (int i = 0; i < baseColumnNames.size() && i < row.getColumnNames().size(); i++) {
                renamed.addValue(baseColumnNames.get(i), row.getValue(i));
            }
            // Preserve any extra synthetic tracking columns (such as cyclePathColumn) if carried
            for (int i = baseColumnNames.size(); i < row.getColumnNames().size(); i++) {
                renamed.addValue(row.getColumnNames().get(i), row.getValue(i));
            }
            aligned.add(renamed);
        }
        return aligned;
    }

    private static List<Object> buildTupleSignature(Tuple row) {
        List<Object> sig = new ArrayList<>(row.getValues().size());
        for (Object v : row.getValues()) {
            sig.add(JoinUtils.normalizeJoinKey(v));
        }
        return sig;
    }

    private List<Object> buildCycleKey(Tuple row) {
        List<Object> key = new ArrayList<>(cycleColumns.size());
        for (String col : cycleColumns) {
            key.add(JoinUtils.findColumnValue(row, col));
        }
        return key;
    }

    private String extractParentPath(Tuple stepRow, List<Tuple> currentWorktable) {
        Object carried = JoinUtils.findColumnValue(stepRow, cyclePathColumn);
        if (carried != null) {
            return carried.toString();
        }
        if (currentWorktable.size() == 1) {
            Object p = JoinUtils.findColumnValue(currentWorktable.get(0), cyclePathColumn);
            return p != null ? p.toString() : null;
        }
        return null;
    }

    private static String formatPath(List<List<Object>> keys) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) sb.append("->");
            sb.append(formatKeyToken(keys.get(i)));
        }
        return sb.toString();
    }

    private static String formatKeyToken(List<Object> key) {
        if (key.size() == 1) {
            return String.valueOf(key.get(0));
        }
        return key.toString();
    }

    private static boolean containsKeyToken(String path, String token) {
        String[] parts = path.split("->");
        for (String part : parts) {
            if (part.equals(token)) {
                return true;
            }
        }
        return false;
    }

    private Tuple cloneWithCycleCols(Tuple row, boolean isCycle, String path) {
        Tuple copy = new Tuple();
        int limit = Math.min(row.getColumnNames().size(), row.getValues().size());
        for (int i = 0; i < limit; i++) {
            String colName = row.getColumnNames().get(i);
            if (!colName.equalsIgnoreCase(cycleMarkColumn) && !colName.equalsIgnoreCase(cyclePathColumn)) {
                copy.addValue(colName, row.getValue(i));
            }
        }
        copy.addValue(cycleMarkColumn, isCycle);
        copy.addValue(cyclePathColumn, path);
        return copy;
    }
}
