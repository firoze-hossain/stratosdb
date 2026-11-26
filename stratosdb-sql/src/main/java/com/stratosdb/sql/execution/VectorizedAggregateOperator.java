package com.stratosdb.sql.execution;

import com.stratosdb.sql.ast.AggregateCall;
import com.stratosdb.sql.ast.WhereExpr;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.storage.vector.ByteVector;
import com.stratosdb.storage.vector.ColumnVector;
import com.stratosdb.storage.vector.DoubleVector;
import com.stratosdb.storage.vector.IntVector;
import com.stratosdb.storage.vector.LongVector;
import com.stratosdb.storage.vector.VectorBatch;
import com.stratosdb.storage.vector.VectorType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Vectorized aggregation operator executing COUNT, SUM, AVG, MIN, and MAX
 * over 1024-row {@link VectorBatch}es using Java 25 SIMD intrinsics ({@link SimdVectorKernels})
 * and a zero-boxing primitive hash table for GROUP BY queries.
 */
public final class VectorizedAggregateOperator implements PhysicalOperator {
    private static final long NULL_KEY_SENTINEL = Long.MIN_VALUE + 7L;

    private final VectorizedOperator child;
    private final List<String> groupByColumns;
    private final List<AggregateCall> aggregates;
    private final WhereExpr havingExpr;
    private final List<String> outputColumns;

    private final int[] aggColIndices;
    private final int[] groupColIndices;

    private List<Tuple> results;
    private int resultCursor;
    private final TupleSlot slot = new TupleSlot();
    private boolean open;

    public VectorizedAggregateOperator(VectorizedOperator child,
                                       List<String> groupByColumns,
                                       List<AggregateCall> aggregates,
                                       List<String> outputColumns) {
        this(child, groupByColumns, aggregates, null, outputColumns);
    }

    public VectorizedAggregateOperator(VectorizedOperator child,
                                       List<String> groupByColumns,
                                       List<AggregateCall> aggregates,
                                       WhereExpr havingExpr,
                                       List<String> outputColumns) {
        this.child = child;
        this.groupByColumns = groupByColumns != null ? new ArrayList<>(groupByColumns) : List.of();
        this.aggregates = aggregates != null ? new ArrayList<>(aggregates) : List.of();
        this.havingExpr = havingExpr;
        this.outputColumns = outputColumns != null ? new ArrayList<>(outputColumns) : buildDefaultOutputColumns();

        List<String> childCols = child.getOutputColumns();
        this.aggColIndices = new int[this.aggregates.size()];
        for (int i = 0; i < this.aggregates.size(); i++) {
            String arg = this.aggregates.get(i).argument();
            this.aggColIndices[i] = "*".equals(arg) ? -1 : resolveColumnIndex(childCols, arg);
        }
        this.groupColIndices = new int[this.groupByColumns.size()];
        for (int i = 0; i < this.groupByColumns.size(); i++) {
            this.groupColIndices[i] = resolveColumnIndex(childCols, this.groupByColumns.get(i));
        }
    }

    private List<String> buildDefaultOutputColumns() {
        List<String> cols = new ArrayList<>(groupByColumns.size() + aggregates.size());
        cols.addAll(groupByColumns);
        for (AggregateCall agg : aggregates) {
            cols.add(agg.displayName());
        }
        return cols;
    }

    private static int resolveColumnIndex(List<String> cols, String name) {
        if (name == null) return -1;
        String target = name.contains(".") ? name.substring(name.lastIndexOf('.') + 1) : name;
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).equalsIgnoreCase(name) || cols.get(i).equalsIgnoreCase(target)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void open() {
        close();
        child.open();
        this.results = executeAll();
        this.resultCursor = 0;
        this.open = true;
    }

    @Override
    public TupleSlot next() {
        if (!open || results == null || resultCursor >= results.size()) {
            slot.clear();
            return slot;
        }
        slot.set(results.get(resultCursor++));
        return slot;
    }

    private List<Tuple> executeAll() {
        List<VectorType> childTypes = child.getOutputTypes();
        try (VectorBatch batch = new VectorBatch(child.getOutputColumns(), childTypes)) {
            if (groupByColumns.isEmpty()) {
                return executeGlobalAggregate(batch);
            } else if (groupByColumns.size() == 1) {
                return executeSingleGroupAggregate(batch);
            } else {
                throw new UnsupportedOperationException("Vectorized multi-column GROUP BY not yet enabled");
            }
        }
    }

    private List<Tuple> executeGlobalAggregate(VectorBatch batch) {
        int aggCount = aggregates.size();
        long[] counts = new long[aggCount];
        long[] intSums = new long[aggCount];
        double[] dblSums = new double[aggCount];
        double[] mins = new double[aggCount];
        double[] maxs = new double[aggCount];
        boolean[] hasVal = new boolean[aggCount];
        Arrays.fill(mins, Double.POSITIVE_INFINITY);
        Arrays.fill(maxs, Double.NEGATIVE_INFINITY);

        while (child.nextBatch(batch)) {
            int n = batch.getNumRows();
            boolean selectedInUse = batch.isSelectedInUse();
            int selectedCount = batch.getSelectedCount();
            int[] sel = batch.getSelectionVector();

            for (int a = 0; a < aggCount; a++) {
                AggregateCall agg = aggregates.get(a);
                String func = agg.function().toUpperCase();
                int colIdx = aggColIndices[a];

                if ("COUNT".equals(func) && colIdx < 0) {
                    counts[a] += selectedCount;
                    hasVal[a] = true;
                    continue;
                }

                ColumnVector col = batch.getColumn(colIdx);
                boolean hasNulls = col.hasNulls();

                if (!selectedInUse && !hasNulls) {
                    switch (func) {
                        case "COUNT" -> {
                            counts[a] += n;
                            hasVal[a] = true;
                        }
                        case "SUM" -> {
                            switch (col.getType()) {
                                case INT -> intSums[a] += SimdVectorKernels.sumInt(col.getDataSegment(), n);
                                case LONG -> intSums[a] += SimdVectorKernels.sumLong(col.getDataSegment(), n);
                                case DOUBLE -> dblSums[a] += SimdVectorKernels.sumDouble(col.getDataSegment(), n);
                                case BYTE -> {
                                    ByteVector bv = (ByteVector) col;
                                    for (int i = 0; i < n; i++) intSums[a] += bv.get(i);
                                }
                            }
                            if (n > 0) hasVal[a] = true;
                        }
                        case "AVG" -> {
                            switch (col.getType()) {
                                case INT -> dblSums[a] += SimdVectorKernels.sumInt(col.getDataSegment(), n);
                                case LONG -> dblSums[a] += SimdVectorKernels.sumLong(col.getDataSegment(), n);
                                case DOUBLE -> dblSums[a] += SimdVectorKernels.sumDouble(col.getDataSegment(), n);
                                case BYTE -> {
                                    ByteVector bv = (ByteVector) col;
                                    for (int i = 0; i < n; i++) dblSums[a] += bv.get(i);
                                }
                            }
                            counts[a] += n;
                            if (n > 0) hasVal[a] = true;
                        }
                        case "MIN" -> {
                            if (n > 0) {
                                double batchMin = switch (col.getType()) {
                                    case INT -> SimdVectorKernels.minInt(col.getDataSegment(), n);
                                    case DOUBLE -> SimdVectorKernels.minDouble(col.getDataSegment(), n);
                                    case LONG -> {
                                        LongVector lv = (LongVector) col;
                                        long m = Long.MAX_VALUE;
                                        for (int i = 0; i < n; i++) if (lv.get(i) < m) m = lv.get(i);
                                        yield m;
                                    }
                                    case BYTE -> {
                                        ByteVector bv = (ByteVector) col;
                                        byte m = Byte.MAX_VALUE;
                                        for (int i = 0; i < n; i++) if (bv.get(i) < m) m = bv.get(i);
                                        yield m;
                                    }
                                };
                                if (batchMin < mins[a]) mins[a] = batchMin;
                                hasVal[a] = true;
                            }
                        }
                        case "MAX" -> {
                            if (n > 0) {
                                double batchMax = switch (col.getType()) {
                                    case INT -> SimdVectorKernels.maxInt(col.getDataSegment(), n);
                                    case DOUBLE -> SimdVectorKernels.maxDouble(col.getDataSegment(), n);
                                    case LONG -> {
                                        LongVector lv = (LongVector) col;
                                        long m = Long.MIN_VALUE;
                                        for (int i = 0; i < n; i++) if (lv.get(i) > m) m = lv.get(i);
                                        yield m;
                                    }
                                    case BYTE -> {
                                        ByteVector bv = (ByteVector) col;
                                        byte m = Byte.MIN_VALUE;
                                        for (int i = 0; i < n; i++) if (bv.get(i) > m) m = bv.get(i);
                                        yield m;
                                    }
                                };
                                if (batchMax > maxs[a]) maxs[a] = batchMax;
                                hasVal[a] = true;
                            }
                        }
                        default -> throw new IllegalArgumentException("Unsupported aggregate: " + func);
                    }
                } else {
                    // Selection vector or nulls present: hoisted tight primitive loops (no inner-loop switches)
                    int loopLimit = selectedInUse ? selectedCount : n;
                    switch (func) {
                        case "COUNT" -> {
                            long c = 0;
                            if (!hasNulls) {
                                c = loopLimit;
                            } else {
                                for (int k = 0; k < loopLimit; k++) {
                                    int idx = selectedInUse ? sel[k] : k;
                                    if (!col.isNull(idx)) c++;
                                }
                            }
                            if (c > 0) {
                                counts[a] += c;
                                hasVal[a] = true;
                            }
                        }
                        case "SUM" -> {
                            switch (col.getType()) {
                                case INT -> {
                                    IntVector iv = (IntVector) col;
                                    long s = 0;
                                    if (!hasNulls && selectedInUse) {
                                        for (int k = 0; k < loopLimit; k++) s += iv.get(sel[k]);
                                        if (loopLimit > 0) hasVal[a] = true;
                                    } else {
                                        for (int k = 0; k < loopLimit; k++) {
                                            int idx = selectedInUse ? sel[k] : k;
                                            if (!col.isNull(idx)) { s += iv.get(idx); hasVal[a] = true; }
                                        }
                                    }
                                    intSums[a] += s;
                                }
                                case LONG -> {
                                    LongVector lv = (LongVector) col;
                                    long s = 0;
                                    for (int k = 0; k < loopLimit; k++) {
                                        int idx = selectedInUse ? sel[k] : k;
                                        if (!hasNulls || !col.isNull(idx)) { s += lv.get(idx); hasVal[a] = true; }
                                    }
                                    intSums[a] += s;
                                }
                                case DOUBLE -> {
                                    DoubleVector dv = (DoubleVector) col;
                                    double s = 0.0;
                                    if (!hasNulls && selectedInUse) {
                                        for (int k = 0; k < loopLimit; k++) s += dv.get(sel[k]);
                                        if (loopLimit > 0) hasVal[a] = true;
                                    } else {
                                        for (int k = 0; k < loopLimit; k++) {
                                            int idx = selectedInUse ? sel[k] : k;
                                            if (!col.isNull(idx)) { s += dv.get(idx); hasVal[a] = true; }
                                        }
                                    }
                                    dblSums[a] += s;
                                }
                                case BYTE -> {
                                    ByteVector bv = (ByteVector) col;
                                    long s = 0;
                                    for (int k = 0; k < loopLimit; k++) {
                                        int idx = selectedInUse ? sel[k] : k;
                                        if (!hasNulls || !col.isNull(idx)) { s += bv.get(idx); hasVal[a] = true; }
                                    }
                                    intSums[a] += s;
                                }
                            }
                        }
                        case "AVG" -> {
                            double s = 0.0;
                            long c = 0;
                            for (int k = 0; k < loopLimit; k++) {
                                int idx = selectedInUse ? sel[k] : k;
                                if (!hasNulls || !col.isNull(idx)) {
                                    s += readAsDouble(col, idx);
                                    c++;
                                }
                            }
                            if (c > 0) {
                                dblSums[a] += s;
                                counts[a] += c;
                                hasVal[a] = true;
                            }
                        }
                        case "MIN" -> {
                            double m = mins[a];
                            for (int k = 0; k < loopLimit; k++) {
                                int idx = selectedInUse ? sel[k] : k;
                                if (!hasNulls || !col.isNull(idx)) {
                                    double v = readAsDouble(col, idx);
                                    if (v < m) m = v;
                                    hasVal[a] = true;
                                }
                            }
                            mins[a] = m;
                        }
                        case "MAX" -> {
                            double m = maxs[a];
                            for (int k = 0; k < loopLimit; k++) {
                                int idx = selectedInUse ? sel[k] : k;
                                if (!hasNulls || !col.isNull(idx)) {
                                    double v = readAsDouble(col, idx);
                                    if (v > m) m = v;
                                    hasVal[a] = true;
                                }
                            }
                            maxs[a] = m;
                        }
                    }
                }
            }
        }

        List<Object> rowVals = new ArrayList<>(aggCount);
        Map<String, Object> aggMap = havingExpr != null ? new LinkedHashMap<>() : null;
        for (int a = 0; a < aggCount; a++) {
            AggregateCall agg = aggregates.get(a);
            String func = agg.function().toUpperCase();
            ColumnVector col = aggColIndices[a] >= 0 ? batch.getColumn(aggColIndices[a]) : null;
            Object resultVal = switch (func) {
                case "COUNT" -> (int) counts[a];
                case "SUM" -> !hasVal[a] ? null : (col != null && col.getType() == VectorType.DOUBLE ? (Object) dblSums[a] : (Object) intSums[a]);
                case "AVG" -> counts[a] > 0 ? (Object) (dblSums[a] / counts[a]) : null;
                case "MIN" -> !hasVal[a] ? null : formatMinMax(mins[a], col);
                case "MAX" -> !hasVal[a] ? null : formatMinMax(maxs[a], col);
                default -> null;
            };
            rowVals.add(resultVal);
            if (aggMap != null) {
                aggMap.put(agg.canonicalForm(), resultVal);
                aggMap.put(agg.displayName(), resultVal);
            }
        }

        Tuple resultTuple = new Tuple(rowVals, outputColumns);
        if (havingExpr != null && !SortAndAggregateUtils.evaluateHaving(havingExpr, aggMap, resultTuple)) {
            return List.of();
        }
        return List.of(resultTuple);
    }

    private List<Tuple> executeSingleGroupAggregate(VectorBatch batch) {
        int groupIdx = groupColIndices[0];
        VectorType groupType = child.getOutputTypes().get(groupIdx);
        PrimitiveGroupTable table = new PrimitiveGroupTable(aggregates.size(), 1024);

        while (child.nextBatch(batch)) {
            int n = batch.getNumRows();
            boolean selectedInUse = batch.isSelectedInUse();
            int loopLimit = selectedInUse ? batch.getSelectedCount() : n;
            int[] sel = batch.getSelectionVector();

            ColumnVector groupCol = batch.getColumn(groupIdx);
            boolean groupHasNulls = groupCol.hasNulls();

            for (int k = 0; k < loopLimit; k++) {
                int idx = selectedInUse ? sel[k] : k;
                long key = (groupHasNulls && groupCol.isNull(idx))
                    ? NULL_KEY_SENTINEL
                    : readKeyAsLong(groupCol, idx);

                int slotIdx = table.findOrInsert(key);
                for (int a = 0; a < aggregates.size(); a++) {
                    AggregateCall agg = aggregates.get(a);
                    String func = agg.function().toUpperCase();
                    int colIdx = aggColIndices[a];
                    if ("COUNT".equals(func) && colIdx < 0) {
                        table.counts[slotIdx][a]++;
                        table.hasVal[slotIdx][a] = true;
                        continue;
                    }
                    ColumnVector valCol = batch.getColumn(colIdx);
                    if (valCol.hasNulls() && valCol.isNull(idx)) {
                        continue;
                    }
                    table.hasVal[slotIdx][a] = true;
                    table.counts[slotIdx][a]++;
                    switch (func) {
                        case "COUNT" -> {}
                        case "SUM" -> {
                            if (valCol.getType() == VectorType.DOUBLE) {
                                table.dblSums[slotIdx][a] += ((DoubleVector) valCol).get(idx);
                            } else {
                                table.intSums[slotIdx][a] += readKeyAsLong(valCol, idx);
                            }
                        }
                        case "AVG" -> table.dblSums[slotIdx][a] += readAsDouble(valCol, idx);
                        case "MIN" -> {
                            double v = readAsDouble(valCol, idx);
                            if (v < table.mins[slotIdx][a]) table.mins[slotIdx][a] = v;
                        }
                        case "MAX" -> {
                            double v = readAsDouble(valCol, idx);
                            if (v > table.maxs[slotIdx][a]) table.maxs[slotIdx][a] = v;
                        }
                    }
                }
            }
        }

        List<Tuple> out = new ArrayList<>(table.size);
        for (int s = 0; s < table.capacity; s++) {
            if (!table.occupied[s]) continue;
            List<Object> rowVals = new ArrayList<>(1 + aggregates.size());
            rowVals.add(decodeGroupKey(table.keys[s], groupType));

            Map<String, Object> aggMap = havingExpr != null ? new LinkedHashMap<>() : null;
            for (int a = 0; a < aggregates.size(); a++) {
                AggregateCall agg = aggregates.get(a);
                String func = agg.function().toUpperCase();
                VectorType valType = aggColIndices[a] >= 0 ? child.getOutputTypes().get(aggColIndices[a]) : null;
                Object val = switch (func) {
                    case "COUNT" -> (int) table.counts[s][a];
                    case "SUM" -> !table.hasVal[s][a] ? null
                        : (valType == VectorType.DOUBLE ? (Object) table.dblSums[s][a] : (Object) table.intSums[s][a]);
                    case "AVG" -> table.counts[s][a] > 0 ? (Object) (table.dblSums[s][a] / table.counts[s][a]) : null;
                    case "MIN" -> !table.hasVal[s][a] ? null : formatMinMaxType(table.mins[s][a], valType);
                    case "MAX" -> !table.hasVal[s][a] ? null : formatMinMaxType(table.maxs[s][a], valType);
                    default -> null;
                };
                rowVals.add(val);
                if (aggMap != null) {
                    aggMap.put(agg.canonicalForm(), val);
                    aggMap.put(agg.displayName(), val);
                }
            }

            Tuple tuple = new Tuple(rowVals, outputColumns);
            if (havingExpr == null || SortAndAggregateUtils.evaluateHaving(havingExpr, aggMap, tuple)) {
                out.add(tuple);
            }
        }
        return out;
    }

    private static long readKeyAsLong(ColumnVector col, int idx) {
        return switch (col.getType()) {
            case INT -> ((IntVector) col).get(idx);
            case LONG -> ((LongVector) col).get(idx);
            case DOUBLE -> Double.doubleToRawLongBits(((DoubleVector) col).get(idx));
            case BYTE -> ((ByteVector) col).get(idx);
        };
    }

    private static Object decodeGroupKey(long key, VectorType type) {
        if (key == NULL_KEY_SENTINEL) return null;
        return switch (type) {
            case INT -> (int) key;
            case LONG -> key;
            case DOUBLE -> Double.longBitsToDouble(key);
            case BYTE -> key != 0L;
        };
    }

    private static double readAsDouble(ColumnVector col, int idx) {
        return switch (col.getType()) {
            case INT -> ((IntVector) col).get(idx);
            case LONG -> (double) ((LongVector) col).get(idx);
            case DOUBLE -> ((DoubleVector) col).get(idx);
            case BYTE -> ((ByteVector) col).get(idx);
        };
    }

    private static Object formatMinMax(double v, ColumnVector col) {
        return formatMinMaxType(v, col != null ? col.getType() : null);
    }

    private static Object formatMinMaxType(double v, VectorType type) {
        if (type == VectorType.INT) return (int) v;
        if (type == VectorType.LONG) return (long) v;
        return v;
    }

    @Override
    public void close() {
        child.close();
        slot.clear();
        results = null;
        open = false;
    }

    @Override
    public List<String> getOutputColumns() {
        return outputColumns;
    }

    /**
     * Zero-boxing open-addressing primitive hash table for single-column GROUP BY aggregations.
     */
    private static final class PrimitiveGroupTable {
        final int numAggs;
        int capacity;
        int mask;
        int size;
        long[] keys;
        boolean[] occupied;
        long[][] counts;
        long[][] intSums;
        double[][] dblSums;
        double[][] mins;
        double[][] maxs;
        boolean[][] hasVal;

        PrimitiveGroupTable(int numAggs, int initialCapacity) {
            this.numAggs = numAggs;
            this.capacity = Integer.highestOneBit(Math.max(16, initialCapacity - 1)) << 1;
            this.mask = this.capacity - 1;
            this.keys = new long[capacity];
            this.occupied = new boolean[capacity];
            this.counts = new long[capacity][numAggs];
            this.intSums = new long[capacity][numAggs];
            this.dblSums = new double[capacity][numAggs];
            this.mins = new double[capacity][numAggs];
            this.maxs = new double[capacity][numAggs];
            this.hasVal = new boolean[capacity][numAggs];
            for (int i = 0; i < capacity; i++) {
                Arrays.fill(mins[i], Double.POSITIVE_INFINITY);
                Arrays.fill(maxs[i], Double.NEGATIVE_INFINITY);
            }
        }

        int findOrInsert(long key) {
            if ((size + 1) * 2 >= capacity) {
                rehash();
            }
            int idx = mix64(key) & mask;
            while (occupied[idx]) {
                if (keys[idx] == key) {
                    return idx;
                }
                idx = (idx + 1) & mask;
            }
            occupied[idx] = true;
            keys[idx] = key;
            size++;
            return idx;
        }

        private void rehash() {
            int newCap = capacity << 1;
            int newMask = newCap - 1;
            long[] nKeys = new long[newCap];
            boolean[] nOcc = new boolean[newCap];
            long[][] nCounts = new long[newCap][numAggs];
            long[][] nIntSums = new long[newCap][numAggs];
            double[][] nDblSums = new double[newCap][numAggs];
            double[][] nMins = new double[newCap][numAggs];
            double[][] nMaxs = new double[newCap][numAggs];
            boolean[][] nHasVal = new boolean[newCap][numAggs];
            for (int i = 0; i < newCap; i++) {
                Arrays.fill(nMins[i], Double.POSITIVE_INFINITY);
                Arrays.fill(nMaxs[i], Double.NEGATIVE_INFINITY);
            }
            for (int i = 0; i < capacity; i++) {
                if (!occupied[i]) continue;
                int slot = mix64(keys[i]) & newMask;
                while (nOcc[slot]) {
                    slot = (slot + 1) & newMask;
                }
                nOcc[slot] = true;
                nKeys[slot] = keys[i];
                nCounts[slot] = counts[i];
                nIntSums[slot] = intSums[i];
                nDblSums[slot] = dblSums[i];
                nMins[slot] = mins[i];
                nMaxs[slot] = maxs[i];
                nHasVal[slot] = hasVal[i];
            }
            this.capacity = newCap;
            this.mask = newMask;
            this.keys = nKeys;
            this.occupied = nOcc;
            this.counts = nCounts;
            this.intSums = nIntSums;
            this.dblSums = nDblSums;
            this.mins = nMins;
            this.maxs = nMaxs;
            this.hasVal = nHasVal;
        }

        private static int mix64(long z) {
            z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
            z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
            return (int) (z ^ (z >>> 33));
        }
    }
}
