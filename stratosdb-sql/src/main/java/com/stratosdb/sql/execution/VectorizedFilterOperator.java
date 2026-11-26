package com.stratosdb.sql.execution;

import com.stratosdb.sql.ast.WhereExpr;
import com.stratosdb.storage.vector.ColumnVector;
import com.stratosdb.storage.vector.VectorBatch;
import com.stratosdb.storage.vector.VectorType;
import jdk.incubator.vector.VectorOperators;

import java.util.List;

/**
 * Vectorized filter operator that evaluates {@link WhereExpr} predicates over 1024-row
 * {@link VectorBatch}es using Java 25 SIMD vector compiler intrinsics ({@link SimdVectorKernels}).
 *
 * Updates the batch's primitive selection vector in place with zero heap allocations.
 */
public final class VectorizedFilterOperator implements VectorizedOperator {
    private final VectorizedOperator child;
    private final WhereExpr predicate;
    private final List<String> outputColumns;
    private final List<VectorType> outputTypes;

    private final long[] resultMask;
    private CompiledExpr compiledPredicate;
    private boolean open;

    public VectorizedFilterOperator(VectorizedOperator child, WhereExpr predicate) {
        this.child = child;
        this.predicate = predicate;
        this.outputColumns = child.getOutputColumns();
        this.outputTypes = child.getOutputTypes();
        this.resultMask = new long[(VectorBatch.DEFAULT_BATCH_SIZE + 63) >>> 6];
    }

    @Override
    public void open() {
        child.open();
        this.compiledPredicate = predicate != null ? compileExpr(predicate) : null;
        this.open = true;
    }

    @Override
    public boolean nextBatch(VectorBatch batch) {
        if (!open) {
            return false;
        }
        while (child.nextBatch(batch)) {
            int count = batch.getNumRows();
            if (count == 0) {
                continue;
            }
            if (compiledPredicate == null) {
                return true;
            }

            evaluateCompiled(compiledPredicate, batch, count, resultMask);

            int outCount;
            if (batch.isSelectedInUse()) {
                int[] sel = batch.getSelectionVector();
                int prevCount = batch.getSelectedCount();
                int kept = 0;
                for (int i = 0; i < prevCount; i++) {
                    int idx = sel[i];
                    if ((resultMask[idx >>> 6] & (1L << (idx & 63))) != 0L) {
                        sel[kept++] = idx;
                    }
                }
                outCount = kept;
            } else {
                outCount = SimdVectorKernels.materializeSelection(resultMask, count, batch.getSelectionVector());
            }

            if (outCount > 0) {
                batch.setSelectedInUse(true);
                batch.setSelectedCount(outCount);
                return true;
            }
        }
        return false;
    }

    private sealed interface CompiledExpr {}
    private record CompiledBool(boolean value) implements CompiledExpr {}
    private record CompiledIntCmp(int colIdx, VectorOperators.Comparison op, int val) implements CompiledExpr {}
    private record CompiledLongCmp(int colIdx, VectorOperators.Comparison op, long val) implements CompiledExpr {}
    private record CompiledDoubleCmp(int colIdx, VectorOperators.Comparison op, double val) implements CompiledExpr {}
    private record CompiledByteCmp(int colIdx, VectorOperators.Comparison op, byte val) implements CompiledExpr {}
    private record CompiledColCmp(int leftIdx, int rightIdx, VectorOperators.Comparison op, VectorType type) implements CompiledExpr {}
    private record CompiledAnd(CompiledExpr left, CompiledExpr right, long[] rightScratch) implements CompiledExpr {}
    private record CompiledOr(CompiledExpr left, CompiledExpr right, long[] rightScratch) implements CompiledExpr {}
    private record CompiledNot(CompiledExpr inner) implements CompiledExpr {}

    private CompiledExpr compileExpr(WhereExpr expr) {
        int maskWords = (VectorBatch.DEFAULT_BATCH_SIZE + 63) >>> 6;
        return switch (expr) {
            case WhereExpr.BooleanLiteral bl -> new CompiledBool(bl.value());
            case WhereExpr.Comparison cmp -> {
                int colIdx = resolveColumnIndex(cmp.column());
                if (colIdx < 0) yield new CompiledBool(false);
                VectorType type = outputTypes.get(colIdx);
                String lit = stripQuotes(cmp.literal());
                VectorOperators.Comparison op = SimdVectorKernels.toVectorOp(cmp.operator());
                yield switch (type) {
                    case INT -> new CompiledIntCmp(colIdx, op, Integer.parseInt(lit));
                    case LONG -> new CompiledLongCmp(colIdx, op, Long.parseLong(lit));
                    case DOUBLE -> new CompiledDoubleCmp(colIdx, op, Double.parseDouble(lit));
                    case BYTE -> new CompiledByteCmp(colIdx, op, parseByteOrBoolean(lit));
                };
            }
            case WhereExpr.ColumnComparison cmp -> {
                int leftIdx = resolveColumnIndex(cmp.leftColumn());
                int rightIdx = resolveColumnIndex(cmp.rightColumn());
                if (leftIdx < 0 || rightIdx < 0) yield new CompiledBool(false);
                VectorType lt = outputTypes.get(leftIdx);
                VectorType rt = outputTypes.get(rightIdx);
                if (lt != rt) yield new CompiledBool(false);
                yield new CompiledColCmp(leftIdx, rightIdx, SimdVectorKernels.toVectorOp(cmp.operator()), lt);
            }
            case WhereExpr.And and -> new CompiledAnd(compileExpr(and.left()), compileExpr(and.right()), new long[maskWords]);
            case WhereExpr.Or or -> new CompiledOr(compileExpr(or.left()), compileExpr(or.right()), new long[maskWords]);
            case WhereExpr.Not not -> new CompiledNot(compileExpr(not.inner()));
            default -> throw new UnsupportedOperationException(
                "WhereExpr variant not yet supported in VectorizedFilterOperator: " + expr.getClass().getSimpleName());
        };
    }

    private void evaluateCompiled(CompiledExpr expr, VectorBatch batch, int count, long[] outMask) {
        int words = (count + 63) >>> 6;
        switch (expr) {
            case CompiledBool bl -> {
                if (bl.value()) SimdVectorKernels.fillMask(outMask, count);
                else SimdVectorKernels.clearMask(outMask, count);
            }
            case CompiledIntCmp c -> {
                ColumnVector col = batch.getColumn(c.colIdx());
                SimdVectorKernels.filterInt(col.getDataSegment(), col.getNullBitmapSegment(), col.hasNulls(),
                    count, c.op(), c.val(), outMask);
            }
            case CompiledLongCmp c -> {
                ColumnVector col = batch.getColumn(c.colIdx());
                SimdVectorKernels.filterLong(col.getDataSegment(), col.getNullBitmapSegment(), col.hasNulls(),
                    count, c.op(), c.val(), outMask);
            }
            case CompiledDoubleCmp c -> {
                ColumnVector col = batch.getColumn(c.colIdx());
                SimdVectorKernels.filterDouble(col.getDataSegment(), col.getNullBitmapSegment(), col.hasNulls(),
                    count, c.op(), c.val(), outMask);
            }
            case CompiledByteCmp c -> {
                ColumnVector col = batch.getColumn(c.colIdx());
                SimdVectorKernels.filterByte(col.getDataSegment(), col.getNullBitmapSegment(), col.hasNulls(),
                    count, c.op(), c.val(), outMask);
            }
            case CompiledColCmp c -> {
                ColumnVector leftCol = batch.getColumn(c.leftIdx());
                ColumnVector rightCol = batch.getColumn(c.rightIdx());
                if (c.type() == VectorType.INT) {
                    SimdVectorKernels.filterIntColumnPair(
                        leftCol.getDataSegment(), leftCol.getNullBitmapSegment(), leftCol.hasNulls(),
                        rightCol.getDataSegment(), rightCol.getNullBitmapSegment(), rightCol.hasNulls(),
                        count, c.op(), outMask);
                } else if (c.type() == VectorType.DOUBLE) {
                    SimdVectorKernels.filterDoubleColumnPair(
                        leftCol.getDataSegment(), leftCol.getNullBitmapSegment(), leftCol.hasNulls(),
                        rightCol.getDataSegment(), rightCol.getNullBitmapSegment(), rightCol.hasNulls(),
                        count, c.op(), outMask);
                } else {
                    SimdVectorKernels.clearMask(outMask, count);
                }
            }
            case CompiledAnd and -> {
                evaluateCompiled(and.left(), batch, count, outMask);
                evaluateCompiled(and.right(), batch, count, and.rightScratch());
                long[] rightMask = and.rightScratch();
                for (int w = 0; w < words; w++) {
                    outMask[w] &= rightMask[w];
                }
            }
            case CompiledOr or -> {
                evaluateCompiled(or.left(), batch, count, outMask);
                evaluateCompiled(or.right(), batch, count, or.rightScratch());
                long[] rightMask = or.rightScratch();
                for (int w = 0; w < words; w++) {
                    outMask[w] |= rightMask[w];
                }
            }
            case CompiledNot not -> {
                evaluateCompiled(not.inner(), batch, count, outMask);
                for (int w = 0; w < words; w++) {
                    outMask[w] = ~outMask[w];
                }
                int rem = count & 63;
                if (rem != 0) {
                    outMask[words - 1] &= (1L << rem) - 1L;
                }
            }
        }
    }

    private int resolveColumnIndex(String colName) {
        if (colName == null) return -1;
        String target = colName.contains(".") ? colName.substring(colName.lastIndexOf('.') + 1) : colName;
        for (int i = 0; i < outputColumns.size(); i++) {
            if (outputColumns.get(i).equalsIgnoreCase(colName) || outputColumns.get(i).equalsIgnoreCase(target)) {
                return i;
            }
        }
        return -1;
    }

    private static String stripQuotes(String s) {
        if (s != null && s.length() >= 2
                && ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\"")))) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private static byte parseByteOrBoolean(String lit) {
        if ("true".equalsIgnoreCase(lit)) return 1;
        if ("false".equalsIgnoreCase(lit)) return 0;
        return Byte.parseByte(lit);
    }

    @Override
    public void close() {
        child.close();
        open = false;
    }

    @Override
    public List<String> getOutputColumns() {
        return outputColumns;
    }

    @Override
    public List<VectorType> getOutputTypes() {
        return outputTypes;
    }
}
