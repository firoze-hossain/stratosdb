package com.stratosdb.sql.execution;

import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.vector.VectorBatch;
import com.stratosdb.storage.vector.VectorType;
import com.stratosdb.storage.vector.VectorizedHeapScanner;
import com.stratosdb.transaction.TransactionManager;
import com.stratosdb.transaction.mvcc.Snapshot;

import java.util.ArrayList;
import java.util.List;

/**
 * Vectorized sequential scan operator that streams {@link VectorBatch}es
 * directly from off-heap {@link HeapTable} pages with zero heap allocations.
 */
public final class VectorizedSeqScanOperator implements VectorizedOperator {
    private final HeapTable table;
    private final Snapshot snapshot;
    private final TransactionManager txnManager;
    private final List<String> outputColumns;
    private final List<VectorType> outputTypes;
    private final int[] projectedColumnIndices;

    private VectorizedHeapScanner scanner;
    private boolean open;

    public VectorizedSeqScanOperator(HeapTable table,
                                     Snapshot snapshot,
                                     TransactionManager txnManager,
                                     List<String> outputColumns,
                                     List<VectorType> outputTypes,
                                     int[] projectedColumnIndices) {
        this.table = table;
        this.snapshot = snapshot;
        this.txnManager = txnManager;
        this.outputColumns = outputColumns != null ? new ArrayList<>(outputColumns) : List.of();
        this.outputTypes = outputTypes != null ? new ArrayList<>(outputTypes) : List.of();
        this.projectedColumnIndices = projectedColumnIndices.clone();
    }

    @Override
    public void open() {
        if (scanner != null) {
            scanner.reset();
        } else if (table != null) {
            scanner = table.scanVectorized(snapshot, txnManager, projectedColumnIndices);
        }
        this.open = true;
    }

    @Override
    public boolean nextBatch(VectorBatch batch) {
        if (!open || scanner == null) {
            return false;
        }
        return scanner.nextBatch(batch);
    }

    @Override
    public void close() {
        if (scanner != null) {
            scanner.close();
            scanner = null;
        }
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
