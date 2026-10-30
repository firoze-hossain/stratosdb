package com.stratosdb.sql.execution;

import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.transaction.TransactionManager;
import com.stratosdb.transaction.mvcc.MVCCVisibility;
import com.stratosdb.transaction.mvcc.Snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Physical operator that performs a sequential scan over a HeapTable.
 * Streams visible tuples page-by-page off disk / buffer pool under read latches,
 * without accumulating tables into ArrayList in memory.
 */
public class SeqScanOperator implements PhysicalOperator {
    private final String tableName;
    private final HeapTable table;
    private final Snapshot snapshot;
    private final TransactionManager txnManager;
    private final List<String> tableColumns;
    private final BiFunction<String, byte[], Tuple> tupleDeserializer;

    private HeapTable.PositionedRowIterator rowIterator;
    private final TupleSlot slot = new TupleSlot();
    private boolean open = false;

    public SeqScanOperator(String tableName,
                          HeapTable table,
                          Snapshot snapshot,
                          TransactionManager txnManager,
                          List<String> tableColumns,
                          BiFunction<String, byte[], Tuple> tupleDeserializer) {
        this.tableName = tableName;
        this.table = table;
        this.snapshot = snapshot;
        this.txnManager = txnManager;
        this.tableColumns = tableColumns != null ? new ArrayList<>(tableColumns) : List.of();
        this.tupleDeserializer = tupleDeserializer;
    }

    @Override
    public void open() {
        close();
        if (table != null) {
            this.rowIterator = table.iteratePositioned();
        }
        this.open = true;
    }

    @Override
    public TupleSlot next() {
        if (!open || rowIterator == null) {
            slot.clear();
            return slot;
        }

        while (rowIterator.hasNext()) {
            HeapTable.PositionedRow row = rowIterator.next();
            if (snapshot == null || txnManager == null || MVCCVisibility.isVisible(row.stored(), snapshot, txnManager)) {
                byte[] payload = MVCCVisibility.readPayload(row.stored());
                Tuple tuple = tupleDeserializer != null
                    ? tupleDeserializer.apply(tableName, payload)
                    : Tuple.deserialize(payload, tableColumns);
                slot.set(tuple);
                return slot;
            }
        }

        slot.clear();
        return slot;
    }

    @Override
    public void close() {
        if (rowIterator != null) {
            try {
                rowIterator.close();
            } catch (Exception ignored) {
            }
            rowIterator = null;
        }
        slot.clear();
        open = false;
    }

    @Override
    public List<String> getOutputColumns() {
        return tableColumns;
    }

    public String getTableName() {
        return tableName;
    }
}
