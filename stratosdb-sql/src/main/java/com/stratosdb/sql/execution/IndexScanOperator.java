package com.stratosdb.sql.execution;

import com.stratosdb.index.KeyValueIndex;
import com.stratosdb.index.btree.BTreeIndex;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.BTreePage;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.transaction.TransactionManager;
import com.stratosdb.transaction.mvcc.MVCCVisibility;
import com.stratosdb.transaction.mvcc.Snapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Physical operator that performs an index scan (point equality or range scan)
 * using a backing KeyValueIndex / BTreeIndex, fetching visible tuples from the heap.
 * Supports index-only scans when visibility map verifies all-visible pages.
 */
public class IndexScanOperator implements PhysicalOperator {
    private final String tableName;
    private final HeapTable table;
    private final Snapshot snapshot;
    private final TransactionManager txnManager;
    private final KeyValueIndex index;
    private final Long loKey;
    private final Long hiKey;
    private final boolean isEquality;
    private final String indexedColumn;
    private final List<String> tableColumns;
    private final BiFunction<String, byte[], Tuple> tupleDeserializer;
    private final boolean indexOnlyEligible;

    private Iterator<BTreePage.RID> ridIterator;
    private final TupleSlot slot = new TupleSlot();
    private boolean open = false;

    public IndexScanOperator(String tableName,
                            HeapTable table,
                            Snapshot snapshot,
                            TransactionManager txnManager,
                            KeyValueIndex index,
                            Long loKey,
                            Long hiKey,
                            boolean isEquality,
                            String indexedColumn,
                            List<String> tableColumns,
                            BiFunction<String, byte[], Tuple> tupleDeserializer,
                            boolean indexOnlyEligible) {
        this.tableName = tableName;
        this.table = table;
        this.snapshot = snapshot;
        this.txnManager = txnManager;
        this.index = index;
        this.loKey = loKey;
        this.hiKey = hiKey;
        this.isEquality = isEquality;
        this.indexedColumn = indexedColumn;
        this.tableColumns = tableColumns != null ? new ArrayList<>(tableColumns) : List.of();
        this.tupleDeserializer = tupleDeserializer;
        this.indexOnlyEligible = indexOnlyEligible;
    }

    @Override
    public void open() {
        close();
        if (index != null && loKey != null) {
            List<BTreePage.RID> rids;
            if (isEquality) {
                rids = index.searchAll(loKey);
            } else if (index instanceof BTreeIndex btree && hiKey != null) {
                rids = btree.rangeScan(loKey, hiKey);
            } else {
                rids = List.of();
            }
            this.ridIterator = rids != null ? rids.iterator() : Collections.emptyIterator();
        } else {
            this.ridIterator = Collections.emptyIterator();
        }
        this.open = true;
    }

    @Override
    public TupleSlot next() {
        if (!open || ridIterator == null) {
            slot.clear();
            return slot;
        }

        while (ridIterator.hasNext()) {
            BTreePage.RID rid = ridIterator.next();
            if (indexOnlyEligible && table != null && table.isAllVisible(rid.pageId())) {
                Object valueToReturn;
                if (loKey >= Integer.MIN_VALUE && loKey <= Integer.MAX_VALUE) {
                    valueToReturn = Integer.valueOf(loKey.intValue());
                } else {
                    valueToReturn = loKey;
                }
                Tuple indexOnlyTuple = new Tuple();
                indexOnlyTuple.addValue(indexedColumn, valueToReturn);
                slot.set(indexOnlyTuple);
                return slot;
            }

            if (table != null) {
                byte[] stored = table.readTuple(rid.pageId(), rid.slot());
                if (stored == null) {
                    continue;
                }
                if (snapshot != null && txnManager != null && !MVCCVisibility.isVisible(stored, snapshot, txnManager)) {
                    continue;
                }

                byte[] payload = MVCCVisibility.readPayload(stored);
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
        ridIterator = null;
        slot.clear();
        open = false;
    }

    @Override
    public List<String> getOutputColumns() {
        return indexOnlyEligible ? List.of(indexedColumn) : tableColumns;
    }

    public String getTableName() {
        return tableName;
    }

    public String getIndexedColumn() {
        return indexedColumn;
    }
}
