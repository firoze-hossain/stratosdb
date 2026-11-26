package com.stratosdb.sql.execution;

import com.stratosdb.index.KeyValueIndex;
import com.stratosdb.index.btree.BTreeIndex;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.BTreePage;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.transaction.TransactionManager;
import com.stratosdb.transaction.mvcc.MVCCVisibility;
import com.stratosdb.transaction.mvcc.Snapshot;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Physical operator that performs an Index-Only Scan over a {@link BTreeIndex}.
 *
 * For each matching {@link BTreeIndex.KeyRidEntry}, consults the table's persistent
 * 2-bit {@link com.stratosdb.storage.heap.VisibilityMap}:
 * - When the target heap page is marked {@code ALL_VISIBLE}, bypasses the heap tuple read
 *   completely (0 heap page reads) and projects the key directly from the B+Tree leaf.
 * - When the target heap page is NOT all-visible, falls back to reading the heap tuple
 *   and checking MVCC visibility, incrementing {@code heapPageReads}.
 */
public class IndexOnlyScanOperator implements PhysicalOperator {
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

    private Iterator<BTreeIndex.KeyRidEntry> entryIterator;
    private final TupleSlot slot = new TupleSlot();
    private boolean open = false;

    private long heapPageReads = 0L;
    private long indexOnlyHits = 0L;
    private long rowsReturned = 0L;

    public IndexOnlyScanOperator(String tableName,
                                 HeapTable table,
                                 Snapshot snapshot,
                                 TransactionManager txnManager,
                                 KeyValueIndex index,
                                 Long loKey,
                                 Long hiKey,
                                 boolean isEquality,
                                 String indexedColumn,
                                 List<String> tableColumns,
                                 BiFunction<String, byte[], Tuple> tupleDeserializer) {
        this.tableName = tableName;
        this.table = table;
        this.snapshot = snapshot;
        this.txnManager = txnManager;
        this.index = index;
        this.loKey = loKey;
        this.hiKey = hiKey;
        this.isEquality = isEquality;
        this.indexedColumn = indexedColumn;
        this.tableColumns = tableColumns != null ? tableColumns : List.of();
        this.tupleDeserializer = tupleDeserializer;
    }

    @Override
    public void open() {
        close();
        this.heapPageReads = 0L;
        this.indexOnlyHits = 0L;
        this.rowsReturned = 0L;

        if (index instanceof BTreeIndex btree && loKey != null) {
            long hi = (isEquality || hiKey == null) ? loKey : hiKey;
            List<BTreeIndex.KeyRidEntry> entries = btree.rangeScanEntries(loKey, hi);
            this.entryIterator = entries != null ? entries.iterator() : Collections.emptyIterator();
        } else {
            this.entryIterator = Collections.emptyIterator();
        }
        this.open = true;
    }

    @Override
    public TupleSlot next() {
        if (!open || entryIterator == null) {
            slot.clear();
            return slot;
        }

        while (entryIterator.hasNext()) {
            BTreeIndex.KeyRidEntry entry = entryIterator.next();
            long pageId = entry.rid().pageId();

            if (table != null && table.isAllVisible(pageId)) {
                indexOnlyHits++;
                rowsReturned++;
                long key = entry.key();
                Object valueToReturn;
                if (key >= Integer.MIN_VALUE && key <= Integer.MAX_VALUE) {
                    valueToReturn = Integer.valueOf((int) key);
                } else {
                    valueToReturn = Long.valueOf(key);
                }
                Tuple indexOnlyTuple = new Tuple();
                indexOnlyTuple.addValue(indexedColumn, valueToReturn);
                slot.set(indexOnlyTuple);
                return slot;
            }

            if (table != null) {
                heapPageReads++;
                byte[] stored = table.readTuple(pageId, entry.rid().slot());
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
                tuple.setPageId(pageId);
                tuple.setSlot(entry.rid().slot());
                tuple.setTableName(tableName);
                rowsReturned++;
                slot.set(tuple);
                return slot;
            }
        }

        slot.clear();
        return slot;
    }

    @Override
    public void close() {
        entryIterator = null;
        slot.clear();
        open = false;
    }

    @Override
    public List<String> getOutputColumns() {
        return List.of(indexedColumn);
    }

    public long getHeapPageReads() {
        return heapPageReads;
    }

    public long getIndexOnlyHits() {
        return indexOnlyHits;
    }

    public long getRowsReturned() {
        return rowsReturned;
    }

    public String getTableName() {
        return tableName;
    }

    public String getIndexedColumn() {
        return indexedColumn;
    }
}
