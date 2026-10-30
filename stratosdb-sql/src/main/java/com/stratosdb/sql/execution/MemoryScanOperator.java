package com.stratosdb.sql.execution;

import com.stratosdb.storage.page.Tuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Physical operator that streams pre-materialized tuples (e.g. from CTEs or virtual catalog views)
 * one-by-one in the Volcano execution pipeline.
 */
public class MemoryScanOperator implements PhysicalOperator {
    private final List<Tuple> rows;
    private final List<String> outputColumns;
    private Iterator<Tuple> iterator;
    private final TupleSlot slot = new TupleSlot();
    private boolean open = false;

    public MemoryScanOperator(List<Tuple> rows, List<String> outputColumns) {
        this.rows = rows != null ? rows : List.of();
        this.outputColumns = outputColumns != null ? new ArrayList<>(outputColumns) : List.of();
    }

    @Override
    public void open() {
        close();
        this.iterator = rows.iterator();
        this.open = true;
    }

    @Override
    public TupleSlot next() {
        if (!open || iterator == null || !iterator.hasNext()) {
            slot.clear();
            return slot;
        }
        slot.set(iterator.next());
        return slot;
    }

    @Override
    public void close() {
        iterator = null;
        slot.clear();
        open = false;
    }

    @Override
    public List<String> getOutputColumns() {
        return outputColumns;
    }
}
