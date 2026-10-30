package com.stratosdb.sql.execution;

import com.stratosdb.storage.page.Tuple;

import java.util.List;
import java.util.function.Predicate;

/**
 * Physical operator that applies a filter predicate to its child operator's stream.
 * Evaluates the predicate row-by-row, returning matching tuples in O(1) memory.
 */
public class FilterOperator implements PhysicalOperator {
    private final PhysicalOperator child;
    private final Predicate<Tuple> predicate;
    private final TupleSlot slot = new TupleSlot();
    private boolean open = false;

    public FilterOperator(PhysicalOperator child, Predicate<Tuple> predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    @Override
    public void open() {
        close();
        if (child != null) {
            child.open();
        }
        this.open = true;
    }

    @Override
    public TupleSlot next() {
        if (!open || child == null) {
            slot.clear();
            return slot;
        }

        while (true) {
            TupleSlot childSlot = child.next();
            if (childSlot == null || childSlot.isEmpty()) {
                slot.clear();
                return slot;
            }

            Tuple tuple = childSlot.getTuple();
            if (predicate == null || predicate.test(tuple)) {
                slot.set(tuple);
                return slot;
            }
        }
    }

    @Override
    public void close() {
        if (child != null) {
            try {
                child.close();
            } catch (Exception ignored) {
            }
        }
        slot.clear();
        open = false;
    }

    @Override
    public List<String> getOutputColumns() {
        return child != null ? child.getOutputColumns() : List.of();
    }

    public PhysicalOperator getChild() {
        return child;
    }

    public Predicate<Tuple> getPredicate() {
        return predicate;
    }
}
