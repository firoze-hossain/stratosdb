package com.stratosdb.sql.execution;

import com.stratosdb.storage.page.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Physical operator that projects columns and evaluates expressions/aliases
 * on each tuple emitted by its child operator in O(1) memory.
 */
public class ProjectOperator implements PhysicalOperator {
    private final PhysicalOperator child;
    private final Function<Tuple, Tuple> projector;
    private final List<String> outputColumns;
    private final TupleSlot slot = new TupleSlot();
    private boolean open = false;

    public ProjectOperator(PhysicalOperator child, Function<Tuple, Tuple> projector, List<String> outputColumns) {
        this.child = child;
        this.projector = projector;
        this.outputColumns = outputColumns != null ? new ArrayList<>(outputColumns) : List.of();
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

        TupleSlot childSlot = child.next();
        if (childSlot == null || childSlot.isEmpty()) {
            slot.clear();
            return slot;
        }

        Tuple projected = projector != null ? projector.apply(childSlot.getTuple()) : childSlot.getTuple();
        slot.set(projected);
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
        slot.clear();
        open = false;
    }

    @Override
    public List<String> getOutputColumns() {
        return (!outputColumns.isEmpty()) ? outputColumns : (child != null ? child.getOutputColumns() : List.of());
    }

    public PhysicalOperator getChild() {
        return child;
    }
}
