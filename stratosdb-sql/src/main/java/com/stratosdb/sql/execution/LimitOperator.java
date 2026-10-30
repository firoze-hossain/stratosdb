package com.stratosdb.sql.execution;

import java.util.List;

/**
 * Physical operator that implements LIMIT and OFFSET streaming over its child operator.
 * Skips the first OFFSET rows and emits at most LIMIT rows, immediately terminating (EOF)
 * and short-circuiting unnecessary work from child operators in O(1) memory.
 */
public class LimitOperator implements PhysicalOperator {
    private final PhysicalOperator child;
    private final long limit;   // -1 means unlimited
    private final long offset;  // 0 means no offset

    private long rowsSkipped = 0;
    private long rowsEmitted = 0;
    private final TupleSlot slot = new TupleSlot();
    private boolean open = false;

    public LimitOperator(PhysicalOperator child, long limit, long offset) {
        this.child = child;
        this.limit = limit;
        this.offset = Math.max(0, offset);
    }

    @Override
    public void open() {
        close();
        if (child != null) {
            child.open();
        }
        rowsSkipped = 0;
        rowsEmitted = 0;
        open = true;

        // Skip offset rows eagerly on open
        while (rowsSkipped < offset && child != null) {
            TupleSlot s = child.next();
            if (s == null || s.isEmpty()) {
                break;
            }
            rowsSkipped++;
        }
    }

    @Override
    public TupleSlot next() {
        if (!open || child == null) {
            slot.clear();
            return slot;
        }

        if (limit >= 0 && rowsEmitted >= limit) {
            close();
            slot.clear();
            return slot;
        }

        TupleSlot childSlot = child.next();
        if (childSlot == null || childSlot.isEmpty()) {
            close();
            slot.clear();
            return slot;
        }

        rowsEmitted++;
        slot.set(childSlot.getTuple());
        if (limit >= 0 && rowsEmitted >= limit) {
            if (child != null) {
                try {
                    child.close();
                } catch (Exception ignored) {
                }
            }
        }
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
        return child != null ? child.getOutputColumns() : List.of();
    }

    public PhysicalOperator getChild() {
        return child;
    }

    public long getLimit() {
        return limit;
    }

    public long getOffset() {
        return offset;
    }

    public long getRowsEmitted() {
        return rowsEmitted;
    }
}
