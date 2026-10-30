package com.stratosdb.sql.execution;

import com.stratosdb.storage.page.Tuple;

import java.util.List;

/**
 * Standard Volcano tuple slot carrier that holds a single Tuple passing between physical operators.
 * Tracks whether the slot currently holds a tuple or is empty (representing EOF / null).
 */
public class TupleSlot {
    private Tuple tuple;
    private boolean empty;

    public TupleSlot() {
        this.tuple = null;
        this.empty = true;
    }

    public TupleSlot(Tuple tuple) {
        set(tuple);
    }

    public static TupleSlot of(Tuple tuple) {
        return new TupleSlot(tuple);
    }

    public static TupleSlot empty() {
        return new TupleSlot();
    }

    public void set(Tuple tuple) {
        this.tuple = tuple;
        this.empty = (tuple == null);
    }

    public void clear() {
        this.tuple = null;
        this.empty = true;
    }

    public boolean isEmpty() {
        return empty || tuple == null;
    }

    public Tuple getTuple() {
        return tuple;
    }

    public Object getValue(int index) {
        return tuple != null ? tuple.getValue(index) : null;
    }

    public Object getValue(String columnName) {
        return tuple != null ? tuple.getValue(columnName) : null;
    }

    public List<String> getColumnNames() {
        return tuple != null ? tuple.getColumnNames() : List.of();
    }

    public int size() {
        return tuple != null ? tuple.size() : 0;
    }

    @Override
    public String toString() {
        return isEmpty() ? "TupleSlot[EMPTY]" : "TupleSlot[" + tuple + "]";
    }
}
