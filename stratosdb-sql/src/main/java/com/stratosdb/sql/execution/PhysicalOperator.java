package com.stratosdb.sql.execution;

import java.util.List;

/**
 * Volcano iterator model interface for query execution physical operators.
 * Operators stream tuples one at a time via open(), next(): TupleSlot, and close(),
 * eliminating monolithic memory accumulation and keeping heap usage O(1).
 */
public interface PhysicalOperator extends AutoCloseable {

    /**
     * Initializes the operator and opens child operators.
     */
    void open();

    /**
     * Advances to and returns the next TupleSlot in the execution pipeline.
     * Returns an empty TupleSlot or null when the operator's stream is exhausted (EOF).
     */
    TupleSlot next();

    /**
     * Closes the operator, releasing all iterators, page pins, latches, and child resources.
     */
    @Override
    void close();

    /**
     * Returns the output schema (column names) of tuples produced by this operator.
     */
    List<String> getOutputColumns();
}
