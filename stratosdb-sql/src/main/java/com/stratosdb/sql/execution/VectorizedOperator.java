package com.stratosdb.sql.execution;

import com.stratosdb.storage.vector.VectorBatch;
import com.stratosdb.storage.vector.VectorType;

import java.util.List;

/**
 * Vectorized execution engine operator interface.
 * Processes batches of 1024 flat primitive values in native off-heap memory
 * ({@link VectorBatch}) instead of row-by-row Volcano Tuple allocation.
 */
public interface VectorizedOperator extends AutoCloseable {

    /**
     * Initializes the vectorized operator and allocates/opens child resources.
     */
    void open();

    /**
     * Populates the next batch into {@code batch} and returns {@code true},
     * or returns {@code false} when the stream is exhausted (EOF).
     */
    boolean nextBatch(VectorBatch batch);

    /**
     * Releases all resources and off-heap cursors held by this operator.
     */
    @Override
    void close();

    /**
     * Returns the output column names produced by this operator.
     */
    List<String> getOutputColumns();

    /**
     * Returns the physical vector types corresponding to {@link #getOutputColumns()}.
     */
    List<VectorType> getOutputTypes();
}
