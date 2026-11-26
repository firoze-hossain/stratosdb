package com.stratosdb.storage.vector;

import java.lang.foreign.Arena;
import java.util.List;

/**
 * Columnar VectorBatch container holding up to {@code capacity} (default 1024)
 * rows across typed native off-heap {@link ColumnVector}s ({@link IntVector},
 * {@link LongVector}, {@link DoubleVector}, {@link ByteVector}).
 *
 * Manages a unified {@link Arena} for zero-GC-overhead native memory lifecycle
 * and provides a reusable primitive selection vector for zero-copy filtering.
 */
public final class VectorBatch implements AutoCloseable {
    public static final int DEFAULT_BATCH_SIZE = ColumnVector.DEFAULT_CAPACITY;

    private final Arena arena;
    private final int capacity;
    private final String[] columnNames;
    private final ColumnVector[] columns;
    private final int[] selectionVector;

    private int numRows;
    private boolean selectedInUse;
    private int selectedCount;

    public VectorBatch(VectorType... types) {
        this(DEFAULT_BATCH_SIZE, defaultNames(types.length), types);
    }

    public VectorBatch(List<String> columnNames, List<VectorType> types) {
        this(DEFAULT_BATCH_SIZE,
             columnNames != null ? columnNames.toArray(new String[0]) : defaultNames(types.size()),
             types.toArray(new VectorType[0]));
    }

    public VectorBatch(int capacity, List<String> columnNames, List<VectorType> types) {
        this(capacity,
             columnNames != null ? columnNames.toArray(new String[0]) : defaultNames(types.size()),
             types.toArray(new VectorType[0]));
    }

    public VectorBatch(int capacity, String[] columnNames, VectorType... types) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Batch capacity must be positive: " + capacity);
        }
        if (types == null) {
            throw new IllegalArgumentException("Vector types must not be null");
        }
        this.capacity = capacity;
        this.arena = Arena.ofShared();
        this.columnNames = columnNames != null ? columnNames.clone() : defaultNames(types.length);
        this.columns = new ColumnVector[types.length];
        this.selectionVector = new int[capacity];

        for (int i = 0; i < types.length; i++) {
            this.columns[i] = createVector(types[i], capacity, this.arena);
        }
        this.numRows = 0;
        this.selectedInUse = false;
        this.selectedCount = 0;
    }

    private static String[] defaultNames(int count) {
        String[] names = new String[count];
        for (int i = 0; i < count; i++) {
            names[i] = "col" + i;
        }
        return names;
    }

    private static ColumnVector createVector(VectorType type, int capacity, Arena arena) {
        return switch (type) {
            case INT -> new IntVector(capacity, arena);
            case LONG -> new LongVector(capacity, arena);
            case DOUBLE -> new DoubleVector(capacity, arena);
            case BYTE -> new ByteVector(capacity, arena);
        };
    }

    /**
     * Resets row count, selection vector state, and all child ColumnVectors
     * for batch reuse with zero heap or native allocations.
     */
    public void reset() {
        this.numRows = 0;
        this.selectedInUse = false;
        this.selectedCount = 0;
        for (ColumnVector col : columns) {
            col.reset();
        }
    }

    public int getCapacity() {
        return capacity;
    }

    public int getColumnCount() {
        return columns.length;
    }

    public String getColumnName(int index) {
        return columnNames[index];
    }

    public ColumnVector getColumn(int index) {
        return columns[index];
    }

    public IntVector getIntVector(int index) {
        return (IntVector) columns[index];
    }

    public LongVector getLongVector(int index) {
        return (LongVector) columns[index];
    }

    public DoubleVector getDoubleVector(int index) {
        return (DoubleVector) columns[index];
    }

    public ByteVector getByteVector(int index) {
        return (ByteVector) columns[index];
    }

    public int getNumRows() {
        return numRows;
    }

    /**
     * Updates the batch row count and synchronizes each column vector's size.
     */
    public void setNumRows(int numRows) {
        if (numRows < 0 || numRows > capacity) {
            throw new IndexOutOfBoundsException("numRows " + numRows + " out of bounds [0, " + capacity + "]");
        }
        this.numRows = numRows;
        for (ColumnVector col : columns) {
            col.setSize(numRows);
        }
    }

    public int[] getSelectionVector() {
        return selectionVector;
    }

    public boolean isSelectedInUse() {
        return selectedInUse;
    }

    public void setSelectedInUse(boolean selectedInUse) {
        this.selectedInUse = selectedInUse;
    }

    public int getSelectedCount() {
        return selectedInUse ? selectedCount : numRows;
    }

    public void setSelectedCount(int selectedCount) {
        this.selectedCount = selectedCount;
    }

    /**
     * Returns total native off-heap memory footprint across all columns in bytes.
     */
    public long getTotalNativeMemoryBytes() {
        long total = 0L;
        for (ColumnVector col : columns) {
            total += col.getMemoryFootprintBytes();
        }
        return total;
    }

    @Override
    public void close() {
        if (arena.scope().isAlive()) {
            arena.close();
        }
    }
}
