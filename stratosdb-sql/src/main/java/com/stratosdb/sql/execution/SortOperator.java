package com.stratosdb.sql.execution;

import com.stratosdb.sql.ast.OrderItem;
import com.stratosdb.storage.page.Tuple;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Volcano physical operator implementing streaming sort.
 * Uses in-memory QuickSort/TimSort when the dataset fits within work_mem,
 * and spills sorted runs to disk for K-way external merge sort when
 * memory exceeds work_mem (eliminating OutOfMemoryError on 1M+ rows).
 */
public class SortOperator implements PhysicalOperator {

    public static final long DEFAULT_WORK_MEM_BYTES = 4L * 1024L * 1024L; // 4 MB

    private final PhysicalOperator child;
    private final List<OrderItem> orderItems;
    private final long workMemBytes;
    private final List<String> columns;
    private final List<String> columnAliases;
    private final Comparator<Tuple> comparator;

    private final List<Tuple> inMemoryBuffer;
    private final List<Path> spilledRunFiles;
    private Iterator<Tuple> inMemoryIterator;
    private PriorityQueue<RunReader> mergeHeap;
    private long currentBufferBytes;

    public SortOperator(PhysicalOperator child, List<OrderItem> orderItems) {
        this(child, orderItems, DEFAULT_WORK_MEM_BYTES, null, null);
    }

    public SortOperator(PhysicalOperator child, List<OrderItem> orderItems, long workMemBytes) {
        this(child, orderItems, workMemBytes, null, null);
    }

    public SortOperator(PhysicalOperator child, List<OrderItem> orderItems, long workMemBytes,
                        List<String> columns, List<String> columnAliases) {
        this.child = child;
        this.orderItems = orderItems != null ? new ArrayList<>(orderItems) : List.of();
        this.workMemBytes = workMemBytes > 0 ? workMemBytes : DEFAULT_WORK_MEM_BYTES;
        this.columns = columns != null ? new ArrayList<>(columns) : List.of();
        this.columnAliases = columnAliases != null ? new ArrayList<>(columnAliases) : List.of();
        this.comparator = createComparator();
        this.inMemoryBuffer = new ArrayList<>();
        this.spilledRunFiles = new ArrayList<>();
        this.currentBufferBytes = 0;
    }

    private Comparator<Tuple> createComparator() {
        return (t1, t2) -> {
            if (orderItems.isEmpty()) {
                return 0;
            }
            for (OrderItem item : orderItems) {
                Object v1 = SortAndAggregateUtils.resolveColumnValue(t1, item.column(), columns, columnAliases);
                Object v2 = SortAndAggregateUtils.resolveColumnValue(t2, item.column(), columns, columnAliases);
                int cmp = SortAndAggregateUtils.compareValues(v1, v2, item.ascending());
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        };
    }

    @Override
    public void open() {
        child.open();
        inMemoryBuffer.clear();
        spilledRunFiles.clear();
        currentBufferBytes = 0;
        inMemoryIterator = null;
        mergeHeap = null;

        try {
            while (true) {
                TupleSlot slot = child.next();
                if (slot == null || slot.isEmpty()) {
                    break;
                }
                Tuple tuple = slot.getTuple();
                long tupleBytes = estimateTupleBytes(tuple);

                if (currentBufferBytes + tupleBytes > workMemBytes && !inMemoryBuffer.isEmpty()) {
                    spillRun();
                }

                inMemoryBuffer.add(tuple);
                currentBufferBytes += tupleBytes;
            }

            if (spilledRunFiles.isEmpty()) {
                // Entire dataset fits into memory - in-memory quicksort / timsort
                inMemoryBuffer.sort(comparator);
                inMemoryIterator = inMemoryBuffer.iterator();
            } else {
                // Disk-spilled runs present - spill remaining tuples then prepare K-way external merge sort
                if (!inMemoryBuffer.isEmpty()) {
                    spillRun();
                }
                mergeHeap = new PriorityQueue<>((r1, r2) -> comparator.compare(r1.currentTuple(), r2.currentTuple()));
                for (Path runPath : spilledRunFiles) {
                    RunReader reader = new RunReader(runPath, child.getOutputColumns());
                    if (reader.hasTuple()) {
                        mergeHeap.offer(reader);
                    } else {
                        reader.close();
                    }
                }
            }
        } catch (IOException e) {
            cleanupTempFiles();
            throw new UncheckedIOException("Failed during SortOperator run generation", e);
        }
    }

    private void spillRun() throws IOException {
        if (inMemoryBuffer.isEmpty()) {
            return;
        }
        inMemoryBuffer.sort(comparator);
        Path runFile = Files.createTempFile("stratos_sort_run_", ".bin");
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(runFile), 65536))) {
            out.writeInt(inMemoryBuffer.size());
            for (Tuple tuple : inMemoryBuffer) {
                byte[] bytes = tuple.serialize();
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            out.flush();
        }
        spilledRunFiles.add(runFile);
        inMemoryBuffer.clear();
        currentBufferBytes = 0;
    }

    private static long estimateTupleBytes(Tuple tuple) {
        if (tuple == null) return 0;
        long bytes = 64; // base object and list overhead
        for (Object val : tuple.getValues()) {
            if (val == null) {
                bytes += 8;
            } else if (val instanceof Integer || val instanceof Float) {
                bytes += 24;
            } else if (val instanceof Long || val instanceof Double) {
                bytes += 32;
            } else if (val instanceof Boolean || val instanceof Byte) {
                bytes += 16;
            } else if (val instanceof String s) {
                bytes += 48 + (long) s.length() * 2;
            } else if (val instanceof byte[] b) {
                bytes += 24 + b.length;
            } else {
                bytes += 64;
            }
        }
        return bytes;
    }

    @Override
    public TupleSlot next() {
        if (inMemoryIterator != null) {
            if (inMemoryIterator.hasNext()) {
                return TupleSlot.of(inMemoryIterator.next());
            }
            return TupleSlot.empty();
        }

        if (mergeHeap != null) {
            if (mergeHeap.isEmpty()) {
                return TupleSlot.empty();
            }
            RunReader minReader = mergeHeap.poll();
            Tuple result = minReader.currentTuple();
            minReader.advance();
            if (minReader.hasTuple()) {
                mergeHeap.offer(minReader);
            } else {
                minReader.close();
            }
            return TupleSlot.of(result);
        }

        return TupleSlot.empty();
    }

    @Override
    public void close() {
        try {
            child.close();
        } finally {
            cleanupTempFiles();
        }
    }

    private void cleanupTempFiles() {
        if (mergeHeap != null) {
            for (RunReader reader : mergeHeap) {
                reader.close();
            }
            mergeHeap.clear();
            mergeHeap = null;
        }
        for (Path runFile : spilledRunFiles) {
            try {
                Files.deleteIfExists(runFile);
            } catch (IOException ignored) {}
        }
        spilledRunFiles.clear();
        inMemoryBuffer.clear();
        inMemoryIterator = null;
        currentBufferBytes = 0;
    }

    @Override
    public List<String> getOutputColumns() {
        return child.getOutputColumns();
    }

    public int getSpilledRunCount() {
        return spilledRunFiles.size();
    }

    /**
     * Streams sorted tuples from a temporary spilled run file on disk.
     */
    private static class RunReader implements AutoCloseable {
        private final Path path;
        private final List<String> schema;
        private final DataInputStream in;
        private final int totalTuples;
        private int tuplesRead = 0;
        private Tuple currentTuple;
        private boolean closed = false;

        public RunReader(Path path, List<String> schema) throws IOException {
            this.path = path;
            this.schema = schema;
            this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 65536));
            this.totalTuples = in.readInt();
            advance();
        }

        public boolean hasTuple() {
            return currentTuple != null;
        }

        public Tuple currentTuple() {
            return currentTuple;
        }

        public void advance() {
            if (closed || tuplesRead >= totalTuples) {
                currentTuple = null;
                close();
                return;
            }
            try {
                int length = in.readInt();
                byte[] bytes = in.readNBytes(length);
                if (bytes.length < length) {
                    currentTuple = null;
                    close();
                    return;
                }
                tuplesRead++;
                currentTuple = Tuple.deserialize(bytes, schema);
            } catch (EOFException e) {
                currentTuple = null;
                close();
            } catch (IOException e) {
                close();
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                currentTuple = null;
                try {
                    in.close();
                } catch (IOException ignored) {}
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {}
            }
        }
    }
}
