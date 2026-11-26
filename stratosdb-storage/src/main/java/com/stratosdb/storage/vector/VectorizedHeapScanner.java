package com.stratosdb.storage.vector;

import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.SlottedPage;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.transaction.TransactionManager;
import com.stratosdb.transaction.mvcc.MVCCVisibility;
import com.stratosdb.transaction.mvcc.Snapshot;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * Zero-allocation vectorized sequential scanner over a {@link HeapTable}.
 *
 * Reads tuple headers, MVCC visibility metadata (xmin/xmax), null bitmaps,
 * and primitive column values directly from off-heap {@link SlottedPage}
 * {@link MemorySegment}s into a reusable {@link VectorBatch} without allocating
 * any Java heap objects (no Tuple, no byte[], no ArrayList, no boxed primitives).
 */
public final class VectorizedHeapScanner implements AutoCloseable {
    private static final ValueLayout.OfShort BE_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfInt BE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfLong BE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfDouble BE_DOUBLE = ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    private static final int PAGE_HEADER_SIZE = 24;
    private static final int ITEM_POINTER_SIZE = 6;
    private static final int PD_LOWER_OFFSET = 12;

    private static final byte TAG_INT = 1;
    private static final byte TAG_LONG = 3;
    private static final byte TAG_BOOLEAN = 4;
    private static final byte TAG_DOUBLE = 5;

    private final HeapTable table;
    private final String tableName;
    private final BufferPool bufferPool;
    private final Snapshot snapshot;
    private final TransactionManager txnManager;
    private final int[] projectedColumnIndices;

    private long maxPageId;
    private long nextPageId;
    private long activePageId = -1;
    private SlottedPage currentPage;
    private MemorySegment currentPageSegment;
    private int currentItemOffset;
    private int currentPdLower;
    private boolean closed;

    public VectorizedHeapScanner(HeapTable table,
                                 Snapshot snapshot,
                                 TransactionManager txnManager,
                                 int[] projectedColumnIndices) {
        this.table = table;
        this.tableName = table.getName();
        this.bufferPool = table.getBufferPool();
        this.snapshot = snapshot;
        this.txnManager = txnManager;
        this.projectedColumnIndices = projectedColumnIndices.clone();
        reset();
    }

    /**
     * Resets the scan cursor back to page 0 without allocating any objects.
     */
    public void reset() {
        releaseCurrentPage();
        this.maxPageId = table.getLastPageId();
        this.nextPageId = 0L;
        this.currentItemOffset = PAGE_HEADER_SIZE;
        this.currentPdLower = PAGE_HEADER_SIZE;
        this.closed = false;
    }

    /**
     * Fills the provided {@link VectorBatch} with up to {@code batch.getCapacity()} visible rows
     * directly from off-heap slotted pages with zero heap allocations.
     *
     * @return {@code true} if at least 1 row was populated; {@code false} at EOF.
     */
    public boolean nextBatch(VectorBatch batch) {
        if (closed) {
            return false;
        }
        batch.reset();
        final int capacity = batch.getCapacity();
        final int projCount = projectedColumnIndices.length;
        int rowIdx = 0;

        while (rowIdx < capacity) {
            if (currentPageSegment == null || currentItemOffset >= currentPdLower) {
                releaseCurrentPage();
                if (nextPageId > maxPageId) {
                    break;
                }
                activePageId = nextPageId++;
                currentPage = (SlottedPage) bufferPool.getPage(tableName, activePageId);
                currentPage.getLatch().readLock().lock();
                currentPageSegment = currentPage.getMemorySegment();
                currentPdLower = Short.toUnsignedInt(currentPageSegment.get(BE_SHORT, PD_LOWER_OFFSET));
                currentItemOffset = PAGE_HEADER_SIZE;
                continue;
            }

            final MemorySegment pageSeg = currentPageSegment;
            while (currentItemOffset < currentPdLower && rowIdx < capacity) {
                final int itemOff = currentItemOffset;
                currentItemOffset += ITEM_POINTER_SIZE;

                final short tupleLen = pageSeg.get(BE_SHORT, itemOff + 4);
                if (tupleLen <= 0) {
                    continue;
                }

                final int tupleOffset = pageSeg.get(BE_INT, itemOff);
                int payloadOffset = -1;

                if (tupleLen >= 6 && pageSeg.get(BE_SHORT, tupleOffset) == Tuple.TUPLE_MAGIC) {
                    payloadOffset = tupleOffset;
                } else if (tupleLen >= 22 && pageSeg.get(BE_SHORT, tupleOffset + 16) == Tuple.TUPLE_MAGIC) {
                    final long xmin = pageSeg.get(BE_LONG, tupleOffset);
                    final long xmax = pageSeg.get(BE_LONG, tupleOffset + 8);
                    if (snapshot != null && txnManager != null
                            && !MVCCVisibility.isVisible(xmin, xmax, snapshot, txnManager)) {
                        continue;
                    }
                    payloadOffset = tupleOffset + 16;
                } else {
                    continue;
                }

                final int numCols = Short.toUnsignedInt(pageSeg.get(BE_SHORT, payloadOffset + 4));
                final int bitmapOffset = payloadOffset + 6;
                final int bitmapBytes = (numCols + 7) >>> 3;
                final int offsetTableStart = bitmapOffset + bitmapBytes;
                final int valuesStart = offsetTableStart + ((numCols + 1) << 1);

                for (int c = 0; c < projCount; c++) {
                    final int colIdx = projectedColumnIndices[c];
                    final ColumnVector colVec = batch.getColumn(c);

                    if (colIdx < 0 || colIdx >= numCols) {
                        colVec.setNull(rowIdx);
                        continue;
                    }

                    final byte bitmapByte = pageSeg.get(ValueLayout.JAVA_BYTE, bitmapOffset + (colIdx >>> 3));
                    if ((bitmapByte & (1 << (colIdx & 7))) == 0) {
                        colVec.setNull(rowIdx);
                        continue;
                    }

                    final int relOff = Short.toUnsignedInt(pageSeg.get(BE_SHORT, offsetTableStart + (colIdx << 1)));
                    final int nextRelOff = Short.toUnsignedInt(pageSeg.get(BE_SHORT, offsetTableStart + ((colIdx + 1) << 1)));
                    if (nextRelOff <= relOff) {
                        colVec.setNull(rowIdx);
                        continue;
                    }

                    final int valPos = valuesStart + relOff;
                    final byte tag = pageSeg.get(ValueLayout.JAVA_BYTE, valPos);

                    switch (colVec.getType()) {
                        case INT -> {
                            final IntVector iv = (IntVector) colVec;
                            if (tag == TAG_INT) {
                                iv.set(rowIdx, pageSeg.get(BE_INT, valPos + 1));
                            } else if (tag == TAG_LONG) {
                                iv.set(rowIdx, (int) pageSeg.get(BE_LONG, valPos + 1));
                            } else {
                                iv.setNull(rowIdx);
                            }
                        }
                        case LONG -> {
                            final LongVector lv = (LongVector) colVec;
                            if (tag == TAG_LONG) {
                                lv.set(rowIdx, pageSeg.get(BE_LONG, valPos + 1));
                            } else if (tag == TAG_INT) {
                                lv.set(rowIdx, pageSeg.get(BE_INT, valPos + 1));
                            } else {
                                lv.setNull(rowIdx);
                            }
                        }
                        case DOUBLE -> {
                            final DoubleVector dv = (DoubleVector) colVec;
                            if (tag == TAG_DOUBLE) {
                                dv.set(rowIdx, pageSeg.get(BE_DOUBLE, valPos + 1));
                            } else if (tag == TAG_INT) {
                                dv.set(rowIdx, pageSeg.get(BE_INT, valPos + 1));
                            } else if (tag == TAG_LONG) {
                                dv.set(rowIdx, (double) pageSeg.get(BE_LONG, valPos + 1));
                            } else {
                                dv.setNull(rowIdx);
                            }
                        }
                        case BYTE -> {
                            final ByteVector bv = (ByteVector) colVec;
                            if (tag == TAG_BOOLEAN) {
                                bv.set(rowIdx, pageSeg.get(ValueLayout.JAVA_BYTE, valPos + 1));
                            } else if (tag == TAG_INT) {
                                bv.set(rowIdx, (byte) pageSeg.get(BE_INT, valPos + 1));
                            } else {
                                bv.setNull(rowIdx);
                            }
                        }
                    }
                }
                rowIdx++;
            }
        }

        if (rowIdx == 0) {
            return false;
        }
        batch.setNumRows(rowIdx);
        return true;
    }

    private void releaseCurrentPage() {
        if (currentPage != null) {
            try {
                currentPage.getLatch().readLock().unlock();
                bufferPool.unpinPage(tableName, activePageId);
            } catch (Exception ignored) {
            }
            currentPage = null;
            currentPageSegment = null;
            activePageId = -1;
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            releaseCurrentPage();
        }
    }
}
