package com.stratosdb.sql.statistics;

import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.transaction.Transaction;
import com.stratosdb.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiFunction;

/**
 * Statistics Engine (ANALYZE 2.0).
 *
 * Performs a comprehensive analytical pass over a table:
 * 1. Computes total row count and disk page count.
 * 2. Computes per-column null fraction and average serialized byte width.
 * 3. Uses HyperLogLog (p=11, m=2048) for bounded-memory distinct value cardinality estimation.
 * 4. Extracts Most Common Values (MCV) lists with exact empirical frequencies for skewed keys.
 * 5. Builds Equi-Depth Histograms for non-MCV numeric and comparable distributions.
 * 6. Calculates physical row-to-value correlation.
 */
public class StatisticsEngine {
    private static final Logger LOG = LoggerFactory.getLogger(StatisticsEngine.class);
    public static final int MAX_MCV_LIST_SIZE = 50;
    public static final int MAX_HISTOGRAM_BUCKETS = 100;
    public static final double MCV_FREQUENCY_THRESHOLD = 0.005; // 0.5% of rows

    public static TableStatistics analyzeTable(
        String tableName,
        HeapTable table,
        List<String> columnNames,
        Transaction txn,
        TransactionManager transactionManager,
        BufferPool bufferPool,
        BiFunction<String, byte[], Tuple> tupleDeserializer,
        BiFunction<Tuple, String, Object> columnValueExtractor
    ) {
        if (table == null) {
            return null;
        }

        long pageCount = 1L;
        if (bufferPool != null) {
            long p = bufferPool.getTablePageCount(tableName);
            if (p > 0) {
                pageCount = p;
            }
        }

        List<String> cols = (columnNames != null) ? new ArrayList<>(columnNames) : List.of();

        // Initialize per-column statistical accumulators
        Map<String, Long> nullCounts = new HashMap<>();
        Map<String, Long> totalBytes = new HashMap<>();
        Map<String, HyperLogLog> hlls = new HashMap<>();
        Map<String, Map<Object, Long>> frequencies = new HashMap<>();
        Map<String, List<Double>> numericValues = new HashMap<>();
        Map<String, List<Integer>> rowIndices = new HashMap<>();
        Map<String, Double> minValues = new HashMap<>();
        Map<String, Double> maxValues = new HashMap<>();

        for (String col : cols) {
            nullCounts.put(col, 0L);
            totalBytes.put(col, 0L);
            hlls.put(col, new HyperLogLog());
            frequencies.put(col, new HashMap<>());
            numericValues.put(col, new ArrayList<>());
            rowIndices.put(col, new ArrayList<>());
        }

        long rowCount = 0L;
        var snapshot = (txn != null) ? txn.getSnapshot() : null;

        for (byte[] raw : table.scanMvcc(snapshot, transactionManager)) {
            Tuple tuple = (tupleDeserializer != null)
                ? tupleDeserializer.apply(tableName, raw)
                : Tuple.deserialize(raw, cols);
            rowCount++;

            for (String col : cols) {
                Object val = (columnValueExtractor != null)
                    ? columnValueExtractor.apply(tuple, col)
                    : tuple.getValue(col);

                if (val == null) {
                    nullCounts.merge(col, 1L, Long::sum);
                    continue;
                }

                // Width calculation
                int byteLen = computeByteWidth(val);
                totalBytes.merge(col, (long) byteLen, Long::sum);

                // HyperLogLog distinct estimation
                hlls.get(col).add(val);

                // Frequency tracking for MCV
                frequencies.get(col).merge(val, 1L, Long::sum);

                // Numeric statistics
                if (val instanceof Number n) {
                    double d = n.doubleValue();
                    numericValues.get(col).add(d);
                    rowIndices.get(col).add((int) rowCount);
                    minValues.merge(col, d, Math::min);
                    maxValues.merge(col, d, Math::max);
                }
            }
        }

        // Build per-column statistical profiles
        Map<String, ColumnStatistics> columnStatsMap = new HashMap<>();
        for (String col : cols) {
            long nulls = nullCounts.getOrDefault(col, 0L);
            double nullFrac = (rowCount > 0) ? (double) nulls / rowCount : 0.0;
            long nonNulls = rowCount - nulls;
            int avgWidth = (nonNulls > 0) ? (int) Math.round((double) totalBytes.getOrDefault(col, 0L) / nonNulls) : 0;

            Map<Object, Long> freqMap = frequencies.get(col);
            double distinctCount;
            if (freqMap.size() <= 2000) {
                distinctCount = freqMap.size();
            } else {
                distinctCount = Math.max(1, hlls.get(col).estimate());
            }

            // 1. Build MCV list
            List<Map.Entry<Object, Long>> sortedFreq = new ArrayList<>(freqMap.entrySet());
            sortedFreq.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));

            List<Object> mcvVals = new ArrayList<>();
            List<Double> mcvFreqs = new ArrayList<>();
            Set<Object> mcvSet = new HashSet<>();

            int maxMcv = Math.min(MAX_MCV_LIST_SIZE, sortedFreq.size());
            for (int i = 0; i < maxMcv; i++) {
                Map.Entry<Object, Long> entry = sortedFreq.get(i);
                double freq = (rowCount > 0) ? (double) entry.getValue() / rowCount : 0.0;
                if (sortedFreq.size() <= MAX_MCV_LIST_SIZE || (entry.getValue() >= 2 && freq >= MCV_FREQUENCY_THRESHOLD)) {
                    mcvVals.add(entry.getKey());
                    mcvFreqs.add(freq);
                    mcvSet.add(entry.getKey());
                } else {
                    break;
                }
            }
            MostCommonValues mcv = new MostCommonValues(mcvVals, mcvFreqs);

            // 2. Build Equi-Depth Histogram for non-MCV numeric values
            List<Double> nonMcvNumerics = new ArrayList<>();
            for (Double d : numericValues.get(col)) {
                // Check if this numeric value is in the MCV list
                boolean isMcv = false;
                for (Object mcvVal : mcvSet) {
                    if (mcvVal instanceof Number n && Math.abs(n.doubleValue() - d) < 1e-9) {
                        isMcv = true;
                        break;
                    }
                }
                if (!isMcv) {
                    nonMcvNumerics.add(d);
                }
            }
            Collections.sort(nonMcvNumerics);
            EquiDepthHistogram histogram = EquiDepthHistogram.build(nonMcvNumerics, MAX_HISTOGRAM_BUCKETS);

            // 3. Compute physical correlation
            double correlation = computePearsonCorrelation(rowIndices.get(col), numericValues.get(col));

            Double min = minValues.get(col);
            Double max = maxValues.get(col);

            ColumnStatistics colStats = new ColumnStatistics(
                tableName,
                col,
                rowCount,
                pageCount,
                nullFrac,
                avgWidth,
                distinctCount,
                mcv,
                histogram,
                min,
                max,
                correlation
            );
            columnStatsMap.put(col, colStats);
        }

        LOG.debug("Analyzed table '{}': {} row(s), {} page(s), {} column(s)", tableName, rowCount, pageCount, cols.size());
        return new TableStatistics(tableName, rowCount, pageCount, columnStatsMap);
    }

    private static int computeByteWidth(Object val) {
        if (val == null) return 0;
        if (val instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8).length;
        } else if (val instanceof Integer) {
            return 4;
        } else if (val instanceof Long || val instanceof Double) {
            return 8;
        } else if (val instanceof Boolean) {
            return 1;
        } else if (val instanceof byte[] b) {
            return b.length;
        }
        return val.toString().getBytes(StandardCharsets.UTF_8).length;
    }

    private static double computePearsonCorrelation(List<Integer> xs, List<Double> ys) {
        if (xs == null || ys == null || xs.size() < 2 || xs.size() != ys.size()) {
            return 0.0;
        }
        int n = xs.size();
        double sumX = 0.0, sumY = 0.0;
        for (int i = 0; i < n; i++) {
            sumX += xs.get(i);
            sumY += ys.get(i);
        }
        double meanX = sumX / n;
        double meanY = sumY / n;

        double num = 0.0;
        double denX = 0.0;
        double denY = 0.0;
        for (int i = 0; i < n; i++) {
            double dx = xs.get(i) - meanX;
            double dy = ys.get(i) - meanY;
            num += dx * dy;
            denX += dx * dx;
            denY += dy * dy;
        }
        double den = Math.sqrt(denX * denY);
        return (den > 0.0) ? Math.max(-1.0, Math.min(1.0, num / den)) : 0.0;
    }
}
