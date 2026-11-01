package com.stratosdb.sql.statistics;

import com.stratosdb.sql.executor.JsonParser;

import java.util.*;

/**
 * Encapsulates the Most Common Values (MCV) list and corresponding empirical frequencies
 * for a table column.
 *
 * In highly skewed datasets, MCV lists allow the query optimizer to assign exact empirical
 * probabilities to frequent keys rather than suffering from the gross misestimates of
 * the uniform distribution assumption.
 */
public class MostCommonValues {
    private final List<Object> values;
    private final List<Double> frequencies;
    private final double sumFrequency;

    public MostCommonValues(List<Object> values, List<Double> frequencies) {
        if (values == null || frequencies == null || values.size() != frequencies.size()) {
            throw new IllegalArgumentException("Values and frequencies must be non-null and equal length");
        }
        this.values = Collections.unmodifiableList(new ArrayList<>(values));
        this.frequencies = Collections.unmodifiableList(new ArrayList<>(frequencies));
        double sum = 0.0;
        for (Double f : frequencies) {
            sum += (f != null ? f : 0.0);
        }
        this.sumFrequency = Math.min(1.0, sum);
    }

    public static MostCommonValues empty() {
        return new MostCommonValues(List.of(), List.of());
    }

    public int size() {
        return values.size();
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public List<Object> getValues() {
        return values;
    }

    public List<Double> getFrequencies() {
        return frequencies;
    }

    public double getSumFrequency() {
        return sumFrequency;
    }

    public boolean contains(Object value) {
        return findIndex(value) >= 0;
    }

    public double getFrequency(Object value) {
        int idx = findIndex(value);
        if (idx >= 0) {
            return frequencies.get(idx);
        }
        return 0.0;
    }

    public double rangeFrequency(Double lo, Double hi) {
        if (values.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (int i = 0; i < values.size(); i++) {
            Double val = toDouble(values.get(i));
            if (val != null) {
                boolean satisfiesLo = (lo == null || val >= lo);
                boolean satisfiesHi = (hi == null || val <= hi);
                if (satisfiesLo && satisfiesHi) {
                    sum += frequencies.get(i);
                }
            }
        }
        return sum;
    }

    public String toJsonValues() {
        return JsonParser.toJsonText(values);
    }

    public String toJsonFrequencies() {
        return JsonParser.toJsonText(frequencies);
    }

    @SuppressWarnings("unchecked")
    public static MostCommonValues fromJson(String valuesJson, String freqsJson) {
        if (valuesJson == null || freqsJson == null || valuesJson.isBlank() || freqsJson.isBlank()) {
            return empty();
        }
        try {
            Object parsedVals = JsonParser.parse(valuesJson);
            Object parsedFreqs = JsonParser.parse(freqsJson);
            if (parsedVals instanceof List<?> valList && parsedFreqs instanceof List<?> freqList) {
                List<Object> vals = new ArrayList<>(valList);
                List<Double> freqs = new ArrayList<>();
                for (Object f : freqList) {
                    if (f instanceof Number n) {
                        freqs.add(n.doubleValue());
                    } else {
                        freqs.add(0.0);
                    }
                }
                if (vals.size() == freqs.size()) {
                    return new MostCommonValues(vals, freqs);
                }
            }
        } catch (Exception ignored) {}
        return empty();
    }

    private int findIndex(Object target) {
        if (target == null) return -1;
        Double targetDouble = toDouble(target);
        String targetStr = target.toString();

        for (int i = 0; i < values.size(); i++) {
            Object v = values.get(i);
            if (Objects.equals(v, target)) {
                return i;
            }
            if (targetDouble != null) {
                Double vDouble = toDouble(v);
                if (vDouble != null && Math.abs(targetDouble - vDouble) < 1e-9) {
                    return i;
                }
            }
            if (v != null && targetStr.equalsIgnoreCase(v.toString())) {
                return i;
            }
        }
        return -1;
    }

    private static Double toDouble(Object val) {
        if (val == null) return null;
        if (val instanceof Number n) {
            return n.doubleValue();
        }
        try {
            return Double.parseDouble(val.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "MCV{values=" + values + ", frequencies=" + frequencies + "}";
    }
}
