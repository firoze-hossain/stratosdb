package com.stratosdb.storage.page;

import java.util.Objects;

/**
 * The real, parsed representation of an INT4RANGE or DATERANGE literal
 * (real Postgres's own range-type family - this engine supports a real,
 * deliberately small subset, not the full family of int8range/numrange/
 * tsrange/tstzrange). Real Postgres's own real range-literal syntax:
 * '[lower,upper)' - '[' or '(' for the lower bound (inclusive/exclusive),
 * ']' or ')' for the upper bound, a real, literal comma separating the
 * two, and either side may be empty (an unbounded lower/upper end). This
 * class stores whichever concrete values it was actually parsed from
 * (Integer for INT4RANGE, java.sql.Date for DATERANGE - see
 * ExecutorEngine.coerceForColumnType) - it is not itself type-parameterized,
 * matching how this engine's own JSON/JSONB values are also stored as a
 * plain, untyped Object rather than a generic container.
 */
public final class RangeValue {
    private final Object lower;
    private final Object upper;
    private final boolean lowerInclusive;
    private final boolean upperInclusive;

    public RangeValue(Object lower, Object upper, boolean lowerInclusive, boolean upperInclusive) {
        this.lower = lower;
        this.upper = upper;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
    }

    public Object lower() { return lower; }
    public Object upper() { return upper; }
    public boolean lowerInclusive() { return lowerInclusive; }
    public boolean upperInclusive() { return upperInclusive; }

    /** Real Postgres's own real range-display format - '[' / '(' then the lower bound (or nothing, for unbounded), a real comma, the upper bound (or nothing), then ']' / ')'. */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(lowerInclusive ? '[' : '(');
        if (lower != null) sb.append(lower);
        sb.append(',');
        if (upper != null) sb.append(upper);
        sb.append(upperInclusive ? ']' : ')');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RangeValue other)) return false;
        return lowerInclusive == other.lowerInclusive && upperInclusive == other.upperInclusive
            && Objects.equals(lower, other.lower) && Objects.equals(upper, other.upper);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lower, upper, lowerInclusive, upperInclusive);
    }

    /**
     * Parses a real Postgres-style range literal: '[' or '(' (lower bound
     * inclusive/exclusive), an optional lower bound, a real, literal comma,
     * an optional upper bound, then ']' or ')' (upper bound inclusive/
     * exclusive). Either bound may be empty text, meaning a real,
     * genuinely unbounded end on that side (stored as a real Java null,
     * not a sentinel value) - matching real Postgres's own real "no lower
     * bound"/"no upper bound" semantics for an open-ended range.
     */
    public static RangeValue parse(String text, boolean isDateRange) {
        String trimmed = text.trim();
        if (trimmed.length() < 3 || (trimmed.charAt(0) != '[' && trimmed.charAt(0) != '(')) {
            throw new IllegalArgumentException("invalid range literal (must start with '[' or '('): " + text);
        }
        char lastChar = trimmed.charAt(trimmed.length() - 1);
        if (lastChar != ']' && lastChar != ')') {
            throw new IllegalArgumentException("invalid range literal (must end with ']' or ')'): " + text);
        }
        boolean lowerInclusive = trimmed.charAt(0) == '[';
        boolean upperInclusive = lastChar == ']';
        String inner = trimmed.substring(1, trimmed.length() - 1);
        int commaIndex = inner.indexOf(',');
        if (commaIndex < 0) {
            throw new IllegalArgumentException("invalid range literal (missing ','): " + text);
        }
        String lowerText = inner.substring(0, commaIndex).trim();
        String upperText = inner.substring(commaIndex + 1).trim();
        Object lower = lowerText.isEmpty() ? null : parseBound(lowerText, isDateRange);
        Object upper = upperText.isEmpty() ? null : parseBound(upperText, isDateRange);
        return new RangeValue(lower, upper, lowerInclusive, upperInclusive);
    }

    private static Object parseBound(String text, boolean isDateRange) {
        if (isDateRange) {
            // This engine's own established convention (see coerceForColumnType) is
            // that a DATE value is stored as a plain, validated String, not a
            // separate java.sql.Date object - matched here for the same real
            // reason: Tuple's own real, tagged serialization format only knows
            // how to encode a small, fixed set of concrete Java types (see
            // Tuple.writeValue), and introducing a new one there is real,
            // separate, further work this round doesn't need to take on when a
            // plain, validated String already fits perfectly.
            try {
                java.sql.Date.valueOf(text); // real, format-only validation - the parsed result itself is discarded
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("invalid date in range bound (expected YYYY-MM-DD): " + text);
            }
            return text;
        }
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid integer in range bound: " + text);
        }
    }
}
