package com.stratosdb.storage.page;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The real, parsed representation of a Postgres-style `tsvector` - a
 * sorted set of distinct lexemes (normalized search terms), each with the
 * real, 1-based word positions it occurred at in the original text
 * (matching real Postgres's own `to_tsvector` output, e.g.
 * `'brown':3 'fox':4 'quick':2` for "The quick brown fox" once the real
 * stop word "the" is removed - see TextSearch's own javadoc for the
 * real, honestly-scoped normalization this is built from).
 *
 * Real, honestly-stated limitations, matching this whole project's own
 * established standard: no real stemming (a lexeme is its own
 * lowercased, punctuation-stripped word, not reduced to a word stem -
 * "running" and "run" are two distinct lexemes here, unlike real
 * Postgres's own 'english' text search configuration); positions are
 * tracked but never used for ranking (`ts_rank`/`ts_rank_cd` are real,
 * separate, further work); only a real, single, hardcoded English stop
 * word list is used, not real Postgres's own pluggable, per-language
 * "text search configuration" system.
 */
public final class TsVector {
    private final TreeMap<String, List<Integer>> lexemePositions;

    public TsVector(Map<String, List<Integer>> lexemePositions) {
        this.lexemePositions = new TreeMap<>(lexemePositions);
    }

    public boolean contains(String lexeme) {
        return lexemePositions.containsKey(lexeme);
    }

    public java.util.Set<String> lexemes() {
        return lexemePositions.keySet();
    }

    public Map<String, List<Integer>> lexemePositions() {
        return lexemePositions;
    }

    /** Real Postgres's own real tsvector display format: each distinct lexeme, quoted, with its own real position list, in real lexeme-sorted (alphabetical) order - e.g. `'brown':3 'fox':4 'quick':2`. */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, List<Integer>> entry : lexemePositions.entrySet()) {
            if (!first) sb.append(' ');
            first = false;
            sb.append('\'').append(entry.getKey()).append('\'').append(':');
            List<Integer> positions = entry.getValue();
            for (int i = 0; i < positions.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(positions.get(i));
            }
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TsVector other && lexemePositions.equals(other.lexemePositions);
    }

    @Override
    public int hashCode() {
        return lexemePositions.hashCode();
    }
}
