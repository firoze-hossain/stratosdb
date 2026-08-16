package com.stratosdb.index.gin;

import com.stratosdb.storage.page.BTreePage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A real GIN (Generalized Inverted Index), scoped honestly to its most
 * common real-world application: full-text search over a VARCHAR/TEXT
 * column. Real Postgres's GIN is a genuinely general framework (also
 * used for indexing arrays, JSONB, and more, via pluggable "operator
 * classes") - building that full generality here, without StratosDB
 * having array/JSONB types to index in the first place, would produce
 * a framework with nothing real to plug into it. This is the concrete,
 * useful instantiation instead: an inverted index mapping each distinct
 * WORD to every row whose indexed column contains it, enabling
 * `WHERE column CONTAINS 'word'` to check a hash lookup rather than
 * scanning and substring-matching every row.
 *
 * Known, honestly-stated limitations: whole-word matching only (not
 * partial/prefix matches - "cat" won't match a row containing
 * "category"), tokenization is a simple lowercase-and-split-on-non-letters
 * (no stemming, no stop-word removal, no ranking by relevance - all real
 * further work matching what real Postgres's own full-text search
 * variants add on top of plain GIN).
 */
public class GinIndex {
    private final String name;
    private final Map<String, Set<BTreePage.RID>> wordToRids = new HashMap<>();

    public GinIndex(String name) {
        this.name = name;
    }

    /** Tokenizes a column's text value and records this row against every distinct word found in it - called once per row during index build, and once per new row on every subsequent INSERT. */
    public void insert(String text, BTreePage.RID rid) {
        if (text == null) {
            return;
        }
        for (String word : tokenize(text)) {
            wordToRids.computeIfAbsent(word, w -> new HashSet<>()).add(rid);
        }
    }

    /** All RIDs whose indexed text contains this exact word (case-insensitive, matching how tokenize() normalizes on insert). */
    public List<BTreePage.RID> search(String word) {
        Set<BTreePage.RID> rids = wordToRids.get(normalize(word));
        return rids == null ? new ArrayList<>() : new ArrayList<>(rids);
    }

    /** Splits on anything that isn't a letter or digit, lowercases, and drops empty tokens - simple but real word boundaries, not a naive whitespace-only split (so "cat," and "cat" both index as the same word "cat"). */
    public static Set<String> tokenize(String text) {
        Set<String> words = new HashSet<>();
        for (String token : text.toLowerCase().split("[^a-z0-9]+")) {
            if (!token.isEmpty()) {
                words.add(token);
            }
        }
        return words;
    }

    private static String normalize(String word) {
        return word.toLowerCase();
    }

    public String getName() { return name; }
    public int getDistinctWordCount() { return wordToRids.size(); }
}
