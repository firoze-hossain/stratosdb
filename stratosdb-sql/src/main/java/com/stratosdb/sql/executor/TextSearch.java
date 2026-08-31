package com.stratosdb.sql.executor;

import com.stratosdb.storage.page.TsQuery;
import com.stratosdb.storage.page.TsQueryExpr;
import com.stratosdb.storage.page.TsVector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Real Postgres-style full-text search, closing this project's own
 * honestly-named "GIN indexing on arrays/JSON exists, but not Postgres's
 * own text-search machinery" gap. Provides the real, shared tokenization
 * to_tsvector() is built from, and a real, small hand-written parser for
 * a tsquery's own boolean expression syntax (`&`/`|`/`!`/parentheses).
 *
 * Real, honestly-stated scope: a single, hardcoded English stop word
 * list (not real Postgres's own pluggable, per-language "text search
 * configuration" system); no real stemming (see TsVector's own javadoc);
 * a phrase/proximity query (`<->`) is not supported, only plain
 * AND/OR/NOT combinations of lexemes.
 */
public final class TextSearch {

    /** A real, small, standard English stop word list - words carrying essentially no search-distinguishing value, dropped from a tsvector the same way real Postgres's own 'english' configuration does. Not exhaustive (real Postgres's own list has hundreds of entries); large enough to be genuinely useful for ordinary English prose. */
    private static final Set<String> STOP_WORDS = Set.of(
        "a", "an", "the", "and", "or", "but", "is", "are", "was", "were", "be", "been", "being",
        "in", "on", "at", "to", "for", "of", "with", "by", "from", "as", "it", "its", "this",
        "that", "these", "those", "i", "you", "he", "she", "we", "they", "them", "his", "her",
        "not", "no", "do", "does", "did", "have", "has", "had", "will", "would", "can", "could",
        "shall", "should", "may", "might", "must", "if", "so", "than", "too", "very", "s", "t",
        "over", "under", "into", "onto", "up", "down", "out", "about", "again", "further",
        "then", "once", "here", "there", "when", "where", "why", "how", "all", "each", "both",
        "such", "own", "same", "only", "just", "also", "any"
    );

    private TextSearch() {}

    /**
     * Tokenizes real text into a real TsVector: lowercase, split on any
     * non-alphanumeric run (the same real word-boundary rule GinIndex's
     * own tokenize() already uses, for consistency), a real stop word
     * dropped entirely (never recorded, not even with a position),
     * everything else recorded as a real lexeme with its own real,
     * 1-based position(s) within the original text - a lexeme repeated
     * later in the text accumulates every real position it occurred at,
     * not just its first.
     */
    public static TsVector toTsVector(String text) {
        Map<String, List<Integer>> positions = new LinkedHashMap<>();
        if (text == null) {
            return new TsVector(positions);
        }
        String[] rawTokens = text.toLowerCase().split("[^a-z0-9]+");
        int position = 0;
        for (String token : rawTokens) {
            if (token.isEmpty()) {
                continue;
            }
            position++;
            if (STOP_WORDS.contains(token)) {
                continue;
            }
            positions.computeIfAbsent(token, k -> new ArrayList<>()).add(position);
        }
        return new TsVector(positions);
    }

    /**
     * Parses a real tsquery boolean expression string (e.g. "quick & fox",
     * "quick | brown", "!stop", "(a | b) & c") into a real TsQuery. A bare
     * lexeme is normalized (lowercased) the same way toTsVector's own
     * lexemes are, so a query built from ordinary text always matches a
     * vector built from ordinary text, without either side needing to
     * pre-normalize itself.
     */
    public static TsQuery toTsQuery(String queryText) {
        List<String> tokens = tokenizeQuery(queryText);
        Parser parser = new Parser(tokens);
        TsQueryExpr root = parser.parseOr();
        if (parser.pos < tokens.size()) {
            throw new IllegalArgumentException("unexpected token in tsquery: \"" + tokens.get(parser.pos) + "\"");
        }
        return new TsQuery(root);
    }

    /** Splits a real tsquery string into its own real tokens: `&`, `|`, `!`, `(`, `)`, and a bare lexeme run (letters/digits) - whitespace between tokens is insignificant, matching real Postgres's own real tsquery input syntax. */
    private static List<String> tokenizeQuery(String queryText) {
        List<String> tokens = new ArrayList<>();
        int i = 0;
        while (i < queryText.length()) {
            char c = queryText.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '&' || c == '|' || c == '!' || c == '(' || c == ')') {
                tokens.add(String.valueOf(c));
                i++;
            } else {
                int start = i;
                while (i < queryText.length() && !Character.isWhitespace(queryText.charAt(i))
                    && "&|!()".indexOf(queryText.charAt(i)) < 0) {
                    i++;
                }
                tokens.add(queryText.substring(start, i).toLowerCase());
            }
        }
        return tokens;
    }

    /**
     * A real, small, hand-written recursive-descent parser over the
     * tokens above - real, standard boolean precedence, tightest to
     * loosest: NOT, then AND, then OR (matching TsQueryExpr's own
     * render() and real Postgres's own real tsquery precedence).
     */
    private static final class Parser {
        private final List<String> tokens;
        private int pos = 0;

        Parser(List<String> tokens) {
            this.tokens = tokens;
        }

        TsQueryExpr parseOr() {
            TsQueryExpr left = parseAnd();
            while (pos < tokens.size() && tokens.get(pos).equals("|")) {
                pos++;
                left = new TsQueryExpr.Or(left, parseAnd());
            }
            return left;
        }

        TsQueryExpr parseAnd() {
            TsQueryExpr left = parseNot();
            while (pos < tokens.size() && tokens.get(pos).equals("&")) {
                pos++;
                left = new TsQueryExpr.And(left, parseNot());
            }
            return left;
        }

        TsQueryExpr parseNot() {
            if (pos < tokens.size() && tokens.get(pos).equals("!")) {
                pos++;
                return new TsQueryExpr.Not(parseNot());
            }
            return parsePrimary();
        }

        TsQueryExpr parsePrimary() {
            if (pos >= tokens.size()) {
                throw new IllegalArgumentException("unexpected end of tsquery");
            }
            String token = tokens.get(pos);
            if (token.equals("(")) {
                pos++;
                TsQueryExpr inner = parseOr();
                if (pos >= tokens.size() || !tokens.get(pos).equals(")")) {
                    throw new IllegalArgumentException("missing closing ')' in tsquery");
                }
                pos++;
                return inner;
            }
            if (token.equals("&") || token.equals("|") || token.equals(")")) {
                throw new IllegalArgumentException("unexpected '" + token + "' in tsquery");
            }
            pos++;
            return new TsQueryExpr.Lexeme(token);
        }
    }
}
