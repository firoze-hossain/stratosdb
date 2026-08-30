package com.stratosdb.sql.executor;

import com.stratosdb.sql.parser.StratosSQLLexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;

/**
 * The real mechanism behind pg_stat_statements-style aggregation: two
 * queries differing only in their own literal values -
 * "SELECT * FROM t WHERE id = 1" and "SELECT * FROM t WHERE id = 2" -
 * should count as the SAME statement for statistics purposes, not two
 * separate ones (a real application typically runs the same shaped
 * query many times with different parameter values, and an operator
 * cares about that shape's own aggregate behavior, not each individual
 * literal's own one-off timing).
 *
 * Uses this engine's own real ANTLR4 lexer to tokenize the SQL - not a
 * fragile, ad hoc regex - so normalization is exactly as correct as
 * this SQL dialect's own real tokenization already is (a literal
 * embedded inside a quoted string, for instance, is never mistaken for
 * a real token boundary, since the lexer's own STRING_LITERAL rule
 * already handles that correctly). Every INTEGER_LITERAL, FLOAT_LITERAL,
 * STRING_LITERAL, TRUE, and FALSE token is replaced with a single `?`
 * placeholder; every other token's own exact text is preserved,
 * joined with single spaces - not a reproduction of the original
 * query's own exact whitespace/formatting, since the whole point here
 * is aggregation by shape, not verbatim storage.
 */
public class QueryNormalizer {

    public static String normalize(String sql) {
        try {
            CharStream charStream = CharStreams.fromString(sql);
            StratosSQLLexer lexer = new StratosSQLLexer(charStream);
            lexer.removeErrorListeners(); // a malformed query is handled by the real parser elsewhere - this method never throws, falling back to the raw SQL below instead
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            tokenStream.fill();

            StringBuilder normalized = new StringBuilder();
            boolean first = true;
            for (Token token : tokenStream.getTokens()) {
                if (token.getType() == Token.EOF) {
                    continue;
                }
                if (!first) {
                    normalized.append(' ');
                }
                first = false;
                normalized.append(isLiteralToken(token.getType()) ? "?" : token.getText());
            }
            return normalized.toString();
        } catch (Exception e) {
            // A real, deliberate fallback, not a crash: statistics are a real, but
            // secondary, observability concern - a query that somehow fails to even
            // tokenize here should still execute normally and still be recorded
            // (under its own raw text, ungrouped) rather than take down the query
            // itself over a normalization failure.
            return sql;
        }
    }

    private static boolean isLiteralToken(int tokenType) {
        return tokenType == StratosSQLLexer.INTEGER_LITERAL
            || tokenType == StratosSQLLexer.FLOAT_LITERAL
            || tokenType == StratosSQLLexer.STRING_LITERAL
            || tokenType == StratosSQLLexer.TRUE
            || tokenType == StratosSQLLexer.FALSE;
    }
}
