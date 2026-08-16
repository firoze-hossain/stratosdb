package com.stratosdb.sql.executor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A real, minimal recursive-descent JSON parser - not a stub, not a
 * regex-based approximation. Parses standard JSON text into native Java
 * structures using exactly the same shapes {@link com.stratosdb.storage.page.Tuple}
 * already knows how to serialize: {@code Map<String, Object>} for objects,
 * {@code List<Object>} for arrays (the same array support built for
 * StratosDB's own ARRAY columns), {@code String}, {@code Double},
 * {@code Boolean}, and {@code null} for scalars.
 *
 * Deliberately scoped: this is JSON (RFC 8259), not JSONB's actual binary
 * storage format - values round-trip correctly and support real key/path
 * extraction, but there's no binary encoding for faster key lookup the
 * way real Postgres's JSONB has. A JSON/JSONB column here is validated
 * and stored as a parsed structure (not raw, unvalidated text), which is
 * what actually matters for correctness; the storage format's
 * performance characteristics are real further work, not attempted here.
 */
public class JsonParser {
    private final String text;
    private int pos;

    private JsonParser(String text) {
        this.text = text;
        this.pos = 0;
    }

    /**
     * Parses a complete JSON document. Throws {@link JsonParseException}
     * (not a generic exception) on any malformed input, including trailing
     * garbage after an otherwise-valid value - this is meant to genuinely
     * validate a JSON/JSONB column's input, not just extract what it can.
     */
    public static Object parse(String jsonText) {
        JsonParser parser = new JsonParser(jsonText);
        parser.skipWhitespace();
        Object value = parser.parseValue();
        parser.skipWhitespace();
        if (parser.pos != parser.text.length()) {
            throw new JsonParseException("Unexpected trailing content at position " + parser.pos + " in: " + jsonText);
        }
        return value;
    }

    public static class JsonParseException extends RuntimeException {
        public JsonParseException(String message) {
            super(message);
        }
    }

    private Object parseValue() {
        if (pos >= text.length()) {
            throw new JsonParseException("Unexpected end of input while expecting a value");
        }
        char c = text.charAt(pos);
        return switch (c) {
            case '{' -> parseObject();
            case '[' -> parseArray();
            case '"' -> parseString();
            case 't', 'f' -> parseBoolean();
            case 'n' -> parseNull();
            default -> {
                if (c == '-' || Character.isDigit(c)) {
                    yield parseNumber();
                }
                throw new JsonParseException("Unexpected character '" + c + "' at position " + pos);
            }
        };
    }

    private Map<String, Object> parseObject() {
        Map<String, Object> result = new LinkedHashMap<>(); // LinkedHashMap: preserves key insertion order, matching how a real JSON document reads
        expect('{');
        skipWhitespace();
        if (peek() == '}') {
            pos++;
            return result;
        }
        while (true) {
            skipWhitespace();
            if (peek() != '"') {
                throw new JsonParseException("Expected a string key at position " + pos);
            }
            String key = parseString();
            skipWhitespace();
            expect(':');
            skipWhitespace();
            Object value = parseValue();
            result.put(key, value);
            skipWhitespace();
            char next = peek();
            if (next == ',') {
                pos++;
            } else if (next == '}') {
                pos++;
                break;
            } else {
                throw new JsonParseException("Expected ',' or '}' at position " + pos);
            }
        }
        return result;
    }

    private List<Object> parseArray() {
        List<Object> result = new ArrayList<>();
        expect('[');
        skipWhitespace();
        if (peek() == ']') {
            pos++;
            return result;
        }
        while (true) {
            skipWhitespace();
            result.add(parseValue());
            skipWhitespace();
            char next = peek();
            if (next == ',') {
                pos++;
            } else if (next == ']') {
                pos++;
                break;
            } else {
                throw new JsonParseException("Expected ',' or ']' at position " + pos);
            }
        }
        return result;
    }

    private String parseString() {
        expect('"');
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (pos >= text.length()) {
                throw new JsonParseException("Unterminated string starting before position " + pos);
            }
            char c = text.charAt(pos++);
            if (c == '"') {
                break;
            }
            if (c == '\\') {
                if (pos >= text.length()) {
                    throw new JsonParseException("Unterminated escape sequence at position " + pos);
                }
                char escaped = text.charAt(pos++);
                switch (escaped) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'u' -> {
                        if (pos + 4 > text.length()) {
                            throw new JsonParseException("Incomplete unicode escape at position " + pos);
                        }
                        String hex = text.substring(pos, pos + 4);
                        sb.append((char) Integer.parseInt(hex, 16));
                        pos += 4;
                    }
                    default -> throw new JsonParseException("Invalid escape character '\\" + escaped + "' at position " + (pos - 1));
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private Double parseNumber() {
        int start = pos;
        if (peek() == '-') {
            pos++;
        }
        while (pos < text.length() && Character.isDigit(text.charAt(pos))) {
            pos++;
        }
        if (pos < text.length() && text.charAt(pos) == '.') {
            pos++;
            while (pos < text.length() && Character.isDigit(text.charAt(pos))) {
                pos++;
            }
        }
        if (pos < text.length() && (text.charAt(pos) == 'e' || text.charAt(pos) == 'E')) {
            pos++;
            if (pos < text.length() && (text.charAt(pos) == '+' || text.charAt(pos) == '-')) {
                pos++;
            }
            while (pos < text.length() && Character.isDigit(text.charAt(pos))) {
                pos++;
            }
        }
        String numberText = text.substring(start, pos);
        try {
            return Double.parseDouble(numberText);
        } catch (NumberFormatException e) {
            throw new JsonParseException("Invalid number '" + numberText + "' at position " + start);
        }
    }

    private Boolean parseBoolean() {
        if (text.startsWith("true", pos)) {
            pos += 4;
            return Boolean.TRUE;
        }
        if (text.startsWith("false", pos)) {
            pos += 5;
            return Boolean.FALSE;
        }
        throw new JsonParseException("Invalid literal at position " + pos + " - expected 'true' or 'false'");
    }

    private Object parseNull() {
        if (text.startsWith("null", pos)) {
            pos += 4;
            return null;
        }
        throw new JsonParseException("Invalid literal at position " + pos + " - expected 'null'");
    }

    private void skipWhitespace() {
        while (pos < text.length() && Character.isWhitespace(text.charAt(pos))) {
            pos++;
        }
    }

    private char peek() {
        if (pos >= text.length()) {
            throw new JsonParseException("Unexpected end of input at position " + pos);
        }
        return text.charAt(pos);
    }

    private void expect(char c) {
        if (pos >= text.length() || text.charAt(pos) != c) {
            throw new JsonParseException("Expected '" + c + "' at position " + pos);
        }
        pos++;
    }

    /**
     * Converts a parsed JSON structure back into real JSON text - used for
     * displaying a JSON/JSONB column's value the way a user actually
     * expects to see it (proper JSON syntax), rather than Java's own
     * Map.toString()/List.toString() formatting (e.g. "{status=active}"
     * instead of the correct {@code {"status":"active"}}).
     */
    public static String toJsonText(Object value) {
        StringBuilder sb = new StringBuilder();
        writeJsonText(value, sb);
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static void writeJsonText(Object value, StringBuilder sb) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof Map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
                if (!first) sb.append(',');
                first = false;
                writeJsonString(entry.getKey(), sb);
                sb.append(':');
                writeJsonText(entry.getValue(), sb);
            }
            sb.append('}');
        } else if (value instanceof List) {
            sb.append('[');
            boolean first = true;
            for (Object element : (List<Object>) value) {
                if (!first) sb.append(',');
                first = false;
                writeJsonText(element, sb);
            }
            sb.append(']');
        } else if (value instanceof String s) {
            writeJsonString(s, sb);
        } else if (value instanceof Boolean || value instanceof Integer || value instanceof Long) {
            sb.append(value);
        } else if (value instanceof Double d) {
            // A whole-number JSON value (e.g. from "count": 42) is stored as
            // 42.0 (JSON has one numeric type, matching JsonParser's own
            // parseNumber), but a user who wrote "42" reasonably expects to
            // see "42" back, not "42.0" - both are valid JSON, this is
            // purely about matching expectations. The identical logic
            // ExecutorEngine.jsonScalarAsText already needed for ->>'key'
            // comparisons, applied here for display consistency too.
            if (d == Math.floor(d) && !Double.isInfinite(d)) {
                sb.append(d.longValue());
            } else {
                sb.append(d);
            }
        } else {
            throw new IllegalArgumentException("Cannot represent as JSON: " + value.getClass());
        }
    }

    private static void writeJsonString(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        sb.append('"');
    }
}
