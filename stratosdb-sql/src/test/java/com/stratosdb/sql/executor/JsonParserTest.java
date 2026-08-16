package com.stratosdb.sql.executor;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A real, minimal recursive-descent JSON parser - not a stub. Tested
 * directly against a range of valid and deliberately invalid input
 * before being trusted anywhere else in the engine (JSON/JSONB column
 * validation, ->>'key' extraction, GIN indexing all depend on this
 * being correct).
 */
class JsonParserTest {

    @Test
    void parsesAFlatObjectCorrectly() {
        Object result = JsonParser.parse("{\"status\": \"active\", \"count\": 42}");
        assertInstanceOf(Map.class, result);
        Map<?, ?> map = (Map<?, ?>) result;
        assertEquals("active", map.get("status"));
        assertEquals(42.0, map.get("count"));
    }

    @Test
    void parsesNestedArraysAndObjects() {
        Object result = JsonParser.parse("{\"tags\": [\"a\", \"b\", \"c\"], \"nested\": {\"x\": 1}}");
        Map<?, ?> map = (Map<?, ?>) result;
        assertInstanceOf(List.class, map.get("tags"));
        assertEquals(3, ((List<?>) map.get("tags")).size());
        assertEquals(1.0, ((Map<?, ?>) map.get("nested")).get("x"));
    }

    @Test
    void parsesBooleansAndNullCorrectly() {
        Map<?, ?> map = (Map<?, ?>) JsonParser.parse("{\"active\": true, \"deleted\": false, \"value\": null}");
        assertEquals(Boolean.TRUE, map.get("active"));
        assertEquals(Boolean.FALSE, map.get("deleted"));
        assertTrue(map.containsKey("value"));
        assertNull(map.get("value"));
    }

    @Test
    void parsesEscapeSequencesCorrectly() {
        Map<?, ?> map = (Map<?, ?>) JsonParser.parse("{\"text\": \"line1\\nline2\\t\\\"quoted\\\"\"}");
        assertEquals("line1\nline2\t\"quoted\"", map.get("text"));
    }

    @Test
    void parsesVariousNumberFormatsCorrectly() {
        Map<?, ?> map = (Map<?, ?>) JsonParser.parse("{\"a\": -5, \"b\": 3.14, \"c\": 1.5e10}");
        assertEquals(-5.0, map.get("a"));
        assertEquals(3.14, map.get("b"));
        assertEquals(1.5e10, map.get("c"));
    }

    @Test
    void parsesTopLevelArraysAndEmptyStructures() {
        Object array = JsonParser.parse("[1, 2, 3]");
        assertInstanceOf(List.class, array);
        assertEquals(3, ((List<?>) array).size());

        assertTrue(((Map<?, ?>) JsonParser.parse("{}")).isEmpty());
        assertTrue(((List<?>) JsonParser.parse("[]")).isEmpty());
    }

    @Test
    void rejectsMalformedJson() {
        assertThrows(JsonParser.JsonParseException.class, () -> JsonParser.parse("{invalid}"));
        assertThrows(JsonParser.JsonParseException.class, () -> JsonParser.parse("{\"a\": 1} trailing garbage"),
            "trailing content after an otherwise-valid value must be rejected, not silently ignored");
        assertThrows(JsonParser.JsonParseException.class, () -> JsonParser.parse("{\"a\": }"));
    }

    @Test
    void toJsonTextRoundTripsCorrectly() {
        String original = "{\"status\":\"active\",\"count\":42}";
        Object parsed = JsonParser.parse(original);
        String rendered = JsonParser.toJsonText(parsed);
        Object reparsed = JsonParser.parse(rendered);
        assertEquals(parsed, reparsed);
    }

    @Test
    void toJsonTextRendersWholeNumbersWithoutTrailingDecimal() {
        // JSON has one numeric type, so 42 parses as Double 42.0 - but a user who
        // wrote "42" reasonably expects to see "42" back, not "42.0", when the
        // value is displayed. Both are valid JSON; this is about matching
        // expectations, and matters for consistency with ->>'key' comparisons too.
        Object parsed = JsonParser.parse("{\"count\": 42}");
        assertEquals("{\"count\":42}", JsonParser.toJsonText(parsed));
    }
}
