package com.stratosdb.storage.page;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tuple's serialize()/deserialize() had no dedicated test coverage at all
 * before this - every other test exercised it only indirectly, through
 * whatever a table insert/scan happened to touch. Given it's the core
 * on-disk row format for the entire engine, that gap mattered enough to
 * close directly, especially since fixing it to support arrays required
 * a real rewrite (see the class's own javadoc): switching from a fixed
 * 1024-byte buffer to a dynamically-growing one, after finding that the
 * old fixed size would throw BufferOverflowException for any row whose
 * total serialized size happened to exceed it - a real, separate,
 * previously-latent bug, not something introduced by adding arrays.
 */
class TupleTest {

    @Test
    void allExistingScalarTypesRoundTripCorrectly() {
        Tuple t = new Tuple();
        t.addValue("i", 42);
        t.addValue("s", "hello");
        t.addValue("l", 123456789012345L);
        t.addValue("b", true);
        t.addValue("d", 3.14159);
        t.addValue("n", null);

        Tuple result = Tuple.deserialize(t.serialize());

        assertEquals(42, result.getValue("i"));
        assertEquals("hello", result.getValue("s"));
        assertEquals(123456789012345L, result.getValue("l"));
        assertEquals(true, result.getValue("b"));
        assertEquals(3.14159, result.getValue("d"));
        assertNull(result.getValue("n"));
    }

    @Test
    void aRowExceedingTheOldFixed1024ByteBufferNoLongerThrows() {
        Tuple t = new Tuple();
        String bigValue = "x".repeat(2000); // guaranteed to exceed the old fixed 1024-byte buffer
        t.addValue("id", 1);
        t.addValue("payload", bigValue);

        byte[] serialized = assertDoesNotThrow(t::serialize, "a row exceeding 1024 bytes must not throw BufferOverflowException");
        Tuple result = Tuple.deserialize(serialized);
        assertEquals(bigValue, result.getValue("payload"));
    }

    @Test
    void arrayValueRoundTripsCorrectly() {
        Tuple t = new Tuple();
        List<Object> array = List.of(1, 2, 3, 4, 5);
        t.addValue("tags", array);

        Tuple result = Tuple.deserialize(t.serialize());
        Object value = result.getValue("tags");

        assertInstanceOf(List.class, value, "an array value must deserialize as a List");
        assertEquals(array, value);
    }

    @Test
    void largeArrayRoundTripsCorrectly() {
        Tuple t = new Tuple();
        List<Object> bigArray = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            bigArray.add("item" + i);
        }
        t.addValue("bigtags", bigArray);

        Tuple result = Tuple.deserialize(t.serialize());
        assertEquals(bigArray, result.getValue("bigtags"));
    }

    @Test
    void mixedTypeArrayRoundTripsCorrectly() {
        // An array's elements can themselves be any supported scalar type - not just all-the-same-type.
        Tuple t = new Tuple();
        List<Object> mixed = List.of(1, "two", 3.0, true);
        t.addValue("mixed", mixed);

        Tuple result = Tuple.deserialize(t.serialize());
        assertEquals(mixed, result.getValue("mixed"));
    }

    @Test
    void emptyArrayRoundTripsCorrectly() {
        Tuple t = new Tuple();
        t.addValue("empty", new ArrayList<>());

        Tuple result = Tuple.deserialize(t.serialize());
        Object value = result.getValue("empty");
        assertInstanceOf(List.class, value);
        assertTrue(((List<?>) value).isEmpty());
    }
}
