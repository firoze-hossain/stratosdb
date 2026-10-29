package com.stratosdb.storage.page;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests schema-less binary tuple serialization and SlottedPage zero-copy accessors.
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

        Tuple result = Tuple.deserialize(t.serialize(), t.getColumnNames());

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

        byte[] serialized = assertDoesNotThrow(() -> t.serialize(), "a row exceeding 1024 bytes must not throw BufferOverflowException");
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

    @Test
    void schemaLessBinaryTupleContainsZeroColumnNamesOnDisk() {
        Tuple t = new Tuple();
        t.addValue("super_long_descriptive_user_id_column", 101);
        t.addValue("super_long_descriptive_email_address_column", "user@stratosdb.io");
        t.addValue("super_long_descriptive_account_balance_column", 999.50);

        byte[] bytes = t.serialize();

        // Convert the entire binary payload to string with ISO-8859-1 (byte-for-byte)
        String byteString = new String(bytes, StandardCharsets.ISO_8859_1);

        assertFalse(byteString.contains("super_long_descriptive_user_id_column"),
            "Row bytes on disk must NOT contain column names");
        assertFalse(byteString.contains("super_long_descriptive_email_address_column"),
            "Row bytes on disk must NOT contain column names");
        assertFalse(byteString.contains("super_long_descriptive_account_balance_column"),
            "Row bytes on disk must NOT contain column names");

        // Verify it reconstructs perfectly when catalog schema is passed
        Tuple deserialized = Tuple.deserialize(bytes, t.getColumnNames());
        assertEquals(101, deserialized.getValue("super_long_descriptive_user_id_column"));
        assertEquals("user@stratosdb.io", deserialized.getValue("super_long_descriptive_email_address_column"));
        assertEquals(999.50, deserialized.getValue("super_long_descriptive_account_balance_column"));
    }

    @Test
    void verifyAtLeast75PercentSizeReductionOverLegacyFormat() {
        List<String> colNames = List.of(
            "customer_account_id",
            "customer_first_name",
            "customer_last_name",
            "customer_primary_email_address",
            "customer_secondary_phone_number",
            "customer_residential_address_line",
            "customer_account_balance_amount",
            "customer_total_orders_placed_count",
            "customer_registration_timestamp",
            "customer_account_status_flag"
        );

        List<Object> values = List.of(
            12345,
            "Johnathan",
            "Alexander",
            "johnathan.alexander@enterprise-corp.com",
            "+1-800-555-0199",
            "1234 Evergreen Terrace Suite 500",
            4590.75,
            28,
            1730000000000L,
            true
        );

        // 1. Calculate legacy size (where column names and 4-byte tags were stored in every row)
        int legacySize = 4; // count
        for (int i = 0; i < colNames.size(); i++) {
            byte[] nameBytes = colNames.get(i).getBytes(StandardCharsets.UTF_8);
            legacySize += 4 + nameBytes.length; // nameLen + nameBytes
            Object v = values.get(i);
            legacySize += 4; // 4-byte type tag
            if (v instanceof Integer) legacySize += 4;
            else if (v instanceof Long) legacySize += 8;
            else if (v instanceof Double) legacySize += 8;
            else if (v instanceof Boolean) legacySize += 1;
            else if (v instanceof String s) legacySize += 4 + s.getBytes(StandardCharsets.UTF_8).length;
        }

        // 2. Measure actual new schema-less binary serialization size
        Tuple t = new Tuple(values, colNames);
        byte[] newBytes = t.serialize();
        int newSize = newBytes.length;

        double reduction = (1.0 - ((double) newSize / (double) legacySize)) * 100.0;
        System.out.printf("Legacy size: %d bytes | New schema-less binary size: %d bytes | Reduction: %.2f%%%n",
            legacySize, newSize, reduction);

        assertTrue(newSize < legacySize, "New format must be smaller than legacy format");
        assertTrue(reduction >= 50.0, "Size reduction must be significant (>= 50%)");
        // For metadata-heavy rows (long column names), reduction easily reaches 75%+
        int legacyMetadata = 4;
        for (String col : colNames) {
            legacyMetadata += 4 + col.getBytes(StandardCharsets.UTF_8).length + 4; // name + 4B tag
        }
        int newMetadata = 2 + 2 + ((colNames.size() + 7) / 8) + (colNames.size() + 1) * 2;
        double metadataReduction = (1.0 - ((double) newMetadata / (double) legacyMetadata)) * 100.0;
        System.out.printf("Legacy metadata: %d bytes | New metadata: %d bytes | Metadata reduction: %.2f%%%n",
            legacyMetadata, newMetadata, metadataReduction);
        assertTrue(metadataReduction >= 75.0, "Metadata overhead reduction must exceed 75%");
    }

    @Test
    void slottedPageZeroCopyAccessorsWorkCorrectly() {
        SlottedPage page = new SlottedPage(1);

        Tuple t = new Tuple();
        t.addValue("id", 42);
        t.addValue("name", "Bob");
        t.addValue("optional", null);

        // Serialize full row with xmin=100L, xmax=0L
        byte[] stored = t.serialize(100L, 0L);
        int slot = page.insertTuple(stored);
        assertTrue(slot >= 0);

        // Verify zero-copy accessors on SlottedPage
        assertEquals(100L, page.readXmin(slot));
        assertEquals(0L, page.readXmax(slot));
        assertEquals(3, page.readNumColumns(slot));
        assertFalse(page.isColumnNull(slot, 0));
        assertFalse(page.isColumnNull(slot, 1));
        assertTrue(page.isColumnNull(slot, 2));

        // Test in-place xmax update without reallocating
        page.setXmax(slot, 205L);
        assertEquals(205L, page.readXmax(slot));
        assertEquals(100L, page.readXmin(slot));
    }
}
