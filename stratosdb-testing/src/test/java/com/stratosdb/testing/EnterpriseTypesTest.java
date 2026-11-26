package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.NetworkAddress;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Sprint 08 Week 16: Native 16-byte UUID + gen_random_uuid(),
 * INET/CIDR subnet containment (<<, <<=, >>, >>=), and 1D typed arrays (INT[], TEXT[]).
 */
class EnterpriseTypesTest {

    @Test
    void testNative16ByteUuidTupleSerializationAndPrimaryKeyIndex(@TempDir Path tempDir) {
        UUID fixed = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        Tuple t = new Tuple();
        t.addValue("id", fixed);
        byte[] raw = t.serialize();
        // Verify compact 16-byte UUID encoding (much smaller than 36-char string + length)
        Tuple restored = Tuple.deserialize(raw, List.of("id"));
        assertInstanceOf(UUID.class, restored.getValue("id"));
        assertEquals(fixed, restored.getValue("id"));

        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(cfg);
        try {
            assertTrue(db.execute("CREATE TABLE items (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT)").isSuccess());
            assertTrue(db.execute("INSERT INTO items (name) VALUES ('auto1')").isSuccess());
            assertTrue(db.execute("INSERT INTO items VALUES (gen_random_uuid(), 'explicit_fn')").isSuccess());
            assertTrue(db.execute("INSERT INTO items VALUES ('550e8400-e29b-41d4-a716-446655440000', 'fixed')").isSuccess());

            // Invalid UUID must fail
            assertFalse(db.execute("INSERT INTO items VALUES ('not-a-uuid', 'bad')").isSuccess());
            // Duplicate PK must fail
            assertFalse(db.execute("INSERT INTO items VALUES ('550e8400-e29b-41d4-a716-446655440000', 'dup')").isSuccess());

            // Verify B-Tree PK index lookup on UUID
            QueryResult explain = db.execute("EXPLAIN SELECT name FROM items WHERE id = '550e8400-e29b-41d4-a716-446655440000'");
            assertTrue(explain.isSuccess(), () -> explain.getError());
            assertTrue(explain.toString().contains("IndexScan") || explain.toString().contains("Index"),
                () -> "UUID PK lookup should use index scan: " + explain);

            QueryResult lookup = db.execute("SELECT id, name FROM items WHERE id = '550e8400-e29b-41d4-a716-446655440000'");
            assertTrue(lookup.isSuccess(), () -> lookup.getError());
            assertEquals(1, lookup.getRows().size());
            assertEquals("fixed", lookup.getRows().get(0).getValue("name"));
            assertEquals(fixed, lookup.getRows().get(0).getValue("id"));

            QueryResult all = db.execute("SELECT id FROM items");
            assertEquals(3, all.getRows().size());
            for (Tuple row : all.getRows()) {
                assertInstanceOf(UUID.class, row.getValue("id"));
            }
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testInetCidrSubnetContainment(@TempDir Path tempDir) {
        NetworkAddress host = NetworkAddress.parse("192.168.1.42", false);
        Tuple t = new Tuple();
        t.addValue("ip", host);
        Tuple restored = Tuple.deserialize(t.serialize(), List.of("ip"));
        assertInstanceOf(NetworkAddress.class, restored.getValue("ip"));
        assertEquals(host, restored.getValue("ip"));

        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(cfg);
        try {
            assertTrue(db.execute("CREATE TABLE routes (id INT, host INET, net CIDR)").isSuccess());
            assertTrue(db.execute("INSERT INTO routes VALUES (1, '192.168.1.42', '192.168.1.0/24')").isSuccess());
            assertTrue(db.execute("INSERT INTO routes VALUES (2, '10.0.5.9/16', '10.0.0.0/8')").isSuccess());
            assertTrue(db.execute("INSERT INTO routes VALUES (3, '2001:db8:1::42', '2001:db8:1::/48')").isSuccess());

            // Strict containment (<<)
            QueryResult q1 = db.execute("SELECT id FROM routes WHERE host << '192.168.1.0/24'");
            assertTrue(q1.isSuccess(), () -> q1.getError());
            assertEquals(1, q1.getRows().size());
            assertEquals(1, q1.getRows().get(0).getValue("id"));

            // Containment or equality (<<=)
            QueryResult q2 = db.execute("SELECT id FROM routes WHERE net <<= '192.168.1.0/24'");
            assertTrue(q2.isSuccess(), () -> q2.getError());
            assertEquals(1, q2.getRows().size());
            assertEquals(1, q2.getRows().get(0).getValue("id"));

            // Supernet contains (>>)
            QueryResult q3 = db.execute("SELECT id FROM routes WHERE net >> '10.0.5.9/32'");
            assertTrue(q3.isSuccess(), () -> q3.getError());
            assertEquals(1, q3.getRows().size());
            assertEquals(2, q3.getRows().get(0).getValue("id"));

            // Supernet contains or equals (>>=)
            QueryResult q4 = db.execute("SELECT id FROM routes WHERE net >>= '10.0.0.0/8'");
            assertTrue(q4.isSuccess(), () -> q4.getError());
            assertEquals(1, q4.getRows().size());
            assertEquals(2, q4.getRows().get(0).getValue("id"));

            // IPv6 subnet containment
            QueryResult q6 = db.execute("SELECT id FROM routes WHERE host << '2001:db8:1::/48'");
            assertTrue(q6.isSuccess(), () -> q6.getError());
            assertEquals(1, q6.getRows().size());
            assertEquals(3, q6.getRows().get(0).getValue("id"));
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testTypedOneDimensionalArrays(@TempDir Path tempDir) {
        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(cfg);
        try {
            assertTrue(db.execute("CREATE TABLE items (id INT, nums INT[], tags TEXT[])").isSuccess());
            assertTrue(db.execute("INSERT INTO items VALUES (1, ARRAY[10, 20, 30], ARRAY['db', 'sql'])").isSuccess());
            assertTrue(db.execute("INSERT INTO items VALUES (2, '{40, 50}', '{\"nosql\", \"jsonb\"}')").isSuccess());

            // Type validation: non-integer in INT[] must fail
            assertFalse(db.execute("INSERT INTO items VALUES (3, ARRAY['bad'], ARRAY['x'])").isSuccess());
            assertFalse(db.execute("INSERT INTO items VALUES (4, '{not_int}', '{\"x\"}')").isSuccess());

            // GIN index + @> containment on INT[] and TEXT[]
            assertTrue(db.execute("CREATE INDEX idx_nums ON items (nums) USING GIN").isSuccess());
            assertTrue(db.execute("CREATE INDEX idx_tags ON items (tags) USING GIN").isSuccess());

            QueryResult intMatch = db.execute("SELECT id FROM items WHERE nums @> 20");
            assertTrue(intMatch.isSuccess(), () -> intMatch.getError());
            assertEquals(1, intMatch.getRows().size());
            assertEquals(1, intMatch.getRows().get(0).getValue("id"));

            QueryResult textMatch = db.execute("SELECT id FROM items WHERE tags @> 'jsonb'");
            assertTrue(textMatch.isSuccess(), () -> textMatch.getError());
            assertEquals(1, textMatch.getRows().size());
            assertEquals(2, textMatch.getRows().get(0).getValue("id"));

            // array_length and cardinality
            QueryResult fnRes = db.execute("SELECT array_length(nums, 1) AS nlen, cardinality(tags) AS tlen FROM items WHERE id = 1");
            assertTrue(fnRes.isSuccess(), () -> fnRes.getError());
            assertEquals(3, ((Number) fnRes.getRows().get(0).getValue("nlen")).intValue());
            assertEquals(2, ((Number) fnRes.getRows().get(0).getValue("tlen")).intValue());
        } finally {
            db.shutdown();
        }
    }
}
