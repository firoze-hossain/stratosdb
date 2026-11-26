package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.JsonParser;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.BinaryJsonb;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Sprint 08 Week 15: Decomposed Binary JSONB format,
 * JSONPath operators (->, ->>, @>, ?), and GIN index integration.
 */
class JsonbTest {

    @Test
    void testBinaryJsonbHeaderOffsetsTagsAndTupleRoundTrip() {
        Object parsed = JsonParser.parse(
            "{\"z_last\":true,\"a_first\":\"hello\",\"nested\":{\"count\":42,\"ratio\":3.5,\"empty\":null},\"arr\":[10,20,30]}"
        );
        BinaryJsonb jsonb = BinaryJsonb.encode(parsed);

        assertEquals(BinaryJsonb.MAGIC, jsonb.getMagic());
        assertEquals(BinaryJsonb.VERSION, jsonb.getVersion());
        assertEquals(BinaryJsonb.TAG_OBJECT, jsonb.getRootTag());
        assertEquals(BinaryJsonb.HEADER_SIZE, jsonb.getRootOffset());

        assertTrue(jsonb.hasKey("a_first"));
        assertTrue(jsonb.hasKey("nested"));
        assertFalse(jsonb.hasKey("missing"));

        assertEquals("hello", jsonb.get("a_first").asText());
        assertEquals("42", jsonb.navigate(List.of("nested", "count")).asText());
        assertTrue(jsonb.navigate(List.of("nested", "empty")).isNull());
        assertEquals("20", jsonb.navigate(List.of("arr", 1)).asText());

        // Tuple binary serialization round-trip
        Tuple t = new Tuple();
        t.addValue("id", 1);
        t.addValue("doc", jsonb);
        Tuple restored = Tuple.deserialize(t.serialize(), t.getColumnNames());
        Object restoredVal = restored.getValue("doc");
        assertInstanceOf(BinaryJsonb.class, restoredVal);
        BinaryJsonb restoredJsonb = (BinaryJsonb) restoredVal;
        assertEquals(jsonb, restoredJsonb);
        assertEquals("hello", restoredJsonb.get("a_first").asText());
        assertEquals("30", restoredJsonb.navigate(List.of("arr", 2)).asText());
    }

    @Test
    void testJsonbOperatorsAndNestedPathExtraction(@TempDir Path tempDir) {
        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(cfg);
        try {
            assertTrue(db.execute("CREATE TABLE users (id INT, doc JSONB)").isSuccess());
            assertTrue(db.execute(
                "INSERT INTO users VALUES (1, '{\"name\":\"Alice\",\"user\":{\"role\":\"admin\",\"address\":{\"city\":\"NYC\"}},\"tags\":[\"ops\",\"urgent\"],\"items\":[{\"sku\":\"A1\"},{\"sku\":\"B2\"}]}')"
            ).isSuccess());
            assertTrue(db.execute(
                "INSERT INTO users VALUES (2, '{\"name\":\"Bob\",\"user\":{\"role\":\"viewer\",\"address\":{\"city\":\"LA\"}},\"tags\":[\"sales\"],\"items\":[{\"sku\":\"C3\"}]}')"
            ).isSuccess());

            // Verify JSONB column stores BinaryJsonb natively
            QueryResult rawRes = db.execute("SELECT doc FROM users WHERE id = 1");
            assertTrue(rawRes.isSuccess(), () -> rawRes.getError());
            assertInstanceOf(BinaryJsonb.class, rawRes.getRows().get(0).getValue("doc"));

            // Nested -> and ->> projection
            QueryResult proj = db.execute(
                "SELECT id, doc->'user'->'address'->>'city' AS city, doc->'items'->1->>'sku' AS second_sku FROM users WHERE id = 1"
            );
            assertTrue(proj.isSuccess(), () -> proj.getError());
            assertEquals(1, proj.getRows().size());
            assertEquals("NYC", proj.getRows().get(0).getValue("city"));
            assertEquals("B2", proj.getRows().get(0).getValue("second_sku"));

            // -> returns BinaryJsonb in projection
            QueryResult objProj = db.execute("SELECT doc->'user' AS u FROM users WHERE id = 2");
            assertTrue(objProj.isSuccess(), () -> objProj.getError());
            assertInstanceOf(BinaryJsonb.class, objProj.getRows().get(0).getValue("u"));

            // WHERE with chained ->>
            QueryResult whereNested = db.execute("SELECT id FROM users WHERE doc->'user'->>'role' = 'admin'");
            assertTrue(whereNested.isSuccess(), () -> whereNested.getError());
            assertEquals(1, whereNested.getRows().size());
            assertEquals(1, whereNested.getRows().get(0).getValue("id"));

            // WHERE with @> containment (nested object & array)
            QueryResult containsObj = db.execute("SELECT id FROM users WHERE doc @> '{\"user\":{\"address\":{\"city\":\"LA\"}}}'");
            assertTrue(containsObj.isSuccess(), () -> containsObj.getError());
            assertEquals(1, containsObj.getRows().size());
            assertEquals(2, containsObj.getRows().get(0).getValue("id"));

            QueryResult containsArr = db.execute("SELECT id FROM users WHERE doc @> '{\"tags\":[\"urgent\"]}'");
            assertTrue(containsArr.isSuccess(), () -> containsArr.getError());
            assertEquals(1, containsArr.getRows().size());
            assertEquals(1, containsArr.getRows().get(0).getValue("id"));

            // WHERE with ? key existence (top-level and nested)
            QueryResult keyExistsTop = db.execute("SELECT id FROM users WHERE doc ? 'tags'");
            assertTrue(keyExistsTop.isSuccess(), () -> keyExistsTop.getError());
            assertEquals(2, keyExistsTop.getRows().size());

            QueryResult keyExistsNested = db.execute("SELECT id FROM users WHERE doc->'user' ? 'address'");
            assertTrue(keyExistsNested.isSuccess(), () -> keyExistsNested.getError());
            assertEquals(2, keyExistsNested.getRows().size());

            QueryResult keyMissing = db.execute("SELECT id FROM users WHERE doc ? 'nonexistent'");
            assertTrue(keyMissing.isSuccess(), () -> keyMissing.getError());
            assertEquals(0, keyMissing.getRows().size());
        } finally {
            db.shutdown();
        }
    }

    @Test
    void testGinIndexedJsonbContainmentSubMillisecond(@TempDir Path tempDir) {
        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(cfg);
        try {
            assertTrue(db.execute("CREATE TABLE events (id INT, payload JSONB)").isSuccess());
            for (int i = 0; i < 1000; i++) {
                String role = (i == 777) ? "superadmin" : "member";
                String region = (i % 10 == 0) ? "eu" : "us";
                assertTrue(db.execute(
                    "INSERT INTO events VALUES (" + i + ", '{\"seq\":" + i + ",\"meta\":{\"role\":\"" + role + "\",\"region\":\"" + region + "\"}}')"
                ).isSuccess());
            }
            QueryResult idx = db.execute("CREATE INDEX idx_events_payload ON events (payload) USING GIN");
            assertTrue(idx.isSuccess(), () -> idx.getError());

            // Verify end-to-end SQL queries with GIN index
            QueryResult res = db.execute("SELECT id FROM events WHERE payload @> '{\"meta\":{\"role\":\"superadmin\",\"region\":\"us\"}}'");
            assertTrue(res.isSuccess(), () -> res.getError());
            assertEquals(1, res.getRows().size());
            assertEquals(777, res.getRows().get(0).getValue("id"));

            // Benchmark GIN-indexed JSONB @> containment search directly (excluding ANTLR4 SQL parse overhead)
            com.stratosdb.index.gin.GinIndex gin = new com.stratosdb.index.gin.GinIndex("bench_gin");
            java.util.List<BinaryJsonb> docs = new java.util.ArrayList<>(1000);
            for (int i = 0; i < 1000; i++) {
                String role = (i == 777) ? "superadmin" : "member";
                String region = (i % 10 == 0) ? "eu" : "us";
                BinaryJsonb jb = BinaryJsonb.encode(JsonParser.parse(
                    "{\"seq\":" + i + ",\"meta\":{\"role\":\"" + role + "\",\"region\":\"" + region + "\"}}"
                ));
                docs.add(jb);
                java.util.List<String> keys = new java.util.ArrayList<>();
                jb.collectGinKeys(keys);
                com.stratosdb.storage.page.BTreePage.RID rid = new com.stratosdb.storage.page.BTreePage.RID(i / 100, i % 100);
                for (String k : keys) {
                    gin.insertExact(k, rid);
                }
            }
            BinaryJsonb queryDoc = BinaryJsonb.encode(JsonParser.parse("{\"meta\":{\"role\":\"superadmin\",\"region\":\"us\"}}"));
            java.util.List<String> queryKeys = new java.util.ArrayList<>();
            queryDoc.collectGinKeys(queryKeys);

            // Warmup JIT
            for (int w = 0; w < 500; w++) {
                java.util.List<com.stratosdb.storage.page.BTreePage.RID> hits = gin.searchAll(queryKeys);
                assertEquals(1, hits.size());
            }

            long startNs = System.nanoTime();
            java.util.List<com.stratosdb.storage.page.BTreePage.RID> hits = gin.searchAll(queryKeys);
            int matched = 0;
            for (com.stratosdb.storage.page.BTreePage.RID rid : hits) {
                int idxId = (int) (rid.pageId() * 100 + rid.slot());
                if (docs.get(idxId).contains(queryDoc)) {
                    matched++;
                }
            }
            long elapsedNs = System.nanoTime() - startNs;
            assertEquals(1, matched);
            assertTrue(elapsedNs < 1_000_000L,
                () -> "GIN-indexed JSONB @> search must execute in < 1ms, took " + (elapsedNs / 1_000_000.0) + " ms");

            // Also verify ? key existence and ->> via GIN
            QueryResult existsRes = db.execute("SELECT id FROM events WHERE payload ? 'meta'");
            assertTrue(existsRes.isSuccess());
            assertEquals(1000, existsRes.getRows().size());

            QueryResult pathRes = db.execute("SELECT id FROM events WHERE payload->'meta'->>'role' = 'superadmin'");
            assertTrue(pathRes.isSuccess());
            assertEquals(1, pathRes.getRows().size());
            assertEquals(777, pathRes.getRows().get(0).getValue("id"));
        } finally {
            db.shutdown();
        }
    }
}
