package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.ast.WindowFunctionCall;
import com.stratosdb.sql.ast.WindowOrderItem;
import com.stratosdb.sql.execution.MemoryScanOperator;
import com.stratosdb.sql.execution.TupleSlot;
import com.stratosdb.sql.execution.WindowOperator;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Sprint 07 Week 13: SQL Window Functions Engine.
 * Verifies OVER (PARTITION BY ... ORDER BY ...), WindowOperator,
 * ROW_NUMBER(), RANK(), DENSE_RANK(), LEAD(), LAG(), and window aggregates
 * against PostgreSQL-compliant financial and leaderboard queries.
 */
public class WindowFunctionsEngineTest {

    @TempDir
    Path tempDir;

    private StratosDB db;

    @BeforeEach
    void setUp() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setBufferPoolSize(64);
        db = new StratosDB(config);
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.shutdown();
        }
    }

    @Test
    void testLeaderboardRankingQueryPostgresCompliant() {
        db.execute("CREATE TABLE scores (id INT, game VARCHAR, player VARCHAR, points INT)");
        db.execute("INSERT INTO scores VALUES (1, 'chess', 'Alice', 2400)");
        db.execute("INSERT INTO scores VALUES (2, 'chess', 'Bob', 2400)");
        db.execute("INSERT INTO scores VALUES (3, 'chess', 'Carol', 2200)");
        db.execute("INSERT INTO scores VALUES (4, 'go', 'Dave', 1900)");
        db.execute("INSERT INTO scores VALUES (5, 'go', 'Eve', 2100)");

        QueryResult res = db.execute(
            "SELECT id, game, player, points, "
            + "ROW_NUMBER() OVER (PARTITION BY game ORDER BY points DESC, id ASC) AS rn, "
            + "RANK() OVER (PARTITION BY game ORDER BY points DESC) AS rk, "
            + "DENSE_RANK() OVER (PARTITION BY game ORDER BY points DESC) AS dr "
            + "FROM scores ORDER BY id ASC");

        assertTrue(res.isSuccess(), res.getError());
        assertEquals(5, res.getRows().size());

        Map<Integer, Tuple> byId = new HashMap<>();
        for (Tuple t : res.getRows()) {
            byId.put(((Number) t.getValue("id")).intValue(), t);
        }

        // Chess: Alice(2400), Bob(2400) tied at rank 1, Carol(2200) at rank 3 / dense_rank 2
        assertEquals(1L, ((Number) byId.get(1).getValue("rn")).longValue());
        assertEquals(1L, ((Number) byId.get(1).getValue("rk")).longValue());
        assertEquals(1L, ((Number) byId.get(1).getValue("dr")).longValue());

        assertEquals(2L, ((Number) byId.get(2).getValue("rn")).longValue());
        assertEquals(1L, ((Number) byId.get(2).getValue("rk")).longValue());
        assertEquals(1L, ((Number) byId.get(2).getValue("dr")).longValue());

        assertEquals(3L, ((Number) byId.get(3).getValue("rn")).longValue());
        assertEquals(3L, ((Number) byId.get(3).getValue("rk")).longValue());
        assertEquals(2L, ((Number) byId.get(3).getValue("dr")).longValue());

        // Go: Eve(2100) is #1, Dave(1900) is #2
        assertEquals(1L, ((Number) byId.get(5).getValue("rn")).longValue());
        assertEquals(2L, ((Number) byId.get(4).getValue("rn")).longValue());
    }

    @Test
    void testFinancialStockSeriesLeadAndLagWithOffsetsAndDefaults() {
        db.execute("CREATE TABLE quotes (id INT, ticker VARCHAR, day INT, close_price DOUBLE)");
        db.execute("INSERT INTO quotes VALUES (1, 'AAPL', 1, 180.0)");
        db.execute("INSERT INTO quotes VALUES (2, 'AAPL', 2, 185.0)");
        db.execute("INSERT INTO quotes VALUES (3, 'AAPL', 3, NULL)");
        db.execute("INSERT INTO quotes VALUES (4, 'AAPL', 4, 192.0)");
        db.execute("INSERT INTO quotes VALUES (5, 'MSFT', 1, 410.0)");
        db.execute("INSERT INTO quotes VALUES (6, 'MSFT', 2, 415.0)");

        QueryResult res = db.execute(
            "SELECT id, ticker, day, close_price, "
            + "LAG(close_price) OVER (PARTITION BY ticker ORDER BY day ASC) AS prev1, "
            + "LAG(close_price, 2, -1.0) OVER (PARTITION BY ticker ORDER BY day ASC) AS prev2, "
            + "LEAD(close_price, 1, 0.0) OVER (PARTITION BY ticker ORDER BY day ASC) AS next1 "
            + "FROM quotes");

        assertTrue(res.isSuccess(), res.getError());
        assertEquals(6, res.getRows().size());

        Map<Integer, Tuple> byId = new HashMap<>();
        for (Tuple t : res.getRows()) {
            byId.put(((Number) t.getValue("id")).intValue(), t);
        }

        // AAPL day 1: prev1=null, prev2=-1.0 (out of bounds), next1=185.0
        assertNull(byId.get(1).getValue("prev1"));
        assertEquals(-1.0, ((Number) byId.get(1).getValue("prev2")).doubleValue(), 1e-9);
        assertEquals(185.0, ((Number) byId.get(1).getValue("next1")).doubleValue(), 1e-9);

        // AAPL day 2: prev1=180.0, prev2=-1.0, next1=null (in-bounds NULL column value!)
        assertEquals(180.0, ((Number) byId.get(2).getValue("prev1")).doubleValue(), 1e-9);
        assertEquals(-1.0, ((Number) byId.get(2).getValue("prev2")).doubleValue(), 1e-9);
        assertNull(byId.get(2).getValue("next1"), "In-bounds NULL must return null, NOT default 0.0");

        // AAPL day 3: prev1=185.0, prev2=180.0, next1=192.0
        assertEquals(185.0, ((Number) byId.get(3).getValue("prev1")).doubleValue(), 1e-9);
        assertEquals(180.0, ((Number) byId.get(3).getValue("prev2")).doubleValue(), 1e-9);
        assertEquals(192.0, ((Number) byId.get(3).getValue("next1")).doubleValue(), 1e-9);

        // AAPL day 4: prev1=null (day 3 close_price is NULL), prev2=185.0, next1=0.0 (out of bounds)
        assertNull(byId.get(4).getValue("prev1"));
        assertEquals(185.0, ((Number) byId.get(4).getValue("prev2")).doubleValue(), 1e-9);
        assertEquals(0.0, ((Number) byId.get(4).getValue("next1")).doubleValue(), 1e-9);

        // MSFT day 1 & 2: independent partition
        assertNull(byId.get(5).getValue("prev1"));
        assertEquals(415.0, ((Number) byId.get(5).getValue("next1")).doubleValue(), 1e-9);
        assertEquals(410.0, ((Number) byId.get(6).getValue("prev1")).doubleValue(), 1e-9);
        assertEquals(0.0, ((Number) byId.get(6).getValue("next1")).doubleValue(), 1e-9);
    }

    @Test
    void testFinancialRunningTotalsAndPartitionAggregatesWithPeers() {
        db.execute("CREATE TABLE ledger (id INT, acct VARCHAR, tx_day INT, amount INT)");
        db.execute("INSERT INTO ledger VALUES (1, 'A', 1, 100)");
        db.execute("INSERT INTO ledger VALUES (2, 'A', 2, 200)");
        db.execute("INSERT INTO ledger VALUES (3, 'A', 2, 300)"); // peer on tx_day=2
        db.execute("INSERT INTO ledger VALUES (4, 'B', 1, 50)");

        QueryResult res = db.execute(
            "SELECT id, acct, tx_day, amount, "
            + "SUM(amount) OVER (PARTITION BY acct ORDER BY tx_day ASC) AS running_sum, "
            + "SUM(amount) OVER (PARTITION BY acct) AS acct_total, "
            + "COUNT(*) OVER (PARTITION BY acct) AS acct_cnt, "
            + "AVG(amount) OVER (PARTITION BY acct) AS acct_avg "
            + "FROM ledger");

        assertTrue(res.isSuccess(), res.getError());
        Map<Integer, Tuple> byId = new HashMap<>();
        for (Tuple t : res.getRows()) {
            byId.put(((Number) t.getValue("id")).intValue(), t);
        }

        // Account A: day 1 -> running_sum=100; day 2 peers (id 2 & 3) -> both get 100+200+300=600 (Postgres RANGE frame!)
        assertEquals(100L, ((Number) byId.get(1).getValue("running_sum")).longValue());
        assertEquals(600L, ((Number) byId.get(2).getValue("running_sum")).longValue());
        assertEquals(600L, ((Number) byId.get(3).getValue("running_sum")).longValue());
        assertEquals(600L, ((Number) byId.get(1).getValue("acct_total")).longValue());
        assertEquals(3L, ((Number) byId.get(1).getValue("acct_cnt")).longValue());
        assertEquals(200.0, ((Number) byId.get(1).getValue("acct_avg")).doubleValue(), 1e-9);

        // Account B
        assertEquals(50L, ((Number) byId.get(4).getValue("running_sum")).longValue());
        assertEquals(50L, ((Number) byId.get(4).getValue("acct_total")).longValue());
        assertEquals(1L, ((Number) byId.get(4).getValue("acct_cnt")).longValue());
    }

    @Test
    void testWindowWithWhereOrderByAndLimit() {
        db.execute("CREATE TABLE emp (id INT, dept VARCHAR, salary INT)");
        db.execute("INSERT INTO emp VALUES (1, 'eng', 100)");
        db.execute("INSERT INTO emp VALUES (2, 'eng', 150)");
        db.execute("INSERT INTO emp VALUES (3, 'eng', 200)");
        db.execute("INSERT INTO emp VALUES (4, 'hr', 90)");

        QueryResult res = db.execute(
            "SELECT id, salary, "
            + "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn, "
            + "LAG(salary, 1, 0) OVER (PARTITION BY dept ORDER BY salary DESC) AS prev_sal "
            + "FROM emp WHERE dept = 'eng' ORDER BY salary DESC LIMIT 2");

        assertTrue(res.isSuccess(), res.getError());
        assertEquals(2, res.getRows().size());
        assertEquals(3, ((Number) res.getRows().get(0).getValue("id")).intValue());
        assertEquals(1L, ((Number) res.getRows().get(0).getValue("rn")).longValue());
        assertEquals(0, ((Number) res.getRows().get(0).getValue("prev_sal")).intValue());
        assertEquals(2, ((Number) res.getRows().get(1).getValue("id")).intValue());
        assertEquals(2L, ((Number) res.getRows().get(1).getValue("rn")).longValue());
        assertEquals(200, ((Number) res.getRows().get(1).getValue("prev_sal")).intValue());
    }

    @Test
    void testDirectWindowOperatorVolcanoPipeline() {
        List<String> cols = List.of("id", "dept", "val");
        List<Tuple> input = List.of(
            new Tuple(List.of(1, "d1", 10), cols),
            new Tuple(List.of(2, "d1", 20), cols),
            new Tuple(List.of(3, "d2", 50), cols)
        );
        MemoryScanOperator scan = new MemoryScanOperator(input, cols);
        List<WindowFunctionCall> calls = List.of(
            new WindowFunctionCall("ROW_NUMBER", null, 1, null,
                List.of("dept"), List.of(new WindowOrderItem("val", false)), "rn"),
            new WindowFunctionCall("LEAD", "val", 1, -99,
                List.of("dept"), List.of(new WindowOrderItem("val", false)), "next_val")
        );

        try (WindowOperator op = new WindowOperator(scan, List.of("id", "val"), calls)) {
            assertEquals(List.of("id", "val", "rn", "next_val"), op.getOutputColumns());
            assertSame(scan, op.getChild());
            op.open();
            List<Tuple> out = new ArrayList<>();
            TupleSlot slot;
            while ((slot = op.next()) != null && !slot.isEmpty()) {
                out.add(slot.getTuple());
            }
            assertEquals(3, out.size());
            assertEquals(1L, out.get(0).getValue("rn"));
            assertEquals(20, out.get(0).getValue("next_val"));
            assertEquals(-99, out.get(1).getValue("next_val"));
            assertEquals(-99, out.get(2).getValue("next_val"));
        }
    }

    @Test
    void testConcurrentWindowQueriesUnderVirtualThreads() throws Exception {
        db.execute("CREATE TABLE trades (id INT, sym VARCHAR, px INT)");
        for (int i = 1; i <= 100; i++) {
            db.execute("INSERT INTO trades VALUES (" + i + ", 'S" + (i % 4) + "', " + (i * 10) + ")");
        }

        try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int t = 0; t < 16; t++) {
                futures.add(exec.submit(() -> {
                    QueryResult qr = db.execute(
                        "SELECT id, ROW_NUMBER() OVER (PARTITION BY sym ORDER BY px DESC) AS rn, "
                        + "LAG(px, 1, 0) OVER (PARTITION BY sym ORDER BY px DESC) AS prev_px FROM trades");
                    assertTrue(qr.isSuccess(), qr.getError());
                    return qr.getRows().size();
                }));
            }
            for (Future<Integer> f : futures) {
                assertEquals(100, f.get());
            }
        }
    }
}
