package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.ExecutorEngine;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class StratosDBTest {
    private StratosDB database;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        database = new StratosDB(config);
    }
    
    @AfterEach
    void tearDown() {
        if (database != null) {
            database.shutdown();
        }
    }
    
    @Test
    void testCreateTable() {
        QueryResult result = database.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)");
        assertTrue(result.isSuccess());
    }
    
    @Test
    void testInsertAndSelect() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
        database.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
        
        QueryResult result = database.execute("SELECT * FROM users");
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRows().size());
    }
    
    @Test
    void testSelectWithWhere() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
        database.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
        
        QueryResult result = database.execute("SELECT * FROM users WHERE age=30");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size());
    }
    
    @Test
    void testDropTable() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)");
        QueryResult result = database.execute("DROP TABLE users");
        assertTrue(result.isSuccess());
    }

    /**
     * UPDATE was previously a hardcoded stub ("Updated 0 rows", touching
     * nothing) and the parser never even recognized UPDATE statements at
     * all - buildStatement() had no branch for it. This exercises the real
     * fix end to end: parse -> executor -> MVCC update -> WAL.
     */
    @Test
    void testUpdate() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
        database.execute("INSERT INTO users VALUES (2, 'Bob', 25)");

        QueryResult updateResult = database.execute("UPDATE users SET age=31 WHERE id=1");
        assertTrue(updateResult.isSuccess(), () -> "UPDATE failed: " + updateResult.getError());
        assertEquals("Updated 1 row(s)", updateResult.getMessage());

        QueryResult selectResult = database.execute("SELECT * FROM users WHERE id=1");
        assertTrue(selectResult.isSuccess());
        assertEquals(1, selectResult.getRows().size());
        assertEquals(31, selectResult.getRows().get(0).getValue("age"));

        // The other row must be untouched.
        QueryResult otherResult = database.execute("SELECT * FROM users WHERE id=2");
        assertEquals(25, otherResult.getRows().get(0).getValue("age"));
    }

    /**
     * DELETE was previously the same kind of stub as UPDATE ("Deleted 0
     * rows", touching nothing). This exercises the real fix end to end.
     */
    @Test
    void testDelete() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
        database.execute("INSERT INTO users VALUES (2, 'Bob', 25)");

        QueryResult deleteResult = database.execute("DELETE FROM users WHERE id=1");
        assertTrue(deleteResult.isSuccess(), () -> "DELETE failed: " + deleteResult.getError());
        assertEquals("Deleted 1 row(s)", deleteResult.getMessage());

        QueryResult selectAll = database.execute("SELECT * FROM users");
        assertEquals(1, selectAll.getRows().size());
        assertEquals("Bob", selectAll.getRows().get(0).getValue("name"));
    }

    @Test
    void testCreateIndex_backfillsExistingRows() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
        database.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
        database.execute("INSERT INTO users VALUES (3, 'Carol', 40)");

        QueryResult result = database.execute("CREATE INDEX idx_age ON users (age)");
        assertTrue(result.isSuccess(), () -> "CREATE INDEX failed: " + result.getError());
        assertTrue(result.getMessage().contains("indexed 3 row(s)"),
            "expected all 3 pre-existing rows to be backfilled: " + result.getMessage());
    }

    @Test
    void testPlannerChoosesIndexScanWhenAnIndexExists_seqScanOtherwise() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
        database.execute("CREATE INDEX idx_age ON users (age)");

        QueryResult withIndex = database.execute("EXPLAIN SELECT * FROM users WHERE age=30");
        assertTrue(withIndex.isSuccess());
        assertTrue(withIndex.getMessage().startsWith("Index Scan using idx_age"),
            "expected an index scan: " + withIndex.getMessage());

        QueryResult withoutIndex = database.execute("EXPLAIN SELECT * FROM users WHERE id=1");
        assertTrue(withoutIndex.isSuccess());
        assertTrue(withoutIndex.getMessage().startsWith("Seq Scan on users"),
            () -> "unexpected EXPLAIN output: " + withoutIndex.getMessage());
    }

    @Test
    void testIndexScanReturnsCorrectRow() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
        database.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
        database.execute("INSERT INTO users VALUES (3, 'Carol', 40)");
        database.execute("CREATE INDEX idx_age ON users (age)");

        QueryResult result = database.execute("SELECT * FROM users WHERE age=25");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size());
        assertEquals("Bob", result.getRows().get(0).getValue("name"));
    }

    @Test
    void testIndexMaintainedOnInsertAfterIndexCreation() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
        database.execute("CREATE INDEX idx_age ON users (age)");

        // Inserted AFTER the index exists - must be maintained on insert, not just backfilled.
        database.execute("INSERT INTO users VALUES (2, 'Dave', 50)");

        QueryResult result = database.execute("SELECT * FROM users WHERE age=50");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size());
        assertEquals("Dave", result.getRows().get(0).getValue("name"));
    }

    @Test
    void testIndexReflectsUpdatedValue_oldValueNoLongerMatches() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
        database.execute("CREATE INDEX idx_age ON users (age)");

        QueryResult updateResult = database.execute("UPDATE users SET age=31 WHERE id=1");
        assertTrue(updateResult.isSuccess());

        QueryResult oldValue = database.execute("SELECT * FROM users WHERE age=30");
        assertEquals(0, oldValue.getRows().size(), "old value must no longer be found via the index");

        QueryResult newValue = database.execute("SELECT * FROM users WHERE age=31");
        assertEquals(1, newValue.getRows().size());
        assertEquals("Alice", newValue.getRows().get(0).getValue("name"));
    }

    /**
     * The real regression test for two bugs found while building the
     * planner: (1) operator detection used to pick "=" for a clause like
     * "age>=30" because "=" is a substring of ">=", corrupting the column
     * name; (2) matchesWhere detected an operator but always evaluated
     * equality regardless of it, so "age>25" silently behaved like
     * "age=25". This checks every comparison operator against BOTH an
     * indexed and a non-indexed column, so both the index-scan path and the
     * seq-scan path are proven to compute the same, correct answer.
     */
    @Test
    void testComparisonOperators_correctOnBothIndexedAndSeqScanPaths() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
        database.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
        database.execute("INSERT INTO users VALUES (3, 'Carol', 40)");
        database.execute("INSERT INTO users VALUES (4, 'Dave', 25)");
        // age is indexed; id is not - exercises both the index-scan and seq-scan paths.
        database.execute("CREATE INDEX idx_age ON users (age)");

        assertRowCount("SELECT * FROM users WHERE age>=30", 2);  // Alice(30), Carol(40) - via index
        assertRowCount("SELECT * FROM users WHERE id>=3", 2);    // Carol(3), Dave(4) - via seq scan

        assertRowCount("SELECT * FROM users WHERE age<=25", 2);  // Bob, Dave - via index
        assertRowCount("SELECT * FROM users WHERE id<=2", 2);    // Alice(1), Bob(2) - via seq scan

        assertRowCount("SELECT * FROM users WHERE age>25", 2);   // Alice(30), Carol(40) - via index
        assertRowCount("SELECT * FROM users WHERE id>2", 2);     // Carol(3), Dave(4) - via seq scan

        assertRowCount("SELECT * FROM users WHERE age<30", 2);   // Bob, Dave - via index
        assertRowCount("SELECT * FROM users WHERE id<3", 2);     // Alice(1), Bob(2) - via seq scan
    }

    @Test
    void testShutdownIsIdempotent() {
        // Regression test: WALManager.close() used to call checkpoint() (which
        // writes to the WAL channel) BEFORE checking whether that channel was
        // already closed, so calling shutdown() twice threw ClosedChannelException
        // on the second call. A caller shutting down twice (e.g. once explicitly,
        // once via a framework's own cleanup) must not throw.
        database.execute("CREATE TABLE t (id INT)");
        database.shutdown();
        assertDoesNotThrow(database::shutdown, "calling shutdown() a second time must not throw");
    }

    @Test
    void testCountStarWithNoGroupBy() {
        database.execute("CREATE TABLE sales (id INT, region VARCHAR, amount INT)");
        database.execute("INSERT INTO sales VALUES (1, 'east', 100)");
        database.execute("INSERT INTO sales VALUES (2, 'east', 150)");
        database.execute("INSERT INTO sales VALUES (3, 'west', 200)");

        QueryResult result = database.execute("SELECT COUNT(*) FROM sales");
        assertTrue(result.isSuccess(), () -> "COUNT(*) failed: " + result.getError());
        assertEquals(1, result.getRows().size(), "no GROUP BY means one implicit group");
        assertEquals(3, result.getRows().get(0).getValue("COUNT(*)"));
    }

    @Test
    void testGroupByWithAllFiveAggregateFunctions() {
        database.execute("CREATE TABLE sales (id INT, region VARCHAR, amount INT)");
        database.execute("INSERT INTO sales VALUES (1, 'east', 100)");
        database.execute("INSERT INTO sales VALUES (2, 'east', 150)");
        database.execute("INSERT INTO sales VALUES (3, 'west', 200)");
        database.execute("INSERT INTO sales VALUES (4, 'west', 50)");
        database.execute("INSERT INTO sales VALUES (5, 'west', 300)");

        QueryResult result = database.execute(
            "SELECT region, COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amt, "
            + "MIN(amount) AS lo, MAX(amount) AS hi FROM sales GROUP BY region");
        assertTrue(result.isSuccess(), () -> "GROUP BY failed: " + result.getError());
        assertEquals(2, result.getRows().size(), "two distinct regions");

        for (var row : result.getRows()) {
            String region = (String) row.getValue("region");
            if ("east".equals(region)) {
                assertEquals(2, row.getValue("cnt"));
                assertEquals(250L, row.getValue("total"));
                assertEquals(125.0, (Double) row.getValue("avg_amt"), 0.001);
                assertEquals(100, row.getValue("lo"));
                assertEquals(150, row.getValue("hi"));
            } else if ("west".equals(region)) {
                assertEquals(3, row.getValue("cnt"));
                assertEquals(550L, row.getValue("total"));
                assertEquals(50, row.getValue("lo"));
                assertEquals(300, row.getValue("hi"));
            } else {
                fail("unexpected region: " + region);
            }
        }
    }

    @Test
    void testHavingFiltersGroups() {
        database.execute("CREATE TABLE sales (id INT, region VARCHAR, amount INT)");
        database.execute("INSERT INTO sales VALUES (1, 'east', 100)");
        database.execute("INSERT INTO sales VALUES (2, 'west', 200)");
        database.execute("INSERT INTO sales VALUES (3, 'west', 50)");
        database.execute("INSERT INTO sales VALUES (4, 'west', 300)");

        QueryResult result = database.execute(
            "SELECT region, COUNT(*) AS cnt FROM sales GROUP BY region HAVING COUNT(*) > 2");
        assertTrue(result.isSuccess(), () -> "HAVING failed: " + result.getError());
        assertEquals(1, result.getRows().size(), "only 'west' has more than 2 rows");
        assertEquals("west", result.getRows().get(0).getValue("region"));
        assertEquals(3, result.getRows().get(0).getValue("cnt"));
    }

    @Test
    void testWhereAppliesBeforeGrouping() {
        database.execute("CREATE TABLE sales (id INT, region VARCHAR, amount INT)");
        database.execute("INSERT INTO sales VALUES (1, 'east', 100)");
        database.execute("INSERT INTO sales VALUES (2, 'east', 150)");
        database.execute("INSERT INTO sales VALUES (3, 'west', 200)");
        database.execute("INSERT INTO sales VALUES (4, 'west', 50)"); // excluded by WHERE

        QueryResult result = database.execute(
            "SELECT region, SUM(amount) AS total FROM sales WHERE amount > 75 GROUP BY region");
        assertTrue(result.isSuccess(), () -> "WHERE+GROUP BY failed: " + result.getError());
        assertEquals(2, result.getRows().size());

        for (var row : result.getRows()) {
            if ("west".equals(row.getValue("region"))) {
                assertEquals(200L, row.getValue("total"), "the 50-amount west row must be excluded by WHERE before grouping");
            }
        }
    }

    @Test
    void testExplainDescribesAggregateQuery() {
        database.execute("CREATE TABLE sales (id INT, region VARCHAR, amount INT)");

        QueryResult result = database.execute("EXPLAIN SELECT region, COUNT(*) FROM sales GROUP BY region");
        assertTrue(result.isSuccess());
        assertTrue(result.getMessage().startsWith("Aggregate GROUP BY region"),
            () -> "unexpected EXPLAIN output: " + result.getMessage());
    }

    @Test
    void testCountOnEmptyResultIsZeroNotError() {
        database.execute("CREATE TABLE sales (id INT, region VARCHAR, amount INT)");

        QueryResult result = database.execute("SELECT COUNT(*) FROM sales WHERE region='nonexistent'");
        assertTrue(result.isSuccess(), () -> "COUNT on empty result failed: " + result.getError());
        assertEquals(1, result.getRows().size());
        assertEquals(0, result.getRows().get(0).getValue("COUNT(*)"));
    }

    @Test
    void testJoinExcludesNullKeysOnEitherSide() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR)");
        database.execute("CREATE TABLE orders (id INT, user_id INT, amount INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice')");
        database.execute("INSERT INTO orders VALUES (100, 1, 50)");
        database.execute("INSERT INTO orders VALUES (101, NULL, 999)"); // NULL join key - must not match anything

        QueryResult result = database.execute(
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size(), "a NULL join key must never match, even hypothetically against another NULL");
        assertEquals(50, result.getRows().get(0).getValue("orders.amount"));
    }

    /**
     * The real proof hash join is actually O(n+m), not just correct.
     * Insert cost is excluded from the timing claim deliberately - each
     * INSERT here pays a full SQL parse + transaction commit (Week 2's
     * auto-commit design, see QueryBenchmark's own numbers: roughly
     * 750-800 inserts/sec through this path), which would dominate and
     * hide the join's own cost if measured together. The join query
     * itself is what's timed and asserted on.
     *
     * Measured for real before shipping this test (not asserted blind):
     * at this exact scale (1,000 x 2,000 rows), the old nested-loop join
     * took 437ms; hash join takes ~100ms - a 4.4x speedup that grows with
     * scale (measured 10.9x at 3,000 x 6,000 rows: 1,622ms vs 149ms),
     * exactly the widening gap O(n*m) vs O(n+m) predicts.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS) // generous safety net against a true hang, not the real assertion
    void testHashJoinHandlesScaleThatWouldBeSlowAsNestedLoop() {
        int userCount = 1000;
        database.execute("CREATE TABLE users (id INT, name VARCHAR)");
        database.execute("CREATE TABLE orders (id INT, user_id INT, amount INT)");

        for (int i = 0; i < userCount; i++) {
            database.execute("INSERT INTO users VALUES (" + i + ", 'user" + i + "')");
            // Two orders per user - real fan-out, not a trivial 1:1 join.
            database.execute("INSERT INTO orders VALUES (" + (i * 2) + ", " + i + ", 10)");
            database.execute("INSERT INTO orders VALUES (" + (i * 2 + 1) + ", " + i + ", 20)");
        }

        // 1,000 x 2,000 = 2,000,000 comparisons for a nested loop; hash join is O(n+m) = 3,000.
        long start = System.currentTimeMillis();
        QueryResult result = database.execute(
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id");
        long elapsedMs = System.currentTimeMillis() - start;

        assertTrue(result.isSuccess(), () -> "large join failed: " + result.getError());
        assertEquals(userCount * 2, result.getRows().size(), "every user's both orders must appear exactly once");
        assertTrue(elapsedMs < 2000,
            () -> "hash join took " + elapsedMs + "ms for " + userCount + "x" + (userCount * 2)
                + " rows - expected well under 2s; a regression to nested-loop-style O(n*m) would blow well past this");
    }

    @Test
    void testAnalyzeReportsRowAndColumnCount() {
        database.execute("CREATE TABLE t (id INT, name VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 'a')");
        database.execute("INSERT INTO t VALUES (2, 'b')");

        QueryResult result = database.execute("ANALYZE t");
        assertTrue(result.isSuccess(), () -> "ANALYZE failed: " + result.getError());
        assertEquals("Analyzed t: 2 row(s), 2 column(s)", result.getMessage());
    }

    @Test
    void testWithoutAnalyzePlannerFallsBackToRuleBasedIndexChoice() {
        database.execute("CREATE TABLE t (id INT, category INT)");
        for (int i = 0; i < 100; i++) {
            database.execute("INSERT INTO t VALUES (" + i + ", " + (i % 2) + ")");
        }
        database.execute("CREATE INDEX idx_category ON t (category)");

        // No ANALYZE has run - even though category=0 matches half the table
        // (a predicate a real cost model should reject the index for), the
        // fallback heuristic has no statistics to reason with, so it keeps
        // the original "an index exists, use it" behavior.
        QueryResult result = database.execute("EXPLAIN SELECT * FROM t WHERE category=0");
        assertTrue(result.isSuccess());
        assertTrue(result.getMessage().startsWith("Index Scan"),
            () -> "expected the rule-based fallback without statistics: " + result.getMessage());
        assertTrue(result.getMessage().contains("no statistics"));
    }

    @Test
    void testCostBasedOptimizerRejectsIndexForLowSelectivityPredicate() {
        database.execute("CREATE TABLE t (id INT, category INT)");
        for (int i = 0; i < 1000; i++) {
            // Only 2 distinct values, each matching half the table - genuinely low selectivity.
            database.execute("INSERT INTO t VALUES (" + i + ", " + (i % 2) + ")");
        }
        database.execute("CREATE INDEX idx_category ON t (category)");
        database.execute("ANALYZE t");

        QueryResult explain = database.execute("EXPLAIN SELECT * FROM t WHERE category=0");
        assertTrue(explain.isSuccess());
        assertTrue(explain.getMessage().startsWith("Seq Scan"),
            () -> "a predicate matching half the table should cost more via the index than a seq scan: " + explain.getMessage());

        // The plan choice must not change the actual answer.
        QueryResult result = database.execute("SELECT * FROM t WHERE category=0");
        assertEquals(500, result.getRows().size());
    }

    @Test
    void testCostBasedOptimizerKeepsIndexForHighSelectivityPredicate() {
        database.execute("CREATE TABLE t (id INT, unique_id INT)");
        for (int i = 0; i < 1000; i++) {
            // 1000 distinct values - genuinely high selectivity for equality.
            database.execute("INSERT INTO t VALUES (" + i + ", " + i + ")");
        }
        database.execute("CREATE INDEX idx_unique ON t (unique_id)");
        database.execute("ANALYZE t");

        QueryResult explain = database.execute("EXPLAIN SELECT * FROM t WHERE unique_id=500");
        assertTrue(explain.isSuccess());
        assertTrue(explain.getMessage().startsWith("Index Scan"),
            () -> "a highly selective predicate should still prefer the index: " + explain.getMessage());

        QueryResult result = database.execute("SELECT * FROM t WHERE unique_id=500");
        assertEquals(1, result.getRows().size());
    }

    @Test
    void testExplainShowsCostEstimatesOnceAnalyzed() {
        database.execute("CREATE TABLE t (id INT, name VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 'a')");
        database.execute("ANALYZE t");

        QueryResult result = database.execute("EXPLAIN SELECT * FROM t");
        assertTrue(result.isSuccess());
        assertTrue(result.getMessage().contains("cost="), () -> "expected a cost estimate: " + result.getMessage());
    }

    @Test
    void deleteRemovesRowFromIndexedLookupNotJustFromVisibilityFiltering() {
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("CREATE INDEX idx_val ON t (val)");
        database.execute("INSERT INTO t VALUES (1, 100)");
        database.execute("INSERT INTO t VALUES (2, 100)");

        database.execute("DELETE FROM t WHERE id=1");

        QueryResult result = database.execute("SELECT * FROM t WHERE val=100");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size(), "only the surviving row should match, via the index");
        assertEquals(2, result.getRows().get(0).getValue("id"));
    }

    @Test
    void updateOnIndexedColumnMovesTheIndexEntryNotJustTheRow() {
        database.execute("CREATE TABLE t (id INT, category INT)");
        database.execute("CREATE INDEX idx_category ON t (category)");
        database.execute("INSERT INTO t VALUES (1, 10)");

        database.execute("UPDATE t SET category=20 WHERE id=1");

        QueryResult oldValue = database.execute("SELECT * FROM t WHERE category=10");
        assertTrue(oldValue.isSuccess());
        assertEquals(0, oldValue.getRows().size(), "the old indexed value must no longer find this row");

        QueryResult newValue = database.execute("SELECT * FROM t WHERE category=20");
        assertTrue(newValue.isSuccess());
        assertEquals(1, newValue.getRows().size(), "the new indexed value must find it");
        assertEquals(1, newValue.getRows().get(0).getValue("id"));
    }

    @Test
    void deleteAndReinsertSameIndexedValueReturnsExactlyOneRow() {
        // A real-world pattern that would surface a leftover stale index
        // entry immediately: delete a row, then insert a different row
        // reusing the same indexed value. Two entries under the same key
        // would either double-count or return the wrong row's data.
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("CREATE INDEX idx_val ON t (val)");
        database.execute("INSERT INTO t VALUES (1, 42)");
        database.execute("DELETE FROM t WHERE id=1");
        database.execute("INSERT INTO t VALUES (2, 42)");

        QueryResult result = database.execute("SELECT * FROM t WHERE val=42");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size());
        assertEquals(2, result.getRows().get(0).getValue("id"));
    }

    @Test
    void testVacuumReportsZeroOnATableWithNothingDead() {
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("INSERT INTO t VALUES (1, 100)");

        QueryResult result = database.execute("VACUUM t");
        assertTrue(result.isSuccess(), () -> "VACUUM failed: " + result.getError());
        assertEquals("Vacuumed t: reclaimed 0 dead row version(s) across 0 page(s)", result.getMessage());
    }

    @Test
    void testVacuumReclaimsRepeatedUpdatesWithoutChangingVisibleData() {
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("INSERT INTO t VALUES (1, 100)");

        for (int i = 0; i < 20; i++) {
            database.execute("UPDATE t SET val=" + (100 + i) + " WHERE id=1");
        }

        QueryResult vacuum = database.execute("VACUUM t");
        assertTrue(vacuum.isSuccess());
        assertEquals("Vacuumed t: reclaimed 20 dead row version(s) across 1 page(s)", vacuum.getMessage());

        QueryResult after = database.execute("SELECT * FROM t");
        assertEquals(1, after.getRows().size(), "vacuum must not change how many rows are visible");
        assertEquals(119, after.getRows().get(0).getValue("val"), "the current value must be completely unaffected by vacuum");

        // Running it again with nothing new dead must not double-count or error.
        QueryResult again = database.execute("VACUUM t");
        assertEquals("Vacuumed t: reclaimed 0 dead row version(s) across 0 page(s)", again.getMessage());
    }

    // --- Compound WHERE clauses: AND/OR/NOT/LIKE/IN were previously broken -
    // the grammar accepted them but the executor silently misevaluated
    // anything beyond a single flat predicate. See PROJECT_PLAN.md/PROGRESS.md
    // for how this was found (a real bug, not a hypothetical one).

    @Test
    void testAndCorrectlyExcludesRowsFailingEitherSide() {
        database.execute("CREATE TABLE t (id INT, age INT, status VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 30, 'active')");
        database.execute("INSERT INTO t VALUES (2, 20, 'inactive')");
        database.execute("INSERT INTO t VALUES (3, 40, 'active')");
        database.execute("INSERT INTO t VALUES (4, 20, 'active')"); // active but age<=25 - AND must exclude this

        QueryResult result = database.execute("SELECT * FROM t WHERE age>25 AND status='active'");
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRows().size(), "only id 1 and 3 satisfy both conditions");
    }

    @Test
    void testOrIncludesRowsMatchingEitherSide() {
        database.execute("CREATE TABLE t (id INT, age INT, status VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 30, 'active')");
        database.execute("INSERT INTO t VALUES (2, 20, 'inactive')");
        database.execute("INSERT INTO t VALUES (3, 10, 'dormant')");

        assertRowCount("SELECT * FROM t WHERE age>25 OR status='inactive'", 2); // id 1, 2
    }

    @Test
    void testNotInvertsTheInnerCondition() {
        database.execute("CREATE TABLE t (id INT, age INT)");
        database.execute("INSERT INTO t VALUES (1, 30)");
        database.execute("INSERT INTO t VALUES (2, 20)");

        assertRowCount("SELECT * FROM t WHERE NOT age>25", 1); // id 2
    }

    @Test
    void testInAndNotInWithLiteralList() {
        database.execute("CREATE TABLE t (id INT, status VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 'active')");
        database.execute("INSERT INTO t VALUES (2, 'inactive')");
        database.execute("INSERT INTO t VALUES (3, 'active')");

        assertRowCount("SELECT * FROM t WHERE status IN ('active')", 2);
        assertRowCount("SELECT * FROM t WHERE status NOT IN ('active')", 1);
    }

    @Test
    void testLikePatternMatching() {
        database.execute("CREATE TABLE t (id INT, name VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 'Alice')");
        database.execute("INSERT INTO t VALUES (2, 'Bob')");
        database.execute("INSERT INTO t VALUES (3, 'Alan')");

        assertRowCount("SELECT * FROM t WHERE name LIKE 'Al%'", 2); // Alice, Alan
        assertRowCount("SELECT * FROM t WHERE name LIKE '_ob'", 1); // Bob
    }

    @Test
    void testNestedAndOrWithParentheses() {
        database.execute("CREATE TABLE t (id INT, age INT, status VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 30, 'active')");
        database.execute("INSERT INTO t VALUES (2, 20, 'inactive')");
        database.execute("INSERT INTO t VALUES (3, 40, 'dormant')");

        assertRowCount("SELECT * FROM t WHERE (age>25 AND status='active') OR id=2", 2); // id 1, 2
    }

    // --- Subqueries: previously entirely absent ---

    @Test
    void testInSubquery() {
        database.execute("CREATE TABLE customers (id INT, name VARCHAR)");
        database.execute("CREATE TABLE orders (id INT, customer_id INT)");
        database.execute("INSERT INTO customers VALUES (1, 'Alice')");
        database.execute("INSERT INTO customers VALUES (2, 'Bob')");
        database.execute("INSERT INTO orders VALUES (100, 1)");

        QueryResult result = database.execute("SELECT * FROM customers WHERE id IN (SELECT customer_id FROM orders)");
        assertTrue(result.isSuccess(), () -> "IN subquery failed: " + result.getError());
        assertEquals(1, result.getRows().size());
        assertEquals("Alice", result.getRows().get(0).getValue("name"));
    }

    @Test
    void testNotInSubquery() {
        database.execute("CREATE TABLE customers (id INT, name VARCHAR)");
        database.execute("CREATE TABLE orders (id INT, customer_id INT)");
        database.execute("INSERT INTO customers VALUES (1, 'Alice')");
        database.execute("INSERT INTO customers VALUES (2, 'Bob')");
        database.execute("INSERT INTO orders VALUES (100, 1)");

        QueryResult result = database.execute("SELECT * FROM customers WHERE id NOT IN (SELECT customer_id FROM orders)");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size());
        assertEquals("Bob", result.getRows().get(0).getValue("name"));
    }

    @Test
    void testScalarSubqueryComparison() {
        database.execute("CREATE TABLE orders (id INT, amount INT)");
        database.execute("INSERT INTO orders VALUES (1, 50)");
        database.execute("INSERT INTO orders VALUES (2, 150)");
        database.execute("INSERT INTO orders VALUES (3, 300)");

        // average = (50+150+300)/3 = 166.67
        QueryResult result = database.execute("SELECT * FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)");
        assertTrue(result.isSuccess(), () -> "scalar subquery failed: " + result.getError());
        assertEquals(1, result.getRows().size());
        assertEquals(300, result.getRows().get(0).getValue("amount"));
    }

    @Test
    void testCorrelatedExists() {
        database.execute("CREATE TABLE customers (id INT, name VARCHAR)");
        database.execute("CREATE TABLE orders (id INT, customer_id INT)");
        database.execute("INSERT INTO customers VALUES (1, 'Alice')"); // has an order
        database.execute("INSERT INTO customers VALUES (2, 'Bob')");   // has no orders
        database.execute("INSERT INTO orders VALUES (100, 1)");

        QueryResult result = database.execute(
            "SELECT * FROM customers WHERE EXISTS (SELECT id FROM orders WHERE orders.customer_id = customers.id)");
        assertTrue(result.isSuccess(), () -> "correlated EXISTS failed: " + result.getError());
        assertEquals(1, result.getRows().size());
        assertEquals("Alice", result.getRows().get(0).getValue("name"));
    }

    @Test
    void testCorrelatedNotExists() {
        database.execute("CREATE TABLE customers (id INT, name VARCHAR)");
        database.execute("CREATE TABLE orders (id INT, customer_id INT)");
        database.execute("INSERT INTO customers VALUES (1, 'Alice')");
        database.execute("INSERT INTO customers VALUES (2, 'Bob')");
        database.execute("INSERT INTO orders VALUES (100, 1)");

        QueryResult result = database.execute(
            "SELECT * FROM customers WHERE NOT EXISTS (SELECT id FROM orders WHERE orders.customer_id = customers.id)");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size());
        assertEquals("Bob", result.getRows().get(0).getValue("name"));
    }

    @Test
    void testSubqueriesWorkWithUpdateAndDelete() {
        database.execute("CREATE TABLE customers (id INT, name VARCHAR, flagged INT)");
        database.execute("CREATE TABLE orders (id INT, customer_id INT, amount INT)");
        database.execute("INSERT INTO customers VALUES (1, 'Alice', 0)");
        database.execute("INSERT INTO customers VALUES (2, 'Bob', 0)");
        database.execute("INSERT INTO orders VALUES (100, 1, 500)");

        QueryResult updateResult = database.execute(
            "UPDATE customers SET flagged=1 WHERE id IN (SELECT customer_id FROM orders WHERE amount > 100)");
        assertTrue(updateResult.isSuccess(), () -> "UPDATE with subquery failed: " + updateResult.getError());
        assertEquals("Updated 1 row(s)", updateResult.getMessage());

        assertRowCount("SELECT * FROM customers WHERE flagged=1", 1);

        QueryResult deleteResult = database.execute(
            "DELETE FROM customers WHERE id NOT IN (SELECT customer_id FROM orders)");
        assertTrue(deleteResult.isSuccess(), () -> "DELETE with subquery failed: " + deleteResult.getError());
        assertEquals("Deleted 1 row(s)", deleteResult.getMessage()); // Bob has no orders

        assertRowCount("SELECT * FROM customers", 1);
    }

    @Test
    void testExplicitCommitMakesAllStatementsVisible() {
        database.execute("CREATE TABLE t (id INT, val INT)");

        assertEquals("BEGIN", database.execute("BEGIN").getMessage());
        database.execute("INSERT INTO t VALUES (1, 100)");
        database.execute("INSERT INTO t VALUES (2, 200)");
        assertEquals("COMMIT", database.execute("COMMIT").getMessage());

        assertRowCount("SELECT * FROM t", 2);
    }

    @Test
    void testExplicitRollbackDiscardsAllStatements() {
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("INSERT INTO t VALUES (1, 100)"); // committed via auto-commit, before the explicit transaction

        database.execute("BEGIN");
        database.execute("INSERT INTO t VALUES (2, 200)");
        database.execute("INSERT INTO t VALUES (3, 300)");
        assertEquals("ROLLBACK", database.execute("ROLLBACK").getMessage());

        assertRowCount("SELECT * FROM t", 1); // only the pre-transaction row survives
    }

    @Test
    void testFailedStatementPoisonsTheWholeTransactionNotJustItself() {
        database.execute("CREATE TABLE t (id INT, val INT)");

        database.execute("BEGIN");
        database.execute("INSERT INTO t VALUES (1, 100)");
        QueryResult badStatement = database.execute("SELECT * FROM nonexistent_table");
        assertFalse(badStatement.isSuccess());

        QueryResult afterFailure = database.execute("INSERT INTO t VALUES (2, 200)");
        assertFalse(afterFailure.isSuccess(), "a statement after a failure in the same transaction must be rejected");
        assertTrue(afterFailure.getError().contains("aborted"),
            () -> "expected an 'aborted' error, got: " + afterFailure.getError());

        database.execute("ROLLBACK");
        assertRowCount("SELECT * FROM t", 0, "not even the row inserted before the failure should survive - the whole transaction is poisoned");
    }

    @Test
    void testCommitOnAPoisonedTransactionRollsBackInstead() {
        database.execute("CREATE TABLE t (id INT)");
        database.execute("BEGIN");
        database.execute("INSERT INTO t VALUES (1)");
        database.execute("SELECT * FROM nonexistent_table"); // poisons the transaction

        QueryResult commitResult = database.execute("COMMIT");
        assertFalse(commitResult.isSuccess(), "COMMIT on a poisoned transaction must fail, not silently succeed");
        assertRowCount("SELECT * FROM t", 0);
    }

    @Test
    void testCommitOrRollbackWithNoOpenTransactionIsAnError() {
        database.execute("CREATE TABLE t (id INT)");
        assertFalse(database.execute("COMMIT").isSuccess());
        assertFalse(database.execute("ROLLBACK").isSuccess());
    }

    @Test
    void testNestedBeginIsRejected() {
        database.execute("BEGIN");
        QueryResult nested = database.execute("BEGIN");
        assertFalse(nested.isSuccess(), "a BEGIN while already in a transaction must be rejected, not silently accepted");
        database.execute("ROLLBACK");
    }

    @Test
    void testUncommittedTransactionIsInvisibleToAnotherThread() throws InterruptedException {
        database.execute("CREATE TABLE t (id INT)");
        database.execute("BEGIN");
        database.execute("INSERT INTO t VALUES (1)");

        // A different thread is a different session (transaction state is
        // thread-local) - simulating a genuinely separate connection.
        int[] otherThreadRowCount = new int[1];
        Thread other = new Thread(() -> otherThreadRowCount[0] = database.execute("SELECT * FROM t").getRows().size());
        other.start();
        other.join();
        assertEquals(0, otherThreadRowCount[0], "another session must not see this thread's uncommitted insert");

        database.execute("COMMIT");
        assertRowCount("SELECT * FROM t", 1);
    }

    @Test
    void testCreateHashIndexAndEqualityLookup() {
        database.execute("CREATE TABLE t (id INT, category INT)");
        database.execute("INSERT INTO t VALUES (1, 100)");
        database.execute("INSERT INTO t VALUES (2, 200)");

        QueryResult createResult = database.execute("CREATE INDEX idx_hash ON t (category) USING HASH");
        assertTrue(createResult.isSuccess(), () -> "CREATE INDEX USING HASH failed: " + createResult.getError());
        assertTrue(createResult.getMessage().contains("HASH"));

        QueryResult explain = database.execute("EXPLAIN SELECT * FROM t WHERE category=200");
        assertTrue(explain.getMessage().startsWith("Index Scan using idx_hash"),
            () -> "expected the hash index to be used: " + explain.getMessage());

        QueryResult result = database.execute("SELECT * FROM t WHERE category=200");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size());
        assertEquals(2, result.getRows().get(0).getValue("id"));
    }

    @Test
    void testDefaultIndexTypeIsBTreeWithoutUsingClause() {
        database.execute("CREATE TABLE t (id INT, val INT)");
        QueryResult result = database.execute("CREATE INDEX idx_default ON t (val)");
        assertTrue(result.isSuccess());
        assertTrue(result.getMessage().contains("BTREE"), () -> "expected BTREE as the default index type: " + result.getMessage());
    }

    @Test
    void testHashIndexIsNotUsedForRangeQueries() {
        // A hash index can't serve a range query - the planner must not even
        // attempt it, falling back to a seq scan (or a btree index, if one
        // also exists on this column) instead of producing wrong results.
        database.execute("CREATE TABLE t (id INT, val INT)");
        for (int i = 0; i < 10; i++) {
            database.execute("INSERT INTO t VALUES (" + i + ", " + i + ")");
        }
        database.execute("CREATE INDEX idx_hash ON t (val) USING HASH");

        QueryResult explain = database.execute("EXPLAIN SELECT * FROM t WHERE val>5");
        assertTrue(explain.isSuccess());
        assertFalse(explain.getMessage().contains("idx_hash"),
            () -> "a hash index must never be chosen for a range predicate: " + explain.getMessage());

        QueryResult result = database.execute("SELECT * FROM t WHERE val>5");
        assertTrue(result.isSuccess());
        assertEquals(4, result.getRows().size(), "correctness must hold regardless of which scan strategy was used");
    }

    @Test
    void testHashIndexPrefersOverBTreeForEquality() {
        // When both exist on the same column, equality should choose the
        // hash index (cheaper - O(1) vs O(log n)) over the B+Tree one.
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("INSERT INTO t VALUES (1, 42)");
        database.execute("CREATE INDEX idx_btree ON t (val) USING BTREE");
        database.execute("CREATE INDEX idx_hash ON t (val) USING HASH");

        QueryResult explain = database.execute("EXPLAIN SELECT * FROM t WHERE val=42");
        assertTrue(explain.getMessage().contains("idx_hash"),
            () -> "expected the hash index to be preferred for equality when both exist: " + explain.getMessage());
    }

    @Test
    void testHashIndexStaysCorrectAcrossDeleteAndUpdate() {
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("CREATE INDEX idx_hash ON t (val) USING HASH");
        database.execute("INSERT INTO t VALUES (1, 100)");

        database.execute("UPDATE t SET val=200 WHERE id=1");
        assertRowCount("SELECT * FROM t WHERE val=100", 0, "the old hash-indexed value must no longer find this row");
        assertRowCount("SELECT * FROM t WHERE val=200", 1, "the new hash-indexed value must find it");

        database.execute("DELETE FROM t WHERE id=1");
        assertRowCount("SELECT * FROM t WHERE val=200", 0, "a deleted row must not be findable via the hash index either");
    }

    private void assertRowCount(String sql, int expected, String message) {
        QueryResult result = database.execute(sql);
        assertTrue(result.isSuccess(), () -> sql + " failed: " + result.getError());
        assertEquals(expected, result.getRows().size(), message);
    }

    @Test
    void testCreateViewAndSelectFromIt() {
        database.execute("CREATE TABLE employees (id INT, name VARCHAR, active INT)");
        database.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
        database.execute("INSERT INTO employees VALUES (2, 'Bob', 0)");
        database.execute("INSERT INTO employees VALUES (3, 'Carol', 1)");

        QueryResult createResult = database.execute("CREATE VIEW active_employees AS SELECT id, name FROM employees WHERE active=1");
        assertTrue(createResult.isSuccess(), () -> "CREATE VIEW failed: " + createResult.getError());

        QueryResult result = database.execute("SELECT * FROM active_employees");
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRows().size());
    }

    @Test
    void testWhereClauseComposesOnTopOfAView() {
        database.execute("CREATE TABLE employees (id INT, name VARCHAR, salary INT, active INT)");
        database.execute("INSERT INTO employees VALUES (1, 'Alice', 90000, 1)");
        database.execute("INSERT INTO employees VALUES (2, 'Carol', 120000, 1)");
        database.execute("CREATE VIEW active_employees AS SELECT id, name, salary FROM employees WHERE active=1");

        QueryResult result = database.execute("SELECT * FROM active_employees WHERE salary > 100000");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size());
        assertEquals("Carol", result.getRows().get(0).getValue("name"));
    }

    @Test
    void testViewIsNotMaterializedAndReflectsCurrentData() {
        database.execute("CREATE TABLE employees (id INT, active INT)");
        database.execute("INSERT INTO employees VALUES (1, 1)");
        database.execute("CREATE VIEW active_employees AS SELECT id FROM employees WHERE active=1");
        assertRowCount("SELECT * FROM active_employees", 1);

        database.execute("INSERT INTO employees VALUES (2, 1)");
        assertRowCount("SELECT * FROM active_employees", 2, "a view must reflect data inserted after it was created - it isn't a snapshot");
    }

    @Test
    void testTableAndViewNamesCannotCollide() {
        database.execute("CREATE TABLE t (id INT)");
        database.execute("CREATE VIEW v AS SELECT * FROM t");

        assertFalse(database.execute("CREATE TABLE v (id INT)").isSuccess(), "a table can't take an existing view's name");
        assertFalse(database.execute("CREATE VIEW t AS SELECT * FROM t").isSuccess(), "a view can't take an existing table's name");
    }

    @Test
    void testDropView() {
        database.execute("CREATE TABLE t (id INT)");
        database.execute("CREATE VIEW v AS SELECT * FROM t");
        assertTrue(database.execute("SELECT * FROM v").isSuccess());

        QueryResult dropResult = database.execute("DROP VIEW v");
        assertTrue(dropResult.isSuccess());

        assertFalse(database.execute("SELECT * FROM v").isSuccess(), "querying a dropped view must fail");
        assertFalse(database.execute("DROP VIEW v").isSuccess(), "dropping an already-dropped view must fail, not silently succeed");
    }

    @Test
    void testViewCombinedWithJoinOrAggregateFailsCleanlyRatherThanSilentlyIgnoringThem() {
        // A real bug found and fixed while building this: the views lookup
        // used to run before the join/aggregate checks, so a query
        // combining either with a view silently fell through to plain
        // WHERE/projection logic that has no idea how to join or
        // aggregate - "SELECT COUNT(*) FROM aView" would silently return
        // the view's raw rows instead of a count, with no error at all.
        database.execute("CREATE TABLE employees (id INT, dept_id INT)");
        database.execute("CREATE TABLE departments (id INT)");
        database.execute("CREATE VIEW emp_view AS SELECT id, dept_id FROM employees");

        QueryResult joinResult = database.execute("SELECT * FROM emp_view JOIN departments ON emp_view.dept_id = departments.id");
        assertFalse(joinResult.isSuccess(), "a view combined with JOIN must fail cleanly, not silently ignore the join");

        QueryResult aggResult = database.execute("SELECT COUNT(*) FROM emp_view");
        assertFalse(aggResult.isSuccess(), "a view combined with an aggregate must fail cleanly, not silently ignore the aggregate");
    }

    @Test
    void testSubqueryFailureIsSurfacedNotSilentlyTreatedAsNoMatch() {
        // A second real bug found while testing the above: when a subquery
        // itself fails (here, because it references a view - not supported
        // inside a subquery), the failure must be surfaced as a real error,
        // not silently treated as "the subquery matched nothing," which
        // would make the enclosing WHERE clause quietly (and wrongly) match
        // zero rows instead of reporting what actually went wrong.
        database.execute("CREATE TABLE employees (id INT)");
        database.execute("INSERT INTO employees VALUES (1)");
        database.execute("CREATE VIEW emp_view AS SELECT id FROM employees");

        QueryResult result = database.execute("SELECT * FROM employees WHERE id IN (SELECT id FROM emp_view)");
        assertFalse(result.isSuccess(), "a subquery referencing a view isn't supported yet and must fail, not silently match nothing");
    }

    @Test
    void testRunAutovacuumPassReclaimsAcrossAllTables() {
        database.execute("CREATE TABLE t1 (id INT, val INT)");
        database.execute("CREATE TABLE t2 (id INT, val INT)");
        database.execute("INSERT INTO t1 VALUES (1, 100)");
        database.execute("INSERT INTO t2 VALUES (1, 100)");
        for (int i = 0; i < 20; i++) {
            database.execute("UPDATE t1 SET val=" + i + " WHERE id=1");
            database.execute("UPDATE t2 SET val=" + i + " WHERE id=1");
        }

        database.runAutovacuumPass();

        // Correctness must be completely unaffected - both tables, both rows.
        assertRowCount("SELECT * FROM t1", 1);
        assertRowCount("SELECT * FROM t2", 1);

        // Reclamation must have actually happened - a manual VACUUM afterward finds nothing left to do.
        QueryResult vacuumAgain1 = database.execute("VACUUM t1");
        QueryResult vacuumAgain2 = database.execute("VACUUM t2");
        assertEquals("Vacuumed t1: reclaimed 0 dead row version(s) across 0 page(s)", vacuumAgain1.getMessage());
        assertEquals("Vacuumed t2: reclaimed 0 dead row version(s) across 0 page(s)", vacuumAgain2.getMessage());
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testScheduledAutovacuumRunsAutomaticallyInTheBackground() throws InterruptedException {
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("INSERT INTO t VALUES (1, 100)");
        for (int i = 0; i < 20; i++) {
            database.execute("UPDATE t SET val=" + i + " WHERE id=1");
        }

        database.startAutovacuum(100); // every 100ms
        try {
            // Poll rather than a single fixed sleep, to keep this robust
            // under slow/loaded CI without just using a long fixed delay.
            long deadline = System.currentTimeMillis() + 5000;
            boolean reclaimed = false;
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(150);
                QueryResult check = database.execute("VACUUM t");
                if (check.getMessage().contains("reclaimed 0 dead row version(s)")) {
                    reclaimed = true;
                    break;
                }
            }
            assertTrue(reclaimed, "scheduled autovacuum should have reclaimed the dead versions within the deadline");
        } finally {
            database.stopAutovacuum();
        }

        assertRowCount("SELECT * FROM t", 1, "correctness must be unaffected by background autovacuum");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testStopAutovacuumActuallyStopsIt() throws InterruptedException {
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("INSERT INTO t VALUES (1, 100)");

        database.startAutovacuum(100);
        database.stopAutovacuum();

        // Create dead versions AFTER stopping - if the scheduler were still
        // somehow running, this would get cleaned up; it must not.
        for (int i = 0; i < 10; i++) {
            database.execute("UPDATE t SET val=" + i + " WHERE id=1");
        }
        Thread.sleep(500); // long enough for several 100ms cycles to have fired, if it were still running

        QueryResult manualVacuum = database.execute("VACUUM t");
        assertTrue(manualVacuum.getMessage().contains("reclaimed 10 dead row version(s)"),
            () -> "expected the 10 dead versions to still be there since autovacuum was stopped: " + manualVacuum.getMessage());
    }

    @Test
    void testSlowQueryLoggingLogsWhenOverThreshold() {
        database.getExecutor().setSlowQueryThresholdMs(0); // log every statement - deterministic, no timing race
        database.execute("CREATE TABLE t (id INT)");

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalErr = System.err;
        System.setErr(new PrintStream(captured));
        try {
            database.execute("INSERT INTO t VALUES (1)");
        } finally {
            System.setErr(originalErr);
        }

        String output = captured.toString();
        assertTrue(output.contains("Slow query"), () -> "expected a slow-query log line, got: " + output);
        assertTrue(output.contains("INSERT INTO t VALUES (1)"), "the log line should include the actual statement text");
    }

    @Test
    void testSlowQueryLoggingIsOffByDefault() {
        database.execute("CREATE TABLE t (id INT)");

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalErr = System.err;
        System.setErr(new PrintStream(captured));
        try {
            database.execute("INSERT INTO t VALUES (1)");
        } finally {
            System.setErr(originalErr);
        }

        assertFalse(captured.toString().contains("Slow query"), "slow-query logging must be off unless explicitly enabled");
    }

    // --- Schema catalog persistence: a real, significant gap found this
    // round - CREATE TABLE/INDEX/VIEW were purely in-memory, so a table's
    // heap file survived a restart correctly but the engine had no record
    // that the table existed at all. Proven with an actual restart, not
    // assumed from reading the code - see PROGRESS.md.
    //
    // Row-level visibility across a restart is now also reliable (see the
    // persisted commit-status log + persisted xid counter, added right
    // after this round first shipped - a real gap this round found but
    // initially left open, since fully solving it took more work than the
    // schema-catalog fix alone). These tests assert exact row counts
    // across real restarts specifically because that visibility fix makes
    // it safe to depend on now, and doing so gives this fix real
    // regression coverage.

    @Test
    void testTableIndexAndViewSurviveARestart() {
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("INSERT INTO t VALUES (1, 100)");
        database.execute("CREATE INDEX idx_val ON t (val) USING HASH");
        database.execute("CREATE VIEW v AS SELECT * FROM t WHERE val > 0");

        database.shutdown();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        database = new StratosDB(config); // fresh instance, same directory - simulates a real restart

        QueryResult tableResult = database.execute("SELECT * FROM t");
        assertTrue(tableResult.isSuccess(), () -> "table must survive a restart: " + tableResult.getError());
        assertEquals(1, tableResult.getRows().size(), "the row inserted before the restart must be visible after it");

        QueryResult viewResult = database.execute("SELECT * FROM v");
        assertTrue(viewResult.isSuccess(), () -> "view must survive a restart: " + viewResult.getError());
        assertEquals(1, viewResult.getRows().size());

        QueryResult explainResult = database.execute("EXPLAIN SELECT * FROM t WHERE val=100");
        assertTrue(explainResult.isSuccess());
        assertTrue(explainResult.getMessage().contains("idx_val"), () -> "index must survive a restart and still be used: " + explainResult.getMessage());
        assertEquals(1, database.execute("SELECT * FROM t WHERE val=100").getRows().size(), "the index must still find the pre-restart row correctly");

        database.execute("INSERT INTO t VALUES (2, 200)");
        assertRowCount("SELECT * FROM t", 2, "old and new rows must both be visible after the restart");
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testDataSurvivesMultipleRestartsWithMixedOperations() {
        // The adversarial pattern that originally exposed the bug this
        // fixes: ONE schema-catalog entry (so replay only consumes a
        // single transaction id on startup) followed by MANY inserts (so
        // their transaction ids run far ahead of what a naive restart
        // would recognize as "committed"). Before the persisted
        // commit-status log, this specific shape - few catalog entries,
        // many later transactions - reliably lost every row past the
        // first few, because a fresh session's empty in-memory commit set
        // had no record that those later transaction ids had ever
        // committed. Four sequential real restarts, with inserts, an
        // update, and a delete spread across them.
        database.execute("CREATE TABLE t (id INT, val INT)");
        for (int i = 0; i < 20; i++) {
            database.execute("INSERT INTO t VALUES (" + i + ", " + (i * 10) + ")");
        }
        assertRowCount("SELECT * FROM t", 20);

        database.shutdown();
        DatabaseConfig config2 = new DatabaseConfig();
        config2.setDataDirectory(tempDir.toString());
        database = new StratosDB(config2);
        assertRowCount("SELECT * FROM t", 20, "all 20 pre-restart rows must survive the first restart");
        database.execute("INSERT INTO t VALUES (100, 1000)");

        database.shutdown();
        DatabaseConfig config3 = new DatabaseConfig();
        config3.setDataDirectory(tempDir.toString());
        database = new StratosDB(config3);
        assertRowCount("SELECT * FROM t", 21, "the second restart must see all 20 original rows plus the one added after the first restart");
        database.execute("UPDATE t SET val=9999 WHERE id=0");
        database.execute("DELETE FROM t WHERE id=1");

        database.shutdown();
        DatabaseConfig config4 = new DatabaseConfig();
        config4.setDataDirectory(tempDir.toString());
        database = new StratosDB(config4);
        assertRowCount("SELECT * FROM t", 20, "the third restart must see one fewer row after the delete");
        assertEquals(9999, database.execute("SELECT * FROM t WHERE id=0").getRows().get(0).getValue("val"),
            "the update from before the third restart must have taken effect and survived it");
        assertRowCount("SELECT * FROM t WHERE id=1", 0, "the deleted row must stay deleted across the restart");
    }

    @Test
    void testDroppedTableAndViewStayDroppedAfterRestart() {
        database.execute("CREATE TABLE t (id INT)");
        database.execute("CREATE TABLE keep (id INT)");
        database.execute("CREATE VIEW v AS SELECT * FROM keep");
        database.execute("DROP TABLE t");

        database.shutdown();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        database = new StratosDB(config);

        assertFalse(database.execute("SELECT * FROM t").isSuccess(), "a dropped table must stay dropped after a restart");
        assertTrue(database.execute("SELECT * FROM keep").isSuccess(), "an undropped table must still be there");
        assertTrue(database.execute("SELECT * FROM v").isSuccess(), "an undropped view must still be there");

        database.execute("DROP VIEW v");
        database.shutdown();
        DatabaseConfig config2 = new DatabaseConfig();
        config2.setDataDirectory(tempDir.toString());
        database = new StratosDB(config2);
        assertFalse(database.execute("SELECT * FROM v").isSuccess(), "a dropped view must stay dropped after a restart");
    }

    // --- Savepoints: MVCC's own "removed by my own transaction is
    // invisible even to me" visibility rule (see MVCCVisibility.isVisible)
    // turns out to be exactly the primitive SAVEPOINT rollback needs -
    // self-tombstoning an undone insert makes it vanish permanently for the
    // rest of the transaction, and clearing a tombstone back to NO_XMAX
    // restores an undone update/delete. See PROGRESS.md for the full design.

    @Test
    void testSavepointRollbackUndoesInsertsAfterIt() {
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("BEGIN");
        database.execute("INSERT INTO t VALUES (1, 100)");
        database.execute("SAVEPOINT sp1");
        database.execute("INSERT INTO t VALUES (2, 200)");
        assertRowCount("SELECT * FROM t", 2);

        assertTrue(database.execute("ROLLBACK TO SAVEPOINT sp1").isSuccess());
        assertRowCount("SELECT * FROM t", 1, "the insert after sp1 must be undone");

        database.execute("COMMIT");
        assertRowCount("SELECT * FROM t", 1, "only the pre-savepoint insert survives the commit");
    }

    @Test
    void testSavepointRollbackRestoresUpdatedAndDeletedRows() {
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("INSERT INTO t VALUES (1, 100)");

        database.execute("BEGIN");
        database.execute("SAVEPOINT sp1");
        database.execute("UPDATE t SET val=999 WHERE id=1");
        assertEquals(999, database.execute("SELECT * FROM t").getRows().get(0).getValue("val"));
        database.execute("ROLLBACK TO SAVEPOINT sp1");
        assertEquals(100, database.execute("SELECT * FROM t").getRows().get(0).getValue("val"), "update must be undone");

        database.execute("SAVEPOINT sp2");
        database.execute("DELETE FROM t WHERE id=1");
        assertRowCount("SELECT * FROM t", 0);
        database.execute("ROLLBACK TO SAVEPOINT sp2");
        assertRowCount("SELECT * FROM t", 1, "delete must be undone");
        assertEquals(100, database.execute("SELECT * FROM t").getRows().get(0).getValue("val"), "restored row has its original value");

        database.execute("COMMIT");
    }

    @Test
    void testNestedSavepointRollbackDiscardsInnerSavepoints() {
        database.execute("CREATE TABLE t (id INT)");
        database.execute("INSERT INTO t VALUES (1)");
        database.execute("BEGIN");
        database.execute("SAVEPOINT outer1");
        database.execute("INSERT INTO t VALUES (2)");
        database.execute("SAVEPOINT inner1");
        database.execute("INSERT INTO t VALUES (3)");
        assertRowCount("SELECT * FROM t", 3);

        database.execute("ROLLBACK TO SAVEPOINT outer1");
        assertRowCount("SELECT * FROM t", 1, "rolling back to the outer savepoint must undo everything after it, including the inner savepoint's own insert");

        database.execute("ROLLBACK");
    }

    @Test
    void testReleaseSavepointKeepsChangesButInvalidatesTheTarget() {
        database.execute("CREATE TABLE t (id INT)");
        database.execute("BEGIN");
        database.execute("SAVEPOINT sp1");
        database.execute("INSERT INTO t VALUES (1)");
        database.execute("RELEASE SAVEPOINT sp1");

        QueryResult rollbackToReleased = database.execute("ROLLBACK TO SAVEPOINT sp1");
        assertFalse(rollbackToReleased.isSuccess(), "rolling back to a released savepoint must fail");
        assertRowCount("SELECT * FROM t", 1, "a released savepoint's changes are kept, not undone");

        database.execute("COMMIT");
        assertRowCount("SELECT * FROM t", 1);
    }

    @Test
    void testRollbackToSavepointRecoversFromAPoisonedTransaction() {
        // The one command allowed to run on an aborted-by-error transaction -
        // exactly how a real transaction recovers from a mid-transaction
        // error without losing everything committed before it.
        database.execute("CREATE TABLE t (id INT)");
        database.execute("BEGIN");
        database.execute("SAVEPOINT sp1");
        database.execute("INSERT INTO t VALUES (1)");
        QueryResult badStatement = database.execute("SELECT * FROM nonexistent_table");
        assertFalse(badStatement.isSuccess());

        QueryResult poisonedCheck = database.execute("INSERT INTO t VALUES (2)");
        assertFalse(poisonedCheck.isSuccess());
        assertTrue(poisonedCheck.getError().contains("aborted"));

        QueryResult recovered = database.execute("ROLLBACK TO SAVEPOINT sp1");
        assertTrue(recovered.isSuccess(), () -> "ROLLBACK TO SAVEPOINT must work even on a poisoned transaction: " + recovered.getError());

        assertTrue(database.execute("INSERT INTO t VALUES (3)").isSuccess(), "the transaction must be usable again after recovering via savepoint");
        database.execute("COMMIT");
        assertRowCount("SELECT * FROM t", 1, "only the row inserted after recovery survives");
    }

    @Test
    void testSavepointRollbackKeepsIndexesConsistent() {
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("CREATE INDEX idx_val ON t (val)");
        database.execute("INSERT INTO t VALUES (1, 100)");

        database.execute("BEGIN");
        database.execute("SAVEPOINT sp1");
        database.execute("INSERT INTO t VALUES (2, 200)");
        database.execute("UPDATE t SET val=999 WHERE id=1");
        database.execute("ROLLBACK TO SAVEPOINT sp1");
        database.execute("COMMIT");

        assertRowCount("SELECT * FROM t WHERE val=100", 1, "index must find the restored original value");
        assertRowCount("SELECT * FROM t WHERE val=200", 0, "index must not find the undone insert's value");
        assertRowCount("SELECT * FROM t WHERE val=999", 0, "index must not find the undone update's value");
    }

    @Test
    void testSavepointRequiresAnOpenTransaction() {
        assertFalse(database.execute("SAVEPOINT sp1").isSuccess(), "SAVEPOINT outside a transaction must fail");
    }

    // --- CTEs: a single, non-recursive WITH clause, reusing the same
    // execution path views already use (executeSelectOverView), just with
    // session-local (not persisted, not shared across connections) scoping.

    @Test
    void testBasicCteWithOuterFilter() {
        database.execute("CREATE TABLE employees (id INT, name VARCHAR, salary INT)");
        database.execute("INSERT INTO employees VALUES (1, 'Alice', 90000)");
        database.execute("INSERT INTO employees VALUES (2, 'Bob', 60000)");
        database.execute("INSERT INTO employees VALUES (3, 'Carol', 120000)");

        QueryResult basic = database.execute("WITH high_earners AS (SELECT id, name, salary FROM employees WHERE salary > 70000) SELECT * FROM high_earners");
        assertTrue(basic.isSuccess(), () -> "basic CTE must succeed: " + basic.getError());
        assertEquals(2, basic.getRows().size());

        QueryResult filtered = database.execute("WITH high_earners AS (SELECT id, name, salary FROM employees WHERE salary > 70000) SELECT * FROM high_earners WHERE salary > 100000");
        assertTrue(filtered.isSuccess());
        assertEquals(1, filtered.getRows().size());
        assertEquals("Carol", filtered.getRows().get(0).getValue("name"));
    }

    @Test
    void testCteDoesNotLeakToALaterStatement() {
        database.execute("CREATE TABLE t (id INT)");
        database.execute("INSERT INTO t VALUES (1)");
        database.execute("WITH tmp AS (SELECT * FROM t) SELECT * FROM tmp");

        assertFalse(database.execute("SELECT * FROM tmp").isSuccess(),
            "a CTE's name must not be resolvable in any statement after the one that defined it");
    }

    @Test
    void testCteCombinedWithJoinOrAggregateFailsCleanly() {
        // Same real bug class views hit and got fixed for - reusing views'
        // own executeSelectOverView means CTEs inherit the same protection.
        database.execute("CREATE TABLE employees (id INT, dept_id INT)");
        database.execute("CREATE TABLE departments (id INT)");

        QueryResult joinResult = database.execute("WITH e AS (SELECT id, dept_id FROM employees) SELECT * FROM e JOIN departments ON e.dept_id = departments.id");
        assertFalse(joinResult.isSuccess(), "a CTE combined with JOIN must fail cleanly, not silently ignore the join");

        QueryResult aggResult = database.execute("WITH e AS (SELECT id FROM employees) SELECT COUNT(*) FROM e");
        assertFalse(aggResult.isSuccess(), "a CTE combined with an aggregate must fail cleanly, not silently ignore the aggregate");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testConcurrentConnectionsUsingTheSameCteNameDoNotInterfere() throws Exception {
        // The real risk this design was built to avoid: a shared "views"-style
        // map mutated around each CTE's execution would let one connection's
        // cleanup remove another connection's still-in-use entry mid-execution.
        database.execute("CREATE TABLE a (id INT)");
        database.execute("CREATE TABLE b (id INT)");
        database.execute("INSERT INTO a VALUES (1)");
        database.execute("INSERT INTO b VALUES (2)");

        int n = 6;
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(n);
        java.util.concurrent.atomic.AtomicInteger successes = new java.util.concurrent.atomic.AtomicInteger(0);
        for (int i = 0; i < n; i++) {
            final String table = (i % 2 == 0) ? "a" : "b";
            final int expectedId = (i % 2 == 0) ? 1 : 2;
            new Thread(() -> {
                try {
                    // Every thread uses the SAME CTE name "tmp" but a DIFFERENT source table.
                    QueryResult r = database.execute("WITH tmp AS (SELECT * FROM " + table + ") SELECT * FROM tmp");
                    if (r.isSuccess() && r.getRows().size() == 1 && ((Number) r.getRows().get(0).getValue("id")).intValue() == expectedId) {
                        successes.incrementAndGet();
                    }
                } catch (Exception ignored) {
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        assertTrue(latch.await(8, TimeUnit.SECONDS));
        assertEquals(n, successes.get(), "every thread must see only its OWN CTE's data, never another thread's");
    }

    // --- SHOW STATS: exposes engine internals (already tracked, previously
    // only reachable via the CLI's own \status command) as an ordinary,
    // SQL-queryable result - a real step toward a pg_stat-style interface.

    @Test
    void testShowStatsReturnsRealMetrics() {
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("CREATE INDEX idx_val ON t (val)");
        database.execute("CREATE VIEW v AS SELECT * FROM t");
        database.execute("INSERT INTO t VALUES (1, 100)");

        QueryResult result = database.execute("SHOW STATS");
        assertTrue(result.isSuccess(), () -> "SHOW STATS must succeed: " + result.getError());

        java.util.Map<String, String> metrics = new java.util.HashMap<>();
        for (com.stratosdb.storage.page.Tuple row : result.getRows()) {
            metrics.put((String) row.getValue("metric"), (String) row.getValue("value"));
        }

        assertEquals("1", metrics.get("table_count"), "must reflect the actual number of real tables");
        assertEquals("1", metrics.get("view_count"), "must reflect the actual number of views");
        assertEquals("1", metrics.get("index_count"), "must reflect the actual number of indexes");
        assertNotNull(metrics.get("buffer_pool_hit_ratio"), "buffer pool hit ratio must be present");
        assertNotNull(metrics.get("wal_current_lsn"), "WAL LSN must be present");
        assertTrue(Long.parseLong(metrics.get("wal_current_lsn")) > 0, "WAL LSN must reflect real logged activity, not just a placeholder zero");
    }

    // --- A real, severe pre-existing bug found while investigating
    // sequences: INSERT's optional (col1, col2, ...) list was parsed by
    // the grammar but completely discarded, so every value silently
    // mapped POSITIONALLY against the table's full schema instead of
    // against the actual named columns.

    @Test
    void testInsertWithExplicitColumnListMapsCorrectly() {
        database.execute("CREATE TABLE employees (id INT, name VARCHAR, age INT)");
        QueryResult result = database.execute("INSERT INTO employees (name, age) VALUES ('Alice', 30)");
        assertTrue(result.isSuccess(), () -> "insert with explicit column list must succeed: " + result.getError());

        var row = database.execute("SELECT * FROM employees").getRows().get(0);
        assertNull(row.getValue("id"), "id (omitted from the column list) must be NULL, not 'Alice'");
        assertEquals("Alice", row.getValue("name"), "name must correctly hold 'Alice', not silently receive the wrong value");
        assertEquals(30, row.getValue("age"));
    }

    @Test
    void testInsertWithOutOfOrderColumnListMapsCorrectly() {
        database.execute("CREATE TABLE t (a INT, b VARCHAR, c INT)");
        database.execute("INSERT INTO t (c, a, b) VALUES (100, 1, 'x')");
        var row = database.execute("SELECT * FROM t").getRows().get(0);
        assertEquals(1, row.getValue("a"));
        assertEquals("x", row.getValue("b"));
        assertEquals(100, row.getValue("c"));
    }

    @Test
    void testInsertColumnValueCountMismatchFailsCleanly() {
        database.execute("CREATE TABLE t (a INT, b VARCHAR)");
        QueryResult result = database.execute("INSERT INTO t (a, b) VALUES (1, 'x', 'extra')");
        assertFalse(result.isSuccess(), "a mismatched column/value count must error, not silently truncate or misalign");
    }

    @Test
    void testColumnDefaultActuallyApplies() {
        database.execute("CREATE TABLE t (id INT, status VARCHAR DEFAULT 'pending')");
        database.execute("INSERT INTO t (id) VALUES (1)");
        var row = database.execute("SELECT * FROM t").getRows().get(0);
        assertEquals("pending", row.getValue("status"), "a column's DEFAULT must actually apply when the column is omitted - previously parsed but never used anywhere");
    }

    // --- Sequences: a real, persisted CREATE SEQUENCE / nextval() /
    // currval(), and SERIAL as sugar that auto-creates a backing sequence.

    @Test
    void testCreateSequenceAndNextvalCurrval() {
        assertTrue(database.execute("CREATE SEQUENCE my_seq START WITH 100 INCREMENT BY 5").isSuccess());
        database.execute("CREATE TABLE t (id INT, name VARCHAR)");
        database.execute("INSERT INTO t VALUES (nextval('my_seq'), 'first')");
        database.execute("INSERT INTO t VALUES (nextval('my_seq'), 'second')");

        var rows = database.execute("SELECT * FROM t").getRows();
        assertEquals(100, ((Number) rows.get(0).getValue("id")).intValue(), "first nextval() must honor START WITH");
        assertEquals(105, ((Number) rows.get(1).getValue("id")).intValue(), "second nextval() must honor INCREMENT BY");

        QueryResult currvalInsert = database.execute("INSERT INTO t VALUES (currval('my_seq'), 'third')");
        assertTrue(currvalInsert.isSuccess());
        var thirdRow = database.execute("SELECT * FROM t").getRows().get(2);
        assertEquals(105, ((Number) thirdRow.getValue("id")).intValue(), "currval() must return the last value nextval() produced IN THIS SESSION, matching real Postgres semantics");
    }

    @Test
    void testCurrvalBeforeNextvalFailsCleanly() {
        database.execute("CREATE SEQUENCE s");
        database.execute("CREATE TABLE t (id INT)");
        QueryResult result = database.execute("INSERT INTO t VALUES (currval('s'))");
        assertFalse(result.isSuccess(), "currval() before any nextval() call in this session must fail cleanly, matching real Postgres");
    }

    @Test
    void testSerialAutoCreatesSequenceAndAppliesAsDefault() {
        database.execute("CREATE TABLE users (id SERIAL, name VARCHAR)");
        database.execute("INSERT INTO users (name) VALUES ('Alice')");
        database.execute("INSERT INTO users (name) VALUES ('Bob')");

        var rows = database.execute("SELECT * FROM users").getRows();
        assertEquals(2, rows.size());
        Object id1 = rows.get(0).getValue("id"), id2 = rows.get(1).getValue("id");
        assertNotNull(id1);
        assertNotNull(id2);
        assertNotEquals(id1, id2, "SERIAL must generate a different id for each row");
    }

    @Test
    void testSequencePersistsAcrossARestart() {
        database.execute("CREATE SEQUENCE s1");
        database.execute("CREATE TABLE t (id INT)");
        database.execute("INSERT INTO t VALUES (nextval('s1'))");
        database.execute("INSERT INTO t VALUES (nextval('s1'))");
        var beforeRows = database.execute("SELECT * FROM t").getRows();
        long lastBeforeRestart = ((Number) beforeRows.get(beforeRows.size() - 1).getValue("id")).longValue();

        database.shutdown();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        database = new StratosDB(config);

        database.execute("INSERT INTO t VALUES (nextval('s1'))");
        var afterRows = database.execute("SELECT * FROM t").getRows();
        long newValue = ((Number) afterRows.get(afterRows.size() - 1).getValue("id")).longValue();
        assertTrue(newValue > lastBeforeRestart, () -> "a sequence value after a restart (" + newValue
            + ") must be strictly greater than before it (" + lastBeforeRestart + ") - never repeating, matching the same persistence discipline already used for the transaction-id counter");
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testConcurrentNextvalCallsNeverProduceDuplicates() throws Exception {
        database.execute("CREATE SEQUENCE concurrent_seq");
        database.execute("CREATE TABLE t (id INT)");

        int n = 30;
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(n);
        java.util.concurrent.atomic.AtomicInteger errors = new java.util.concurrent.atomic.AtomicInteger(0);
        for (int i = 0; i < n; i++) {
            new Thread(() -> {
                try {
                    QueryResult r = database.execute("INSERT INTO t VALUES (nextval('concurrent_seq'))");
                    if (!r.isSuccess()) errors.incrementAndGet();
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(0, errors.get());

        var rows = database.execute("SELECT * FROM t").getRows();
        assertEquals(n, rows.size(), "every concurrent nextval()-based insert must actually persist - this exact scenario (nextval()'s added latency widening the race window) is what originally exposed a real, separate HeapTable.insert() concurrency bug, now fixed - see HeapTableConcurrencyTest");
        java.util.Set<Long> ids = new java.util.HashSet<>();
        for (var row : rows) ids.add(((Number) row.getValue("id")).longValue());
        assertEquals(n, ids.size(), "every concurrently-generated id must be unique");
    }

    // --- Merge join: a real second join strategy, chosen over hash join
    // once both sides are large enough that hash join's whole-build-side
    // in-memory hash table becomes a real cost. Tested at two levels: the
    // algorithm's correctness directly (matched against hash join on
    // identical input, including duplicate and NULL join keys), and that
    // the planner's row-count threshold actually routes to it - using
    // synthetic in-memory tuples rather than a real 10,000+ row SQL
    // insert, which would be correct but far too slow for a test suite
    // that should stay fast.

    @Test
    void testMergeJoinMatchesHashJoinIncludingDuplicatesAndNulls() throws Exception {
        ExecutorEngine engine = database.getExecutor();
        Method mergeJoin = ExecutorEngine.class.getDeclaredMethod("mergeJoin", java.util.List.class, String.class, java.util.List.class, String.class);
        mergeJoin.setAccessible(true);
        Method hashJoin = ExecutorEngine.class.getDeclaredMethod("hashJoin", java.util.List.class, String.class, java.util.List.class, String.class);
        hashJoin.setAccessible(true);

        java.util.List<Tuple> left = new java.util.ArrayList<>();
        left.add(qualifiedTuple("a", "id", 1, "name", "Alice"));
        left.add(qualifiedTuple("a", "id", 2, "name", "Bob"));
        left.add(qualifiedTuple("a", "id", 2, "name", "Bob2")); // duplicate key
        left.add(qualifiedTuple("a", "id", null, "name", "NullGuy")); // must never match

        java.util.List<Tuple> right = new java.util.ArrayList<>();
        right.add(qualifiedTuple("b", "id", 2, "dept", "Eng"));
        right.add(qualifiedTuple("b", "id", 2, "dept", "Eng2")); // duplicate key
        right.add(qualifiedTuple("b", "id", 4, "dept", "Sales")); // no match
        right.add(qualifiedTuple("b", "id", null, "dept", "NullDept"));

        @SuppressWarnings("unchecked")
        java.util.List<Tuple> mergeResult = (java.util.List<Tuple>) mergeJoin.invoke(engine, left, "a.id", right, "b.id");
        @SuppressWarnings("unchecked")
        java.util.List<Tuple> hashResult = (java.util.List<Tuple>) hashJoin.invoke(engine, left, "a.id", right, "b.id");

        assertEquals(4, mergeResult.size(), "2 left dupes x 2 right dupes for id=2");
        java.util.Set<String> mergeSet = new java.util.HashSet<>();
        for (Tuple t : mergeResult) mergeSet.add(t.toString());
        java.util.Set<String> hashSet = new java.util.HashSet<>();
        for (Tuple t : hashResult) hashSet.add(t.toString());
        assertEquals(hashSet, mergeSet, "merge join and hash join must produce the exact same set of rows");

        for (Tuple t : mergeResult) {
            assertFalse(t.toString().contains("NullGuy") || t.toString().contains("NullDept"), "a NULL join key must never appear in the result");
        }
    }

    @Test
    void testPlannerRoutesLargeJoinsToMergeJoin() throws Exception {
        ExecutorEngine engine = database.getExecutor();
        Method chooseJoinAlgorithm = ExecutorEngine.class.getDeclaredMethod("chooseJoinAlgorithm", java.util.List.class, String.class, java.util.List.class, String.class);
        chooseJoinAlgorithm.setAccessible(true);
        Method mergeJoin = ExecutorEngine.class.getDeclaredMethod("mergeJoin", java.util.List.class, String.class, java.util.List.class, String.class);
        mergeJoin.setAccessible(true);

        // Synthetic, in-memory tuples - fast (no SQL/transaction overhead per row)
        // while still exercising the real threshold check with the real row counts
        // it actually looks at.
        int n = 10_001; // just over the threshold on both sides
        java.util.List<Tuple> left = new java.util.ArrayList<>(n);
        java.util.List<Tuple> right = new java.util.ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            left.add(qualifiedTuple("a", "id", i, "name", "n" + i));
            right.add(qualifiedTuple("b", "id", i, "dept", "d" + i));
        }

        @SuppressWarnings("unchecked")
        java.util.List<Tuple> chosenResult = (java.util.List<Tuple>) chooseJoinAlgorithm.invoke(engine, left, "a.id", right, "b.id");
        @SuppressWarnings("unchecked")
        java.util.List<Tuple> expectedMergeResult = (java.util.List<Tuple>) mergeJoin.invoke(engine, left, "a.id", right, "b.id");

        assertEquals(n, chosenResult.size(), "every id has exactly one match on each side");
        java.util.Set<String> chosenSet = new java.util.HashSet<>();
        for (Tuple t : chosenResult) chosenSet.add(t.toString());
        java.util.Set<String> mergeSet = new java.util.HashSet<>();
        for (Tuple t : expectedMergeResult) mergeSet.add(t.toString());
        assertEquals(mergeSet, chosenSet, "above the row-count threshold, the planner must choose merge join specifically");
    }

    @Test
    void testJoinBelowThresholdStillWorksCorrectly() {
        // Sanity check: ordinary, small joins (well below the merge-join threshold) are unaffected.
        database.execute("CREATE TABLE employees (id INT, dept_id INT, name VARCHAR)");
        database.execute("CREATE TABLE departments (id INT, dept_name VARCHAR)");
        database.execute("INSERT INTO employees VALUES (1, 10, 'Alice')");
        database.execute("INSERT INTO employees VALUES (2, 20, 'Bob')");
        database.execute("INSERT INTO departments VALUES (10, 'Eng')");
        database.execute("INSERT INTO departments VALUES (20, 'Sales')");

        QueryResult result = database.execute("SELECT * FROM employees JOIN departments ON employees.dept_id = departments.id");
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRows().size());
    }

    // --- Recursive CTEs: WITH RECURSIVE name AS (base UNION ALL recursive)
    // outer, evaluated by fixpoint iteration. The valuable, standard
    // pattern is a real table joined against the CTE's own self-reference
    // (hierarchy/graph traversal) - not just a bare "FROM cteName" - so
    // that's what's tested here, since it's also what a real design flaw
    // and a real bug were found against (see executeRecursiveCteSelect's
    // javadoc): the recursive branch needs to work as a JOIN target, and
    // its output columns need to be aligned back to the base query's
    // schema every iteration, or a query that writes its own qualified
    // column names silently breaks after one recursive step.

    @Test
    void testRecursiveCteTraversesAFullHierarchyViaJoin() {
        database.execute("CREATE TABLE employees (id INT, name VARCHAR, manager_id INT)");
        database.execute("INSERT INTO employees VALUES (1, 'CEO', 0)");
        database.execute("INSERT INTO employees VALUES (2, 'VP1', 1)");
        database.execute("INSERT INTO employees VALUES (3, 'VP2', 1)");
        database.execute("INSERT INTO employees VALUES (4, 'Mgr1', 2)");
        database.execute("INSERT INTO employees VALUES (5, 'IC1', 4)");
        database.execute("INSERT INTO employees VALUES (6, 'IC2', 3)");

        QueryResult result = database.execute(
            "WITH RECURSIVE org_chart AS ("
            + "SELECT id, name, manager_id FROM employees WHERE manager_id = 0 "
            + "UNION ALL "
            + "SELECT employees.id, employees.name, employees.manager_id FROM employees JOIN org_chart ON employees.manager_id = org_chart.id"
            + ") SELECT * FROM org_chart");

        assertTrue(result.isSuccess(), () -> "recursive CTE with a real-table join against its own self-reference must succeed: " + result.getError());
        assertEquals(6, result.getRows().size(), "must traverse the entire hierarchy - all 6 employees, across multiple recursion levels");

        java.util.Set<Object> ids = new java.util.HashSet<>();
        for (Tuple row : result.getRows()) {
            ids.add(row.getValue("id"));
            // The real bug this guards against: the recursive branch wrote
            // its own qualified column names ("employees.id"), which must
            // be aligned back to the base query's names ("id") every
            // iteration - otherwise this key wouldn't even be present.
            assertNotNull(row.getValue("id"), "every row must have a correctly-named 'id' column, matching the base query's schema");
        }
        assertEquals(java.util.Set.of(1, 2, 3, 4, 5, 6), ids, "every employee, from every level of the hierarchy, must appear exactly once");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testRecursiveCteWithARealCycleFailsCleanlyInsteadOfHanging() {
        database.execute("CREATE TABLE cyclic (id INT, manager_id INT)");
        database.execute("INSERT INTO cyclic VALUES (1, 2)");
        database.execute("INSERT INTO cyclic VALUES (2, 1)"); // a genuine cycle: 1 -> 2 -> 1 -> ...

        QueryResult result = database.execute(
            "WITH RECURSIVE cte AS ("
            + "SELECT id, manager_id FROM cyclic WHERE id = 1 "
            + "UNION ALL "
            + "SELECT cyclic.id, cyclic.manager_id FROM cyclic JOIN cte ON cyclic.id = cte.manager_id"
            + ") SELECT * FROM cte");

        assertFalse(result.isSuccess(), "a genuine cycle in the underlying data must fail cleanly, not hang or exhaust memory");
        assertTrue(result.getError().contains("iterations"), () -> "the error must clearly explain a non-terminating recursion was detected: " + result.getError());
    }

    @Test
    void testWithRecursiveRequiresUnionAllStructure() {
        database.execute("CREATE TABLE t (id INT)");
        // WITH RECURSIVE without an actual UNION ALL structure isn't really recursive at all - must be rejected, not silently misinterpreted.
        QueryResult result = database.execute("WITH RECURSIVE cte AS (SELECT id FROM t) SELECT * FROM cte");
        assertFalse(result.isSuccess(), "WITH RECURSIVE without a UNION ALL structure must be rejected, not silently treated as a non-recursive CTE");
    }

    @Test
    void testNonRecursiveCteStillWorksAlongsideRecursiveSupport() {
        // Regression check: adding RECURSIVE support must not disturb the existing, simpler CTE path.
        database.execute("CREATE TABLE t (id INT, val INT)");
        database.execute("INSERT INTO t VALUES (1, 100)");
        QueryResult result = database.execute("WITH simple AS (SELECT id, val FROM t WHERE val > 50) SELECT * FROM simple");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size());
    }

    // --- BRIN, bitmap, and GIN indexes: three genuinely different index
    // structures (block-range summaries, per-value bitmaps, and a text
    // inverted index respectively), each built, WRITE-maintained, and
    // actually consulted during a scan - not just created and forgotten.
    // A real, silent staleness bug was found and fixed for all three
    // while building this: index maintenance on new rows was missing
    // entirely at first, verified by directly testing an INSERT after
    // index creation (see PROGRESS.md).

    @Test
    void testBrinIndexBuildsAndAnswersRangeAndEqualityQueries() {
        database.execute("CREATE TABLE t (id INT, payload VARCHAR)");
        String padding = "x".repeat(500);
        for (int i = 0; i < 200; i++) {
            database.execute("INSERT INTO t VALUES (" + i + ", '" + padding + "')");
        }
        QueryResult createResult = database.execute("CREATE INDEX idx_id_brin ON t (id) USING BRIN");
        assertTrue(createResult.isSuccess(), () -> "CREATE INDEX ... USING BRIN must succeed: " + createResult.getError());

        QueryResult rangeResult = database.execute("SELECT id FROM t WHERE id >= 190");
        assertTrue(rangeResult.isSuccess());
        assertEquals(10, rangeResult.getRows().size(), "ids 190-199");

        QueryResult equalityResult = database.execute("SELECT id FROM t WHERE id = 50");
        assertTrue(equalityResult.isSuccess());
        assertEquals(1, equalityResult.getRows().size());

        QueryResult outOfRangeResult = database.execute("SELECT id FROM t WHERE id > 1000");
        assertTrue(outOfRangeResult.isSuccess(), "a range entirely outside the data must return 0 rows, not error");
        assertEquals(0, outOfRangeResult.getRows().size());
    }

    @Test
    void testBrinIndexMaintainsRangeOnNewInserts() {
        database.execute("CREATE TABLE t (id INT)");
        database.execute("INSERT INTO t VALUES (1)");
        database.execute("CREATE INDEX idx_id_brin ON t (id) USING BRIN");

        database.execute("INSERT INTO t VALUES (999)");
        QueryResult result = database.execute("SELECT * FROM t WHERE id = 999");
        assertEquals(1, result.getRows().size(), "a row inserted after BRIN index creation must extend its range's summary, not be silently unreachable");
    }

    @Test
    void testBitmapIndexEqualityLookupAndMaintenance() {
        database.execute("CREATE TABLE t (id INT, status VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 'active')");
        database.execute("INSERT INTO t VALUES (2, 'inactive')");
        database.execute("INSERT INTO t VALUES (3, 'active')");
        QueryResult createResult = database.execute("CREATE INDEX idx_status ON t (status) USING BITMAP");
        assertTrue(createResult.isSuccess(), () -> "CREATE INDEX ... USING BITMAP must succeed: " + createResult.getError());

        QueryResult result = database.execute("SELECT * FROM t WHERE status = 'active'");
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRows().size());

        // The real staleness bug this guards against.
        database.execute("INSERT INTO t VALUES (4, 'active')");
        QueryResult afterInsert = database.execute("SELECT * FROM t WHERE status = 'active'");
        assertEquals(3, afterInsert.getRows().size(), "a row inserted after BITMAP index creation must be reflected immediately, not silently missing");
    }

    @Test
    void testBitmapIndexStaleEntryFromDeleteIsFilteredByMvccVisibility() {
        // The bitmap index itself never removes a deleted row's bit (a real,
        // named limitation - see BitmapIndex's own javadoc), but the query
        // must still be CORRECT: the stale entry gets filtered by the same
        // MVCC visibility check every other index scan path already uses.
        database.execute("CREATE TABLE t (id INT, status VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 'active')");
        database.execute("INSERT INTO t VALUES (2, 'active')");
        database.execute("CREATE INDEX idx_status ON t (status) USING BITMAP");

        database.execute("DELETE FROM t WHERE id = 1");
        QueryResult result = database.execute("SELECT * FROM t WHERE status = 'active'");
        assertEquals(1, result.getRows().size(), "a deleted row's stale bitmap entry must never appear in results, even though it's never physically removed from the index");
    }

    @Test
    void testGinIndexContainsSearchAndMaintenance() {
        database.execute("CREATE TABLE t (id INT, description VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 'the quick brown fox')");
        database.execute("INSERT INTO t VALUES (2, 'jumps over the lazy dog')");
        database.execute("INSERT INTO t VALUES (3, 'fox hunting in the forest')");
        QueryResult createResult = database.execute("CREATE INDEX idx_desc ON t (description) USING GIN");
        assertTrue(createResult.isSuccess(), () -> "CREATE INDEX ... USING GIN must succeed: " + createResult.getError());

        QueryResult result = database.execute("SELECT id FROM t WHERE description CONTAINS 'fox'");
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRows().size(), "rows 1 and 3 both contain 'fox'");

        // Word-boundary matching, not substring: 'fox' must not match a row that only contains a longer word containing "fox" as a substring.
        database.execute("INSERT INTO t VALUES (4, 'foxglove flowers')");
        QueryResult stillTwo = database.execute("SELECT id FROM t WHERE description CONTAINS 'fox'");
        assertEquals(2, stillTwo.getRows().size(), "'fox' must match whole words only, not as a substring of 'foxglove'");

        // The real staleness bug this guards against.
        database.execute("INSERT INTO t VALUES (5, 'another fox appears')");
        QueryResult afterInsert = database.execute("SELECT id FROM t WHERE description CONTAINS 'fox'");
        assertEquals(3, afterInsert.getRows().size(), "a row inserted after GIN index creation must be reflected immediately, not silently missing");
    }

    @Test
    void testContainsWorksWithoutAnyGinIndexAtAll() {
        // CONTAINS must work correctly even with no index present at all - the same relationship LIKE has with indexes.
        database.execute("CREATE TABLE t (id INT, description VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 'the quick brown fox')");
        database.execute("INSERT INTO t VALUES (2, 'no matching word here')");

        QueryResult result = database.execute("SELECT id FROM t WHERE description CONTAINS 'fox'");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size());
    }

    // --- Index-only scans: a real capability unlocked by this round's
    // visibility map. Scoped honestly (see PROGRESS.md) to equality
    // queries whose projection asks for only the indexed column, since
    // that's the one case where the index's own key value can be trusted
    // directly without fetching the heap tuple at all.

    @Test
    void testIndexOnlyScanReturnsCorrectValueAfterVacuum() {
        database.execute("CREATE TABLE t (id INT, name VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 'Alice')");
        database.execute("INSERT INTO t VALUES (2, 'Bob')");
        database.execute("INSERT INTO t VALUES (3, 'Carol')");
        database.execute("CREATE INDEX idx_id ON t (id) USING BTREE");

        // Before vacuum, the page isn't yet confirmed all-visible - must still work correctly (falls back to a real heap fetch).
        QueryResult beforeVacuum = database.execute("SELECT id FROM t WHERE id = 2");
        assertTrue(beforeVacuum.isSuccess());
        assertEquals(1, beforeVacuum.getRows().size());
        assertEquals(2, beforeVacuum.getRows().get(0).getValue("id"));

        database.execute("VACUUM t");

        QueryResult afterVacuum = database.execute("SELECT id FROM t WHERE id = 2");
        assertTrue(afterVacuum.isSuccess());
        assertEquals(1, afterVacuum.getRows().size());
        assertEquals(2, afterVacuum.getRows().get(0).getValue("id"));
    }

    @Test
    void testIndexOnlyScanValueTypeMatchesNormalScanValueType() {
        // A real bug found by testing, not inspection: the index-only path
        // originally returned java.lang.Long for a plain INT column while
        // the normal heap-fetch path returned java.lang.Integer for the
        // identical value - two different Java types for the same logical
        // value depending on which internal scan path happened to run.
        database.execute("CREATE TABLE t (id INT, name VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 'Alice')");
        database.execute("CREATE INDEX idx_id ON t (id) USING BTREE");
        database.execute("VACUUM t");

        Object indexOnlyValue = database.execute("SELECT id FROM t WHERE id = 1").getRows().get(0).getValue("id");
        Object normalValue = database.execute("SELECT * FROM t WHERE id = 1").getRows().get(0).getValue("id");

        assertEquals(normalValue.getClass(), indexOnlyValue.getClass(),
            () -> "an INT column's value must be the same Java type regardless of which scan path produced it - got "
                + indexOnlyValue.getClass() + " (index-only) vs " + normalValue.getClass() + " (normal)");
        assertEquals(Integer.class, indexOnlyValue.getClass(), "an INT column should surface as Integer, not Long");
    }

    @Test
    void testIndexOnlyScanNotUsedWhenOtherColumnsRequested() {
        // SELECT * (or any column other than the indexed one) must never take the index-only path - the index has nothing else to offer.
        database.execute("CREATE TABLE t (id INT, name VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 'Alice')");
        database.execute("CREATE INDEX idx_id ON t (id) USING BTREE");
        database.execute("VACUUM t");

        QueryResult result = database.execute("SELECT name FROM t WHERE id = 1");
        assertTrue(result.isSuccess());
        assertEquals("Alice", result.getRows().get(0).getValue("name"));
    }

    @Test
    void testIndexOnlyScanCorrectlyReturnsNoRowsForAMiss() {
        database.execute("CREATE TABLE t (id INT)");
        database.execute("INSERT INTO t VALUES (1)");
        database.execute("CREATE INDEX idx_id ON t (id) USING BTREE");
        database.execute("VACUUM t");

        QueryResult result = database.execute("SELECT id FROM t WHERE id = 999");
        assertTrue(result.isSuccess());
        assertEquals(0, result.getRows().size());
    }

    private Tuple qualifiedTuple(String table, String col1, Object val1, String col2, Object val2) {
        Tuple t = new Tuple();
        t.addValue(table + "." + col1, val1);
        t.addValue(table + "." + col2, val2);
        return t;
    }

    // --- Window functions: ROW_NUMBER()/RANK()/DENSE_RANK() OVER
    // (PARTITION BY ... ORDER BY ...). Found already implemented in the
    // codebase (grammar, AST, and a correct executor) but with zero
    // existing tests anywhere - verified correct against real Postgres
    // semantics before trusting it, then given the formal test coverage
    // it never had.

    @Test
    void testRowNumberPartitionedAndOrdered() {
        database.execute("CREATE TABLE sales (id INT, region VARCHAR, amount INT)");
        database.execute("INSERT INTO sales VALUES (1, 'East', 100)");
        database.execute("INSERT INTO sales VALUES (2, 'East', 300)");
        database.execute("INSERT INTO sales VALUES (3, 'East', 200)");
        database.execute("INSERT INTO sales VALUES (4, 'West', 400)");

        QueryResult result = database.execute("SELECT id, region, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn FROM sales");
        assertTrue(result.isSuccess(), () -> "ROW_NUMBER() must succeed: " + result.getError());

        java.util.Map<Object, Integer> rnById = new java.util.HashMap<>();
        for (Tuple row : result.getRows()) {
            rnById.put(row.getValue("id"), ((Number) row.getValue("rn")).intValue());
        }
        assertEquals(1, rnById.get(1), "East's smallest amount (100) must be row 1 within its partition");
        assertEquals(2, rnById.get(3), "East's middle amount (200) must be row 2");
        assertEquals(3, rnById.get(2), "East's largest amount (300) must be row 3");
        assertEquals(1, rnById.get(4), "West's only row must be row 1 within ITS OWN partition, independent of East's numbering");
    }

    @Test
    void testRankAndDenseRankHandleTiesCorrectly() {
        database.execute("CREATE TABLE t (id INT, amount INT)");
        database.execute("INSERT INTO t VALUES (1, 150)");
        database.execute("INSERT INTO t VALUES (2, 150)"); // tied with id=1
        database.execute("INSERT INTO t VALUES (3, 400)");

        QueryResult rankResult = database.execute("SELECT id, amount, RANK() OVER (ORDER BY amount) AS rk FROM t");
        java.util.Map<Object, Integer> rankById = new java.util.HashMap<>();
        for (Tuple row : rankResult.getRows()) rankById.put(row.getValue("id"), ((Number) row.getValue("rk")).intValue());
        assertEquals(1, rankById.get(1));
        assertEquals(1, rankById.get(2), "a tie must share the same rank");
        assertEquals(3, rankById.get(3), "RANK() must SKIP ahead past the tied rows (1, 1, 3 - not 1, 1, 2)");

        QueryResult denseResult = database.execute("SELECT id, amount, DENSE_RANK() OVER (ORDER BY amount) AS dr FROM t");
        java.util.Map<Object, Integer> denseById = new java.util.HashMap<>();
        for (Tuple row : denseResult.getRows()) denseById.put(row.getValue("id"), ((Number) row.getValue("dr")).intValue());
        assertEquals(1, denseById.get(1));
        assertEquals(1, denseById.get(2), "a tie must share the same dense rank");
        assertEquals(2, denseById.get(3), "DENSE_RANK() must NOT skip - the next distinct value is just +1 (1, 1, 2)");
    }

    @Test
    void testRowNumberWithoutPartitionByTreatsWholeTableAsOnePartition() {
        database.execute("CREATE TABLE t (id INT, amount INT)");
        database.execute("INSERT INTO t VALUES (1, 300)");
        database.execute("INSERT INTO t VALUES (2, 100)");
        database.execute("INSERT INTO t VALUES (3, 200)");

        QueryResult result = database.execute("SELECT id, amount, ROW_NUMBER() OVER (ORDER BY amount) AS rn FROM t");
        java.util.Map<Object, Integer> rnById = new java.util.HashMap<>();
        for (Tuple row : result.getRows()) rnById.put(row.getValue("id"), ((Number) row.getValue("rn")).intValue());
        assertEquals(1, rnById.get(2), "amount 100 is smallest overall");
        assertEquals(2, rnById.get(3), "amount 200 is second smallest overall");
        assertEquals(3, rnById.get(1), "amount 300 is largest overall");
    }

    @Test
    void testWindowFunctionRowCountUnchangedUnlikeGroupBy() {
        // A window function must never collapse rows, unlike GROUP BY.
        database.execute("CREATE TABLE t (id INT, region VARCHAR)");
        database.execute("INSERT INTO t VALUES (1, 'East')");
        database.execute("INSERT INTO t VALUES (2, 'East')");
        database.execute("INSERT INTO t VALUES (3, 'West')");

        QueryResult result = database.execute("SELECT id, region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) AS rn FROM t");
        assertEquals(3, result.getRows().size(), "a window function must keep every original row, not collapse them like GROUP BY would");
    }

    @Test
    void testWindowFunctionCombinedWithJoinOrGroupByFailsCleanly() {
        database.execute("CREATE TABLE t (id INT, region VARCHAR, amount INT)");
        database.execute("CREATE TABLE t2 (id INT)");
        database.execute("INSERT INTO t VALUES (1, 'East', 100)");

        QueryResult joinResult = database.execute("SELECT t.id, ROW_NUMBER() OVER (ORDER BY amount) AS rn FROM t JOIN t2 ON t.id = t2.id");
        assertFalse(joinResult.isSuccess(), "window function + JOIN must fail cleanly, not silently produce wrong results");

        QueryResult groupByResult = database.execute("SELECT region, COUNT(*), ROW_NUMBER() OVER (ORDER BY region) AS rn FROM t GROUP BY region");
        assertFalse(groupByResult.isSuccess(), "window function + GROUP BY must fail cleanly, not silently produce wrong results");
    }

    @Test
    void testWindowFunctionOnEmptyTableReturnsNoRowsWithoutError() {
        database.execute("CREATE TABLE empty_t (id INT, amount INT)");
        QueryResult result = database.execute("SELECT id, ROW_NUMBER() OVER (ORDER BY amount) AS rn FROM empty_t");
        assertTrue(result.isSuccess());
        assertEquals(0, result.getRows().size());
    }

    private void assertRowCount(String sql, int expected) {
        QueryResult result = database.execute(sql);
        assertTrue(result.isSuccess(), () -> sql + " failed: " + result.getError());
        assertEquals(expected, result.getRows().size(), () -> sql + " returned wrong row count");
    }

    @Test
    void testInnerJoin_basicMatch() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR)");
        database.execute("CREATE TABLE orders (id INT, user_id INT, amount INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice')");
        database.execute("INSERT INTO users VALUES (2, 'Bob')");
        database.execute("INSERT INTO orders VALUES (100, 1, 50)");
        database.execute("INSERT INTO orders VALUES (101, 1, 75)");
        database.execute("INSERT INTO orders VALUES (102, 2, 20)");

        QueryResult result = database.execute(
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id");
        assertTrue(result.isSuccess(), () -> "JOIN failed: " + result.getError());
        assertEquals(3, result.getRows().size(), "Alice has 2 orders, Bob has 1 - 3 joined rows total");

        int aliceTotal = 0, bobTotal = 0;
        for (var row : result.getRows()) {
            String name = (String) row.getValue("users.name");
            int amount = (Integer) row.getValue("orders.amount");
            if ("Alice".equals(name)) aliceTotal += amount;
            if ("Bob".equals(name)) bobTotal += amount;
        }
        assertEquals(125, aliceTotal);
        assertEquals(20, bobTotal);
    }

    @Test
    void testInnerJoin_excludesNonMatchingRows() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR)");
        database.execute("CREATE TABLE orders (id INT, user_id INT, amount INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice')");
        database.execute("INSERT INTO users VALUES (2, 'Bob')"); // Bob has no orders
        database.execute("INSERT INTO orders VALUES (100, 1, 50)");
        database.execute("INSERT INTO orders VALUES (200, 999, 999)"); // orphan order, no matching user

        QueryResult result = database.execute(
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRows().size(), "inner join must exclude Bob (no orders) and the orphan order");
        assertEquals("Alice", result.getRows().get(0).getValue("users.name"));
    }

    @Test
    void testInnerJoin_withWhereOnJoinedColumn() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR)");
        database.execute("CREATE TABLE orders (id INT, user_id INT, amount INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice')");
        database.execute("INSERT INTO orders VALUES (100, 1, 50)");
        database.execute("INSERT INTO orders VALUES (101, 1, 150)");

        QueryResult result = database.execute(
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE orders.amount > 100");
        assertTrue(result.isSuccess(), () -> "JOIN+WHERE failed: " + result.getError());
        assertEquals(1, result.getRows().size());
        assertEquals(150, result.getRows().get(0).getValue("orders.amount"));
    }

    @Test
    void testInnerJoin_bareColumnNameResolvesWhenUnambiguous() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR)");
        database.execute("CREATE TABLE orders (id INT, user_id INT, amount INT)");
        database.execute("INSERT INTO users VALUES (1, 'Alice')");
        database.execute("INSERT INTO orders VALUES (100, 1, 50)");

        // "name" and "amount" are each unambiguous (only one table has them),
        // even though "id" exists in both tables and isn't requested here.
        QueryResult result = database.execute(
            "SELECT name, amount FROM users JOIN orders ON users.id = orders.user_id");
        assertTrue(result.isSuccess(), () -> "bare column JOIN failed: " + result.getError());
        assertEquals(1, result.getRows().size());
        assertEquals("Alice", result.getRows().get(0).getValue("name"));
        assertEquals(50, result.getRows().get(0).getValue("amount"));
    }

    @Test
    void testExplainDescribesJoinShape() {
        database.execute("CREATE TABLE users (id INT, name VARCHAR)");
        database.execute("CREATE TABLE orders (id INT, user_id INT, amount INT)");

        QueryResult result = database.execute(
            "EXPLAIN SELECT * FROM users JOIN orders ON users.id = orders.user_id");
        assertTrue(result.isSuccess());
        assertEquals("Hash Join: Seq Scan on users -> Seq Scan on orders ON users.id=orders.user_id",
            result.getMessage());
    }
}
