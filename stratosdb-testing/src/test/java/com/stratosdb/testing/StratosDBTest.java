package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

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
        assertEquals("Seq Scan on users", withoutIndex.getMessage());
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
        assertEquals("Nested Loop Join: Seq Scan on users -> Seq Scan on orders ON users.id=orders.user_id",
            result.getMessage());
    }
}
