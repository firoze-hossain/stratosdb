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
}
