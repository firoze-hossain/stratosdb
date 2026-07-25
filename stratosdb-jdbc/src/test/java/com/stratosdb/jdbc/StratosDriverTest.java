package com.stratosdb.jdbc;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.server.StratosServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.sql.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises the JDBC driver exactly as a real Java application would:
 * through java.sql.DriverManager, not by referencing StratosDriver
 * directly. This is the actual proof that "the engine is usable from any
 * Java tool" - a real socket connection, a real server, real SQL over the
 * wire.
 */
class StratosDriverTest {

    @TempDir
    Path tempDir;

    private StratosDB db;
    private StratosServer server;
    private String url;

    @BeforeEach
    void setUp() throws Exception {
        int port;
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setPort(port);
        db = new StratosDB(config);
        server = new StratosServer(port, db);
        server.start();
        url = "jdbc:stratos://localhost:" + port + "/";
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void fullCrudRoundTripThroughDriverManager() throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            assertFalse(conn.isClosed());

            try (Statement stmt = conn.createStatement()) {
                assertFalse(stmt.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)"),
                    "DDL execute() should report false - no result set");

                assertEquals(1, stmt.executeUpdate("INSERT INTO users VALUES (1, 'Alice', 30)"));
                assertEquals(1, stmt.executeUpdate("INSERT INTO users VALUES (2, 'Bob', 25)"));

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE age >= 25")) {
                    ResultSetMetaData md = rs.getMetaData();
                    assertEquals(3, md.getColumnCount());

                    int count = 0;
                    while (rs.next()) {
                        count++;
                        assertFalse(rs.wasNull());
                        String name = rs.getString("name");
                        assertTrue(name.equals("Alice") || name.equals("Bob"));
                    }
                    assertEquals(2, count);
                }

                assertEquals(1, stmt.executeUpdate("UPDATE users SET age=31 WHERE id=1"));

                try (ResultSet rs = stmt.executeQuery("SELECT age FROM users WHERE id=1")) {
                    assertTrue(rs.next());
                    assertEquals(31, rs.getInt(1)); // 1-based index
                    assertEquals(31, rs.getInt("age")); // by name
                }

                assertEquals(1, stmt.executeUpdate("DELETE FROM users WHERE id=2"));

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM users")) {
                    assertTrue(rs.next());
                    assertFalse(rs.next(), "only Alice should remain");
                }
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void serverSideErrorBecomesARealSQLException() throws Exception {
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class,
                () -> stmt.executeQuery("SELECT * FROM a_table_that_does_not_exist"));
            assertTrue(ex.getMessage().contains("not found"));
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void unsupportedFeaturesThrowClearlyRatherThanSilentlyNoOp() throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            assertThrows(SQLFeatureNotSupportedException.class, () -> conn.setAutoCommit(false));
            assertThrows(SQLFeatureNotSupportedException.class, conn::rollback);
            // setAutoCommit(true) and commit() ARE supported (as honest no-ops) - only the
            // "not how this engine works" cases should throw.
            conn.setAutoCommit(true);
            conn.commit();
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void driverRejectsUrlsItDoesNotOwn() throws Exception {
        StratosDriver driver = new StratosDriver();
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432/db"));
        assertTrue(driver.acceptsURL("jdbc:stratos://localhost:5432/"));
        assertNull(driver.connect("jdbc:postgresql://localhost:5432/db", null));
    }
}
