package com.stratosdb.jdbc;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.auth.UserStore;
import com.stratosdb.network.stdwire.StdWireServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises the JDBC driver exactly as a real Java application would:
 * through java.sql.DriverManager, not by referencing StratosDriver
 * directly - against StdWireServer, this project's own real, current
 * server (not the old, dead {@code StratosServer}/{@code WireProtocol}
 * this test used to pair the driver with).
 *
 * This whole test file replaces the previous version entirely, for a
 * real, previously-undiscovered reason worth stating plainly rather than
 * silently swapping the server class: the driver used to speak a small,
 * custom binary protocol that only the old, now-deleted StratosServer
 * ever understood - a protocol that could never talk to StdWireServer,
 * this project's own actual, current server, at all. A real client built
 * against the old driver would hang forever connecting to StdWireServer
 * (confirmed directly with a real client-side thread dump showing the
 * connecting thread permanently blocked in the old handshake code's own
 * socket read - not a timing bug, a genuine protocol mismatch). The
 * driver has been rewritten from scratch to speak StdWireServer's own
 * real, current PostgreSQL-wire-protocol-v3-compatible protocol - see
 * StratosConnection's own javadoc for the full account - and this test
 * now proves that real rewrite against the real server, including
 * capabilities the old driver never had at all: a real PreparedStatement
 * (via the real extended query protocol) and a real DatabaseMetaData
 * (via this engine's own real, native SHOW TABLES/SHOW CATALOG
 * introspection, since StratosDB has no pg_catalog to delegate to).
 */
class StratosDriverTest {

    @TempDir
    Path tempDir;

    private StratosDB db;
    private StdWireServer server;
    private String url;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setPort(port);
        db = new StratosDB(config);
        server = new StdWireServer(port, db);
        server.start();
        url = "jdbc:stratos://localhost:" + port + "/testdb";
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    private Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "anyuser");
        return DriverManager.getConnection(url, props);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void fullCrudRoundTripThroughDriverManager() throws Exception {
        try (Connection conn = connect()) {
            assertFalse(conn.isClosed());

            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE items (id INT NOT NULL PRIMARY KEY, name VARCHAR NOT NULL, price FLOAT DEFAULT 0)");
                assertEquals(1, s.executeUpdate("INSERT INTO items VALUES (1, 'Widget', 9.99)"));
                s.executeUpdate("INSERT INTO items VALUES (2, 'Gadget', 19.99)");

                try (ResultSet rs = s.executeQuery("SELECT id, name, price FROM items ORDER BY id")) {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    assertEquals(3, rsmd.getColumnCount());
                    assertEquals("id", rsmd.getColumnName(1));

                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("id"));
                    assertEquals("Widget", rs.getString("name"));
                    assertEquals(9.99, rs.getDouble("price"), 0.001);
                    assertTrue(rs.next());
                    assertFalse(rs.next());
                }

                assertEquals(1, s.executeUpdate("UPDATE items SET price = 29.99 WHERE id = 2"));
                assertEquals(1, s.executeUpdate("DELETE FROM items WHERE id = 1"));

                try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM items")) {
                    rs.next();
                    assertEquals(1, rs.getInt(1));
                }
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void preparedStatementUsesRealBoundParametersOverTheRealExtendedProtocol() throws Exception {
        try (Connection conn = connect(); Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE people (id INT, name VARCHAR)");

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO people VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "Alice");
                assertEquals(1, ps.executeUpdate());
            }

            // A real ? inside a string literal must never be mistaken for a
            // real parameter marker.
            s.executeUpdate("INSERT INTO people VALUES (2, 'What?')");
            try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM people WHERE name = 'What?' AND id = ?")) {
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("What?", rs.getString(1));
                }
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void databaseMetaDataReflectsRealNativeIntrospection() throws Exception {
        try (Connection conn = connect(); Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, total FLOAT DEFAULT 0)");
            s.execute("CREATE INDEX idx_total ON orders (total)");

            DatabaseMetaData meta = conn.getMetaData();

            boolean foundTable = false;
            try (ResultSet rs = meta.getTables(null, null, "%", null)) {
                while (rs.next()) {
                    if ("orders".equals(rs.getString("TABLE_NAME"))) foundTable = true;
                }
            }
            assertTrue(foundTable, "getTables() must find the real, created table");

            boolean sawNotNullId = false, sawDefaultTotal = false;
            try (ResultSet rs = meta.getColumns(null, null, "orders", "%")) {
                while (rs.next()) {
                    String col = rs.getString("COLUMN_NAME");
                    if ("id".equalsIgnoreCase(col)) sawNotNullId = rs.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls;
                    if ("total".equalsIgnoreCase(col)) sawDefaultTotal = rs.getString("COLUMN_DEF") != null;
                }
            }
            assertTrue(sawNotNullId, "getColumns() must correctly report id's real NOT NULL constraint");
            assertTrue(sawDefaultTotal, "getColumns() must correctly report total's real DEFAULT");

            boolean foundPk = false;
            try (ResultSet rs = meta.getPrimaryKeys(null, null, "orders")) {
                while (rs.next()) {
                    if ("id".equalsIgnoreCase(rs.getString("COLUMN_NAME"))) foundPk = true;
                }
            }
            assertTrue(foundPk, "getPrimaryKeys() must find the real primary key");

            boolean foundIndex = false;
            try (ResultSet rs = meta.getIndexInfo(null, null, "orders", false, false)) {
                while (rs.next()) {
                    if ("idx_total".equalsIgnoreCase(rs.getString("INDEX_NAME"))) foundIndex = true;
                }
            }
            assertTrue(foundIndex, "getIndexInfo() must find the real, created index");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void transactionControlCommitsAndRollsBackForReal() throws Exception {
        try (Connection conn = connect(); Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t (id INT)");

            conn.setAutoCommit(false);
            s.executeUpdate("INSERT INTO t VALUES (1)");
            conn.rollback();
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM t")) {
                rs.next();
                assertEquals(0, rs.getInt(1), "a real rollback must genuinely undo the insert");
            }

            s.executeUpdate("INSERT INTO t VALUES (2)");
            conn.commit();
            conn.setAutoCommit(true);
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM t")) {
                rs.next();
                assertEquals(1, rs.getInt(1), "a real commit must genuinely persist the insert");
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void scramAuthenticationSucceedsWithCorrectPasswordAndFailsCleanlyWithAWrongOne() throws Exception {
        UserStore userStore = new UserStore();
        userStore.addUser("alice", "correcthorsebattery");
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        server.stop();
        db.shutdown();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.resolve("scram").toString());
        db = new StratosDB(config);
        server = new StdWireServer(port, db, userStore);
        server.start();
        String scramUrl = "jdbc:stratos://localhost:" + port + "/testdb";

        Properties goodProps = new Properties();
        goodProps.setProperty("user", "alice");
        goodProps.setProperty("password", "correcthorsebattery");
        try (Connection conn = DriverManager.getConnection(scramUrl, goodProps)) {
            assertTrue(conn.isValid(5), "a real, correct SCRAM password must succeed");
        }

        Properties badProps = new Properties();
        badProps.setProperty("user", "alice");
        badProps.setProperty("password", "wrongpassword");
        assertThrows(SQLException.class, () -> DriverManager.getConnection(scramUrl, badProps),
            "a real, wrong SCRAM password must be cleanly rejected, not hang or silently succeed");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void unsupportedFeaturesThrowClearlyRatherThanSilentlyNoOp() throws Exception {
        try (Connection conn = connect()) {
            assertThrows(SQLFeatureNotSupportedException.class, conn::getTypeMap,
                "a genuinely unsupported Connection method must throw, not silently pretend to succeed");
        }
    }

    /**
     * A real, previously-undiscovered bug, found only by a real, end-to-end
     * DBNavigator integration test - every other test in this file drives
     * this driver directly via {@code DriverManager}, which never exercises
     * what a real connection pool does. HikariCP (and, by extension, any
     * other real pool - this is standard, widespread pool behavior, not a
     * HikariCP quirk) calls a real, fixed set of "connection setup" methods
     * on every fresh connection, unconditionally, before ever handing it
     * back to application code: {@code setReadOnly()} chief among them.
     * This driver used to throw {@code SQLFeatureNotSupportedException} for
     * it, since it fell through to the strict "throw for anything
     * unrecognized" default - meaning a real HikariCP pool could never even
     * finish initializing against this driver at all, a silent, total
     * failure for any real, pool-based consumer that no DriverManager-based
     * test here would ever have caught.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void connectionSurvivesRealHikariCpPoolSetup() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername("anyuser");
        config.setMaximumPoolSize(2);
        try (HikariDataSource pool = new HikariDataSource(config)) {
            try (Connection conn = pool.getConnection()) {
                assertTrue(conn.isValid(5), "a real connection obtained through a real HikariCP pool must be usable");
                try (Statement s = conn.createStatement()) {
                    s.execute("CREATE TABLE hikari_pool_test (id INT)");
                    s.executeUpdate("INSERT INTO hikari_pool_test VALUES (1)");
                    try (ResultSet rs = s.executeQuery("SELECT id FROM hikari_pool_test")) {
                        assertTrue(rs.next(), "a real query through a real pooled connection must return real data");
                        assertEquals(1, rs.getInt(1));
                    }
                }
            }
        }
    }
}
