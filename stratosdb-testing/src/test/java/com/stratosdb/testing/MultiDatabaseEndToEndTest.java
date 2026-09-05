package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosCluster;
import com.stratosdb.network.stdwire.StdWireServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, end-to-end proof that {@link StratosCluster} - StratosDB's own
 * real, PostgreSQL-style "one server, many independently-isolated
 * databases" support - actually works over a real connection, through
 * the real, unmodified JDBC driver (the same one DBNavigator, or any
 * other real consumer, actually uses): a fresh cluster starts with a
 * real, connectable default database; {@code CREATE DATABASE}/
 * {@code DROP DATABASE}/{@code SHOW DATABASES} all genuinely work; two
 * databases are genuinely, fully isolated from each other (their own
 * separate tables, separate data - not a shared catalog with a
 * name-prefix trick); the real safety rule (refusing to drop your own
 * currently-open database) matches PostgreSQL's own real behavior
 * exactly; and connecting to a database that doesn't exist is refused
 * cleanly, not silently accepted or left to fail confusingly later.
 *
 * Found and fixed along the way, while building this: a real,
 * pre-existing bug in how the wire protocol tags a row-returning
 * result - see StdWireServer's own real fix, verified indirectly here
 * every time {@code SHOW DATABASES} is queried via a plain
 * {@code executeQuery()} call.
 */
public class MultiDatabaseEndToEndTest {

    private StdWireServer server;
    private int port;

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
    }

    private StratosCluster startClusterServer(Path tempDir) throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        StratosCluster cluster = new StratosCluster(config);
        server = new StdWireServer(port, cluster);
        server.start();
        Thread.sleep(200);
        return cluster;
    }

    private Connection connect(String database) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "anyuser");
        return DriverManager.getConnection("jdbc:stratos://localhost:" + port + "/" + database, props);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void freshClusterStartsWithARealConnectableDefaultDatabase(@TempDir Path tempDir) throws Exception {
        startClusterServer(tempDir);

        try (Connection conn = connect("")) {
            assertTrue(conn.isValid(5));
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("SHOW DATABASES");
            List<String> names = new ArrayList<>();
            while (rs.next()) names.add(rs.getString(1));
            assertEquals(List.of(StratosCluster.DEFAULT_DATABASE), names);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void createDatabaseAndTwoDatabasesAreGenuinelyIsolated(@TempDir Path tempDir) throws Exception {
        startClusterServer(tempDir);

        try (Connection conn = connect(StratosCluster.DEFAULT_DATABASE)) {
            Statement s = conn.createStatement();
            s.executeUpdate("CREATE DATABASE mydb");

            ResultSet rs = s.executeQuery("SHOW DATABASES");
            List<String> names = new ArrayList<>();
            while (rs.next()) names.add(rs.getString(1));
            assertEquals(List.of("mydb", StratosCluster.DEFAULT_DATABASE), names);

            s.execute("CREATE TABLE default_only (id INT)");
            s.executeUpdate("INSERT INTO default_only VALUES (1)");
        }

        try (Connection mydbConn = connect("mydb")) {
            Statement s2 = mydbConn.createStatement();

            // Real isolation: the default database's own table is genuinely not visible here.
            assertThrows(SQLException.class, () -> s2.executeQuery("SELECT * FROM default_only"),
                "a table created in a different database must not be visible here at all");

            s2.execute("CREATE TABLE mydb_only (id INT)");
            s2.executeUpdate("INSERT INTO mydb_only VALUES (42)");
            ResultSet rs2 = s2.executeQuery("SELECT id FROM mydb_only");
            assertTrue(rs2.next());
            assertEquals(42, rs2.getInt(1));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void cannotDropTheCurrentlyConnectedDatabaseButCanDropAnotherOne(@TempDir Path tempDir) throws Exception {
        startClusterServer(tempDir);

        try (Connection conn = connect(StratosCluster.DEFAULT_DATABASE)) {
            conn.createStatement().executeUpdate("CREATE DATABASE mydb");
        }

        try (Connection mydbConn = connect("mydb")) {
            Statement s = mydbConn.createStatement();
            SQLException e = assertThrows(SQLException.class, () -> s.executeUpdate("DROP DATABASE mydb"),
                "must refuse to drop the database this very connection is currently using");
            assertTrue(e.getMessage().contains("currently open database"));
        }

        try (Connection conn = connect(StratosCluster.DEFAULT_DATABASE)) {
            Statement s = conn.createStatement();
            s.executeUpdate("DROP DATABASE mydb"); // not connected to it - must succeed
            ResultSet rs = s.executeQuery("SHOW DATABASES");
            List<String> names = new ArrayList<>();
            while (rs.next()) names.add(rs.getString(1));
            assertEquals(List.of(StratosCluster.DEFAULT_DATABASE), names);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void connectingToANonexistentDatabaseIsRefusedCleanly(@TempDir Path tempDir) throws Exception {
        startClusterServer(tempDir);

        SQLException e = assertThrows(SQLException.class, () -> connect("doesnotexist"),
            "a real connection attempt to an unknown database must fail, not silently succeed or hang");
        assertTrue(e.getMessage().contains("does not exist"));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void createDatabaseAndShowDatabasesHonestlyRefuseOnAPlainNonClusteredInstance(@TempDir Path tempDir) throws Exception {
        // A plain, single-database StdWireServer(port, StratosDB) - the original,
        // unchanged, real single-database constructor - must keep working exactly
        // as it always has, AND must honestly refuse CREATE DATABASE/DROP DATABASE
        // rather than silently doing nothing or throwing a confusing NPE.
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        com.stratosdb.core.StratosDB plainDb = new com.stratosdb.core.StratosDB(config);
        server = new StdWireServer(port, plainDb);
        server.start();
        Thread.sleep(200);

        try (Connection conn = connect("")) {
            Statement s = conn.createStatement();
            SQLException e = assertThrows(SQLException.class, () -> s.executeUpdate("CREATE DATABASE mydb"),
                "a plain, non-clustered instance must honestly refuse CREATE DATABASE, not silently accept it");
            assertTrue(e.getMessage().contains("cluster"));

            // SHOW DATABASES on a plain instance is a real, honest empty result, not an error.
            ResultSet rs = s.executeQuery("SHOW DATABASES");
            assertFalse(rs.next());
        } finally {
            plainDb.shutdown();
        }
    }

    /**
     * Real, previously-latent bug found via a real, live DBNavigator
     * session: DROP DATABASE followed by CREATE DATABASE of the exact
     * same name appeared to "keep" the previous database's own data.
     * The real root cause: BufferPoolManager.close() flushed dirty pages
     * and closed file handles, but never cleared its own in-memory page
     * cache - so any connection that was already open and had touched a
     * table before the drop could keep silently serving its own already
     * -cached pages indefinitely, even after the real underlying files
     * were deleted and a genuinely new, empty database created at that
     * same path. This is exactly what DBNavigator's own per-database
     * connection caching (one pooled connection per catalog) would hit:
     * re-expanding the same database name after a drop+recreate, without
     * DBNavigator itself ever knowing to open a fresh connection.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void dropThenRecreateSameNameNeverLeaksThePreviousDatabasesData(@TempDir Path tempDir) throws Exception {
        startClusterServer(tempDir);

        try (Connection admin = connect(StratosCluster.DEFAULT_DATABASE)) {
            admin.createStatement().executeUpdate("CREATE DATABASE testdb");
        }

        // A connection that stays open THROUGH the drop+recreate - exactly
        // matching what a real, pooled, per-catalog client connection (like
        // DBNavigator's own) would do if it never explicitly reconnects.
        try (Connection stale = connect("testdb")) {
            stale.createStatement().execute("CREATE TABLE t (id INT)");
            stale.createStatement().executeUpdate("INSERT INTO t VALUES (999)");

            try (Connection admin = connect(StratosCluster.DEFAULT_DATABASE)) {
                admin.createStatement().executeUpdate("DROP DATABASE testdb");
                admin.createStatement().executeUpdate("CREATE DATABASE testdb");
            }

            // The real, decisive check: the stale connection must NEVER see
            // the previous database's own data - it may fail outright, or
            // (as this fix produces) see nothing at all now that the table
            // that stale connection thinks it knows about is genuinely gone,
            // but it must not return the old value 999 under any
            // circumstance.
            try {
                ResultSet rs = stale.createStatement().executeQuery("SELECT * FROM t");
                assertFalse(rs.next(), "a stale connection must never still see the previous database's own row");
            } catch (SQLException expectedOrNot) {
                // A clean failure here is an acceptable, honest outcome too -
                // the one and only real requirement is that 999 never comes back.
            }
        }

        // A genuinely fresh connection must see the real, newly-created,
        // empty database - not the previous one's own leftover table.
        try (Connection fresh = connect("testdb")) {
            SQLException e = assertThrows(SQLException.class,
                () -> fresh.createStatement().executeQuery("SELECT * FROM t"),
                "the recreated database must be genuinely empty - it never had this table created in it");
            assertTrue(e.getMessage().contains("not found") || e.getMessage().contains("Table"));
        }
    }

    /**
     * Real, previously-latent bug found via a real, live DBNavigator
     * session: expanding the Tables folder of a genuinely empty database
     * (a completely normal, common situation - a freshly-created database
     * with nothing in it yet, not an error) failed with "Expected column
     * not found in native introspection result: table_name". The real
     * root cause: SHOW TABLES (and several sibling SHOW-style statements)
     * derived their own column names from the first matching row - which
     * doesn't exist when there are genuinely zero rows, so the wire
     * protocol reported zero *columns*, not just zero rows, breaking any
     * real client that expects a fixed column shape regardless of row
     * count.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void emptyDatabaseStillReportsARealTableNameColumnShape(@TempDir Path tempDir) throws Exception {
        startClusterServer(tempDir);

        try (Connection admin = connect(StratosCluster.DEFAULT_DATABASE)) {
            admin.createStatement().executeUpdate("CREATE DATABASE emptydb");
        }

        try (Connection conn = connect("emptydb")) {
            // The real, decisive check: DatabaseMetaData.getTables() must
            // succeed with zero rows, not throw.
            ResultSet rs = conn.getMetaData().getTables(null, null, "%", null);
            assertFalse(rs.next(), "a genuinely empty database has zero tables, not an error");

            // Directly confirms the wire protocol itself declares the real
            // column, not just that the JDBC-level call happens to survive.
            ResultSet direct = conn.createStatement().executeQuery("SHOW TABLES");
            assertEquals(1, direct.getMetaData().getColumnCount());
            assertEquals("table_name", direct.getMetaData().getColumnName(1));
            assertFalse(direct.next());
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
