package com.stratosdb.jdbc;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireServer;
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
 * A real, honest replacement for what used to be here - the previous
 * version of this test exercised real TLS against the old, now-deleted
 * {@code StratosServer} (a real certificate, a real SSLContext, a real
 * encrypted handshake). That server is gone (see StratosDriverTest's own
 * javadoc for the full account of why), and StdWireServer - this
 * project's own real, current server, which the rewritten driver now
 * speaks to exclusively - has no TLS support at all yet: every SSL
 * negotiation attempt is unconditionally declined (see StdWireServer's
 * own startup handling).
 *
 * Rather than deleting TLS test coverage entirely, or leaving a test
 * that would silently test nothing real, this verifies the one real,
 * honest thing that IS true today: requesting {@code ssl=true} throws a
 * clear, immediate SQLException explaining that the real, current server
 * doesn't support it yet - not a silent, unencrypted fallback, and not a
 * hang waiting on a negotiation the server will never complete (see
 * StratosConnection's own javadoc for this exact design choice). Real
 * TLS support against StdWireServer is real, separate, future work, not
 * something this test pretends already exists.
 */
class TlsIntegrationTest {

    @TempDir
    Path tempDir;

    private StratosDB db;
    private StdWireServer server;
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
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void sslTrueThrowsAClearHonestErrorRatherThanHangingOrSilentlyConnectingUnencrypted() {
        Properties props = new Properties();
        props.setProperty("user", "anyuser");
        props.setProperty("ssl", "true");
        String url = "jdbc:stratos://localhost:" + port + "/testdb";

        SQLException ex = assertThrows(SQLException.class, () -> DriverManager.getConnection(url, props));
        assertTrue(ex.getMessage().toLowerCase().contains("tls"),
            () -> "expected a clear, honest message about missing TLS support, got: " + ex.getMessage());
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void plainUnencryptedConnectionStillWorksNormally() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "anyuser");
        String url = "jdbc:stratos://localhost:" + port + "/testdb";
        try (Connection conn = DriverManager.getConnection(url, props)) {
            assertTrue(conn.isValid(5));
        }
    }
}
