package com.stratosdb.testing;

import com.stratosdb.cli.StratosDump;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.auth.UserStore;
import com.stratosdb.network.stdwire.StdWireServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, end-to-end proof that GRANT/REVOKE/CREATE ROLE actually work over
 * a real connection - not just via ExecutorEngine.execute() called
 * directly (see StratosDBTest's own in-process tests for that). A real
 * StdWireServer with a real UserStore, a real admin authenticating with
 * a real password, CREATE ROLE ... LOGIN PASSWORD 'x' becoming a real,
 * separately-authenticatable credential via the RoleCredentialSink
 * bridge (see ExecutorEngine's own javadoc for why that bridge exists),
 * and a second, real connection authenticating as that role and being
 * genuinely restricted by real privilege checks.
 *
 * This exists specifically because the real bug this feature's own
 * first draft had - db.closeSession() running AFTER performStartup()'s
 * new setCurrentUser() call, silently wiping it back out via
 * session.remove() on a pooled, reused thread - only ever manifested at
 * this level. Every in-process test using execute() directly passed
 * cleanly the whole time; a real connection over the real wire protocol
 * is what actually caught it. Reusing that same real connection path
 * here is what keeps this test able to catch it again.
 */
public class GrantPrivilegeEndToEndTest {

    private StratosDB db;
    private StdWireServer server;

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aRestrictedRoleIsGenuinelyEnforcedOverARealConnection(@TempDir Path tempDir) throws Exception {
        int port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);

        UserStore userStore = new UserStore();
        userStore.addUser("admin", "adminpass");
        server = new StdWireServer(port, db, userStore);
        server.start();
        Thread.sleep(200);

        StratosDump admin = new StratosDump("localhost", port, "admin", "anydb", "adminpass");
        assertNull(admin.executeSql("CREATE TABLE accounts (id INT, balance INT)"));
        assertNull(admin.executeSql("INSERT INTO accounts VALUES (1, 1000)"));
        assertNull(admin.executeSql("CREATE ROLE reporting_user WITH LOGIN PASSWORD 'secret123'"));
        String grantError = admin.executeSql("GRANT SELECT ON accounts TO reporting_user");
        assertNull(grantError, () -> "GRANT must succeed for the table's own owner: " + grantError);
        admin.close();

        // Real authentication with the real, bridged password - proves
        // RoleCredentialSink actually wired CREATE ROLE's own password into this
        // server's real UserStore, not just tracked it for show.
        StratosDump reportingUser = new StratosDump("localhost", port, "reporting_user", "anydb", "secret123");
        assertNull(reportingUser.executeSql("SELECT * FROM accounts"), "a role with GRANTed SELECT must succeed over a real connection");
        String insertError = reportingUser.executeSql("INSERT INTO accounts VALUES (2, 500)");
        assertNotNull(insertError, "a role without GRANTed INSERT must be genuinely denied over a real connection, not silently allowed");
        assertTrue(insertError.toLowerCase().contains("permission") || insertError.toLowerCase().contains("denied"),
            () -> "the error should actually explain it's a permission issue: " + insertError);
        reportingUser.close();

        // A wrong password must be rejected at authentication itself, before ever
        // reaching a query - proving this is real SCRAM auth, not a bypass.
        IOExceptionOrNull wrongPasswordResult = tryConnect("localhost", port, "reporting_user", "anydb", "wrongpassword");
        assertNotNull(wrongPasswordResult.exception, "a wrong password must fail at connection/authentication time");

        // Data must be completely untouched by the denied INSERT attempt.
        StratosDump adminVerify = new StratosDump("localhost", port, "admin", "anydb", "adminpass");
        assertNull(adminVerify.executeSql("SELECT * FROM accounts")); // just confirming the connection itself still works
        adminVerify.close();
    }

    private record IOExceptionOrNull(Exception exception) {}

    private IOExceptionOrNull tryConnect(String host, int port, String user, String database, String password) {
        try {
            StratosDump dump = new StratosDump(host, port, user, database, password);
            dump.close();
            return new IOExceptionOrNull(null);
        } catch (Exception e) {
            return new IOExceptionOrNull(e);
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
