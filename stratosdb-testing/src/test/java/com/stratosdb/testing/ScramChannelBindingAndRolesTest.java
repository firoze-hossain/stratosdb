package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosCluster;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.auth.ScramClient;
import com.stratosdb.network.auth.ScramSha256;
import com.stratosdb.network.auth.UserStore;
import com.stratosdb.network.stdwire.StdWireMessages;
import com.stratosdb.network.stdwire.StdWireServer;
import com.stratosdb.network.tls.TlsConfig;
import com.stratosdb.network.tls.TlsSupport;
import com.stratosdb.sql.executor.QueryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end and integration tests for Sprint 10 Week 20:
 * 1. RFC 5802 / RFC 5929 SCRAM-SHA-256-PLUS Channel Binding (tls-server-end-point and tls-unique).
 * 2. Man-in-the-Middle (MITM) attack detection and downgrade prevention.
 * 3. Multi-tier role inheritance DAG (transitive permissions through arbitrary role hierarchies).
 * 4. Cycle prevention in role grants and WITH ADMIN OPTION delegation.
 * 5. Database-level permissions (CONNECT, CREATE) enforced at connection startup and DDL execution.
 * 6. Catalog persistence and recovery of role hierarchies and database permissions across restarts.
 */
public class ScramChannelBindingAndRolesTest {

    static {
        try {
            Class.forName("com.stratosdb.jdbc.StratosDriver");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @TempDir
    static Path sharedTempDir;

    private static String serverKeystorePath;
    private static final char[] KEYSTORE_PASSWORD = "test-cert-password".toCharArray();

    private StratosDB db;
    private StratosCluster cluster;
    private StdWireServer server;
    private int port;

    @BeforeAll
    static void generateTlsCertificates() throws Exception {
        serverKeystorePath = sharedTempDir.resolve("server_scram_keystore.p12").toString();
        TlsSupport.generateKeyStore(
            serverKeystorePath,
            KEYSTORE_PASSWORD,
            "CN=localhost, OU=Security, O=StratosDB, C=US",
            "dns:localhost,ip:127.0.0.1"
        );
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
            server = null;
        }
        if (cluster != null) {
            cluster.shutdown();
            cluster = null;
        }
        if (db != null) {
            db.shutdown();
            db = null;
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    private TlsConfig createServerTlsConfig() {
        TlsConfig config = new TlsConfig();
        config.setEnabled(true);
        config.setKeystorePath(serverKeystorePath);
        config.setKeystorePassword(KEYSTORE_PASSWORD);
        config.setProtocols(new String[]{"TLSv1.3", "TLSv1.2"});
        return config;
    }

    // =========================================================================
    // 1. SCRAM-SHA-256-PLUS CHANNEL BINDING (tls-server-end-point & tls-unique)
    // =========================================================================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testScramSha256PlusTlsServerEndPointHandshakeAndQueries(@TempDir Path tempDir) throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);

        UserStore userStore = new UserStore();
        userStore.addUser("alice", "AliceSuperSecretPassword123!");

        TlsConfig tlsConfig = createServerTlsConfig();
        server = new StdWireServer(port, db, userStore, tlsConfig);
        server.start();
        Thread.sleep(200);

        // Connect via JDBC with SSL enabled and user credentials.
        // StratosConnection detects SSLSocket, extracts peer cert SHA-256,
        // and automatically negotiates SCRAM-SHA-256-PLUS with tls-server-end-point.
        Properties props = new Properties();
        props.setProperty("user", "alice");
        props.setProperty("password", "AliceSuperSecretPassword123!");
        props.setProperty("ssl", "true");

        String url = "jdbc:stratos://localhost:" + port + "/stratos";
        try (Connection conn = DriverManager.getConnection(url, props)) {
            assertTrue(conn.isValid(5));
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE secure_audit (id INT, note TEXT);");
                s.execute("INSERT INTO secure_audit VALUES (1, 'ChannelBindingAuthenticated');");
                try (ResultSet rs = s.executeQuery("SELECT note FROM secure_audit WHERE id = 1;")) {
                    assertTrue(rs.next());
                    assertEquals("ChannelBindingAuthenticated", rs.getString(1));
                }
            }
        }
    }

    @Test
    void testScramSha256PlusTlsUniqueHandshakeDirect() throws Exception {
        UserStore userStore = new UserStore();
        userStore.addUser("charlie", "CharliePassword456!");

        byte[] fakeTlsUniqueBytes = "mock-tls-unique-finished-data-32b".getBytes(StandardCharsets.UTF_8);
        Map<String, byte[]> serverBindings = Map.of("tls-unique", fakeTlsUniqueBytes);

        UserStore.ScramCredential credential = userStore.getScramCredential("charlie");
        ScramSha256.Handshake serverHandshake = new ScramSha256.Handshake(
            "charlie", credential, true, serverBindings, ScramSha256.MECHANISM_NAME_PLUS);

        // Client configured with tls-unique channel binding
        ScramClient client = new ScramClient("charlie", "CharliePassword456!", "tls-unique", fakeTlsUniqueBytes);
        String clientFirst = client.buildClientFirstMessage();
        assertTrue(clientFirst.startsWith("p=tls-unique,,"));

        String serverFirst = serverHandshake.clientFirst(clientFirst);
        assertTrue(serverFirst.contains("r="));
        assertTrue(serverFirst.contains("s="));
        assertTrue(serverFirst.contains("i="));

        String clientFinal = client.buildClientFinalMessage(serverFirst);
        assertTrue(clientFinal.contains("c="));
        assertTrue(clientFinal.contains("p="));

        String serverFinal = serverHandshake.clientFinal(clientFinal);
        assertTrue(serverFinal.startsWith("v="));
        assertTrue(client.verifyServerFinalMessage(serverFinal));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testScramSha256PlusMitmDetectionWithForgedCertificateHash(@TempDir Path tempDir) throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);

        UserStore userStore = new UserStore();
        userStore.addUser("david", "DavidPassword789!");

        TlsConfig tlsConfig = createServerTlsConfig();
        server = new StdWireServer(port, db, userStore, tlsConfig);
        server.start();
        Thread.sleep(200);

        // Establish TLS connection
        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream rawOut = new DataOutputStream(socket.getOutputStream());
            DataInputStream rawIn = new DataInputStream(socket.getInputStream());

            StdWireMessages.writeSslRequest(rawOut);
            assertEquals('S', rawIn.read());

            SSLContext clientCtx = TlsSupport.insecureTrustAllClientContext();
            try (SSLSocket sslSocket = (SSLSocket) clientCtx.getSocketFactory().createSocket(socket, "localhost", port, true)) {
                sslSocket.setUseClientMode(true);
                sslSocket.startHandshake();

                DataOutputStream encOut = new DataOutputStream(new BufferedOutputStream(sslSocket.getOutputStream()));
                DataInputStream encIn = new DataInputStream(new BufferedInputStream(sslSocket.getInputStream()));

                StdWireMessages.writeStartupMessage(encOut, "david", "stratos");

                // Expect AuthenticationSASL ('R' with code 10)
                StdWireMessages.TypedMessage saslMsg = StdWireMessages.readTypedMessage(encIn);
                assertEquals('R', saslMsg.type());

                // MITM Attacker scenario:
                // The client believes it is talking to a MITM proxy with a DIFFERENT certificate hash.
                byte[] forgedCertHash = new byte[32];
                for (int i = 0; i < 32; i++) forgedCertHash[i] = (byte) 0xEE;

                ScramClient mitmClient = new ScramClient("david", "DavidPassword789!", "tls-server-end-point", forgedCertHash);
                String clientFirst = mitmClient.buildClientFirstMessage();

                // Send SASLInitialResponse with SCRAM-SHA-256-PLUS
                byte[] mechBytes = ScramSha256.MECHANISM_NAME_PLUS.getBytes(StandardCharsets.UTF_8);
                byte[] dataBytes = clientFirst.getBytes(StandardCharsets.UTF_8);
                encOut.writeByte('p');
                encOut.writeInt(mechBytes.length + 1 + 4 + dataBytes.length + 4);
                encOut.write(mechBytes);
                encOut.writeByte(0);
                encOut.writeInt(dataBytes.length);
                encOut.write(dataBytes);
                encOut.flush();

                // Server responds with SASLContinue ('R' code 11)
                StdWireMessages.TypedMessage contMsg = StdWireMessages.readTypedMessage(encIn);
                assertEquals('R', contMsg.type());
                String serverFirst = new String(contMsg.body(), 4, contMsg.body().length - 4, StandardCharsets.UTF_8);

                String clientFinal = mitmClient.buildClientFinalMessage(serverFirst);
                byte[] cfBytes = clientFinal.getBytes(StandardCharsets.UTF_8);
                encOut.writeByte('p');
                encOut.writeInt(cfBytes.length + 4);
                encOut.write(cfBytes);
                encOut.flush();

                // Server MUST reject the connection due to channel binding verification mismatch (MITM detected)
                StdWireMessages.TypedMessage errorMsg = StdWireMessages.readTypedMessage(encIn);
                assertEquals('E', errorMsg.type(), "Server must reject forged channel binding with ErrorResponse");
                String errorText = new String(errorMsg.body(), StandardCharsets.UTF_8);
                assertTrue(errorText.contains("password authentication failed") || errorText.contains("failed"),
                    "Error text should indicate authentication failure: " + errorText);
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testScramSha256PlusDowngradePrevention(@TempDir Path tempDir) throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);

        UserStore userStore = new UserStore();
        userStore.addUser("eve", "EvePassword123!");

        TlsConfig tlsConfig = createServerTlsConfig();
        server = new StdWireServer(port, db, userStore, tlsConfig);
        server.start();
        Thread.sleep(200);

        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream rawOut = new DataOutputStream(socket.getOutputStream());
            DataInputStream rawIn = new DataInputStream(socket.getInputStream());

            StdWireMessages.writeSslRequest(rawOut);
            assertEquals('S', rawIn.read());

            SSLContext clientCtx = TlsSupport.insecureTrustAllClientContext();
            try (SSLSocket sslSocket = (SSLSocket) clientCtx.getSocketFactory().createSocket(socket, "localhost", port, true)) {
                sslSocket.setUseClientMode(true);
                sslSocket.startHandshake();

                DataOutputStream encOut = new DataOutputStream(new BufferedOutputStream(sslSocket.getOutputStream()));
                DataInputStream encIn = new DataInputStream(new BufferedInputStream(sslSocket.getInputStream()));

                StdWireMessages.writeStartupMessage(encOut, "eve", "stratos");
                StdWireMessages.TypedMessage saslMsg = StdWireMessages.readTypedMessage(encIn);
                assertEquals('R', saslMsg.type());

                // Downgrade attack: Client sends 'y,,' over a TLS connection supporting channel binding
                String downgradeClientFirst = "y,,n=eve,r=downgradeNonce123456";
                byte[] mechBytes = ScramSha256.MECHANISM_NAME.getBytes(StandardCharsets.UTF_8);
                byte[] dataBytes = downgradeClientFirst.getBytes(StandardCharsets.UTF_8);
                encOut.writeByte('p');
                encOut.writeInt(mechBytes.length + 1 + 4 + dataBytes.length + 4);
                encOut.write(mechBytes);
                encOut.writeByte(0);
                encOut.writeInt(dataBytes.length);
                encOut.write(dataBytes);
                encOut.flush();

                StdWireMessages.TypedMessage errorMsg = StdWireMessages.readTypedMessage(encIn);
                assertEquals('E', errorMsg.type(), "Server must reject downgrade attempt with ErrorResponse");
                String errorText = new String(errorMsg.body(), StandardCharsets.UTF_8);
                assertTrue(errorText.toLowerCase().contains("downgrade"),
                    "Error text should state downgrade detected: " + errorText);
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testScramSha256PlusRejectedOnPlaintextConnection(@TempDir Path tempDir) throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);

        UserStore userStore = new UserStore();
        userStore.addUser("frank", "FrankPassword123!");

        server = new StdWireServer(port, db, userStore);
        server.start();
        Thread.sleep(200);

        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            StdWireMessages.writeStartupMessage(out, "frank", "stratos");
            StdWireMessages.TypedMessage saslMsg = StdWireMessages.readTypedMessage(in);
            assertEquals('R', saslMsg.type());

            // Plaintext connection requesting SCRAM-SHA-256-PLUS
            String clientFirst = "p=tls-server-end-point,,n=frank,r=somePlaintextNonce";
            byte[] mechBytes = ScramSha256.MECHANISM_NAME_PLUS.getBytes(StandardCharsets.UTF_8);
            byte[] dataBytes = clientFirst.getBytes(StandardCharsets.UTF_8);
            out.writeByte('p');
            out.writeInt(mechBytes.length + 1 + 4 + dataBytes.length + 4);
            out.write(mechBytes);
            out.writeByte(0);
            out.writeInt(dataBytes.length);
            out.write(dataBytes);
            out.flush();

            StdWireMessages.TypedMessage errorMsg = StdWireMessages.readTypedMessage(in);
            assertEquals('E', errorMsg.type());
            String errorText = new String(errorMsg.body(), StandardCharsets.UTF_8);
            assertTrue(errorText.contains("requires a TLS connection"),
                "Server must reject PLUS on plaintext connection: " + errorText);
        }
    }

    // =========================================================================
    // 2. MULTI-TIER ROLE INHERITANCE (DAG)
    // =========================================================================

    @Test
    void testMultiTierRoleInheritanceAndPrivilegeResolution(@TempDir Path tempDir) {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);

        // Setup roles: admin -> r_lead -> r_senior -> r_junior -> user_bob
        db.setCurrentUser("admin");
        db.execute("CREATE ROLE r_lead;");
        db.execute("CREATE ROLE r_senior;");
        db.execute("CREATE ROLE r_junior;");
        db.execute("CREATE ROLE user_bob WITH LOGIN;");

        db.execute("CREATE TABLE projects (id INT, name TEXT);");
        db.execute("INSERT INTO projects VALUES (1, 'Mercury');");
        db.execute("CREATE TABLE finances (id INT, budget INT);");
        db.execute("INSERT INTO finances VALUES (1, 500000);");

        // Grant table permissions to role r_lead
        db.execute("GRANT SELECT, INSERT ON projects TO r_lead;");
        db.execute("GRANT SELECT ON finances TO r_lead;");

        // Build hierarchy chain: r_lead -> r_senior -> r_junior -> user_bob
        db.execute("GRANT r_lead TO r_senior;");
        db.execute("GRANT r_senior TO r_junior;");
        db.execute("GRANT r_junior TO user_bob;");

        // Verify effective roles transitive closure
        Set<String> effective = db.getExecutor().getEffectiveRoles("user_bob");
        assertTrue(effective.contains("user_bob"));
        assertTrue(effective.contains("r_junior"));
        assertTrue(effective.contains("r_senior"));
        assertTrue(effective.contains("r_lead"));

        // Switch to user_bob session
        db.setCurrentUser("user_bob");

        // Inherited SELECT on projects through 3 intermediate roles
        QueryResult selProjects = db.execute("SELECT name FROM projects WHERE id = 1;");
        assertTrue(selProjects.isSuccess());
        assertEquals("Mercury", selProjects.getRows().get(0).getValue(0));

        // Inherited INSERT on projects
        QueryResult insProjects = db.execute("INSERT INTO projects VALUES (2, 'Gemini');");
        assertTrue(insProjects.isSuccess());

        // Inherited SELECT on finances
        QueryResult selFinances = db.execute("SELECT budget FROM finances WHERE id = 1;");
        assertTrue(selFinances.isSuccess());
        assertEquals(500000, selFinances.getRows().get(0).getValue(0));

        // Disallowed INSERT on finances (r_lead only had SELECT)
        QueryResult insFinances = db.execute("INSERT INTO finances VALUES (2, 999999);");
        assertFalse(insFinances.isSuccess(), "user_bob should not have INSERT on finances");
        assertTrue(insFinances.getError().toLowerCase().contains("permission denied"));
    }

    @Test
    void testRoleCyclePrevention(@TempDir Path tempDir) {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);

        db.setCurrentUser("admin");
        db.execute("CREATE ROLE node_a;");
        db.execute("CREATE ROLE node_b;");
        db.execute("CREATE ROLE node_c;");

        QueryResult g1 = db.execute("GRANT node_a TO node_b;");
        assertTrue(g1.isSuccess());

        QueryResult g2 = db.execute("GRANT node_b TO node_c;");
        assertTrue(g2.isSuccess());

        // Attempting to grant node_c back to node_a would create a cycle (node_a -> node_b -> node_c -> node_a)
        QueryResult cycleGrant = db.execute("GRANT node_c TO node_a;");
        assertFalse(cycleGrant.isSuccess(), "Grant causing a cycle must be rejected");
        assertTrue(cycleGrant.getError().toLowerCase().contains("cycle"));

        // Self-grant must also be rejected
        QueryResult selfGrant = db.execute("GRANT node_a TO node_a;");
        assertFalse(selfGrant.isSuccess(), "Self grant must be rejected");
        assertTrue(selfGrant.getError().toLowerCase().contains("itself") || selfGrant.getError().toLowerCase().contains("cycle"));
    }

    @Test
    void testRevokeRoleSeveringInheritance(@TempDir Path tempDir) {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);

        db.setCurrentUser("admin");
        db.execute("CREATE ROLE role_top;");
        db.execute("CREATE ROLE role_mid;");
        db.execute("CREATE ROLE role_leaf;");
        db.execute("CREATE TABLE classified (id INT, secret TEXT);");
        db.execute("INSERT INTO classified VALUES (10, 'Area51');");

        db.execute("GRANT SELECT ON classified TO role_top;");
        db.execute("GRANT role_top TO role_mid;");
        db.execute("GRANT role_mid TO role_leaf;");

        // Verify access works initially
        db.setCurrentUser("role_leaf");
        QueryResult resBefore = db.execute("SELECT secret FROM classified;");
        assertTrue(resBefore.isSuccess());

        // Revoke the intermediate role link
        db.setCurrentUser("admin");
        QueryResult revRes = db.execute("REVOKE role_top FROM role_mid;");
        assertTrue(revRes.isSuccess());

        // Now role_leaf no longer inherits role_top
        db.setCurrentUser("role_leaf");
        QueryResult resAfter = db.execute("SELECT secret FROM classified;");
        assertFalse(resAfter.isSuccess(), "Access should be denied after intermediate role is revoked");
        assertTrue(resAfter.getError().toLowerCase().contains("permission denied"));
    }

    @Test
    void testAdminOptionDelegationAndRevocation(@TempDir Path tempDir) {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);

        db.setCurrentUser("admin");
        db.execute("CREATE ROLE auditor;");
        db.execute("CREATE ROLE sec_manager;");
        db.execute("CREATE ROLE clerk;");
        db.execute("CREATE ROLE intern;");

        // Grant auditor TO sec_manager WITH ADMIN OPTION
        QueryResult gAdmin = db.execute("GRANT auditor TO sec_manager WITH ADMIN OPTION;");
        assertTrue(gAdmin.isSuccess());

        // As sec_manager, grant auditor TO clerk (should succeed due to ADMIN OPTION)
        db.setCurrentUser("sec_manager");
        QueryResult gClerk = db.execute("GRANT auditor TO clerk;");
        assertTrue(gClerk.isSuccess(), "sec_manager has ADMIN OPTION so can grant auditor");

        // As clerk (without ADMIN OPTION), attempt to grant auditor TO intern (should fail)
        db.setCurrentUser("clerk");
        QueryResult gIntern = db.execute("GRANT auditor TO intern;");
        assertFalse(gIntern.isSuccess(), "clerk lacks ADMIN OPTION to grant auditor");
        assertTrue(gIntern.getError().toLowerCase().contains("permission denied"));

        // Admin revokes ADMIN OPTION from sec_manager
        db.setCurrentUser("admin");
        QueryResult revOpt = db.execute("REVOKE ADMIN OPTION FOR auditor FROM sec_manager;");
        assertTrue(revOpt.isSuccess());

        // sec_manager should still be a member of auditor, but can no longer grant it
        Set<String> mgrRoles = db.getExecutor().getEffectiveRoles("sec_manager");
        assertTrue(mgrRoles.contains("auditor"), "sec_manager remains a member of auditor");

        db.setCurrentUser("sec_manager");
        QueryResult gAttempt = db.execute("GRANT auditor TO intern;");
        assertFalse(gAttempt.isSuccess(), "sec_manager can no longer grant auditor after ADMIN OPTION revoked");
        assertTrue(gAttempt.getError().toLowerCase().contains("permission denied"));
    }

    // =========================================================================
    // 3. DATABASE-LEVEL PERMISSIONS (CONNECT & CREATE)
    // =========================================================================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testDatabaseConnectPrivilegeEnforcementOverWire(@TempDir Path tempDir) throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        cluster = new StratosCluster(config);

        UserStore userStore = new UserStore();
        userStore.addUser("admin", "adminpass");
        userStore.addUser("analyst", "analystpass");
        userStore.addUser("guest", "guestpass");

        server = new StdWireServer(port, cluster, userStore);
        server.start();
        Thread.sleep(200);

        // Superuser creates database and configures permissions
        Properties adminProps = new Properties();
        adminProps.setProperty("user", "admin");
        adminProps.setProperty("password", "adminpass");
        try (Connection adminConn = DriverManager.getConnection("jdbc:stratos://localhost:" + port + "/stratos", adminProps)) {
            try (Statement s = adminConn.createStatement()) {
                s.execute("CREATE DATABASE analytics;");
                s.execute("CREATE ROLE analyst WITH LOGIN;");
                s.execute("CREATE ROLE guest WITH LOGIN;");
                s.execute("GRANT CONNECT ON DATABASE analytics TO analyst;");
            }
        }

        // Analyst has CONNECT privilege -> connection to analytics must succeed
        Properties analystProps = new Properties();
        analystProps.setProperty("user", "analyst");
        analystProps.setProperty("password", "analystpass");
        try (Connection analystConn = DriverManager.getConnection("jdbc:stratos://localhost:" + port + "/analytics", analystProps)) {
            assertTrue(analystConn.isValid(5));
        }

        // Guest lacks CONNECT privilege -> connection to analytics must fail
        Properties guestProps = new Properties();
        guestProps.setProperty("user", "guest");
        guestProps.setProperty("password", "guestpass");
        SQLException ex = assertThrows(SQLException.class,
            () -> DriverManager.getConnection("jdbc:stratos://localhost:" + port + "/analytics", guestProps));
        assertTrue(ex.getMessage().contains("CONNECT privilege") || ex.getMessage().contains("permission denied"),
            "Expected CONNECT permission denied error, got: " + ex.getMessage());
    }

    @Test
    void testDatabaseCreatePrivilegeEnforcement(@TempDir Path tempDir) {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);

        db.setCurrentUser("admin");
        db.execute("CREATE ROLE developer WITH LOGIN;");
        db.execute("GRANT CONNECT ON DATABASE stratos TO developer;");

        // Developer has CONNECT but lacks CREATE ON DATABASE
        db.setCurrentUser("developer");
        QueryResult createTabFail = db.execute("CREATE TABLE dev_scratchpad (id INT);");
        assertFalse(createTabFail.isSuccess(), "developer without CREATE privilege on database cannot CREATE TABLE");
        assertTrue(createTabFail.getError().toLowerCase().contains("create privilege"));

        // Admin grants CREATE ON DATABASE stratos TO developer
        db.setCurrentUser("admin");
        QueryResult grantCreate = db.execute("GRANT CREATE ON DATABASE stratos TO developer;");
        assertTrue(grantCreate.isSuccess());

        // Developer can now CREATE TABLE
        db.setCurrentUser("developer");
        QueryResult createTabSuccess = db.execute("CREATE TABLE dev_scratchpad (id INT);");
        assertTrue(createTabSuccess.isSuccess(), "developer with CREATE privilege can now CREATE TABLE");

        // Revoking CREATE ON DATABASE immediately restores protection
        db.setCurrentUser("admin");
        QueryResult revokeCreate = db.execute("REVOKE CREATE ON DATABASE stratos FROM developer;");
        assertTrue(revokeCreate.isSuccess());

        db.setCurrentUser("developer");
        QueryResult createTabFailAgain = db.execute("CREATE TABLE dev_scratchpad2 (id INT);");
        assertFalse(createTabFailAgain.isSuccess(), "developer should again be denied CREATE TABLE");
        assertTrue(createTabFailAgain.getError().toLowerCase().contains("create privilege"));
    }

    // =========================================================================
    // 4. PERSISTENCE ACROSS RESTARTS
    // =========================================================================

    @Test
    void testRoleHierarchyAndDatabasePermissionsPersistAcrossRestart(@TempDir Path tempDir) {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);

        db.setCurrentUser("admin");
        db.execute("CREATE ROLE corp_admin;");
        db.execute("CREATE ROLE corp_staff;");
        db.execute("CREATE ROLE employee_sam WITH LOGIN;");
        db.execute("CREATE TABLE assets (id INT, label TEXT);");
        db.execute("INSERT INTO assets VALUES (100, 'ConfidentialPatent');");

        db.execute("GRANT SELECT ON assets TO corp_admin;");
        db.execute("GRANT corp_admin TO corp_staff;");
        db.execute("GRANT corp_staff TO employee_sam;");
        db.execute("GRANT CONNECT, CREATE ON DATABASE stratos TO employee_sam;");

        // Verify working before shutdown
        db.setCurrentUser("employee_sam");
        QueryResult sel1 = db.execute("SELECT label FROM assets;");
        assertTrue(sel1.isSuccess());
        assertEquals("ConfidentialPatent", sel1.getRows().get(0).getValue(0));

        // Shutdown and restart database instance from the same directory
        db.shutdown();
        db = new StratosDB(config);

        // Verify role hierarchy restored
        Set<String> restoredRoles = db.getExecutor().getEffectiveRoles("employee_sam");
        assertTrue(restoredRoles.contains("employee_sam"));
        assertTrue(restoredRoles.contains("corp_staff"));
        assertTrue(restoredRoles.contains("corp_admin"));

        // Verify inherited table access restored
        db.setCurrentUser("employee_sam");
        QueryResult sel2 = db.execute("SELECT label FROM assets;");
        assertTrue(sel2.isSuccess());
        assertEquals("ConfidentialPatent", sel2.getRows().get(0).getValue(0));

        // Verify database privilege restored (CREATE TABLE works)
        QueryResult createRes = db.execute("CREATE TABLE sam_notes (id INT);");
        assertTrue(createRes.isSuccess());
    }
}
