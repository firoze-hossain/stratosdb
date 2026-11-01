package com.stratosdb.network.stdwire;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.tls.TlsConfig;
import com.stratosdb.network.tls.TlsConfig.ClientAuth;
import com.stratosdb.network.tls.TlsSupport;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.*;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class StdWireTlsIntegrationTest {

    @TempDir
    static Path sharedTempDir;

    private static String serverKeystorePath;
    private static String serverCertPath;
    private static String clientKeystorePath;
    private static String clientCertPath;
    private static String serverTruststorePath;
    private static final char[] PASSWORD = "test-password".toCharArray();

    @TempDir
    Path testDir;

    private int port;
    private StratosDB db;
    private StdWireServer server;

    @BeforeAll
    static void generateCertificates() throws Exception {
        serverKeystorePath = sharedTempDir.resolve("server.p12").toString();
        serverCertPath = sharedTempDir.resolve("server.crt").toString();
        clientKeystorePath = sharedTempDir.resolve("client.p12").toString();
        clientCertPath = sharedTempDir.resolve("client.crt").toString();
        serverTruststorePath = sharedTempDir.resolve("truststore.p12").toString();

        // 1. Generate server keypair & certificate with SAN=dns:localhost,ip:127.0.0.1
        TlsSupport.generateKeyStore(
            serverKeystorePath,
            PASSWORD,
            "CN=localhost, OU=Engineering, O=StratosDB, C=US",
            "dns:localhost,ip:127.0.0.1"
        );
        TlsSupport.exportCertificate(serverKeystorePath, PASSWORD, serverCertPath);

        // 2. Generate client keypair & certificate for CN=alice
        TlsSupport.generateKeyStore(
            clientKeystorePath,
            PASSWORD,
            "CN=alice, OU=Engineering, O=StratosDB, C=US",
            null
        );
        TlsSupport.exportCertificate(clientKeystorePath, PASSWORD, clientCertPath);

        // 3. Import client certificate into server truststore
        TlsSupport.importCertificate(serverTruststorePath, PASSWORD, "alice", clientCertPath);
    }

    @BeforeEach
    void setUp() throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(testDir.toString());
        db = new StratosDB(config);
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    private TlsConfig createServerTlsConfig() {
        TlsConfig config = new TlsConfig();
        config.setEnabled(true);
        config.setKeystorePath(serverKeystorePath);
        config.setKeystorePassword(PASSWORD);
        config.setProtocols(new String[]{"TLSv1.3", "TLSv1.2"});
        config.setApplicationProtocols(new String[]{"postgresql", "stratosdb"});
        return config;
    }

    @Test
    public void testTls13HandshakeAndEncryptedCommunication() throws Exception {
        TlsConfig tlsConfig = createServerTlsConfig();
        server = new StdWireServer(port, db, tlsConfig);
        server.start();

        // Client connects over raw socket
        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream rawOut = new DataOutputStream(socket.getOutputStream());
            DataInputStream rawIn = new DataInputStream(socket.getInputStream());

            // 1. Send SSLRequest (8 bytes)
            StdWireMessages.writeSslRequest(rawOut);

            // 2. Read 1-byte response ('S' = accepted)
            int response = rawIn.read();
            assertEquals('S', response, "Server should accept SSL with 'S'");

            // 3. Initiate TLS handshake on top of socket
            SSLContext clientCtx = TlsSupport.insecureTrustAllClientContext();
            SSLSocketFactory sf = clientCtx.getSocketFactory();
            try (SSLSocket sslSocket = (SSLSocket) sf.createSocket(socket, "localhost", port, true)) {
                sslSocket.setUseClientMode(true);
                sslSocket.setEnabledProtocols(new String[]{"TLSv1.3"});
                sslSocket.startHandshake();

                assertEquals("TLSv1.3", sslSocket.getSession().getProtocol());

                DataOutputStream encOut = new DataOutputStream(new BufferedOutputStream(sslSocket.getOutputStream()));
                DataInputStream encIn = new DataInputStream(new BufferedInputStream(sslSocket.getInputStream()));

                // 4. Send StartupMessage inside TLS
                StdWireMessages.writeStartupMessage(encOut, "testuser", "testdb");

                // 5. Read startup responses until ReadyForQuery ('Z')
                readUntilReady(encIn);

                // 6. Execute DDL and DML inside TLS
                sendSimpleQuery(encOut, "CREATE TABLE encrypted_vault (id INT, secret_info TEXT);");
                QueryOutcome ddlRes = readQueryOutcome(encIn);
                assertNull(ddlRes.error);

                sendSimpleQuery(encOut, "INSERT INTO encrypted_vault VALUES (1, 'TopSecretTLS13Data');");
                QueryOutcome insRes = readQueryOutcome(encIn);
                assertNull(insRes.error);
                assertEquals("INSERT 0 1", insRes.commandTag);

                sendSimpleQuery(encOut, "SELECT id, secret_info FROM encrypted_vault WHERE id = 1;");
                QueryOutcome selRes = readQueryOutcome(encIn);
                assertNull(selRes.error);
                assertEquals(1, selRes.rows.size());
                assertEquals("1", selRes.rows.get(0).get(0));
                assertEquals("TopSecretTLS13Data", selRes.rows.get(0).get(1));
            }
        }
    }

    @Test
    public void testEncryptedTrafficVerificationWiresharkStyle() throws Exception {
        TlsConfig tlsConfig = createServerTlsConfig();
        server = new StdWireServer(port, db, tlsConfig);
        server.start();

        // Wire tap capturing every raw byte transmitted over the wire
        ByteArrayOutputStream capturedClientToServer = new ByteArrayOutputStream();
        ByteArrayOutputStream capturedServerToClient = new ByteArrayOutputStream();

        int proxyPort = freePort();
        try (ServerSocket proxyServer = new ServerSocket(proxyPort)) {
            Thread proxyThread = new Thread(() -> {
                try {
                    Socket clientSock = proxyServer.accept();
                    Socket targetSock = new Socket("localhost", port);

                    // Forward client -> target with capture
                    Thread t1 = new Thread(() -> {
                        try (InputStream in = clientSock.getInputStream();
                             OutputStream out = targetSock.getOutputStream()) {
                            byte[] buf = new byte[4096];
                            int r;
                            while ((r = in.read(buf)) != -1) {
                                synchronized (capturedClientToServer) {
                                    capturedClientToServer.write(buf, 0, r);
                                }
                                out.write(buf, 0, r);
                                out.flush();
                            }
                        } catch (IOException ignored) {}
                    });

                    // Forward target -> client with capture
                    Thread t2 = new Thread(() -> {
                        try (InputStream in = targetSock.getInputStream();
                             OutputStream out = clientSock.getOutputStream()) {
                            byte[] buf = new byte[4096];
                            int r;
                            while ((r = in.read(buf)) != -1) {
                                synchronized (capturedServerToClient) {
                                    capturedServerToClient.write(buf, 0, r);
                                }
                                out.write(buf, 0, r);
                                out.flush();
                            }
                        } catch (IOException ignored) {}
                    });

                    t1.start();
                    t2.start();
                } catch (IOException ignored) {}
            });
            proxyThread.setDaemon(true);
            proxyThread.start();

            // Client connects through the proxy tap
            String sensitiveString = "SENSITIVE_CREDIT_CARD_4111_2222_3333_4444";
            try (Socket socket = new Socket("localhost", proxyPort)) {
                DataOutputStream rawOut = new DataOutputStream(socket.getOutputStream());
                DataInputStream rawIn = new DataInputStream(socket.getInputStream());

                StdWireMessages.writeSslRequest(rawOut);
                int resp = rawIn.read();
                assertEquals('S', resp);

                SSLContext clientCtx = TlsSupport.insecureTrustAllClientContext();
                try (SSLSocket sslSocket = (SSLSocket) clientCtx.getSocketFactory().createSocket(socket, "localhost", proxyPort, true)) {
                    sslSocket.setUseClientMode(true);
                    sslSocket.startHandshake();

                    DataOutputStream encOut = new DataOutputStream(new BufferedOutputStream(sslSocket.getOutputStream()));
                    DataInputStream encIn = new DataInputStream(new BufferedInputStream(sslSocket.getInputStream()));

                    StdWireMessages.writeStartupMessage(encOut, "testuser", "testdb");
                    readUntilReady(encIn);

                    sendSimpleQuery(encOut, "CREATE TABLE cards (id INT, num TEXT);");
                    readQueryOutcome(encIn);

                    sendSimpleQuery(encOut, "INSERT INTO cards VALUES (1, '" + sensitiveString + "');");
                    readQueryOutcome(encIn);

                    sendSimpleQuery(encOut, "SELECT num FROM cards;");
                    QueryOutcome outcome = readQueryOutcome(encIn);
                    assertEquals(sensitiveString, outcome.rows.get(0).get(0));
                }
            }

            // Inspect the captured raw wire bytes
            byte[] clientBytes = capturedClientToServer.toByteArray();
            byte[] serverBytes = capturedServerToClient.toByteArray();

            // 1. Initial 8 bytes from client are plaintext SSLRequest
            ByteBuffer bb = ByteBuffer.wrap(clientBytes);
            assertEquals(8, bb.getInt());
            assertEquals(StdWireMessages.SSL_REQUEST_CODE, bb.getInt());

            // 2. Initial 1 byte from server is 'S'
            assertEquals('S', serverBytes[0]);

            // 3. After 'S', verify TLS Record Layer framing:
            // Content Type 0x16 (Handshake = 22) or 0x17 (Application Data = 23)
            // Major/Minor version 0x03, 0x03 (TLS 1.2 record header format used by TLS 1.3)
            int secondPacketType = serverBytes[1] & 0xFF;
            assertTrue(secondPacketType == 22 || secondPacketType == 23,
                "Subsequent packet must have TLS record framing (Handshake or AppData), got: " + secondPacketType);
            assertEquals(3, serverBytes[2]); // TLS 1.2/1.3 legacy record version 3.3
            assertEquals(3, serverBytes[3]);

            // 4. Assert sensitive payload is NEVER present in the raw wire bytes!
            String clientWireDump = new String(clientBytes, StandardCharsets.ISO_8859_1);
            String serverWireDump = new String(serverBytes, StandardCharsets.ISO_8859_1);
            assertFalse(clientWireDump.contains(sensitiveString),
                "Plaintext sensitive SQL payload was found in raw wire traffic!");
            assertFalse(serverWireDump.contains(sensitiveString),
                "Plaintext sensitive data was found in server response wire traffic!");
            assertFalse(clientWireDump.contains("CREATE TABLE cards"),
                "Plaintext SQL DDL was found on the wire!");
        }
    }

    @Test
    public void testAlpnNegotiation() throws Exception {
        TlsConfig tlsConfig = createServerTlsConfig();
        server = new StdWireServer(port, db, tlsConfig);
        server.start();

        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream rawOut = new DataOutputStream(socket.getOutputStream());
            DataInputStream rawIn = new DataInputStream(socket.getInputStream());

            StdWireMessages.writeSslRequest(rawOut);
            assertEquals('S', rawIn.read());

            SSLContext clientCtx = TlsSupport.insecureTrustAllClientContext();
            try (SSLSocket sslSocket = (SSLSocket) clientCtx.getSocketFactory().createSocket(socket, "localhost", port, true)) {
                sslSocket.setUseClientMode(true);
                SSLParameters params = sslSocket.getSSLParameters();
                params.setApplicationProtocols(new String[]{"postgresql"});
                sslSocket.setSSLParameters(params);
                sslSocket.startHandshake();

                String negotiated = sslSocket.getApplicationProtocol();
                assertEquals("postgresql", negotiated, "ALPN should negotiate postgresql protocol");
            }
        }
    }

    @Test
    public void testClientCertificateAuthenticationSuccess() throws Exception {
        TlsConfig tlsConfig = createServerTlsConfig();
        tlsConfig.setTruststorePath(serverTruststorePath);
        tlsConfig.setTruststorePassword(PASSWORD);
        tlsConfig.setClientAuth(ClientAuth.NEED);
        tlsConfig.setClientCertAuth(true); // mTLS: username must match cert CN

        server = new StdWireServer(port, db, tlsConfig);
        server.start();

        // Client connects with client keystore containing cert CN=alice
        SSLContext clientCtx = TlsSupport.insecureTrustAllClientContext(clientKeystorePath, PASSWORD);
        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream rawOut = new DataOutputStream(socket.getOutputStream());
            DataInputStream rawIn = new DataInputStream(socket.getInputStream());

            StdWireMessages.writeSslRequest(rawOut);
            assertEquals('S', rawIn.read());

            try (SSLSocket sslSocket = (SSLSocket) clientCtx.getSocketFactory().createSocket(socket, "localhost", port, true)) {
                sslSocket.setUseClientMode(true);
                sslSocket.startHandshake();

                DataOutputStream encOut = new DataOutputStream(new BufferedOutputStream(sslSocket.getOutputStream()));
                DataInputStream encIn = new DataInputStream(new BufferedInputStream(sslSocket.getInputStream()));

                // Alice connects with user=alice matching her cert CN
                StdWireMessages.writeStartupMessage(encOut, "alice", "testdb");
                readUntilReady(encIn);

                // Successfully authenticated via mTLS client certificate!
                sendSimpleQuery(encOut, "SELECT 1;");
                QueryOutcome res = readQueryOutcome(encIn);
                assertNull(res.error);
                assertEquals("1", res.rows.get(0).get(0));
            }
        }
    }

    @Test
    public void testClientCertificateAuthenticationFailureMismatch() throws Exception {
        TlsConfig tlsConfig = createServerTlsConfig();
        tlsConfig.setTruststorePath(serverTruststorePath);
        tlsConfig.setTruststorePassword(PASSWORD);
        tlsConfig.setClientAuth(ClientAuth.NEED);
        tlsConfig.setClientCertAuth(true);

        server = new StdWireServer(port, db, tlsConfig);
        server.start();

        // Client presents cert CN=alice, but attempts to log in as user=bob
        SSLContext clientCtx = TlsSupport.insecureTrustAllClientContext(clientKeystorePath, PASSWORD);
        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream rawOut = new DataOutputStream(socket.getOutputStream());
            DataInputStream rawIn = new DataInputStream(socket.getInputStream());

            StdWireMessages.writeSslRequest(rawOut);
            assertEquals('S', rawIn.read());

            try (SSLSocket sslSocket = (SSLSocket) clientCtx.getSocketFactory().createSocket(socket, "localhost", port, true)) {
                sslSocket.setUseClientMode(true);
                sslSocket.startHandshake();

                DataOutputStream encOut = new DataOutputStream(new BufferedOutputStream(sslSocket.getOutputStream()));
                DataInputStream encIn = new DataInputStream(new BufferedInputStream(sslSocket.getInputStream()));

                StdWireMessages.writeStartupMessage(encOut, "bob", "testdb");

                // Expect ErrorResponse rejecting user mismatch
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(encIn);
                assertEquals('E', msg.type(), "Expected ErrorResponse for mismatched client cert CN");
                String error = new String(msg.body(), StandardCharsets.UTF_8);
                assertTrue(error.contains("certificate authentication failed for user \"bob\""),
                    "Expected CN mismatch error, got: " + error);
            }
        }
    }

    @Test
    public void testClientCertificateRequiredNoCertFails() throws Exception {
        TlsConfig tlsConfig = createServerTlsConfig();
        tlsConfig.setTruststorePath(serverTruststorePath);
        tlsConfig.setTruststorePassword(PASSWORD);
        tlsConfig.setClientAuth(ClientAuth.NEED);

        server = new StdWireServer(port, db, tlsConfig);
        server.start();

        // Client connects with NO client keystore / cert
        SSLContext clientCtx = TlsSupport.insecureTrustAllClientContext(null, null);
        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream rawOut = new DataOutputStream(socket.getOutputStream());
            DataInputStream rawIn = new DataInputStream(socket.getInputStream());

            StdWireMessages.writeSslRequest(rawOut);
            assertEquals('S', rawIn.read());

            try (SSLSocket sslSocket = (SSLSocket) clientCtx.getSocketFactory().createSocket(socket, "localhost", port, true)) {
                sslSocket.setUseClientMode(true);
                assertThrows(IOException.class, () -> {
                    sslSocket.startHandshake();
                    DataOutputStream encOut = new DataOutputStream(sslSocket.getOutputStream());
                    DataInputStream encIn = new DataInputStream(sslSocket.getInputStream());
                    StdWireMessages.writeStartupMessage(encOut, "anyuser", "testdb");
                    int b = encIn.read();
                    if (b == -1) {
                        throw new EOFException("Connection closed by server due to missing client certificate");
                    }
                }, "Expected connection to fail when client cert is missing under ClientAuth.NEED");
            }
        }
    }

    @Test
    public void testSslDeclineWhenTlsDisabled() throws Exception {
        // Start server with NO TLS (disabled)
        server = new StdWireServer(port, db);
        server.start();

        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream rawOut = new DataOutputStream(socket.getOutputStream());
            DataInputStream rawIn = new DataInputStream(socket.getInputStream());

            // Send SSLRequest
            StdWireMessages.writeSslRequest(rawOut);

            // Server must decline with 'N'
            int response = rawIn.read();
            assertEquals('N', response, "Server should decline SSL with 'N' when TLS is not enabled");

            // Client falls back to unencrypted StartupMessage on same socket
            StdWireMessages.writeStartupMessage(rawOut, "testuser", "testdb");
            readUntilReady(rawIn);

            sendSimpleQuery(rawOut, "SELECT 42;");
            QueryOutcome outcome = readQueryOutcome(rawIn);
            assertNull(outcome.error);
            assertEquals("42", outcome.rows.get(0).get(0));
        }
    }

    @Test
    public void testRequireTlsEnforcement() throws Exception {
        TlsConfig tlsConfig = createServerTlsConfig();
        tlsConfig.setRequireTls(true); // Plaintext forbidden!

        server = new StdWireServer(port, db, tlsConfig);
        server.start();

        try (Socket socket = new Socket("localhost", port)) {
            DataOutputStream rawOut = new DataOutputStream(socket.getOutputStream());
            DataInputStream rawIn = new DataInputStream(socket.getInputStream());

            // Directly send StartupMessage without SSLRequest
            StdWireMessages.writeStartupMessage(rawOut, "testuser", "testdb");

            // Server must immediately reject with ErrorResponse
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(rawIn);
            assertEquals('E', msg.type(), "Expected ErrorResponse when connecting plaintext to requireTls=true server");
            String err = new String(msg.body(), StandardCharsets.UTF_8);
            assertTrue(err.toLowerCase().contains("ssl connection is required"), "Expected SSL required message, got: " + err);
        }
    }

    @Test
    public void testRealPsqlSslmodeRequireAndVerifyFull() throws Exception {
        File psqlBin = new File("/usr/bin/psql");
        if (!psqlBin.exists()) {
            return; // Skip if psql is not installed on this environment
        }

        TlsConfig tlsConfig = createServerTlsConfig();
        server = new StdWireServer(port, db, tlsConfig);
        server.start();

        // 1. Test real psql with sslmode=require
        ProcessBuilder pbRequire = new ProcessBuilder(
            psqlBin.getAbsolutePath(),
            "-d", String.format("sslmode=require host=localhost port=%d user=psqluser dbname=testdb", port),
            "-c", "SELECT 123 AS ans;"
        );
        pbRequire.redirectErrorStream(true);
        Process pRequire = pbRequire.start();
        String outRequire = new String(pRequire.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        boolean doneRequire = pRequire.waitFor(10, TimeUnit.SECONDS);
        assertTrue(doneRequire, "psql sslmode=require timed out");
        assertEquals(0, pRequire.exitValue(), "psql sslmode=require failed:\n" + outRequire);
        assertTrue(outRequire.contains("123"), "psql sslmode=require did not return expected result:\n" + outRequire);

        // 2. Test real psql with sslmode=verify-full and sslrootcert
        ProcessBuilder pbVerifyFull = new ProcessBuilder(
            psqlBin.getAbsolutePath(),
            "-d", String.format("sslmode=verify-full host=localhost port=%d user=psqluser dbname=testdb sslrootcert=%s", port, serverCertPath),
            "-c", "SELECT 'verified_tls_success' AS status;"
        );
        pbVerifyFull.redirectErrorStream(true);
        Process pVerifyFull = pbVerifyFull.start();
        String outVerifyFull = new String(pVerifyFull.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        boolean doneVerifyFull = pVerifyFull.waitFor(10, TimeUnit.SECONDS);
        assertTrue(doneVerifyFull, "psql sslmode=verify-full timed out");
        assertEquals(0, pVerifyFull.exitValue(), "psql sslmode=verify-full failed:\n" + outVerifyFull);
        assertTrue(outVerifyFull.contains("verified_tls_success"), "psql sslmode=verify-full output mismatch:\n" + outVerifyFull);
    }

    // --- Helpers ---

    private static void sendSimpleQuery(DataOutputStream out, String sql) throws IOException {
        StdWireMessages.writeQuery(out, sql);
        out.flush();
    }

    private static void readUntilReady(DataInputStream in) throws IOException {
        while (true) {
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
            if (msg.type() == 'Z') break;
            if (msg.type() == 'E') {
                throw new IOException("Error during startup: " + new String(msg.body(), StandardCharsets.UTF_8));
            }
        }
    }

    private static final class QueryOutcome {
        final List<String> columns = new ArrayList<>();
        final List<List<String>> rows = new ArrayList<>();
        String commandTag;
        String error;
    }

    private static QueryOutcome readQueryOutcome(DataInputStream in) throws IOException {
        QueryOutcome outcome = new QueryOutcome();
        while (true) {
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
            if (msg.type() == 'Z') break;
            if (msg.type() == 'T') {
                ByteBuffer buf = ByteBuffer.wrap(msg.body());
                int count = buf.getShort();
                for (int i = 0; i < count; i++) {
                    ByteArrayOutputStream name = new ByteArrayOutputStream();
                    byte b;
                    while ((b = buf.get()) != 0) name.write(b);
                    outcome.columns.add(new String(name.toByteArray(), StandardCharsets.UTF_8));
                    buf.getInt(); buf.getShort(); buf.getInt(); buf.getShort(); buf.getInt(); buf.getShort();
                }
            }
            if (msg.type() == 'D') {
                ByteBuffer buf = ByteBuffer.wrap(msg.body());
                int count = buf.getShort();
                List<String> row = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    int len = buf.getInt();
                    if (len == -1) {
                        row.add(null);
                    } else {
                        byte[] bytes = new byte[len];
                        buf.get(bytes);
                        row.add(new String(bytes, StandardCharsets.UTF_8));
                    }
                }
                outcome.rows.add(row);
            }
            if (msg.type() == 'C') {
                outcome.commandTag = new String(msg.body(), 0, msg.body().length - 1, StandardCharsets.UTF_8);
            }
            if (msg.type() == 'E') {
                outcome.error = new String(msg.body(), StandardCharsets.UTF_8);
            }
        }
        return outcome;
    }
}
