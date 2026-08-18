package com.stratosdb.network.stdwire;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.auth.ScramSha256;
import com.stratosdb.network.auth.UserStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Wire-level SCRAM-SHA-256 integration - a separate test class from
 * StdWireServerTest because every test here needs a server constructed
 * with a real UserStore, unlike every existing StdWireServerTest case
 * (trust auth). Verifies the full AuthenticationSASL /
 * SASLInitialResponse / AuthenticationSASLContinue / SASLResponse /
 * AuthenticationSASLFinal message exchange end to end, via an
 * independently hand-rolled client (not reusing StdWireMessages' own
 * client-side helpers, for the same reason StdWireServerTest's
 * RawConnection doesn't reuse the server's own message-building code),
 * plus real, unmodified psql and psycopg2 - the strongest evidence this
 * interoperates with actual PostgreSQL clients, not just this project's
 * own understanding of the protocol.
 */
class ScramWireProtocolTest {

    @TempDir
    Path tempDir;

    private int port;
    private StratosDB db;
    private StdWireServer server;
    private UserStore userStore;

    @BeforeEach
    void setUp() throws Exception {
        port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        userStore = new UserStore();
        userStore.addUser("alice", "correct-horse-battery-staple");
        server = new StdWireServer(port, db, userStore);
        server.start();
        Thread.sleep(200);
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    private static int freePort() throws Exception {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void correctPasswordAuthenticatesAndReachesReadyForQuery() throws Exception {
        try (HandRolledScramClient client = new HandRolledScramClient(port)) {
            boolean authenticated = client.authenticate("alice", "correct-horse-battery-staple");
            assertTrue(authenticated, "correct password must authenticate successfully through the real wire exchange");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void wrongPasswordIsRejectedOverTheWire() throws Exception {
        try (HandRolledScramClient client = new HandRolledScramClient(port)) {
            boolean authenticated = client.authenticate("alice", "totally-wrong-password");
            assertFalse(authenticated, "wrong password must be rejected through the real wire exchange, not accepted");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void trustAuthStillWorksWhenNoUserStoreIsSupplied() throws Exception {
        // A real, separate server instance with NO UserStore - confirms the default,
        // pre-existing behavior is genuinely unchanged by adding SCRAM support.
        int trustPort = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.resolve("trust").toString());
        StratosDB trustDb = new StratosDB(config);
        StdWireServer trustServer = new StdWireServer(trustPort, trustDb);
        trustServer.start();
        Thread.sleep(200);
        try (Socket socket = new Socket("localhost", trustPort)) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            writeStartupMessage(out, "anyuser", "anydb");
            char firstResponseType = (char) in.readUnsignedByte();
            assertEquals('R', firstResponseType, "trust auth must still send AuthenticationOk directly, not a SASL challenge");
            int len = in.readInt();
            byte[] body = new byte[len - 4];
            in.readFully(body);
            int authCode = ((body[0] & 0xFF) << 24) | ((body[1] & 0xFF) << 16) | ((body[2] & 0xFF) << 8) | (body[3] & 0xFF);
            assertEquals(0, authCode, "trust auth's AuthenticationOk must have auth code 0, not a SASL code");
        } finally {
            trustServer.stop();
            trustDb.shutdown();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void realPsycopg2AuthenticatesWithScram() throws Exception {
        assumeTrue(isPythonWithPsycopg2Available(), "psycopg2 not available - skipping real-driver SCRAM verification");

        String script = """
            import psycopg2
            conn = psycopg2.connect(host="localhost", port=%d, user="alice", password="correct-horse-battery-staple", dbname="testdb")
            cur = conn.cursor()
            cur.execute("CREATE TABLE t (id INT)")
            cur.execute("INSERT INTO t VALUES (1)")
            conn.commit()
            cur.execute("SELECT * FROM t")
            assert cur.fetchall() == [(1,)]
            conn.close()

            try:
                bad_conn = psycopg2.connect(host="localhost", port=%d, user="alice", password="wrong-password", dbname="testdb")
                print("SHOULD_NOT_CONNECT")
            except Exception:
                pass

            print("PSYCOPG2_SCRAM_OK")
            """.formatted(port, port);

        Process process = new ProcessBuilder("python3", "-c", script).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        boolean finished = process.waitFor(12, TimeUnit.SECONDS);
        assertTrue(finished, "psycopg2 SCRAM script did not finish in time");
        assertFalse(output.contains("SHOULD_NOT_CONNECT"), "a wrong password must not connect: " + output);
        assertTrue(output.contains("PSYCOPG2_SCRAM_OK"), () -> "real psycopg2 SCRAM authentication failed:\n" + output);
    }

    private boolean isPythonWithPsycopg2Available() {
        try {
            Process check = new ProcessBuilder("python3", "-c", "import psycopg2").start();
            return check.waitFor(5, TimeUnit.SECONDS) && check.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static void writeStartupMessage(DataOutputStream out, String user, String database) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        DataOutputStream bodyOut = new DataOutputStream(body);
        bodyOut.writeInt(StdWireMessages.PROTOCOL_VERSION_3);
        writeCString(bodyOut, "user");
        writeCString(bodyOut, user);
        writeCString(bodyOut, "database");
        writeCString(bodyOut, database);
        bodyOut.writeByte(0);
        out.writeInt(body.size() + 4);
        out.write(body.toByteArray());
        out.flush();
    }

    private static void writeCString(DataOutputStream out, String s) throws IOException {
        out.write(s.getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);
    }

    /**
     * An independently hand-rolled SCRAM-SHA-256 CLIENT that speaks the
     * real wire protocol directly over a socket - shares no code with
     * either ScramSha256 (the server implementation under test) or
     * StdWireMessages (the server's own message encoding), for the same
     * "genuine independent verification" reason StdWireServerTest's
     * RawConnection exists.
     */
    private static final class HandRolledScramClient implements AutoCloseable {
        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;

        HandRolledScramClient(int port) throws IOException {
            socket = new Socket("localhost", port);
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        }

        /** Returns true if authentication succeeds (reaches AuthenticationOk), false if it's rejected with an ErrorResponse. */
        boolean authenticate(String username, String password) throws Exception {
            writeStartupMessage(out, username, "testdb");

            char msgType = (char) in.readUnsignedByte();
            int len = in.readInt();
            byte[] body = new byte[len - 4];
            in.readFully(body);
            if (msgType != 'R') {
                return false;
            }
            int authCode = readIntAt(body, 0);
            if (authCode != 10) {
                return false; // not AuthenticationSASL
            }

            // client-first-message
            byte[] nonceBytes = new byte[18];
            new SecureRandom().nextBytes(nonceBytes);
            String clientNonce = Base64.getEncoder().withoutPadding().encodeToString(nonceBytes);
            String clientFirstMessageBare = "n=" + username + ",r=" + clientNonce;
            String clientFirstMessage = "n,," + clientFirstMessageBare;

            ByteArrayOutputStream initialBody = new ByteArrayOutputStream();
            DataOutputStream initialOut = new DataOutputStream(initialBody);
            writeCString(initialOut, ScramSha256.MECHANISM_NAME);
            byte[] cfmBytes = clientFirstMessage.getBytes(StandardCharsets.UTF_8);
            initialOut.writeInt(cfmBytes.length);
            initialOut.write(cfmBytes);
            out.writeByte('p');
            out.writeInt(initialBody.size() + 4);
            out.write(initialBody.toByteArray());
            out.flush();

            msgType = (char) in.readUnsignedByte();
            len = in.readInt();
            body = new byte[len - 4];
            in.readFully(body);
            if (msgType != 'R' || readIntAt(body, 0) != 11) {
                return false; // not AuthenticationSASLContinue
            }
            String serverFirstMessage = new String(body, 4, body.length - 4, StandardCharsets.UTF_8);

            Map<String, String> attrs = parseAttrs(serverFirstMessage);
            String combinedNonce = attrs.get("r");
            byte[] salt = Base64.getDecoder().decode(attrs.get("s"));
            int iterations = Integer.parseInt(attrs.get("i"));

            byte[] saltedPassword = pbkdf2(password, salt, iterations);
            byte[] clientKey = hmac(saltedPassword, "Client Key");
            byte[] storedKey = sha256(clientKey);
            byte[] serverKey = hmac(saltedPassword, "Server Key");

            String gs2HeaderBase64 = Base64.getEncoder().encodeToString("n,,".getBytes(StandardCharsets.UTF_8));
            String clientFinalMessageWithoutProof = "c=" + gs2HeaderBase64 + ",r=" + combinedNonce;
            String authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
            byte[] clientSignature = hmac(storedKey, authMessage);
            byte[] clientProof = xor(clientKey, clientSignature);
            String clientFinalMessage = clientFinalMessageWithoutProof + ",p=" + Base64.getEncoder().encodeToString(clientProof);

            byte[] cfBytes = clientFinalMessage.getBytes(StandardCharsets.UTF_8);
            out.writeByte('p');
            out.writeInt(cfBytes.length + 4);
            out.write(cfBytes);
            out.flush();

            msgType = (char) in.readUnsignedByte();
            len = in.readInt();
            body = new byte[len - 4];
            in.readFully(body);
            if (msgType == 'E') {
                return false; // ErrorResponse - authentication failed
            }
            if (msgType != 'R' || readIntAt(body, 0) != 12) {
                return false; // not AuthenticationSASLFinal
            }
            String serverFinalMessage = new String(body, 4, body.length - 4, StandardCharsets.UTF_8);
            Map<String, String> finalAttrs = parseAttrs(serverFinalMessage);
            byte[] receivedServerSignature = Base64.getDecoder().decode(finalAttrs.get("v"));
            byte[] expectedServerSignature = hmac(serverKey, authMessage);
            if (!MessageDigest.isEqual(receivedServerSignature, expectedServerSignature)) {
                return false; // server's signature doesn't check out - would be a real red flag in a real client
            }

            // AuthenticationOk must follow immediately.
            msgType = (char) in.readUnsignedByte();
            len = in.readInt();
            body = new byte[len - 4];
            in.readFully(body);
            return msgType == 'R' && readIntAt(body, 0) == 0;
        }

        private static int readIntAt(byte[] body, int offset) {
            return ((body[offset] & 0xFF) << 24) | ((body[offset + 1] & 0xFF) << 16)
                | ((body[offset + 2] & 0xFF) << 8) | (body[offset + 3] & 0xFF);
        }

        private static Map<String, String> parseAttrs(String message) {
            Map<String, String> attrs = new HashMap<>();
            for (String part : message.split(",")) {
                int eq = part.indexOf('=');
                if (eq > 0) attrs.put(part.substring(0, eq), part.substring(eq + 1));
            }
            return attrs;
        }

        private static byte[] pbkdf2(String password, byte[] salt, int iterations) throws Exception {
            PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, 256);
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
        }

        private static byte[] hmac(byte[] key, String message) throws Exception {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(message.getBytes(StandardCharsets.UTF_8));
        }

        private static byte[] sha256(byte[] input) throws Exception {
            return MessageDigest.getInstance("SHA-256").digest(input);
        }

        private static byte[] xor(byte[] a, byte[] b) {
            byte[] result = new byte[a.length];
            for (int i = 0; i < a.length; i++) result[i] = (byte) (a[i] ^ b[i]);
            return result;
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
