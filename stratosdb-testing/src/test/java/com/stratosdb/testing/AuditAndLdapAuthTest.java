package com.stratosdb.testing;

import com.stratosdb.common.security.AuditLogger;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.auth.LdapAuthProvider;
import com.stratosdb.network.auth.LdapConfig;
import com.stratosdb.network.stdwire.StdWireMessages;
import com.stratosdb.network.stdwire.StdWireServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Sprint 10 Week 20: Tamper-Evident SHA-256 Chained Audit Logger
 * (DDL, DCL, Failed Logins) and External LDAP / Active Directory Authentication.
 */
class AuditAndLdapAuthTest {

    @Test
    void testAuditLoggerHashChainForDdlDclAndTamperDetection(@TempDir Path tempDir) throws Exception {
        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(cfg);
        try {
            assertTrue(db.execute("CREATE TABLE accounts (id INT, balance INT)").isSuccess());
            assertTrue(db.execute("ALTER TABLE accounts ADD COLUMN note TEXT").isSuccess());
            assertTrue(db.execute("CREATE ROLE auditor LOGIN PASSWORD 'pw'").isSuccess());
            assertTrue(db.execute("GRANT SELECT ON accounts TO auditor").isSuccess());
            assertTrue(db.execute("REVOKE SELECT ON accounts FROM auditor").isSuccess());
            assertTrue(db.execute("DROP TABLE accounts").isSuccess());

            AuditLogger audit = db.getAuditLogger();
            AuditLogger.VerificationResult valid = audit.verifyIntegrity();
            assertTrue(valid.valid(), () -> "Audit chain must be valid: " + valid.reason());
            assertEquals(6L, valid.recordCount());

            List<AuditLogger.AuditRecord> records = audit.readAllRecords();
            assertEquals("DDL", records.get(0).category());
            assertEquals("DCL", records.get(2).category());
            assertEquals(AuditLogger.GENESIS_HASH, records.get(0).prevHash());
            for (int i = 1; i < records.size(); i++) {
                assertEquals(records.get(i - 1).hash(), records.get(i).prevHash());
            }
        } finally {
            db.shutdown();
        }

        // Tamper with record #3 on disk -> verifyIntegrity must detect broken hash chain
        Path logPath = tempDir.resolve("audit.log");
        List<String> lines = Files.readAllLines(logPath, StandardCharsets.UTF_8);
        String tampered = lines.get(2).replace("auditor", "hacker");
        lines.set(2, tampered);
        Files.write(logPath, lines, StandardCharsets.UTF_8);

        AuditLogger reloaded = new AuditLogger(logPath);
        AuditLogger.VerificationResult broken = reloaded.verifyIntegrity();
        assertFalse(broken.valid(), "Tampered audit log must fail integrity verification");
        assertEquals(3L, broken.brokenAtSequence());
    }

    @Test
    void testLdapAuthenticationAndFailedLoginAuditOverWire(@TempDir Path tempDir) throws Exception {
        try (MiniLdapServer ldap = new MiniLdapServer(Map.of("uid=alice,ou=users,dc=stratos,dc=local", "secret123"))) {
            ldap.start();

            LdapConfig ldapCfg = new LdapConfig(
                "ldap://127.0.0.1:" + ldap.getPort(),
                "uid={0},ou=users,dc=stratos,dc=local"
            );
            LdapAuthProvider provider = new LdapAuthProvider(ldapCfg);

            // Anonymous / empty password must always be rejected
            assertFalse(provider.authenticate("alice", ""));
            assertFalse(provider.authenticate("", "secret123"));

            DatabaseConfig dbCfg = new DatabaseConfig();
            dbCfg.setDataDirectory(tempDir.toString());
            StratosDB db = new StratosDB(dbCfg);

            int wirePort;
            try (ServerSocket s = new ServerSocket(0)) {
                wirePort = s.getLocalPort();
            }
            StdWireServer server = new StdWireServer(wirePort, db);
            server.setLdapAuthProvider(provider);
            server.start();

            try {
                // 1. Valid LDAP login over PostgreSQL wire protocol
                assertTrue(attemptCleartextWireLogin("127.0.0.1", wirePort, "alice", "secret123"));

                // 2. Invalid LDAP password must fail AND write FAILED_LOGIN to AuditLogger
                assertFalse(attemptCleartextWireLogin("127.0.0.1", wirePort, "alice", "wrongpass"));

                AuditLogger audit = db.getAuditLogger();
                assertTrue(audit.verifyIntegrity().valid());
                List<AuditLogger.AuditRecord> records = audit.readAllRecords();
                assertTrue(records.stream().anyMatch(r ->
                    r.category().equals("AUTH") && r.action().equals("FAILED_LOGIN") && r.actor().equals("alice")
                ), "Failed LDAP login must be recorded in tamper-evident audit log");
            } finally {
                server.stop();
                db.shutdown();
            }
        }
    }

    private static boolean attemptCleartextWireLogin(String host, int port, String user, String password) throws IOException {
        try (Socket sock = new Socket(host, port)) {
            DataOutputStream out = new DataOutputStream(sock.getOutputStream());
            DataInputStream in = new DataInputStream(sock.getInputStream());

            // Send StartupMessage (protocol 3.0)
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            DataOutputStream bout = new DataOutputStream(body);
            bout.writeInt(196608);
            bout.write("user\0".getBytes(StandardCharsets.UTF_8));
            bout.write((user + "\0").getBytes(StandardCharsets.UTF_8));
            bout.write("database\0stratos\0\0".getBytes(StandardCharsets.UTF_8));
            byte[] payload = body.toByteArray();
            out.writeInt(4 + payload.length);
            out.write(payload);
            out.flush();

            // Expect AuthenticationCleartextPassword ('R' type 3)
            StdWireMessages.TypedMessage authReq = StdWireMessages.readTypedMessage(in);
            if (authReq.type() != 'R') return false;

            // Send PasswordMessage ('p')
            byte[] pwBytes = (password + "\0").getBytes(StandardCharsets.UTF_8);
            out.writeByte('p');
            out.writeInt(4 + pwBytes.length);
            out.write(pwBytes);
            out.flush();

            StdWireMessages.TypedMessage resp = StdWireMessages.readTypedMessage(in);
            return resp.type() == 'R';
        }
    }

    /**
     * Minimal RFC 4511 LDAP v3 ASN.1 BER server supporting BindRequest/BindResponse
     * for verifying real JNDI InitialDirContext LDAP authentication over TCP.
     */
    private static final class MiniLdapServer implements AutoCloseable {
        private final Map<String, String> directoryUsers;
        private final ServerSocket serverSocket;
        private volatile boolean running = true;

        MiniLdapServer(Map<String, String> directoryUsers) throws IOException {
            this.directoryUsers = directoryUsers;
            this.serverSocket = new ServerSocket(0);
        }

        int getPort() {
            return serverSocket.getLocalPort();
        }

        void start() {
            Thread t = new Thread(() -> {
                while (running) {
                    try {
                        Socket client = serverSocket.accept();
                        Thread.ofVirtual().start(() -> handleClient(client));
                    } catch (IOException ignored) {
                    }
                }
            }, "mini-ldap-server");
            t.setDaemon(true);
            t.start();
        }

        private void handleClient(Socket client) {
            try (Socket s = client;
                 InputStream in = s.getInputStream();
                 OutputStream out = s.getOutputStream()) {
                while (running) {
                    int tag = in.read();
                    if (tag == -1) break;
                    int len = readBerLength(in);
                    byte[] msg = in.readNBytes(len);
                    if (msg.length < 5) break;

                    // Parse messageID (INTEGER tag 0x02)
                    int pos = 0;
                    if (msg[pos++] != 0x02) break;
                    int idLen = msg[pos++] & 0xFF;
                    int msgId = 0;
                    for (int i = 0; i < idLen; i++) {
                        msgId = (msgId << 8) | (msg[pos++] & 0xFF);
                    }

                    int opTag = msg[pos++] & 0xFF;
                    int opLen = readBerLengthFromArray(msg, pos);
                    pos += berLenBytes(msg, pos);
                    if (opTag == 0x60) { // BindRequest
                        // version (0x02)
                        pos++; // 0x02
                        int vLen = msg[pos++] & 0xFF;
                        pos += vLen;
                        // name (0x04 OCTET STRING)
                        pos++; // 0x04
                        int dnLen = readBerLengthFromArray(msg, pos);
                        pos += berLenBytes(msg, pos);
                        String dn = new String(msg, pos, dnLen, StandardCharsets.UTF_8);
                        pos += dnLen;
                        // simple auth (0x80)
                        int authTag = msg[pos++] & 0xFF;
                        int pwLen = readBerLengthFromArray(msg, pos);
                        pos += berLenBytes(msg, pos);
                        String pw = (authTag == 0x80) ? new String(msg, pos, pwLen, StandardCharsets.UTF_8) : "";

                        boolean ok = directoryUsers.containsKey(dn) && directoryUsers.get(dn).equals(pw);
                        out.write(encodeBindResponse(msgId, ok ? 0 : 49));
                        out.flush();
                    } else if (opTag == 0x42) { // UnbindRequest
                        break;
                    } else {
                        // Unsupported op -> return success/empty
                        break;
                    }
                }
            } catch (IOException ignored) {
            }
        }

        private static byte[] encodeBindResponse(int msgId, int resultCode) {
            byte[] inner = new byte[]{
                0x0A, 0x01, (byte) resultCode, // resultCode ENUMERATED
                0x04, 0x00,                    // matchedDN
                0x04, 0x00                     // diagnosticMessage
            };
            byte[] resp = new byte[2 + 1 + 2 + 2 + inner.length];
            int idx = 0;
            resp[idx++] = 0x30;
            resp[idx++] = (byte) (3 + 2 + inner.length);
            resp[idx++] = 0x02;
            resp[idx++] = 0x01;
            resp[idx++] = (byte) msgId;
            resp[idx++] = 0x61; // BindResponse
            resp[idx++] = (byte) inner.length;
            System.arraycopy(inner, 0, resp, idx, inner.length);
            return resp;
        }

        private static int readBerLength(InputStream in) throws IOException {
            int b = in.read();
            if (b < 0) return 0;
            if ((b & 0x80) == 0) return b;
            int count = b & 0x7F;
            int len = 0;
            for (int i = 0; i < count; i++) {
                len = (len << 8) | (in.read() & 0xFF);
            }
            return len;
        }

        private static int readBerLengthFromArray(byte[] buf, int pos) {
            int b = buf[pos] & 0xFF;
            if ((b & 0x80) == 0) return b;
            int count = b & 0x7F;
            int len = 0;
            for (int i = 0; i < count; i++) {
                len = (len << 8) | (buf[pos + 1 + i] & 0xFF);
            }
            return len;
        }

        private static int berLenBytes(byte[] buf, int pos) {
            int b = buf[pos] & 0xFF;
            return (b & 0x80) == 0 ? 1 : 1 + (b & 0x7F);
        }

        @Override
        public void close() throws IOException {
            running = false;
            serverSocket.close();
        }
    }
}
