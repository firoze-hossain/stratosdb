package com.stratosdb.network.auth;

import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ScramSha256's real, from-scratch server-side implementation of RFC
 * 5802, tested against an independent, from-scratch CLIENT-side
 * implementation written separately in this same file (IndependentClient,
 * below) that shares no helper code with ScramSha256 itself - so a
 * passing test proves genuine interoperability against the spec, not
 * just that the server agrees with its own internal logic. This mirrors
 * the strongest verification this class actually received during
 * development: real, unmodified psql and psycopg2 clients successfully
 * authenticating against a real running server (see PROGRESS.md) - this
 * test suite is the fast, repeatable, in-process form of that same
 * independent-implementation principle.
 */
class ScramSha256Test {

    @Test
    void correctPasswordAuthenticatesAndServerSignatureVerifies() throws Exception {
        UserStore store = new UserStore();
        store.addUser("alice", "correct-horse-battery-staple");
        UserStore.ScramCredential cred = store.getScramCredential("alice");

        IndependentClient client = new IndependentClient("alice", "correct-horse-battery-staple");
        String clientFirst = client.buildClientFirstMessage();

        ScramSha256.Handshake server = new ScramSha256.Handshake("alice", cred);
        String serverFirst = server.clientFirst(clientFirst);
        String clientFinal = client.buildClientFinalMessage(serverFirst);
        String serverFinal = server.clientFinal(clientFinal);

        assertTrue(client.verifyServerFinalMessage(serverFinal),
            "the client's own independent computation of the server's expected signature must match what the server actually sent - mutual authentication, not just one-directional password checking");
    }

    @Test
    void wrongPasswordIsRejected() throws Exception {
        UserStore store = new UserStore();
        store.addUser("bob", "the-real-password");
        UserStore.ScramCredential cred = store.getScramCredential("bob");

        IndependentClient client = new IndependentClient("bob", "a-wrong-password");
        String clientFirst = client.buildClientFirstMessage();
        ScramSha256.Handshake server = new ScramSha256.Handshake("bob", cred);
        String serverFirst = server.clientFirst(clientFirst);
        String clientFinal = client.buildClientFinalMessage(serverFirst);

        assertThrows(ScramSha256.ScramAuthenticationException.class, () -> server.clientFinal(clientFinal));
    }

    @Test
    void unknownUserGetsANormalServerFirstMessageButIsRejectedAtTheEnd() throws Exception {
        // Real Postgres's own username-enumeration defense: an unknown user must not
        // fail early or differently from a wrong-password case - both look identical
        // until the very last message.
        IndependentClient client = new IndependentClient("nonexistent", "whatever");
        String clientFirst = client.buildClientFirstMessage();

        ScramSha256.Handshake server = new ScramSha256.Handshake("nonexistent", null);
        String serverFirst = server.clientFirst(clientFirst);
        assertTrue(serverFirst.startsWith("r="), "an unknown user must still receive a normal, well-formed server-first-message");

        String clientFinal = client.buildClientFinalMessage(serverFirst);
        assertThrows(ScramSha256.ScramAuthenticationException.class, () -> server.clientFinal(clientFinal));
    }

    @Test
    void tamperedProofIsRejected() throws Exception {
        UserStore store = new UserStore();
        store.addUser("carol", "carols-password");
        UserStore.ScramCredential cred = store.getScramCredential("carol");

        IndependentClient client = new IndependentClient("carol", "carols-password");
        String clientFirst = client.buildClientFirstMessage();
        ScramSha256.Handshake server = new ScramSha256.Handshake("carol", cred);
        String serverFirst = server.clientFirst(clientFirst);
        String clientFinal = client.buildClientFinalMessage(serverFirst);

        int pIndex = clientFinal.indexOf(",p=");
        String tampered = clientFinal.substring(0, pIndex + 3) + "X" + clientFinal.substring(pIndex + 4);

        assertThrows(Exception.class, () -> server.clientFinal(tampered));
    }

    @Test
    void nonceMismatchIsDetected() throws Exception {
        UserStore store = new UserStore();
        store.addUser("dave", "daves-password");
        UserStore.ScramCredential cred = store.getScramCredential("dave");

        IndependentClient client = new IndependentClient("dave", "daves-password");
        String clientFirst = client.buildClientFirstMessage();
        ScramSha256.Handshake server = new ScramSha256.Handshake("dave", cred);
        String serverFirst = server.clientFirst(clientFirst);
        String clientFinal = client.buildClientFinalMessage(serverFirst);
        String forged = clientFinal.replaceFirst("r=[^,]+", "r=totally-different-nonce");

        ScramSha256.ScramAuthenticationException ex = assertThrows(
            ScramSha256.ScramAuthenticationException.class, () -> server.clientFinal(forged));
        assertTrue(ex.getMessage().contains("nonce"));
    }

    /** An independent, from-scratch CLIENT-side SCRAM-SHA-256 implementation - deliberately shares no code with ScramSha256 itself, computing SaltedPassword itself from the plaintext password the way a real client would. */
    private static class IndependentClient {
        private final String username;
        private final String password;
        private final String clientNonce;
        private String clientFirstMessageBare;
        private String serverFirstMessage;
        private byte[] clientKey;
        private byte[] serverKey;
        private String clientFinalMessageWithoutProof;

        IndependentClient(String username, String password) {
            this.username = username;
            this.password = password;
            byte[] nonceBytes = new byte[18];
            new SecureRandom().nextBytes(nonceBytes);
            this.clientNonce = Base64.getEncoder().withoutPadding().encodeToString(nonceBytes);
        }

        String buildClientFirstMessage() {
            clientFirstMessageBare = "n=" + username + ",r=" + clientNonce;
            return "n,," + clientFirstMessageBare;
        }

        String buildClientFinalMessage(String serverFirstMessage) throws Exception {
            this.serverFirstMessage = serverFirstMessage;
            Map<String, String> attrs = parseAttrs(serverFirstMessage);
            String combinedNonce = attrs.get("r");
            byte[] salt = Base64.getDecoder().decode(attrs.get("s"));
            int iterations = Integer.parseInt(attrs.get("i"));

            byte[] saltedPassword = pbkdf2(password, salt, iterations);
            clientKey = hmac(saltedPassword, "Client Key");
            byte[] storedKey = sha256(clientKey);
            serverKey = hmac(saltedPassword, "Server Key");

            String gs2HeaderBase64 = Base64.getEncoder().encodeToString("n,,".getBytes(StandardCharsets.UTF_8));
            clientFinalMessageWithoutProof = "c=" + gs2HeaderBase64 + ",r=" + combinedNonce;

            String authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
            byte[] clientSignature = hmac(storedKey, authMessage);
            byte[] clientProof = xor(clientKey, clientSignature);

            return clientFinalMessageWithoutProof + ",p=" + Base64.getEncoder().encodeToString(clientProof);
        }

        boolean verifyServerFinalMessage(String serverFinalMessage) throws Exception {
            Map<String, String> attrs = parseAttrs(serverFinalMessage);
            byte[] received = Base64.getDecoder().decode(attrs.get("v"));
            String authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
            byte[] expected = hmac(serverKey, authMessage);
            return MessageDigest.isEqual(received, expected);
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
    }
}
