package com.stratosdb.network.auth;

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

/**
 * The CLIENT side of SCRAM-SHA-256 (RFC 5802) - a real implementation,
 * not a stand-in, deliberately kept separate from {@link ScramSha256}
 * (the server side) since the two roles compute genuinely different
 * things at each step (the server never sees the plaintext password;
 * the client never sees StoredKey/ServerKey directly). Used by
 * {@code stdsql}, this project's own native stdwire client, to
 * authenticate against a server configured with a UserStore.
 */
public final class ScramClient {
    private final String username;
    private final String password;
    private final String clientNonce;
    private String clientFirstMessageBare;
    private String serverFirstMessage;
    private byte[] clientKey;
    private byte[] serverKey;
    private String clientFinalMessageWithoutProof;

    public ScramClient(String username, String password) {
        this.username = username;
        this.password = password;
        byte[] nonceBytes = new byte[18];
        new SecureRandom().nextBytes(nonceBytes);
        this.clientNonce = Base64.getEncoder().withoutPadding().encodeToString(nonceBytes);
    }

    /** The GS2 header "n,," (no channel binding, no authzid) plus "n=<user>,r=<nonce>" - sent as SCRAM's initial response data. */
    public String buildClientFirstMessage() {
        clientFirstMessageBare = "n=" + username + ",r=" + clientNonce;
        return "n,," + clientFirstMessageBare;
    }

    /**
     * Given the server-first-message, derives all the cryptographic
     * material this client needs (SaltedPassword via real PBKDF2, then
     * ClientKey/StoredKey/ServerKey per the spec) and returns the
     * client-final-message to send back, including the proof that this
     * client knows the password without ever transmitting it.
     */
    public String buildClientFinalMessage(String serverFirstMessage) {
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

    /** Independently verifies the server's own signature against this client's own derived ServerKey - real proof the SERVER also knows the password (mutual authentication), not just one-directional password checking. A real client should treat a mismatch as seriously as a failed login. */
    public boolean verifyServerFinalMessage(String serverFinalMessage) {
        Map<String, String> attrs = parseAttrs(serverFinalMessage);
        String vAttr = attrs.get("v");
        if (vAttr == null) {
            return false;
        }
        byte[] received = Base64.getDecoder().decode(vAttr);
        String authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
        byte[] expected = hmac(serverKey, authMessage);
        return MessageDigest.isEqual(received, expected);
    }

    private static Map<String, String> parseAttrs(String message) {
        Map<String, String> attrs = new HashMap<>();
        for (String part : message.split(",")) {
            int eq = part.indexOf('=');
            if (eq > 0) {
                attrs.put(part.substring(0, eq), part.substring(eq + 1));
            }
        }
        return attrs;
    }

    private static byte[] pbkdf2(String password, byte[] salt, int iterations) {
        try {
            PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, 256);
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
        } catch (Exception e) {
            throw new IllegalStateException("PBKDF2WithHmacSHA256 should always be available on a standard JVM", e);
        }
    }

    private static byte[] hmac(byte[] key, String message) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(message.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("HmacSHA256 should always be available on a standard JVM", e);
        }
    }

    private static byte[] sha256(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 should always be available on a standard JVM", e);
        }
    }

    private static byte[] xor(byte[] a, byte[] b) {
        byte[] result = new byte[a.length];
        for (int i = 0; i < a.length; i++) {
            result[i] = (byte) (a[i] ^ b[i]);
        }
        return result;
    }
}
