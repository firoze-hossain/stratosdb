package com.stratosdb.network.auth;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * A real, from-scratch server-side implementation of SCRAM-SHA-256 (RFC
 * 5802), the actual mechanism real PostgreSQL clients (psql, JDBC,
 * psycopg2) negotiate by default since Postgres 10 - not a password sent
 * in the clear, and not a simplified stand-in. The password itself is
 * never transmitted in either direction at any point in this exchange;
 * what crosses the wire is a proof that both sides independently derived
 * the same cryptographic material from it.
 *
 * One instance of {@link Handshake} is created per authentication
 * attempt and holds state across its three messages (client-first,
 * server-first, client-final/server-final) - the handshake genuinely
 * cannot be completed from a single message in isolation, since later
 * steps depend on exact values exchanged in earlier ones (the combined
 * nonce, and the three concatenated message fragments that make up
 * "AuthMessage", the value both sides sign to prove they hold the same
 * derived key without ever sending that key itself).
 *
 * Channel binding (the "cbind" mechanism variant, tied to the TLS
 * channel a connection is using) is a real, separate SCRAM feature not
 * implemented here - this handshake accepts the client's "n,,"
 * (no-channel-binding) GS2 header at face value rather than verifying
 * it against anything, matching SCRAM-SHA-256 (not SCRAM-SHA-256-PLUS,
 * the channel-binding variant) - the same mechanism name real Postgres
 * offers by default over a plain, non-channel-bound connection.
 */
public final class ScramSha256 {
    public static final String MECHANISM_NAME = "SCRAM-SHA-256";

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int SERVER_NONCE_BYTES = 18; // 24 base64 characters - a comfortable, real amount of server-contributed entropy

    private ScramSha256() {
    }

    public static final class ScramAuthenticationException extends Exception {
        public ScramAuthenticationException(String message) {
            super(message);
        }
    }

    /**
     * Stateful handshake for one authentication attempt. Construct with
     * the user's stored ScramCredential (or null for an unknown user -
     * see clientFirst's own javadoc for why this still needs to proceed
     * rather than fail immediately).
     */
    public static final class Handshake {
        private final UserStore.ScramCredential credential;
        private final String username;
        private String clientFirstMessageBare;
        private String serverFirstMessage;
        private String combinedNonce;

        public Handshake(String username, UserStore.ScramCredential credential) {
            this.username = username;
            this.credential = credential;
        }

        /**
         * Processes the client-first-message ("n,,n=<user>,r=<nonce>")
         * and returns the server-first-message to send back.
         *
         * A null credential (unknown username) still produces a normal,
         * well-formed server-first-message with a freshly-generated,
         * fake salt and a plausible iteration count, rather than an
         * immediate error - exactly the real Postgres/RFC-recommended
         * defense against username enumeration: an attacker probing
         * usernames sees the same "please continue the handshake"
         * response either way, and only finds out authentication failed
         * at the very end, indistinguishable from a genuine wrong
         * password. clientFinal is responsible for actually failing in
         * this case, since verification is impossible without a real
         * StoredKey.
         */
        public String clientFirst(String clientFirstMessage) throws ScramAuthenticationException {
            Map<String, String> attrs = parseAttributes(stripGs2Header(clientFirstMessage));
            this.clientFirstMessageBare = stripGs2Header(clientFirstMessage);
            String clientNonce = attrs.get("r");
            if (clientNonce == null) {
                throw new ScramAuthenticationException("client-first-message is missing its nonce (r=)");
            }

            byte[] serverNonceBytes = new byte[SERVER_NONCE_BYTES];
            RANDOM.nextBytes(serverNonceBytes);
            String serverNoncePart = Base64.getEncoder().withoutPadding().encodeToString(serverNonceBytes);
            this.combinedNonce = clientNonce + serverNoncePart;

            byte[] salt = credential != null ? credential.salt() : fakeSaltFor(username);
            int iterations = credential != null ? credential.iterations() : 100_000;

            this.serverFirstMessage = "r=" + combinedNonce + ",s=" + Base64.getEncoder().encodeToString(salt) + ",i=" + iterations;
            return serverFirstMessage;
        }

        /**
         * Processes the client-final-message
         * ("c=biws,r=<nonce>,p=<base64 proof>") and returns the
         * server-final-message ("v=<base64 signature>") on success, or
         * throws on any failure - wrong nonce (a real, if unlikely,
         * replay/mixup defense), unknown user, or a proof that doesn't
         * verify against the stored credential.
         */
        public String clientFinal(String clientFinalMessage) throws ScramAuthenticationException {
            int proofFieldStart = clientFinalMessage.lastIndexOf(",p=");
            if (proofFieldStart < 0) {
                throw new ScramAuthenticationException("client-final-message is missing its proof (p=)");
            }
            String clientFinalMessageWithoutProof = clientFinalMessage.substring(0, proofFieldStart);
            Map<String, String> attrs = parseAttributes(clientFinalMessage);
            String receivedNonce = attrs.get("r");
            String proofBase64 = attrs.get("p");
            if (receivedNonce == null || proofBase64 == null) {
                throw new ScramAuthenticationException("client-final-message is missing required fields");
            }
            if (!receivedNonce.equals(combinedNonce)) {
                throw new ScramAuthenticationException("nonce mismatch - possible replay or a handshake mixup");
            }
            if (credential == null) {
                // No real StoredKey to verify against (unknown user, deferred from
                // clientFirst - see its own javadoc) - fail now, but only now.
                throw new ScramAuthenticationException("authentication failed");
            }

            byte[] clientProof = Base64.getDecoder().decode(proofBase64);
            String authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;

            byte[] clientKey = hmac(credential.saltedPassword(), "Client Key");
            byte[] storedKey = sha256(clientKey);
            byte[] clientSignature = hmac(storedKey, authMessage);
            byte[] recoveredClientKey = xor(clientProof, clientSignature);

            if (!MessageDigest.isEqual(sha256(recoveredClientKey), storedKey)) {
                throw new ScramAuthenticationException("authentication failed");
            }

            byte[] serverKey = hmac(credential.saltedPassword(), "Server Key");
            byte[] serverSignature = hmac(serverKey, authMessage);
            return "v=" + Base64.getEncoder().encodeToString(serverSignature);
        }
    }

    /** The GS2 header ("n,," for no channel binding, no authzid) is a fixed prefix real clients send before the actual SCRAM attributes - stripped here since it isn't itself a comma-separated attribute list, but the exact bytes still need to feed into client-first-message-bare for AuthMessage, which this returns unmodified aside from removing that one known prefix. */
    private static String stripGs2Header(String clientFirstMessage) {
        int secondComma = clientFirstMessage.indexOf(',', clientFirstMessage.indexOf(',') + 1);
        return clientFirstMessage.substring(secondComma + 1);
    }

    private static Map<String, String> parseAttributes(String message) {
        Map<String, String> attrs = new HashMap<>();
        for (String part : message.split(",")) {
            int eq = part.indexOf('=');
            if (eq > 0) {
                attrs.put(part.substring(0, eq), part.substring(eq + 1));
            }
        }
        return attrs;
    }

    /** A deterministic-per-username, but not-actually-derived-from-any-real-password, salt for the unknown-user case - see Handshake.clientFirst's own javadoc on why this branch exists at all. Deterministic (not random) specifically so repeated probing of the same nonexistent username can't be used to distinguish it from a real one by checking whether the salt changes between attempts. */
    private static byte[] fakeSaltFor(String username) {
        return sha256(("no-such-user:" + username).getBytes(StandardCharsets.UTF_8));
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
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 should always be available on a standard JVM", e);
        }
    }

    private static byte[] xor(byte[] a, byte[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("XOR operands must be the same length, got " + a.length + " and " + b.length);
        }
        byte[] result = new byte[a.length];
        for (int i = 0; i < a.length; i++) {
            result[i] = (byte) (a[i] ^ b[i]);
        }
        return result;
    }
}
