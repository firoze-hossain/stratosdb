package com.stratosdb.network.auth;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory user credential store: usernames mapped to a random salt and a
 * PBKDF2-HMAC-SHA256 hash of (password, salt) - not a plain or single-round
 * hash, which would be fast to brute-force. 100,000 iterations matches
 * OWASP's current baseline recommendation for PBKDF2-SHA256.
 *
 * Real, if minimal: passwords are never stored or compared in plaintext,
 * each user gets an independent random salt (so two users with the same
 * password don't get the same hash, and precomputed rainbow tables don't
 * help an attacker), and verification uses a constant-time comparison
 * (MessageDigest.isEqual) so response timing doesn't leak how many
 * leading bytes of the hash matched.
 *
 * What this deliberately does NOT do, stated plainly: there is no
 * persistence (users are configured in code/at startup and lost on
 * restart - there's no CREATE USER statement or a users table yet), no
 * password complexity policy, and no account lockout after repeated
 * failures. All real further work, not attempted here.
 */
public class UserStore {
    private static final int SALT_LENGTH_BYTES = 16;
    private static final int HASH_LENGTH_BITS = 256;
    private static final int PBKDF2_ITERATIONS = 100_000;
    private static final String ALGORITHM = "PBKDF2WithHmacSHA256";

    private final Map<String, StoredCredential> users = new ConcurrentHashMap<>();
    private final SecureRandom random = new SecureRandom();

    public void addUser(String username, String plaintextPassword) {
        byte[] salt = new byte[SALT_LENGTH_BYTES];
        random.nextBytes(salt);
        byte[] hash = hash(plaintextPassword, salt);
        users.put(username, new StoredCredential(salt, hash));
    }

    public void removeUser(String username) {
        users.remove(username);
    }

    /** True iff username exists and plaintextPassword matches, via a constant-time comparison. */
    public boolean verify(String username, String plaintextPassword) {
        StoredCredential cred = users.get(username);
        if (cred == null) {
            // Still do a hash computation even for an unknown user, so that
            // "unknown username" and "known username, wrong password" take
            // roughly the same amount of time - a real, if small, defense
            // against username enumeration via response timing.
            hash(plaintextPassword, DUMMY_SALT);
            return false;
        }
        byte[] candidateHash = hash(plaintextPassword, cred.salt());
        return MessageDigest.isEqual(candidateHash, cred.hash());
    }

    public boolean hasAnyUsers() {
        return !users.isEmpty();
    }

    private static final byte[] DUMMY_SALT = new byte[SALT_LENGTH_BYTES];

    private byte[] hash(String password, byte[] salt) {
        try {
            PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, PBKDF2_ITERATIONS, HASH_LENGTH_BITS);
            SecretKeyFactory factory = SecretKeyFactory.getInstance(ALGORITHM);
            return factory.generateSecret(spec).getEncoded();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Failed to hash password - " + ALGORITHM + " should always be available on a standard JVM", e);
        }
    }

    private record StoredCredential(byte[] salt, byte[] hash) {}
}
