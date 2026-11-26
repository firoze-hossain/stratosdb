package com.stratosdb.storage.crypto;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Master key management for Transparent Data Encryption (TDE):
 * supports PBKDF2-HMAC-SHA256 key derivation from a master passphrase
 * (with persisted salt) as well as external 256-bit key files.
 */
public final class TdeKeyManager {
    public static final int KEY_BITS = 256;
    public static final int KEY_BYTES = KEY_BITS / 8;
    public static final int SALT_BYTES = 16;
    public static final int PBKDF2_ITERATIONS = 65_536;
    private static final String PBKDF2_ALGO = "PBKDF2WithHmacSHA256";

    private TdeKeyManager() {}

    public static SecretKey generateRandomKey() {
        try {
            KeyGenerator gen = KeyGenerator.getInstance("AES");
            gen.init(KEY_BITS, new SecureRandom());
            return gen.generateKey();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Failed to generate AES-256 key", e);
        }
    }

    public static SecretKey deriveKey(char[] passphrase, byte[] salt) {
        if (passphrase == null || passphrase.length == 0) {
            throw new IllegalArgumentException("TDE passphrase cannot be empty");
        }
        if (salt == null || salt.length < 8) {
            throw new IllegalArgumentException("TDE salt must be at least 8 bytes");
        }
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance(PBKDF2_ALGO);
            PBEKeySpec spec = new PBEKeySpec(passphrase, salt, PBKDF2_ITERATIONS, KEY_BITS);
            byte[] derived = factory.generateSecret(spec).getEncoded();
            spec.clearPassword();
            return new SecretKeySpec(derived, "AES");
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Failed to derive AES-256 key via PBKDF2", e);
        }
    }

    public static byte[] loadOrCreateSalt(Path saltFile) throws IOException {
        if (Files.exists(saltFile)) {
            byte[] salt = Files.readAllBytes(saltFile);
            if (salt.length >= 8) {
                return salt;
            }
        }
        byte[] salt = new byte[SALT_BYTES];
        new SecureRandom().nextBytes(salt);
        if (saltFile.getParent() != null) {
            Files.createDirectories(saltFile.getParent());
        }
        Files.write(saltFile, salt);
        return salt;
    }

    public static void saveKeyFile(Path keyFile, SecretKey key) throws IOException {
        if (keyFile.getParent() != null) {
            Files.createDirectories(keyFile.getParent());
        }
        String encoded = Base64.getEncoder().encodeToString(key.getEncoded());
        Files.writeString(keyFile, encoded, StandardCharsets.UTF_8);
    }

    public static SecretKey loadFromKeyFile(Path keyFile) throws IOException {
        if (!Files.exists(keyFile)) {
            throw new IOException("TDE key file not found: " + keyFile);
        }
        byte[] raw = Files.readAllBytes(keyFile);
        if (raw.length == KEY_BYTES) {
            return new SecretKeySpec(raw, "AES");
        }
        String text = new String(raw, StandardCharsets.UTF_8).trim();
        byte[] decoded = Base64.getDecoder().decode(text);
        if (decoded.length != KEY_BYTES) {
            throw new IllegalArgumentException("Invalid AES-256 key length in " + keyFile + ": " + decoded.length + " bytes");
        }
        return new SecretKeySpec(decoded, "AES");
    }
}
