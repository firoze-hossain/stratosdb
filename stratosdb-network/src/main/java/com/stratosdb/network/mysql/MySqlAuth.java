package com.stratosdb.network.mysql;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * Implements MySQL {@code caching_sha2_password} and {@code mysql_native_password}
 * scramble generation and verification.
 */
public final class MySqlAuth {
    public static final String PLUGIN_CACHING_SHA2 = "caching_sha2_password";
    public static final String PLUGIN_NATIVE_PASSWORD = "mysql_native_password";

    private static final SecureRandom RANDOM = new SecureRandom();

    private MySqlAuth() {}

    public static byte[] generateScramble20() {
        byte[] scramble = new byte[20];
        RANDOM.nextBytes(scramble);
        // Avoid NUL bytes in scramble for strict C-string compatibility
        for (int i = 0; i < scramble.length; i++) {
            if (scramble[i] == 0) {
                scramble[i] = 1;
            }
        }
        return scramble;
    }

    /**
     * Computes {@code caching_sha2_password} token:
     * {@code XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble20))}.
     */
    public static byte[] scrambleCachingSha2(String password, byte[] scramble20) {
        if (password == null || password.isEmpty()) {
            return new byte[0];
        }
        byte[] passBytes = password.getBytes(StandardCharsets.UTF_8);
        byte[] dig1 = sha256(passBytes);
        byte[] dig2 = sha256(dig1);
        byte[] dig3 = sha256Concat(dig2, scramble20);
        return xor(dig1, dig3);
    }

    /**
     * Computes {@code mysql_native_password} token:
     * {@code XOR(SHA1(password), SHA1(scramble20 || SHA1(SHA1(password))))}.
     */
    public static byte[] scrambleNativePassword(String password, byte[] scramble20) {
        if (password == null || password.isEmpty()) {
            return new byte[0];
        }
        byte[] passBytes = password.getBytes(StandardCharsets.UTF_8);
        byte[] stage1 = sha1(passBytes);
        byte[] stage2 = sha1(stage1);
        byte[] challenge = sha1Concat(scramble20, stage2);
        return xor(stage1, challenge);
    }

    public static boolean verifyCachingSha2(String expectedPassword, byte[] scramble20, byte[] clientToken) {
        if (expectedPassword == null || expectedPassword.isEmpty()) {
            return clientToken == null || clientToken.length == 0;
        }
        byte[] expectedToken = scrambleCachingSha2(expectedPassword, scramble20);
        return MessageDigest.isEqual(expectedToken, clientToken);
    }

    public static boolean verifyNativePassword(String expectedPassword, byte[] scramble20, byte[] clientToken) {
        if (expectedPassword == null || expectedPassword.isEmpty()) {
            return clientToken == null || clientToken.length == 0;
        }
        byte[] expectedToken = scrambleNativePassword(expectedPassword, scramble20);
        return MessageDigest.isEqual(expectedToken, clientToken);
    }

    private static byte[] sha256(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] sha256Concat(byte[] a, byte[] b) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(a);
            md.update(b);
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] sha1(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-1").digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] sha1Concat(byte[] a, byte[] b) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update(a);
            md.update(b);
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] xor(byte[] a, byte[] b) {
        byte[] out = new byte[a.length];
        for (int i = 0; i < a.length; i++) {
            out[i] = (byte) (a[i] ^ b[i]);
        }
        return out;
    }
}
