package com.stratosdb.storage.crypto;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

/**
 * Thread-safe AES-256-GCM cipher for Transparent Data Encryption (TDE)
 * of table pages, index pages, and WAL segments.
 *
 * <p>Uses thread-local {@link Cipher} and {@link SecureRandom} instances so
 * HotSpot AES-NI intrinsics run with zero lock contention and minimal allocation.
 */
public final class TdeCipher {
    public static final int IV_BYTES = 12;
    public static final int TAG_BITS = 128;
    public static final int TAG_BYTES = TAG_BITS / 8;
    public static final int OVERHEAD_BYTES = IV_BYTES + TAG_BYTES; // 28 bytes

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";

    private final SecretKey masterKey;
    private final ThreadLocal<Cipher> threadCipher = ThreadLocal.withInitial(() -> {
        try {
            return Cipher.getInstance(TRANSFORMATION);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES/GCM/NoPadding not available", e);
        }
    });
    private final ThreadLocal<SecureRandom> threadRandom = ThreadLocal.withInitial(SecureRandom::new);
    private final ThreadLocal<byte[]> threadIv = ThreadLocal.withInitial(() -> new byte[IV_BYTES]);

    public TdeCipher(SecretKey masterKey) {
        if (masterKey == null || masterKey.getEncoded().length != TdeKeyManager.KEY_BYTES) {
            throw new IllegalArgumentException("TdeCipher requires a 256-bit AES key");
        }
        this.masterKey = masterKey;
    }

    /**
     * Encrypts {@code plaintext} into {@code [12-byte IV][ciphertext + 16-byte GCM tag]}.
     */
    public byte[] encrypt(byte[] plaintext) {
        try {
            byte[] iv = new byte[IV_BYTES];
            threadRandom.get().nextBytes(iv);
            Cipher cipher = threadCipher.get();
            cipher.init(Cipher.ENCRYPT_MODE, masterKey, new GCMParameterSpec(TAG_BITS, iv));
            byte[] ciphertext = cipher.doFinal(plaintext);
            byte[] out = new byte[IV_BYTES + ciphertext.length];
            System.arraycopy(iv, 0, out, 0, IV_BYTES);
            System.arraycopy(ciphertext, 0, out, IV_BYTES, ciphertext.length);
            return out;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("TDE encryption failed", e);
        }
    }

    /**
     * Decrypts {@code [12-byte IV][ciphertext + 16-byte GCM tag]} back to plaintext.
     */
    public byte[] decrypt(byte[] ivAndCiphertext) {
        if (ivAndCiphertext == null || ivAndCiphertext.length < OVERHEAD_BYTES) {
            throw new IllegalArgumentException("Encrypted TDE buffer too short");
        }
        try {
            Cipher cipher = threadCipher.get();
            cipher.init(Cipher.DECRYPT_MODE, masterKey, new GCMParameterSpec(TAG_BITS, ivAndCiphertext, 0, IV_BYTES));
            return cipher.doFinal(ivAndCiphertext, IV_BYTES, ivAndCiphertext.length - IV_BYTES);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("TDE decryption/authentication failed (wrong key or tampered data)", e);
        }
    }

    /**
     * Encrypts a {@code ByteBuffer} page (from position 0 to {@code pageSize}) into
     * {@code outEncrypted} (which must have capacity >= {@code pageSize + OVERHEAD_BYTES}).
     */
    public void encryptPage(ByteBuffer plaintextPage, int pageSize, ByteBuffer outEncrypted) {
        try {
            byte[] iv = threadIv.get();
            threadRandom.get().nextBytes(iv);
            Cipher cipher = threadCipher.get();
            cipher.init(Cipher.ENCRYPT_MODE, masterKey, new GCMParameterSpec(TAG_BITS, iv));
            outEncrypted.clear();
            outEncrypted.put(iv);
            ByteBuffer src = plaintextPage.duplicate();
            src.position(0);
            src.limit(pageSize);
            cipher.doFinal(src, outEncrypted);
            outEncrypted.flip();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("TDE page encryption failed", e);
        }
    }

    /**
     * Decrypts an encrypted page buffer (size {@code pageSize + OVERHEAD_BYTES}) into
     * {@code outPlaintext} at position 0..{@code pageSize}.
     */
    public void decryptPage(ByteBuffer encryptedPage, int pageSize, ByteBuffer outPlaintext) {
        try {
            byte[] iv = threadIv.get();
            ByteBuffer src = encryptedPage.duplicate();
            src.position(0);
            src.limit(pageSize + OVERHEAD_BYTES);
            src.get(iv);
            Cipher cipher = threadCipher.get();
            cipher.init(Cipher.DECRYPT_MODE, masterKey, new GCMParameterSpec(TAG_BITS, iv));
            outPlaintext.position(0);
            outPlaintext.limit(pageSize);
            cipher.doFinal(src, outPlaintext);
            outPlaintext.position(0);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("TDE page decryption/authentication failed (wrong key or tampered data)", e);
        }
    }
}
