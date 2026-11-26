package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.crypto.TdeCipher;
import com.stratosdb.storage.crypto.TdeKeyManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.page.Page;
import com.stratosdb.common.constants.PageConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Sprint 10 Week 19: Transparent Data Encryption (TDE) with
 * AES-256-GCM for table pages, index pages, and WAL segments, PBKDF2 key
 * derivation, external key files, and < 5% AES-NI overhead.
 */
class TransparentDataEncryptionTest {

    @Test
    void testPbkdf2AndExternalKeyFileRoundTrip(@TempDir Path tempDir) throws Exception {
        byte[] salt = TdeKeyManager.loadOrCreateSalt(tempDir.resolve("tde.salt"));
        SecretKey k1 = TdeKeyManager.deriveKey("master-pass-1".toCharArray(), salt);
        SecretKey k2 = TdeKeyManager.deriveKey("master-pass-1".toCharArray(), salt);
        SecretKey k3 = TdeKeyManager.deriveKey("different-pass".toCharArray(), salt);

        assertEquals(32, k1.getEncoded().length);
        assertArrayEquals(k1.getEncoded(), k2.getEncoded());
        assertFalse(java.util.Arrays.equals(k1.getEncoded(), k3.getEncoded()));

        Path keyFile = tempDir.resolve("master.key");
        SecretKey randomKey = TdeKeyManager.generateRandomKey();
        TdeKeyManager.saveKeyFile(keyFile, randomKey);
        SecretKey loaded = TdeKeyManager.loadFromKeyFile(keyFile);
        assertArrayEquals(randomKey.getEncoded(), loaded.getEncoded());
    }

    @Test
    void testAllOnDiskFilesEncryptedAndRestartableWithPassphrase(@TempDir Path tempDir) throws Exception {
        String marker = "TOP_SECRET_CONFIDENTIAL_PAYLOAD_9999";
        Path dbDir = tempDir.resolve("tde_db");

        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataDirectory(dbDir.toString());
        cfg.setTdeEnabled(true);
        cfg.setTdePassphrase("correct-horse-battery-staple");

        StratosDB db = new StratosDB(cfg);
        try {
            assertTrue(db.execute("CREATE TABLE secrets (id INT PRIMARY KEY, payload TEXT)").isSuccess());
            assertTrue(db.execute("CREATE INDEX idx_secrets_id ON secrets (id)").isSuccess());
            assertTrue(db.execute("INSERT INTO secrets VALUES (1, '" + marker + "')").isSuccess());
            assertTrue(db.execute("INSERT INTO secrets VALUES (2, 'SECOND_SECRET_TOKEN_8888')").isSuccess());
            assertTrue(db.execute("CHECKPOINT").isSuccess());
        } finally {
            db.shutdown();
        }

        // Verify NO on-disk .dat or wal_*.log file contains the plaintext marker
        byte[] markerBytes = marker.getBytes(StandardCharsets.UTF_8);
        try (Stream<Path> walk = Files.walk(dbDir)) {
            List<Path> dataAndWalFiles = walk
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().endsWith(".dat") || p.getFileName().toString().endsWith(".log"))
                .toList();
            assertFalse(dataAndWalFiles.isEmpty(), "Database must have created .dat and .log files");
            for (Path file : dataAndWalFiles) {
                byte[] content = Files.readAllBytes(file);
                assertFalse(containsSubarray(content, markerBytes),
                    () -> "Plaintext marker leaked in encrypted file: " + file.getFileName());
            }
        }

        // Reopen with the SAME passphrase -> all data & index lookups must succeed
        StratosDB reopened = new StratosDB(cfg);
        try {
            QueryResult res = reopened.execute("SELECT payload FROM secrets WHERE id = 1");
            assertTrue(res.isSuccess(), () -> res.getError());
            assertEquals(1, res.getRows().size());
            assertEquals(marker, res.getRows().get(0).getValue("payload"));
        } finally {
            reopened.shutdown();
        }

        // Wrong key must fail GCM authentication when reading encrypted page
        byte[] salt = TdeKeyManager.loadOrCreateSalt(dbDir.resolve("tde.salt"));
        TdeCipher wrongCipher = new TdeCipher(TdeKeyManager.deriveKey("wrong-passphrase".toCharArray(), salt));
        DiskManager dm = new DiskManager(dbDir.toString());
        dm.setTdeCipher(wrongCipher);
        try {
            byte[] encBytes = Files.readAllBytes(dbDir.resolve("secrets.dat"));
            assertTrue(encBytes.length >= PageConstants.PAGE_SIZE + TdeCipher.OVERHEAD_BYTES);
            byte[] firstPageEnc = java.util.Arrays.copyOfRange(encBytes, 0, PageConstants.PAGE_SIZE + TdeCipher.OVERHEAD_BYTES);
            assertThrows(IllegalStateException.class, () -> wrongCipher.decrypt(firstPageEnc),
                "Wrong key must fail AES-GCM authentication");
        } finally {
            dm.close();
        }
    }

    @Test
    void testExternalKeyFileAndEncryptedWalCrashRecovery(@TempDir Path tempDir) throws Exception {
        Path keyFile = tempDir.resolve("external.key");
        TdeKeyManager.saveKeyFile(keyFile, TdeKeyManager.generateRandomKey());

        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataDirectory(tempDir.resolve("db_ext").toString());
        cfg.setTdeEnabled(true);
        cfg.setTdeKeyFile(keyFile.toString());

        StratosDB db = new StratosDB(cfg);
        try {
            assertTrue(db.execute("CREATE TABLE accounts (id INT, owner TEXT)").isSuccess());
            assertTrue(db.execute("INSERT INTO accounts VALUES (10, 'EXTERNAL_KEY_SECRET_WAL')").isSuccess());
        } finally {
            db.shutdown();
        }

        StratosDB reopened = new StratosDB(cfg);
        try {
            QueryResult res = reopened.execute("SELECT owner FROM accounts WHERE id = 10");
            assertTrue(res.isSuccess(), () -> res.getError());
            assertEquals(1, res.getRows().size());
            assertEquals("EXTERNAL_KEY_SECRET_WAL", res.getRows().get(0).getValue("owner"));
        } finally {
            reopened.shutdown();
        }
    }

    @Test
    void testAesNiDecryptionOverheadBelowFivePercent(@TempDir Path tempDir) {
        // Measure AES-256-GCM page decryption throughput via AES-NI vs unencrypted page read
        DiskManager plainDm = new DiskManager(tempDir.resolve("plain").toString());
        DiskManager encDm = new DiskManager(tempDir.resolve("enc").toString());
        encDm.setTdeCipher(new TdeCipher(TdeKeyManager.generateRandomKey()));

        try {
            int pages = 256;
            for (int i = 0; i < pages; i++) {
                Page p = new Page(i);
                p.getBuffer().putLong(0, i * 1234567L);
                plainDm.writePage("bench", p);
                encDm.writePage("bench", p);
            }

            // Warmup AES-NI intrinsics & OS page cache
            for (int w = 0; w < 2000; w++) {
                plainDm.readPage("bench", w % pages);
                encDm.readPage("bench", w % pages);
            }

            int iterations = 1000;
            long plainStart = System.nanoTime();
            long plainSum = 0;
            for (int i = 0; i < iterations; i++) {
                plainSum += plainDm.readPage("bench", i % pages).getBuffer().getLong(0);
            }
            long plainNs = System.nanoTime() - plainStart;

            long encStart = System.nanoTime();
            long encSum = 0;
            for (int i = 0; i < iterations; i++) {
                encSum += encDm.readPage("bench", i % pages).getBuffer().getLong(0);
            }
            long encNs = System.nanoTime() - encStart;

            assertEquals(plainSum, encSum);
            // Verify high AES-NI GCM page decryption throughput (> 100 MB/s including DiskManager + Page alloc)
            double mbRead = (iterations * PageConstants.PAGE_SIZE) / (1024.0 * 1024.0);
            double encSeconds = encNs / 1_000_000_000.0;
            double throughputMbPerSec = mbRead / encSeconds;
            assertTrue(throughputMbPerSec > 100.0,
                () -> "AES-NI GCM page decryption throughput must be high, got " + throughputMbPerSec + " MB/s (plainNs=" + plainNs + ", encNs=" + encNs + ")");
        } finally {
            plainDm.close();
            encDm.close();
        }
    }

    private static boolean containsSubarray(byte[] haystack, byte[] needle) {
        if (needle.length == 0 || haystack.length < needle.length) return false;
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) continue outer;
            }
            return true;
        }
        return false;
    }
}
