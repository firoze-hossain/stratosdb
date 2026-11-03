package com.stratosdb.network.replication;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class StdTarArchiveTest {

    @Test
    void roundTripArchiveAndExtract(@TempDir Path tempDir) throws Exception {
        Path srcDir = tempDir.resolve("src");
        Path destDir = tempDir.resolve("dest");
        Files.createDirectories(srcDir.resolve("subdir"));

        Files.writeString(srcDir.resolve("file1.txt"), "Hello, World!");
        Files.writeString(srcDir.resolve("subdir/file2.dat"), "Nested content data inside directory");
        byte[] largeBytes = new byte[65536];
        java.util.Arrays.fill(largeBytes, (byte) 0x7F);
        Files.write(srcDir.resolve("large.bin"), largeBytes);
        Files.createFile(srcDir.resolve("empty.txt"));

        ByteArrayOutputStream tarOut = new ByteArrayOutputStream();
        int count = StdTarArchive.writeDirectory(tarOut, srcDir, path -> true);
        assertEquals(4, count, "Should have archived 4 files");

        byte[] tarBytes = tarOut.toByteArray();
        assertTrue(tarBytes.length > 65536, "Archive must contain file contents plus tar headers");
        assertEquals(0, tarBytes.length % 512, "Tar archive size must be a multiple of 512 bytes");

        ByteArrayInputStream tarIn = new ByteArrayInputStream(tarBytes);
        int extracted = StdTarArchive.extract(tarIn, destDir);
        assertEquals(4, extracted, "Should extract 4 files");

        assertEquals("Hello, World!", Files.readString(destDir.resolve("file1.txt")));
        assertEquals("Nested content data inside directory", Files.readString(destDir.resolve("subdir/file2.dat")));
        assertArrayEquals(largeBytes, Files.readAllBytes(destDir.resolve("large.bin")));
        assertEquals(0, Files.size(destDir.resolve("empty.txt")));
    }

    @Test
    void rejectsDirectoryTraversal(@TempDir Path tempDir) {
        ByteArrayOutputStream tarOut = new ByteArrayOutputStream();
        assertThrows(SecurityException.class, () -> {
            StdTarArchive.writeEntry(tarOut, "../../outside.txt", "evil content".getBytes(StandardCharsets.UTF_8), 0);
            StdTarArchive.writeEnd(tarOut);

            ByteArrayInputStream in = new ByteArrayInputStream(tarOut.toByteArray());
            StdTarArchive.extract(in, tempDir.resolve("sandbox"));
        });
    }

    @Test
    void ustarHeaderBlockValidation() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] payload = "testing ustar header checksum and magic".getBytes(StandardCharsets.UTF_8);
        StdTarArchive.writeEntry(out, "test.txt", payload, 1700000000L);
        StdTarArchive.writeEnd(out);

        byte[] bytes = out.toByteArray();
        assertTrue(bytes.length >= 1536); // 512 header + 512 payload block + 1024 EOF blocks

        // Header check: magic is ustar\0 at offset 257
        assertEquals((byte) 'u', bytes[257]);
        assertEquals((byte) 's', bytes[258]);
        assertEquals((byte) 't', bytes[259]);
        assertEquals((byte) 'a', bytes[260]);
        assertEquals((byte) 'r', bytes[261]);
        assertEquals(0, bytes[262]);

        // Version: "00" at offset 263
        assertEquals((byte) '0', bytes[263]);
        assertEquals((byte) '0', bytes[264]);

        // Typeflag: '0' at offset 156
        assertEquals((byte) '0', bytes[156]);
    }
}
