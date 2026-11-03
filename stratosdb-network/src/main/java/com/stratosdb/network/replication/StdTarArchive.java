package com.stratosdb.network.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.function.Predicate;

/**
 * Standard POSIX ustar (tar) archiving and extraction utility for StratosDB replication
 * and base backups. Pure Java implementation with zero external dependencies, 100% compliant
 * with POSIX.1-1988 and GNU tar formats.
 *
 * Used by {@link com.stratosdb.network.stdwire.StdWireServer} during {@code BASE_BACKUP}
 * to stream the database data directory over the network as a tar stream, and by
 * {@link StdBaseBackupClient} to unpack the incoming stream directly into the replica's
 * data directory without intermediate disk spooling.
 */
public final class StdTarArchive {
    private static final Logger LOG = LoggerFactory.getLogger(StdTarArchive.class);

    public static final int RECORD_SIZE = 512;
    private static final byte[] ZERO_BLOCK = new byte[RECORD_SIZE];
    private static final byte[] USTAR_MAGIC = "ustar\0".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] USTAR_VERSION = "00".getBytes(StandardCharsets.US_ASCII);

    private StdTarArchive() {}

    /**
     * Writes a tar entry header followed by the file contents from byte array, padded to 512-byte block.
     */
    public static void writeEntry(OutputStream out, String name, byte[] data, long mtimeSeconds) throws IOException {
        byte[] header = createHeader(name, data.length, mtimeSeconds, false);
        out.write(header);
        if (data.length > 0) {
            out.write(data);
            int pad = (int) ((RECORD_SIZE - (data.length % RECORD_SIZE)) % RECORD_SIZE);
            if (pad > 0) {
                out.write(ZERO_BLOCK, 0, pad);
            }
        }
    }

    /**
     * Writes a tar entry header followed by the contents of a file on disk, padded to 512-byte block.
     */
    public static void writeEntry(OutputStream out, String name, Path file) throws IOException {
        long size = Files.size(file);
        long mtime = Files.getLastModifiedTime(file).toMillis() / 1000L;
        byte[] header = createHeader(name, size, mtime, false);
        out.write(header);
        try (InputStream in = Files.newInputStream(file)) {
            byte[] buf = new byte[16384];
            int read;
            while ((read = in.read(buf)) != -1) {
                out.write(buf, 0, read);
            }
        }
        int pad = (int) ((RECORD_SIZE - (size % RECORD_SIZE)) % RECORD_SIZE);
        if (pad > 0) {
            out.write(ZERO_BLOCK, 0, pad);
        }
    }

    /**
     * Writes a directory entry into the tar archive.
     */
    public static void writeDirectoryEntry(OutputStream out, String name, long mtimeSeconds) throws IOException {
        String dirName = name.endsWith("/") ? name : name + "/";
        byte[] header = createHeader(dirName, 0, mtimeSeconds, true);
        out.write(header);
    }

    /**
     * Packages an entire directory into the given output stream using the specified filter,
     * writing directory and file entries, followed by the two 512-byte tar EOF blocks.
     */
    public static int writeDirectory(OutputStream out, Path sourceDir, Predicate<Path> filter) throws IOException {
        int[] fileCount = {0};
        Files.walkFileTree(sourceDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                if (filter != null && !filter.test(dir)) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                if (!dir.equals(sourceDir)) {
                    String relPath = sourceDir.relativize(dir).toString().replace('\\', '/');
                    writeDirectoryEntry(out, relPath, attrs.lastModifiedTime().toMillis() / 1000L);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (filter != null && !filter.test(file)) {
                    return FileVisitResult.CONTINUE;
                }
                String relPath = sourceDir.relativize(file).toString().replace('\\', '/');
                writeEntry(out, relPath, file);
                fileCount[0]++;
                return FileVisitResult.CONTINUE;
            }
        });
        writeEnd(out);
        return fileCount[0];
    }

    /**
     * Writes the two 512-byte end-of-archive blocks of zero bytes.
     */
    public static void writeEnd(OutputStream out) throws IOException {
        out.write(ZERO_BLOCK);
        out.write(ZERO_BLOCK);
    }

    /**
     * Extracts an incoming tar archive stream into the destination directory.
     * Prevents path traversal vulnerabilities ("Zip Slip") by checking normalized paths.
     *
     * @param in input stream delivering raw tar bytes
     * @param destDir destination root directory
     * @return number of files extracted
     */
    public static int extract(InputStream in, Path destDir) throws IOException {
        Files.createDirectories(destDir);
        Path normalizedDest = destDir.toAbsolutePath().normalize();
        byte[] headerBuf = new byte[RECORD_SIZE];
        int extractedCount = 0;

        while (true) {
            int read = readFully(in, headerBuf, 0, RECORD_SIZE);
            if (read < RECORD_SIZE) {
                break; // EOF
            }
            if (isAllZeros(headerBuf)) {
                // First zero block: could be EOF. Read next block to verify.
                int secondRead = readFully(in, headerBuf, 0, RECORD_SIZE);
                if (secondRead == RECORD_SIZE && isAllZeros(headerBuf)) {
                    break; // Proper tar EOF (two 512-byte zero blocks)
                }
                if (secondRead <= 0) {
                    break;
                }
            }

            TarHeader header = parseHeader(headerBuf);
            if (header == null || header.name.isBlank()) {
                break;
            }

            Path target = destDir.resolve(header.name).normalize();
            if (!target.startsWith(normalizedDest)) {
                throw new SecurityException("Tar entry attempted directory traversal: " + header.name);
            }

            if (header.isDir) {
                Files.createDirectories(target);
            } else {
                if (target.getParent() != null) {
                    Files.createDirectories(target.getParent());
                }
                try (OutputStream fileOut = new BufferedOutputStream(Files.newOutputStream(target))) {
                    copyBytes(in, fileOut, header.size);
                }
                extractedCount++;

                int pad = (int) ((RECORD_SIZE - (header.size % RECORD_SIZE)) % RECORD_SIZE);
                if (pad > 0) {
                    skipFully(in, pad);
                }
            }
        }
        return extractedCount;
    }

    private static byte[] createHeader(String name, long size, long mtimeSeconds, boolean isDir) {
        byte[] header = new byte[RECORD_SIZE];

        String namePart = name;
        String prefixPart = "";
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        if (nameBytes.length > 100) {
            int lastSlash = name.lastIndexOf('/');
            if (lastSlash > 0 && lastSlash <= 155 && (name.length() - lastSlash - 1) <= 100) {
                prefixPart = name.substring(0, lastSlash);
                namePart = name.substring(lastSlash + 1);
            } else {
                // Truncate safely if needed
                namePart = name.substring(Math.max(0, name.length() - 100));
            }
        }

        putAscii(header, 0, 100, namePart);
        putOctal(header, 100, 8, isDir ? 0755 : 0644);
        putOctal(header, 108, 8, 0); // uid
        putOctal(header, 116, 8, 0); // gid
        putOctal(header, 124, 12, size);
        putOctal(header, 136, 12, mtimeSeconds);

        // Pre-fill checksum field with spaces (0x20) for checksum calculation
        Arrays.fill(header, 148, 156, (byte) ' ');

        header[156] = isDir ? (byte) '5' : (byte) '0'; // typeflag

        System.arraycopy(USTAR_MAGIC, 0, header, 257, USTAR_MAGIC.length);
        System.arraycopy(USTAR_VERSION, 0, header, 263, USTAR_VERSION.length);
        putAscii(header, 265, 32, "stratos");
        putAscii(header, 297, 32, "stratos");

        if (!prefixPart.isEmpty()) {
            putAscii(header, 345, 155, prefixPart);
        }

        // Calculate unsigned checksum over 512 bytes
        long sum = 0;
        for (byte b : header) {
            sum += (b & 0xFF);
        }

        // Checksum format: 6 octal digits, null byte, space
        String chksumStr = String.format("%06o\0 ", sum);
        byte[] chkBytes = chksumStr.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(chkBytes, 0, header, 148, Math.min(chkBytes.length, 8));

        return header;
    }

    private static TarHeader parseHeader(byte[] header) {
        String name = readString(header, 0, 100);
        String prefix = readString(header, 345, 155);
        if (!prefix.isEmpty()) {
            name = prefix + "/" + name;
        }
        if (name.isEmpty()) return null;

        long size = parseOctal(header, 124, 12);
        byte typeflag = header[156];
        boolean isDir = typeflag == '5' || name.endsWith("/");

        return new TarHeader(name, size, isDir);
    }

    private record TarHeader(String name, long size, boolean isDir) {}

    private static void putAscii(byte[] dest, int offset, int maxLen, String val) {
        byte[] bytes = val.getBytes(StandardCharsets.UTF_8);
        int copyLen = Math.min(bytes.length, maxLen);
        System.arraycopy(bytes, 0, dest, offset, copyLen);
    }

    private static void putOctal(byte[] dest, int offset, int len, long val) {
        String s = Long.toOctalString(val);
        int pad = len - 1 - s.length();
        int cur = offset;
        for (int i = 0; i < pad; i++) {
            dest[cur++] = '0';
        }
        for (int i = 0; i < s.length(); i++) {
            dest[cur++] = (byte) s.charAt(i);
        }
        dest[cur] = 0; // null termination or space
    }

    private static long parseOctal(byte[] buf, int offset, int len) {
        long val = 0;
        for (int i = offset; i < offset + len; i++) {
            byte b = buf[i];
            if (b >= '0' && b <= '7') {
                val = (val << 3) + (b - '0');
            } else if (b == 0 || b == ' ') {
                if (val > 0) break;
            }
        }
        return val;
    }

    private static String readString(byte[] buf, int offset, int len) {
        int end = offset;
        while (end < offset + len && buf[end] != 0) {
            end++;
        }
        return new String(buf, offset, end - offset, StandardCharsets.UTF_8).trim();
    }

    private static boolean isAllZeros(byte[] buf) {
        for (byte b : buf) {
            if (b != 0) return false;
        }
        return true;
    }

    private static int readFully(InputStream in, byte[] b, int off, int len) throws IOException {
        int total = 0;
        while (total < len) {
            int count = in.read(b, off + total, len - total);
            if (count < 0) break;
            total += count;
        }
        return total;
    }

    private static void copyBytes(InputStream in, OutputStream out, long bytesToCopy) throws IOException {
        byte[] buf = new byte[16384];
        long remaining = bytesToCopy;
        while (remaining > 0) {
            int toRead = (int) Math.min(buf.length, remaining);
            int read = in.read(buf, 0, toRead);
            if (read < 0) {
                throw new EOFException("Premature EOF reading tar entry data (remaining: " + remaining + ")");
            }
            out.write(buf, 0, read);
            remaining -= read;
        }
    }

    private static void skipFully(InputStream in, long bytesToSkip) throws IOException {
        long remaining = bytesToSkip;
        while (remaining > 0) {
            long skipped = in.skip(remaining);
            if (skipped <= 0) {
                if (in.read() == -1) {
                    throw new EOFException("Premature EOF while skipping tar padding");
                }
                remaining--;
            } else {
                remaining -= skipped;
            }
        }
    }
}
