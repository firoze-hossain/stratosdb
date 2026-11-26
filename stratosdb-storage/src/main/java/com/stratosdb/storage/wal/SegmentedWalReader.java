package com.stratosdb.storage.wal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Reader for segmented WAL logs that seamlessly navigates across
 * 16MB segment files (wal_00000001.log, wal_00000002.log, etc.).
 *
 * Handles cross-segment record boundary transitions, EOF detection,
 * and zero-filled tails from pre-allocated segment files.
 */
public class SegmentedWalReader implements AutoCloseable {

    private final File walDirectory;
    private final long segmentSize;
    private final List<Long> availableSegmentNumbers;
    private int currentSegmentIndex;
    private long currentLsn;
    private final long maxLsn;

    private RandomAccessFile currentRaf;
    private long currentOpenSegNum = -1L;
    private final byte[] decryptedStream;
    private int decryptedPos = 0;

    public SegmentedWalReader(File walDirectory, long segmentSize, List<Long> availableSegmentNumbers,
                              long startLsn, long maxLsn) throws IOException {
        this(walDirectory, segmentSize, availableSegmentNumbers, startLsn, maxLsn, null);
    }

    public SegmentedWalReader(File walDirectory, long segmentSize, List<Long> availableSegmentNumbers,
                              long startLsn, long maxLsn, com.stratosdb.storage.crypto.TdeCipher tdeCipher) throws IOException {
        this.walDirectory = walDirectory;
        this.segmentSize = segmentSize;
        this.availableSegmentNumbers = availableSegmentNumbers;
        this.currentLsn = startLsn;
        this.maxLsn = maxLsn;
        this.currentSegmentIndex = findSegmentIndexForLsn(startLsn);
        openSegmentForLsn(startLsn);
        if (tdeCipher != null) {
            this.decryptedStream = decryptAllFrames(tdeCipher);
        } else {
            this.decryptedStream = null;
        }
    }

    private byte[] decryptAllFrames(com.stratosdb.storage.crypto.TdeCipher tdeCipher) throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        while (hasRemainingRaw()) {
            byte[] lenBytes = new byte[4];
            int r = readRawPartialBytes(lenBytes, 0, 4);
            if (r < 4) {
                break;
            }
            int frameLen = ((lenBytes[0] & 0xFF) << 24) | ((lenBytes[1] & 0xFF) << 16)
                | ((lenBytes[2] & 0xFF) << 8) | (lenBytes[3] & 0xFF);
            if (frameLen <= 0) {
                break;
            }
            byte[] enc = readRawBytes(frameLen);
            byte[] plain = tdeCipher.decrypt(enc);
            out.write(plain);
        }
        return out.toByteArray();
    }

    private int findSegmentIndexForLsn(long lsn) {
        long targetSegNum = (lsn / segmentSize) + 1;
        for (int i = 0; i < availableSegmentNumbers.size(); i++) {
            if (availableSegmentNumbers.get(i) == targetSegNum) {
                return i;
            }
        }
        return 0;
    }

    private void openSegmentForLsn(long lsn) throws IOException {
        long targetSegNum = (lsn / segmentSize) + 1;
        if (targetSegNum == currentOpenSegNum && currentRaf != null) {
            return;
        }

        if (currentRaf != null) {
            try {
                currentRaf.close();
            } catch (IOException ignored) {}
            currentRaf = null;
            currentOpenSegNum = -1L;
        }

        File segFile = new File(walDirectory, WALManager.formatSegmentFileName(targetSegNum));
        if (!segFile.exists()) {
            // Check legacy wal.log fallback
            File legacyFile = new File(walDirectory, "wal.log");
            if (legacyFile.exists() && targetSegNum == 1) {
                segFile = legacyFile;
            } else {
                return; // Segment file does not exist
            }
        }

        this.currentRaf = new RandomAccessFile(segFile, "r");
        this.currentOpenSegNum = targetSegNum;
        long segOffset = lsn % segmentSize;
        if (segOffset < currentRaf.length()) {
            currentRaf.seek(segOffset);
        }
    }

    public boolean hasRemaining() {
        if (decryptedStream != null) {
            return decryptedPos < decryptedStream.length;
        }
        return hasRemainingRaw();
    }

    private boolean hasRemainingRaw() {
        if (maxLsn > 0 && currentLsn >= maxLsn) {
            return false;
        }
        long targetSegNum = (currentLsn / segmentSize) + 1;
        File segFile = new File(walDirectory, WALManager.formatSegmentFileName(targetSegNum));
        if (!segFile.exists()) {
            File legacy = new File(walDirectory, "wal.log");
            if (!legacy.exists() || targetSegNum != 1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads the next record type opcode, or null if end of log or zero-filled preallocated tail is reached.
     */
    public Integer readIntOrNull() throws IOException {
        if (!hasRemaining()) {
            return null;
        }
        byte[] b = new byte[4];
        int r = readPartialBytes(b, 0, 4);
        if (r < 4) {
            return null;
        }
        int val = ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
        if (val == 0) {
            // Unwritten zero-fill in pre-allocated segment
            currentLsn -= 4; // Rewind the 4 zero bytes
            return null;
        }
        return val;
    }

    public int readInt() throws IOException {
        return readExact(4).getInt();
    }

    public long readLong() throws IOException {
        return readExact(8).getLong();
    }

    public byte[] readBytes(int n) throws IOException {
        if (decryptedStream != null) {
            if (n <= 0) return new byte[0];
            if (decryptedPos + n > decryptedStream.length) {
                throw new IOException("Decrypted WAL ends prematurely mid-record");
            }
            byte[] data = java.util.Arrays.copyOfRange(decryptedStream, decryptedPos, decryptedPos + n);
            decryptedPos += n;
            return data;
        }
        return readRawBytes(n);
    }

    private byte[] readRawBytes(int n) throws IOException {
        if (n <= 0) {
            return new byte[0];
        }
        byte[] data = new byte[n];
        int read = 0;
        while (read < n) {
            openSegmentForLsn(currentLsn);
            if (currentRaf == null) {
                throw new IOException("WAL file ends prematurely mid-record at LSN " + currentLsn);
            }
            long segOffset = currentLsn % segmentSize;
            int availableInSegment = (int) Math.min(n - read, segmentSize - segOffset);
            currentRaf.seek(segOffset);
            int r = currentRaf.read(data, read, availableInSegment);
            if (r == -1) {
                throw new IOException("WAL truncated unexpectedly mid-record at LSN " + currentLsn);
            }
            read += r;
            currentLsn += r;
        }
        return data;
    }

    public String readLengthPrefixedString() throws IOException {
        int len = readInt();
        byte[] b = readBytes(len);
        return new String(b, StandardCharsets.UTF_8);
    }

    private ByteBuffer readExact(int n) throws IOException {
        byte[] b = readBytes(n);
        return ByteBuffer.wrap(b);
    }

    private int readPartialBytes(byte[] dest, int offset, int length) throws IOException {
        if (decryptedStream != null) {
            int avail = Math.min(length, decryptedStream.length - decryptedPos);
            if (avail <= 0) return 0;
            System.arraycopy(decryptedStream, decryptedPos, dest, offset, avail);
            decryptedPos += avail;
            return avail;
        }
        return readRawPartialBytes(dest, offset, length);
    }

    private int readRawPartialBytes(byte[] dest, int offset, int length) throws IOException {
        int totalRead = 0;
        while (totalRead < length) {
            openSegmentForLsn(currentLsn);
            if (currentRaf == null) {
                break;
            }
            long segOffset = currentLsn % segmentSize;
            long fileLength = currentRaf.length();
            if (segOffset >= fileLength) {
                break;
            }
            int availableInSeg = (int) Math.min(length - totalRead, segmentSize - segOffset);
            availableInSeg = (int) Math.min(availableInSeg, fileLength - segOffset);
            if (availableInSeg <= 0) {
                break;
            }
            currentRaf.seek(segOffset);
            int r = currentRaf.read(dest, offset + totalRead, availableInSeg);
            if (r == -1) {
                break;
            }
            totalRead += r;
            currentLsn += r;
        }
        return totalRead;
    }

    public long position() {
        return decryptedStream != null ? decryptedPos : currentLsn;
    }

    public void seek(long lsn) throws IOException {
        if (decryptedStream != null) {
            this.decryptedPos = (int) Math.max(0, Math.min(lsn, decryptedStream.length));
            return;
        }
        this.currentLsn = lsn;
        openSegmentForLsn(lsn);
    }

    @Override
    public void close() throws IOException {
        if (currentRaf != null) {
            try {
                currentRaf.close();
            } finally {
                currentRaf = null;
                currentOpenSegNum = -1L;
            }
        }
    }
}
