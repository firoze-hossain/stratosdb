package com.stratosdb.network.replication;

import com.stratosdb.network.stdwire.StdWireMessages;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * An {@link OutputStream} adapter that chunks output data into PostgreSQL wire protocol
 * {@code CopyData} ('d') messages.
 *
 * Used by {@link com.stratosdb.network.stdwire.StdWireServer} during {@code BASE_BACKUP}
 * to stream tar archive contents over the wire with configurable buffer sizing and optional
 * bandwidth rate limiting (e.g. {@code MAX_RATE} parameter).
 */
public class StdCopyDataOutputStream extends OutputStream {
    public static final int DEFAULT_BUFFER_SIZE = 32768; // 32 KB chunk size

    private final DataOutputStream out;
    private final byte[] buffer;
    private int count = 0;
    private final long maxRateBytesPerSec; // 0 = unlimited
    private long bytesSentInWindow = 0;
    private long windowStartTime = System.currentTimeMillis();

    public StdCopyDataOutputStream(DataOutputStream out) {
        this(out, DEFAULT_BUFFER_SIZE, 0);
    }

    public StdCopyDataOutputStream(DataOutputStream out, int bufferSize, long maxRateBytesPerSec) {
        this.out = out;
        this.buffer = new byte[Math.max(512, bufferSize)];
        this.maxRateBytesPerSec = maxRateBytesPerSec;
        this.windowStartTime = System.currentTimeMillis();
    }

    @Override
    public synchronized void write(int b) throws IOException {
        buffer[count++] = (byte) b;
        if (count == buffer.length) {
            flushBuffer();
        }
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (b == null) throw new NullPointerException();
        if (off < 0 || len < 0 || off + len > b.length) throw new IndexOutOfBoundsException();

        int remaining = len;
        int currentOff = off;
        while (remaining > 0) {
            int space = buffer.length - count;
            if (remaining < space) {
                System.arraycopy(b, currentOff, buffer, count, remaining);
                count += remaining;
                break;
            } else {
                System.arraycopy(b, currentOff, buffer, count, space);
                count += space;
                currentOff += space;
                remaining -= space;
                flushBuffer();
            }
        }
    }

    private void flushBuffer() throws IOException {
        if (count > 0) {
            throttleIfNeeded(count);
            StdWireMessages.writeCopyData(out, buffer, 0, count);
            out.flush();
            count = 0;
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        flushBuffer();
        out.flush();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private void throttleIfNeeded(int bytes) {
        if (maxRateBytesPerSec <= 0) return;
        bytesSentInWindow += bytes;
        long elapsed = System.currentTimeMillis() - windowStartTime;
        if (elapsed >= 1000) {
            bytesSentInWindow = bytes;
            windowStartTime = System.currentTimeMillis();
            return;
        }
        if (bytesSentInWindow > maxRateBytesPerSec) {
            long sleepTime = 1000 - elapsed;
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            bytesSentInWindow = 0;
            windowStartTime = System.currentTimeMillis();
        }
    }
}
