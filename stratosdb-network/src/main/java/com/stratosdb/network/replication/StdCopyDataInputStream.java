package com.stratosdb.network.replication;

import com.stratosdb.network.stdwire.StdWireMessages;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * An {@link InputStream} adapter that reads binary stream data delivered over
 * PostgreSQL wire protocol {@code CopyData} ('d') messages until {@code CopyDone} ('c').
 *
 * Used by {@link StdBaseBackupClient} to consume the tarball streamed by the primary
 * during a {@code BASE_BACKUP} operation.
 */
public class StdCopyDataInputStream extends InputStream {
    private final DataInputStream in;
    private byte[] currentChunk = null;
    private int chunkPos = 0;
    private boolean eof = false;

    public StdCopyDataInputStream(DataInputStream in) {
        this.in = in;
    }

    @Override
    public int read() throws IOException {
        byte[] single = new byte[1];
        int n = read(single, 0, 1);
        return n == -1 ? -1 : (single[0] & 0xFF);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) throw new NullPointerException();
        if (off < 0 || len < 0 || off + len > b.length) throw new IndexOutOfBoundsException();
        if (len == 0) return 0;
        if (eof) return -1;

        while (currentChunk == null || chunkPos >= currentChunk.length) {
            StdWireMessages.TypedMessage msg;
            try {
                msg = StdWireMessages.readTypedMessage(in);
            } catch (EOFException e) {
                eof = true;
                return -1;
            }

            if (msg.type() == 'd') { // CopyData
                currentChunk = msg.body();
                chunkPos = 0;
            } else if (msg.type() == 'c') { // CopyDone
                eof = true;
                currentChunk = null;
                return -1;
            } else if (msg.type() == 'f') { // CopyFail
                eof = true;
                throw new IOException("COPY aborted by remote side: " + new String(msg.body(), StandardCharsets.UTF_8));
            } else if (msg.type() == 'E') { // ErrorResponse
                eof = true;
                throw new IOException("Server error during replication stream: " + new String(msg.body(), StandardCharsets.UTF_8));
            } else {
                throw new IOException("Unexpected message type during COPY data stream: '" + msg.type() + "'");
            }
        }

        int available = currentChunk.length - chunkPos;
        int toRead = Math.min(len, available);
        System.arraycopy(currentChunk, chunkPos, b, off, toRead);
        chunkPos += toRead;
        return toRead;
    }

    @Override
    public void close() throws IOException {
        // Do not close the underlying socket DataInputStream, as further messages
        // (second result set, CommandComplete, ReadyForQuery) follow CopyDone.
        eof = true;
    }

    public boolean isEof() {
        return eof;
    }
}
