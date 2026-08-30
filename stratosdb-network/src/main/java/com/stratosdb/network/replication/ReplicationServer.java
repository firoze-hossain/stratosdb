package com.stratosdb.network.replication;

import com.stratosdb.storage.wal.WALManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The primary side of real physical (WAL-shipping) replication. Accepts
 * replica connections and streams raw WAL bytes to each one - literally
 * the same bytes {@link WALManager}'s own logInsert/logUpdate/logDelete/
 * logCommit already write to disk, read back via
 * {@link WALManager#readBytesFromChecked}, not a separate, invented
 * replication wire format. A connected replica parses and applies those
 * exact bytes itself via {@link com.stratosdb.storage.wal.StreamingWalApplier}.
 *
 * Wire protocol, deliberately minimal:
 *   Replica sends: 8 bytes (the epoch it last applied against - 0 for a
 *   fresh replica with nothing applied yet), then 8 bytes (a signed
 *   long, the byte offset to start streaming from within that epoch).
 *   Primary sends, once, immediately: 8 bytes, its own current epoch. If
 *   this differs from what the replica sent, the replica's own offset
 *   can no longer be trusted at all (see WALManager.walEpoch's own
 *   javadoc for the real, serious bug this prevents) - the replica must
 *   treat this as fatal and require a fresh base backup, not retry with
 *   the same stale offset. If the epoch matches, the primary then sends,
 *   repeatedly, for as long as the connection stays open: 4 bytes (a
 *   signed int, the chunk length) followed by exactly that many bytes of
 *   raw WAL data. A primary with nothing new to send simply doesn't send
 *   a chunk yet - see pollIntervalMillis - rather than sending an empty,
 *   zero-length one; there is no separate heartbeat/keepalive message.
 *   If the epoch changes mid-stream (a CHECKPOINT happens while this
 *   replica is actively connected), the primary closes the connection
 *   rather than silently continuing - the replica's own reconnect logic
 *   will send its (now stale) epoch again and correctly be told to
 *   resync.
 *
 * Real, honestly-stated limitations: polling-based (checks for new WAL
 * bytes on an interval, not woken immediately when a write happens) -
 * replication lag is therefore bounded below by pollIntervalMillis, not
 * pushed the instant a commit happens; asynchronous only (a commit on
 * the primary never waits for any replica to acknowledge receipt, let
 * alone apply, before returning success - real Postgres's own separate
 * synchronous replication mode is not attempted here); no replication
 * slots (a replica that disconnects and later reconnects must supply
 * its own last-applied offset itself - this server has no persistent
 * memory of which replicas exist or where each one left off); multiple
 * concurrently connected replicas are each served independently and
 * correctly, but there's no cross-replica coordination of any kind
 * (no quorum, no read consistency guarantee across two different
 * replicas at the same moment). An epoch mismatch (see above) requires
 * a fresh base backup to recover from - there is no automatic resync
 * from archived WAL segments (see WALManager.setWalArchiveDirectory)
 * yet, a real, separate, further piece of work.
 */
public class ReplicationServer {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationServer.class);
    private static final long DEFAULT_POLL_INTERVAL_MILLIS = 100;

    private final int port;
    private final WALManager walManager;
    private final long pollIntervalMillis;
    private volatile boolean running = false;
    private ServerSocket serverSocket;
    private ExecutorService connectionExecutor;

    public ReplicationServer(int port, WALManager walManager) {
        this(port, walManager, DEFAULT_POLL_INTERVAL_MILLIS);
    }

    public ReplicationServer(int port, WALManager walManager, long pollIntervalMillis) {
        this.port = port;
        this.walManager = walManager;
        this.pollIntervalMillis = pollIntervalMillis;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        connectionExecutor = Executors.newVirtualThreadPerTaskExecutor();
        LOG.info("ReplicationServer listening on port {}", port);

        Thread acceptThread = new Thread(() -> {
            while (running) {
                try {
                    Socket replica = serverSocket.accept();
                    connectionExecutor.submit(() -> handleReplica(replica));
                } catch (IOException e) {
                    if (running) LOG.error("Replication accept failed", e);
                }
            }
        }, "replication-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {
        }
        if (connectionExecutor != null) connectionExecutor.shutdownNow();
    }

    private void handleReplica(Socket socket) {
        String remote = socket.getRemoteSocketAddress().toString();
        LOG.info("Replica connected: {}", remote);
        try (Socket s = socket;
             DataInputStream in = new DataInputStream(s.getInputStream());
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()))) {

            long replicaEpoch = in.readLong();
            long fromOffset = in.readLong();
            long primaryEpoch = walManager.getWalEpoch();
            out.writeLong(primaryEpoch);
            out.flush();

            if (replicaEpoch != primaryEpoch) {
                LOG.warn("Replica {} requested epoch {} but primary is now at epoch {} - refusing to stream from a stale offset; the replica must resync from a fresh base backup",
                    remote, replicaEpoch, primaryEpoch);
                return; // closes the connection - the replica's own reconnect logic sees this and must not retry with the same offset
            }

            long sentUpTo = fromOffset;
            LOG.info("Replica {} starting replication from epoch {} offset {}", remote, primaryEpoch, fromOffset);

            while (running && !socket.isClosed()) {
                byte[] newBytes = walManager.readBytesFromChecked(primaryEpoch, sentUpTo);
                if (newBytes == null) {
                    // The primary's own epoch advanced (a CHECKPOINT happened) while this
                    // replica was actively streaming - continuing would silently hand it
                    // bytes from an unrelated, post-truncation file at the same numeric
                    // offset. Close cleanly; the replica's own reconnect will send its
                    // now-stale epoch again and correctly be told to resync above.
                    LOG.warn("Primary's WAL epoch advanced while replica {} was streaming - closing so it resyncs", remote);
                    return;
                }
                if (newBytes.length > 0) {
                    out.writeInt(newBytes.length);
                    out.write(newBytes);
                    out.flush();
                    sentUpTo += newBytes.length;
                } else {
                    Thread.sleep(pollIntervalMillis);
                }
            }
        } catch (IOException e) {
            LOG.info("Replica {} disconnected: {}", remote, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
