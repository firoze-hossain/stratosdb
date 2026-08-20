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
 * {@link WALManager#readBytesFrom}, not a separate, invented replication
 * wire format. A connected replica parses and applies those exact bytes
 * itself via {@link com.stratosdb.storage.wal.StreamingWalApplier}.
 *
 * Wire protocol, deliberately minimal:
 *   Replica sends: 8 bytes, a signed long - the byte offset to start
 *   streaming from (0 for a fresh replica with nothing applied yet).
 *   Primary sends, repeatedly, for as long as the connection stays open:
 *   4 bytes (a signed int, the chunk length) followed by exactly that
 *   many bytes of raw WAL data. A primary with nothing new to send
 *   simply doesn't send a chunk yet - see pollIntervalMillis - rather
 *   than sending an empty, zero-length one; there is no separate
 *   heartbeat/keepalive message.
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
 * replicas at the same moment).
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

            long fromOffset = in.readLong();
            long sentUpTo = fromOffset;
            LOG.info("Replica {} starting replication from offset {}", remote, fromOffset);

            while (running && !socket.isClosed()) {
                byte[] newBytes = walManager.readBytesFrom(sentUpTo);
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
