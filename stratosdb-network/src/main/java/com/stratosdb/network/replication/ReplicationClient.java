package com.stratosdb.network.replication;

import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.wal.StreamingWalApplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * The replica side of real physical (WAL-shipping) replication - connects
 * to a primary's {@link ReplicationServer}, receives its raw WAL byte
 * stream, and applies it to local storage via
 * {@link StreamingWalApplier}, so a client querying THIS instance
 * through the normal wire protocol sees the primary's own committed
 * writes appear, asynchronously, a real, running background process
 * doing genuine physical replication - not a simulation, and not
 * something a caller has to poll or drive by hand.
 *
 * A real, honestly-stated design choice: reconnection on a dropped
 * connection is automatic (a background thread keeps retrying), but
 * ALWAYS resumes from this client's own in-memory last-applied-offset -
 * there is no persistence of that offset across a REPLICA's OWN
 * restart. A replica that itself restarts starts replication over from
 * offset 0 again, re-streaming (and, per StreamingWalApplier's own
 * known limitation, re-applying, since neither side is LSN-gated /
 * idempotent yet) everything the primary has logged since ITS OWN last
 * restart. Safe and correct for the common case this was built and
 * tested against - a replica that stays up continuously behind a
 * primary that also stays up continuously - not yet a general-purpose,
 * production-grade standby that can be restarted independently of the
 * primary without data duplication.
 *
 * A real, separate, more serious hazard this class now protects
 * against instead of risking silently: this client's own
 * lastAppliedOffset is only meaningful relative to the primary's WAL
 * epoch it was recorded against (see WALManager.walEpoch's own
 * javadoc) - a primary CHECKPOINT truncates and renumbers its own
 * active WAL file, so the same numeric offset can mean something
 * completely different before and after one. Every (re)connection now
 * sends its own remembered epoch alongside its offset; if the primary
 * reports back a different epoch, this client sets needsResync and
 * stops retrying automatically, rather than silently applying
 * unrelated bytes at a stale offset - see needsResync's own javadoc.
 */
public class ReplicationClient {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationClient.class);
    private static final long RECONNECT_DELAY_MILLIS = 1000;

    private final String primaryHost;
    private final int primaryPort;
    private final StreamingWalApplier applier;
    private volatile boolean running = false;
    private Thread replicationThread;
    private volatile long lastAppliedOffset = 0;
    private volatile long lastAppliedEpoch = 0;
    /** Set once the primary reports a WAL epoch different from what this replica last recorded - see this class's own javadoc for the real corruption risk this prevents. Once true, the replication loop stops retrying automatically; a fresh ReplicationClient against a fresh base backup is required, not a silent, automatic recovery. */
    private volatile boolean needsResync = false;
    private volatile boolean connected = false;
    private volatile Socket activeSocket;

    public ReplicationClient(String primaryHost, int primaryPort, DiskManager diskManager, BufferPoolManager bufferPool) {
        this.primaryHost = primaryHost;
        this.primaryPort = primaryPort;
        this.applier = new StreamingWalApplier(diskManager, bufferPool);
    }

    public void start() {
        running = true;
        replicationThread = new Thread(this::runLoop, "replication-client");
        replicationThread.setDaemon(true);
        replicationThread.start();
        LOG.info("ReplicationClient started, connecting to primary at {}:{}", primaryHost, primaryPort);
    }

    /**
     * Stops replication. Closing activeSocket directly, not just
     * interrupting the thread, is the real fix here: a thread blocked in
     * a plain java.net.Socket's blocking read does not respond to
     * Thread.interrupt() the way NIO channels do - without this, stop()
     * would leave the replication thread stuck until the PRIMARY side
     * happened to close the connection first, and even then would log a
     * spurious "connection lost" warning for what was actually a normal,
     * intentional shutdown.
     */
    public void stop() {
        running = false;
        if (replicationThread != null) {
            replicationThread.interrupt();
        }
        if (activeSocket != null) {
            try {
                activeSocket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void runLoop() {
        while (running && !needsResync) {
            try {
                connectAndStream();
            } catch (IOException e) {
                if (running) {
                    LOG.warn("Replication connection to {}:{} lost or failed ({}); retrying in {}ms",
                        primaryHost, primaryPort, e.getMessage(), RECONNECT_DELAY_MILLIS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            connected = false;
            if (running && !needsResync) {
                try {
                    Thread.sleep(RECONNECT_DELAY_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        if (needsResync) {
            LOG.error("Replication to {}:{} stopped permanently: this replica's own WAL epoch is stale relative to the primary's current one. "
                + "A fresh base backup is required - see WALManager.walEpoch's own javadoc for why continuing would risk silent data corruption instead.",
                primaryHost, primaryPort);
        }
    }

    private void connectAndStream() throws IOException, InterruptedException {
        try (Socket socket = new Socket()) {
            activeSocket = socket;
            socket.connect(new InetSocketAddress(primaryHost, primaryPort), 5000);
            java.io.DataOutputStream out = new java.io.DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            out.writeLong(lastAppliedEpoch);
            out.writeLong(lastAppliedOffset);
            out.flush();
            long primaryEpoch = in.readLong();

            if (primaryEpoch != lastAppliedEpoch) {
                needsResync = true;
                LOG.error("Primary {}:{} reports WAL epoch {} but this replica last applied against epoch {} - "
                    + "this replica's own offset can no longer be trusted (see WALManager.walEpoch's own javadoc). "
                    + "Stopping replication; a fresh base backup is required.",
                    primaryHost, primaryPort, primaryEpoch, lastAppliedEpoch);
                return;
            }

            connected = true;
            LOG.info("Connected to primary {}:{}, resuming from epoch {} offset {}", primaryHost, primaryPort, primaryEpoch, lastAppliedOffset);

            while (running) {
                int chunkLength = in.readInt();
                byte[] chunk = new byte[chunkLength];
                in.readFully(chunk);
                applier.feed(chunk);
                lastAppliedOffset += chunkLength;
            }
        }
    }

    /** True once a WAL epoch mismatch has been detected - see this field's own javadoc. Once true, this ReplicationClient will never resume automatically; a fresh instance against a fresh base backup is required. */
    public boolean needsResync() {
        return needsResync;
    }

    public boolean isConnected() {
        return connected;
    }

    public long getLastAppliedOffset() {
        return lastAppliedOffset;
    }

    public StreamingWalApplier getApplier() {
        return applier;
    }
}
