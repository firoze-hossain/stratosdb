package com.stratosdb.network.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Maintains an active time-bounded leader lease with a Raft consensus cluster.
 * If the node is partitioned away and cannot renew before expiration, the lease
 * expires, triggering automatic self-fencing to prevent split-brain.
 */
public class StdRaftLeaseClient implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(StdRaftLeaseClient.class);

    private final List<StdRaftPeer> consensusPeers;
    private final String holderId;
    private final String leaseKey;
    private final long ttlMillis;
    private final long renewIntervalMillis;

    private volatile long currentExpiresAt = 0;
    private volatile String currentLeaderId = null;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean hadValidLease = new AtomicBoolean(false);
    private volatile java.util.function.Predicate<String> partitionFilter;

    private Runnable onLeaseLost;
    private Runnable onLeaseAcquired;

    private Thread renewThread;

    public StdRaftLeaseClient(
        List<StdRaftPeer> consensusPeers,
        String holderId,
        String leaseKey,
        long ttlMillis,
        long renewIntervalMillis
    ) {
        this.consensusPeers = new ArrayList<>(consensusPeers);
        this.holderId = holderId;
        this.leaseKey = leaseKey != null ? leaseKey : "primary";
        this.ttlMillis = ttlMillis;
        this.renewIntervalMillis = renewIntervalMillis;
    }

    public void setOnLeaseLost(Runnable callback) {
        this.onLeaseLost = callback;
    }

    public void setOnLeaseAcquired(Runnable callback) {
        this.onLeaseAcquired = callback;
    }

    public void setPartitionFilter(java.util.function.Predicate<String> filter) {
        this.partitionFilter = filter;
    }

    public boolean hasValidLease() {
        return running.get() && System.currentTimeMillis() < currentExpiresAt;
    }

    public long getCurrentExpiresAt() {
        return currentExpiresAt;
    }

    public synchronized void start() {
        if (running.get()) return;
        running.set(true);

        // Perform initial synchronous renewal
        renewOnce();

        renewThread = new Thread(this::renewLoop, "raft-lease-client-" + holderId);
        renewThread.setDaemon(true);
        renewThread.start();
        LOG.info("StdRaftLeaseClient started for {} (key={}, ttl={}ms, renewEvery={}ms)",
            holderId, leaseKey, ttlMillis, renewIntervalMillis);
    }

    public synchronized void stop() {
        if (!running.getAndSet(false)) return;
        if (renewThread != null) {
            renewThread.interrupt();
        }
        currentExpiresAt = 0;
        if (hadValidLease.getAndSet(false) && onLeaseLost != null) {
            try {
                onLeaseLost.run();
            } catch (Exception ignored) {}
        }
    }

    @Override
    public void close() {
        stop();
    }

    private void renewLoop() {
        while (running.get()) {
            try {
                Thread.sleep(renewIntervalMillis);
                renewOnce();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.debug("Error in lease renewal loop: {}", e.getMessage());
            }
        }
    }

    public synchronized boolean renewOnce() {
        boolean renewed = tryAcquireOrRenew();
        long now = System.currentTimeMillis();

        if (renewed) {
            if (!hadValidLease.getAndSet(true)) {
                LOG.info("Consensus lease '{}' acquired for {} until {}", leaseKey, holderId, currentExpiresAt);
                if (onLeaseAcquired != null) {
                    try {
                        onLeaseAcquired.run();
                    } catch (Exception e) {
                        LOG.error("Error in onLeaseAcquired callback", e);
                    }
                }
            }
            return true;
        } else {
            if (now >= currentExpiresAt && hadValidLease.getAndSet(false)) {
                LOG.warn("Consensus lease '{}' EXPIRED for {} at {} - self-fencing activated", leaseKey, holderId, currentExpiresAt);
                if (onLeaseLost != null) {
                    try {
                        onLeaseLost.run();
                    } catch (Exception e) {
                        LOG.error("Error in onLeaseLost callback", e);
                    }
                }
            }
            return false;
        }
    }

    private boolean tryAcquireOrRenew() {
        if (currentLeaderId != null) {
            StdRaftPeer leaderPeer = findPeer(currentLeaderId);
            if (leaderPeer != null && attemptPeer(leaderPeer)) {
                return true;
            }
        }

        for (StdRaftPeer peer : consensusPeers) {
            if (attemptPeer(peer)) {
                return true;
            }
            if (currentLeaderId != null) {
                StdRaftPeer leaderPeer = findPeer(currentLeaderId);
                if (leaderPeer != null && !leaderPeer.id().equals(peer.id())) {
                    if (attemptPeer(leaderPeer)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean attemptPeer(StdRaftPeer peer) {
        if (partitionFilter != null && !partitionFilter.test(peer.id())) {
            return false;
        }
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(peer.host(), peer.port()), 250);
            socket.setSoTimeout(400);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            StdRaftMessages.LeaseAcquire req = new StdRaftMessages.LeaseAcquire(holderId, leaseKey, ttlMillis);
            StdRaftMessages.writePacket(out, StdRaftMessages.MSG_LEASE_ACQUIRE, StdRaftMessages.encodeLeaseAcquire(req));

            StdRaftMessages.RaftPacket pkt = StdRaftMessages.readPacket(in);
            if (pkt.type() == StdRaftMessages.MSG_LEASE_ACQUIRE_RESP) {
                StdRaftMessages.LeaseAcquireResponse resp = StdRaftMessages.decodeLeaseAcquireResponse(pkt.payload());
                if (resp.leaderId() != null && !resp.leaderId().isEmpty()) {
                    currentLeaderId = resp.leaderId();
                }
                if (resp.granted()) {
                    currentExpiresAt = resp.expiresAt();
                    return true;
                }
            }
        } catch (Exception ignored) {}
        return false;
    }

    private StdRaftPeer findPeer(String id) {
        for (StdRaftPeer p : consensusPeers) {
            if (p.id().equals(id)) return p;
        }
        return null;
    }
}
