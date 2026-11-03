package com.stratosdb.network.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * An individual Raft consensus node implementing leader election, log replication,
 * quorum heartbeats, and time-bounded distributed leader leases for split-brain prevention.
 */
public class StdRaftNode implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(StdRaftNode.class);

    private final String nodeId;
    private final String host;
    private final int port;
    private final Map<String, StdRaftPeer> peers = new ConcurrentHashMap<>();

    private final long minElectionTimeoutMillis;
    private final long maxElectionTimeoutMillis;
    private final long heartbeatIntervalMillis;

    private volatile StdRaftRole role = StdRaftRole.FOLLOWER;
    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicReference<String> votedFor = new AtomicReference<>(null);
    private volatile String leaderId = null;

    private volatile long lastHeartbeatTime = 0;
    private volatile long lastQuorumAckTime = 0;

    private final List<StdRaftLogEntry> log = new CopyOnWriteArrayList<>();
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;

    // Leader lease state
    private volatile String leaseKey = null;
    private volatile String leaseHolder = null;
    private volatile long leaseExpiresAt = 0;
    private volatile long leaseGrantedTerm = 0;

    // Network partition simulation filter: if non-null, only peers matching this predicate can be reached/accepted
    private volatile Predicate<String> partitionFilter = null;

    private ServerSocket serverSocket;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread listenerThread;
    private Thread timerThread;
    private final ExecutorService rpcExecutor = Executors.newCachedThreadPool();

    public StdRaftNode(String nodeId, String host, int port, List<StdRaftPeer> peerList) {
        this(nodeId, host, port, peerList, 250, 450, 75);
    }

    public StdRaftNode(
        String nodeId,
        String host,
        int port,
        List<StdRaftPeer> peerList,
        long minElectionTimeoutMillis,
        long maxElectionTimeoutMillis,
        long heartbeatIntervalMillis
    ) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        if (peerList != null) {
            for (StdRaftPeer p : peerList) {
                if (!p.id().equals(nodeId)) {
                    this.peers.put(p.id(), p);
                }
            }
        }
        this.minElectionTimeoutMillis = minElectionTimeoutMillis;
        this.maxElectionTimeoutMillis = maxElectionTimeoutMillis;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.lastHeartbeatTime = System.currentTimeMillis();
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getPort() {
        return port;
    }

    public StdRaftRole getRole() {
        return role;
    }

    public boolean isLeader() {
        if (role != StdRaftRole.LEADER) return false;
        if (!peers.isEmpty() && (System.currentTimeMillis() - lastQuorumAckTime > maxElectionTimeoutMillis)) {
            return false;
        }
        return true;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public long getCurrentTerm() {
        return currentTerm.get();
    }

    public String getLeaseHolder() {
        return leaseHolder;
    }

    public long getLeaseExpiresAt() {
        return leaseExpiresAt;
    }

    public boolean isLeaseActive(String key) {
        long now = System.currentTimeMillis();
        return now < leaseExpiresAt && leaseHolder != null && (key == null || key.equals(leaseKey));
    }

    public void setPartitionFilter(Predicate<String> filter) {
        this.partitionFilter = filter;
    }

    public Predicate<String> getPartitionFilter() {
        return partitionFilter;
    }

    public boolean canCommunicateWith(String peerId) {
        Predicate<String> filter = this.partitionFilter;
        return filter == null || filter.test(peerId);
    }

    public int getQuorumSize() {
        return ((peers.size() + 1) / 2) + 1;
    }

    public synchronized void start() throws IOException {
        if (running.get()) return;
        running.set(true);

        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(host, port));

        listenerThread = new Thread(this::listenLoop, "raft-listener-" + nodeId);
        listenerThread.setDaemon(true);
        listenerThread.start();

        timerThread = new Thread(this::timerLoop, "raft-timer-" + nodeId);
        timerThread.setDaemon(true);
        timerThread.start();

        LOG.info("StdRaftNode {} started on {}:{} with {} peer(s), quorum={}", nodeId, host, port, peers.size(), getQuorumSize());
    }

    public synchronized void stop() {
        if (!running.getAndSet(false)) return;
        role = StdRaftRole.FOLLOWER;
        leaderId = null;

        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {}
        }

        if (listenerThread != null) {
            listenerThread.interrupt();
        }
        if (timerThread != null) {
            timerThread.interrupt();
        }
        rpcExecutor.shutdownNow();
        LOG.info("StdRaftNode {} stopped", nodeId);
    }

    @Override
    public void close() {
        stop();
    }

    private void listenLoop() {
        while (running.get()) {
            try {
                Socket socket = serverSocket.accept();
                if (!running.get()) {
                    try { socket.close(); } catch (Exception ignored) {}
                    break;
                }
                socket.setSoTimeout(1500);
                try {
                    rpcExecutor.submit(() -> handleIncomingConnection(socket));
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    try { socket.close(); } catch (Exception ignored) {}
                    if (!running.get()) break;
                }
            } catch (IOException e) {
                if (!running.get()) break;
                LOG.debug("Raft listener accept failed on node {}: {}", nodeId, e.getMessage());
            }
        }
    }

    private void handleIncomingConnection(Socket socket) {
        try (socket;
             InputStream in = socket.getInputStream();
             OutputStream out = socket.getOutputStream()) {

            StdRaftMessages.RaftPacket packet = StdRaftMessages.readPacket(in);
            switch (packet.type()) {
                case StdRaftMessages.MSG_REQUEST_VOTE -> {
                    StdRaftMessages.RequestVote req = StdRaftMessages.decodeRequestVote(packet.payload());
                    if (!canCommunicateWith(req.candidateId())) {
                        socket.close();
                        return;
                    }
                    StdRaftMessages.RequestVoteResponse resp = handleRequestVote(req);
                    StdRaftMessages.writePacket(out, StdRaftMessages.MSG_REQUEST_VOTE_RESP, StdRaftMessages.encodeRequestVoteResponse(resp));
                }
                case StdRaftMessages.MSG_APPEND_ENTRIES -> {
                    StdRaftMessages.AppendEntries req = StdRaftMessages.decodeAppendEntries(packet.payload());
                    if (!canCommunicateWith(req.leaderId())) {
                        socket.close();
                        return;
                    }
                    StdRaftMessages.AppendEntriesResponse resp = handleAppendEntries(req);
                    StdRaftMessages.writePacket(out, StdRaftMessages.MSG_APPEND_ENTRIES_RESP, StdRaftMessages.encodeAppendEntriesResponse(resp));
                }
                case StdRaftMessages.MSG_LEASE_ACQUIRE -> {
                    StdRaftMessages.LeaseAcquire req = StdRaftMessages.decodeLeaseAcquire(packet.payload());
                    StdRaftMessages.LeaseAcquireResponse resp = handleLeaseAcquire(req.holderId(), req.leaseKey(), req.ttlMillis());
                    StdRaftMessages.writePacket(out, StdRaftMessages.MSG_LEASE_ACQUIRE_RESP, StdRaftMessages.encodeLeaseAcquireResponse(resp));
                }
                case StdRaftMessages.MSG_LEASE_QUERY -> {
                    StdRaftMessages.LeaseQuery req = StdRaftMessages.decodeLeaseQuery(packet.payload());
                    StdRaftMessages.LeaseQueryResponse resp = handleLeaseQuery(req.leaseKey());
                    StdRaftMessages.writePacket(out, StdRaftMessages.MSG_LEASE_QUERY_RESP, StdRaftMessages.encodeLeaseQueryResponse(resp));
                }
                default -> LOG.warn("Unknown Raft packet type: {}", packet.type());
            }
        } catch (Exception e) {
            LOG.debug("Error handling Raft connection on {}: {}", nodeId, e.getMessage());
        }
    }

    private synchronized StdRaftMessages.RequestVoteResponse handleRequestVote(StdRaftMessages.RequestVote req) {
        long myTerm = currentTerm.get();
        if (req.term() > myTerm) {
            currentTerm.set(req.term());
            role = StdRaftRole.FOLLOWER;
            votedFor.set(null);
            leaderId = null;
        }

        boolean canVote = (req.term() == currentTerm.get()) &&
            (votedFor.get() == null || votedFor.get().equals(req.candidateId()));

        if (canVote) {
            votedFor.set(req.candidateId());
            lastHeartbeatTime = System.currentTimeMillis();
            LOG.debug("Node {} voted for {} in term {}", nodeId, req.candidateId(), req.term());
            return new StdRaftMessages.RequestVoteResponse(nodeId, currentTerm.get(), true);
        } else {
            return new StdRaftMessages.RequestVoteResponse(nodeId, currentTerm.get(), false);
        }
    }

    private synchronized StdRaftMessages.AppendEntriesResponse handleAppendEntries(StdRaftMessages.AppendEntries req) {
        long myTerm = currentTerm.get();
        if (req.term() < myTerm) {
            return new StdRaftMessages.AppendEntriesResponse(nodeId, myTerm, false, 0);
        }

        if (req.term() > myTerm) {
            currentTerm.set(req.term());
            votedFor.set(null);
        }

        role = StdRaftRole.FOLLOWER;
        leaderId = req.leaderId();
        lastHeartbeatTime = System.currentTimeMillis();

        if (req.leaseExpiresAt() > 0) {
            this.leaseHolder = req.leaseHolder();
            this.leaseExpiresAt = req.leaseExpiresAt();
        }

        return new StdRaftMessages.AppendEntriesResponse(nodeId, currentTerm.get(), true, 0);
    }

    public synchronized StdRaftMessages.LeaseAcquireResponse handleLeaseAcquire(String holderId, String key, long ttlMillis) {
        if (role != StdRaftRole.LEADER) {
            return new StdRaftMessages.LeaseAcquireResponse(false, currentTerm.get(), leaseExpiresAt, leaderId);
        }

        long now = System.currentTimeMillis();
        // Quorum liveness check: leader must have confirmed quorum recently
        if (peers.size() > 0 && (now - lastQuorumAckTime > maxElectionTimeoutMillis * 2)) {
            LOG.warn("Leader {} cannot grant lease: quorum heartbeat lost (last ack {}ms ago)", nodeId, now - lastQuorumAckTime);
            return new StdRaftMessages.LeaseAcquireResponse(false, currentTerm.get(), leaseExpiresAt, null);
        }

        // Check if an existing conflicting lease has not yet expired
        if (now < leaseExpiresAt && leaseHolder != null && !leaseHolder.equals(holderId)) {
            LOG.warn("Leader {} cannot grant lease to {}: active lease held by {} until {}", nodeId, holderId, leaseHolder, leaseExpiresAt);
            return new StdRaftMessages.LeaseAcquireResponse(false, currentTerm.get(), leaseExpiresAt, nodeId);
        }

        this.leaseKey = key;
        this.leaseHolder = holderId;
        this.leaseExpiresAt = now + ttlMillis;
        this.leaseGrantedTerm = currentTerm.get();

        LOG.debug("Leader {} granted/renewed lease '{}' to {} for {}ms (expires at {})", nodeId, key, holderId, ttlMillis, leaseExpiresAt);
        // Fast-replicate lease update to peers
        broadcastHeartbeatAsync();
        return new StdRaftMessages.LeaseAcquireResponse(true, currentTerm.get(), this.leaseExpiresAt, nodeId);
    }

    public synchronized StdRaftMessages.LeaseQueryResponse handleLeaseQuery(String key) {
        long now = System.currentTimeMillis();
        boolean active = (now < leaseExpiresAt) && leaseHolder != null && (key == null || key.equals(leaseKey));
        return new StdRaftMessages.LeaseQueryResponse(active, leaseHolder, leaseExpiresAt, leaderId);
    }

    private void timerLoop() {
        long nextTimeout = pickElectionTimeout();
        while (running.get()) {
            try {
                if (role == StdRaftRole.LEADER) {
                    if (!peers.isEmpty() && (System.currentTimeMillis() - lastQuorumAckTime > maxElectionTimeoutMillis)) {
                        synchronized (this) {
                            if (role == StdRaftRole.LEADER) {
                                LOG.warn("Node {} stepping down from LEADER to FOLLOWER due to quorum loss", nodeId);
                                role = StdRaftRole.FOLLOWER;
                                leaderId = null;
                            }
                        }
                    } else {
                        sendHeartbeats();
                    }
                    Thread.sleep(heartbeatIntervalMillis);
                } else {
                    long elapsed = System.currentTimeMillis() - lastHeartbeatTime;
                    if (elapsed >= nextTimeout) {
                        startElection();
                        nextTimeout = pickElectionTimeout();
                    }
                    Thread.sleep(20);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.error("Error in Raft timer loop for node {}: {}", nodeId, e.getMessage());
            }
        }
    }

    private long pickElectionTimeout() {
        return minElectionTimeoutMillis + ThreadLocalRandom.current().nextLong(maxElectionTimeoutMillis - minElectionTimeoutMillis);
    }

    private synchronized void startElection() {
        if (!running.get()) return;

        role = StdRaftRole.CANDIDATE;
        long term = currentTerm.incrementAndGet();
        votedFor.set(nodeId);
        leaderId = null;
        lastHeartbeatTime = System.currentTimeMillis();

        LOG.info("Node {} starting election for term {}, quorum={}", nodeId, term, getQuorumSize());

        if (peers.isEmpty()) {
            becomeLeader(term);
            return;
        }

        AtomicInteger votesGranted = new AtomicInteger(1); // vote for self
        List<StdRaftPeer> eligiblePeers = new ArrayList<>();
        for (StdRaftPeer p : peers.values()) {
            if (canCommunicateWith(p.id())) {
                eligiblePeers.add(p);
            }
        }

        for (StdRaftPeer peer : eligiblePeers) {
            rpcExecutor.submit(() -> {
                try {
                    StdRaftMessages.RequestVote req = new StdRaftMessages.RequestVote(nodeId, term, log.size(), 0);
                    StdRaftMessages.RequestVoteResponse resp = sendRequestVoteRpc(peer, req);
                    if (resp != null) {
                        synchronized (this) {
                            if (resp.term() > currentTerm.get()) {
                                currentTerm.set(resp.term());
                                role = StdRaftRole.FOLLOWER;
                                votedFor.set(null);
                                return;
                            }
                            if (role == StdRaftRole.CANDIDATE && resp.term() == term && resp.voteGranted()) {
                                int count = votesGranted.incrementAndGet();
                                if (count >= getQuorumSize() && role == StdRaftRole.CANDIDATE) {
                                    becomeLeader(term);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.debug("RequestVote to {} failed: {}", peer.id(), e.getMessage());
                }
            });
        }
    }

    private synchronized void becomeLeader(long term) {
        if (role != StdRaftRole.CANDIDATE || currentTerm.get() != term) return;
        role = StdRaftRole.LEADER;
        leaderId = nodeId;
        lastQuorumAckTime = System.currentTimeMillis();
        LOG.info("Node {} elected LEADER for term {}", nodeId, term);
        broadcastHeartbeatAsync();
    }

    private void broadcastHeartbeatAsync() {
        rpcExecutor.submit(this::sendHeartbeats);
    }

    private void sendHeartbeats() {
        if (role != StdRaftRole.LEADER || !running.get()) return;

        long term = currentTerm.get();
        String currentLeaseHolder = this.leaseHolder;
        long currentLeaseExpiry = this.leaseExpiresAt;

        AtomicInteger acks = new AtomicInteger(1); // self
        List<StdRaftPeer> targetPeers = new ArrayList<>();
        for (StdRaftPeer p : peers.values()) {
            if (canCommunicateWith(p.id())) {
                targetPeers.add(p);
            }
        }

        for (StdRaftPeer peer : targetPeers) {
            rpcExecutor.submit(() -> {
                try {
                    StdRaftMessages.AppendEntries req = new StdRaftMessages.AppendEntries(
                        nodeId,
                        term,
                        log.size(),
                        0,
                        commitIndex,
                        Collections.emptyList(),
                        currentLeaseHolder,
                        currentLeaseExpiry
                    );
                    StdRaftMessages.AppendEntriesResponse resp = sendAppendEntriesRpc(peer, req);
                    if (resp != null) {
                        synchronized (this) {
                            if (resp.term() > currentTerm.get()) {
                                currentTerm.set(resp.term());
                                role = StdRaftRole.FOLLOWER;
                                leaderId = null;
                                votedFor.set(null);
                                return;
                            }
                            if (resp.success() && role == StdRaftRole.LEADER) {
                                if (acks.incrementAndGet() >= getQuorumSize()) {
                                    lastQuorumAckTime = System.currentTimeMillis();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.debug("AppendEntries to {} failed: {}", peer.id(), e.getMessage());
                }
            });
        }
    }

    /**
     * Checks whether failover promotion can be safely executed for the given primary node.
     * Prevents split-brain by strictly guaranteeing:
     * 1. This watchdog is the authoritative elected Raft leader backed by quorum.
     * 2. The old primary's lease has definitively expired (or old primary is already down).
     */
    public boolean canPromote(String oldPrimaryId, long maxWaitMillis) {
        if (role != StdRaftRole.LEADER) {
            LOG.warn("Cannot promote: node {} is not the Raft leader (role={})", nodeId, role);
            return false;
        }

        long now = System.currentTimeMillis();
        // Quorum health check
        if (peers.size() > 0 && (now - lastQuorumAckTime > maxElectionTimeoutMillis * 2)) {
            LOG.warn("Cannot promote: Raft leader {} lost quorum connectivity", nodeId);
            return false;
        }

        // Check old primary lease expiry - ensure ANY active primary lease has expired before promoting
        if (leaseHolder != null) {
            long remainingLease = leaseExpiresAt - now;
            if (remainingLease > 0) {
                if (remainingLease > maxWaitMillis) {
                    LOG.warn("Cannot promote: active lease held by '{}' remains valid for {}ms (> maxWait {}ms)", leaseHolder, remainingLease, maxWaitMillis);
                    return false;
                }
                LOG.info("Waiting {}ms for active lease held by '{}' to expire before safe promotion...", remainingLease + 50, leaseHolder);
                try {
                    Thread.sleep(remainingLease + 50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return true;
    }

    // --- RPC Client helpers ---

    private StdRaftMessages.RequestVoteResponse sendRequestVoteRpc(StdRaftPeer peer, StdRaftMessages.RequestVote req) {
        if (!canCommunicateWith(peer.id())) return null;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(peer.host(), peer.port()), 200);
            s.setSoTimeout(250);
            OutputStream out = s.getOutputStream();
            InputStream in = s.getInputStream();

            StdRaftMessages.writePacket(out, StdRaftMessages.MSG_REQUEST_VOTE, StdRaftMessages.encodeRequestVote(req));
            StdRaftMessages.RaftPacket pkt = StdRaftMessages.readPacket(in);
            if (pkt.type() == StdRaftMessages.MSG_REQUEST_VOTE_RESP) {
                return StdRaftMessages.decodeRequestVoteResponse(pkt.payload());
            }
        } catch (Exception ignored) {}
        return null;
    }

    private StdRaftMessages.AppendEntriesResponse sendAppendEntriesRpc(StdRaftPeer peer, StdRaftMessages.AppendEntries req) {
        if (!canCommunicateWith(peer.id())) return null;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(peer.host(), peer.port()), 200);
            s.setSoTimeout(250);
            OutputStream out = s.getOutputStream();
            InputStream in = s.getInputStream();

            StdRaftMessages.writePacket(out, StdRaftMessages.MSG_APPEND_ENTRIES, StdRaftMessages.encodeAppendEntries(req));
            StdRaftMessages.RaftPacket pkt = StdRaftMessages.readPacket(in);
            if (pkt.type() == StdRaftMessages.MSG_APPEND_ENTRIES_RESP) {
                return StdRaftMessages.decodeAppendEntriesResponse(pkt.payload());
            }
        } catch (Exception ignored) {}
        return null;
    }
}
