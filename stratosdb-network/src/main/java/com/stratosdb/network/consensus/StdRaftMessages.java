package com.stratosdb.network.consensus;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Binary wire framing and serialization for the Raft consensus RPCs and distributed leases.
 */
public final class StdRaftMessages {

    public static final int MAGIC = 0x53545241; // "STRA" in ASCII

    public static final byte MSG_REQUEST_VOTE = 1;
    public static final byte MSG_REQUEST_VOTE_RESP = 2;
    public static final byte MSG_APPEND_ENTRIES = 3;
    public static final byte MSG_APPEND_ENTRIES_RESP = 4;
    public static final byte MSG_LEASE_ACQUIRE = 5;
    public static final byte MSG_LEASE_ACQUIRE_RESP = 6;
    public static final byte MSG_LEASE_QUERY = 7;
    public static final byte MSG_LEASE_QUERY_RESP = 8;

    private StdRaftMessages() {}

    public record RequestVote(String candidateId, long term, long lastLogIndex, long lastLogTerm) {}
    public record RequestVoteResponse(String fromNodeId, long term, boolean voteGranted) {}

    public record AppendEntries(
        String leaderId,
        long term,
        long prevLogIndex,
        long prevLogTerm,
        long leaderCommit,
        List<StdRaftLogEntry> entries,
        String leaseHolder,
        long leaseExpiresAt
    ) {}

    public record AppendEntriesResponse(String fromNodeId, long term, boolean success, long matchIndex) {}

    public record LeaseAcquire(String holderId, String leaseKey, long ttlMillis) {}
    public record LeaseAcquireResponse(boolean granted, long term, long expiresAt, String leaderId) {}

    public record LeaseQuery(String leaseKey) {}
    public record LeaseQueryResponse(boolean active, String holderId, long expiresAt, String leaderId) {}

    public record RaftPacket(byte type, byte[] payload) {}

    public static void writePacket(OutputStream out, byte type, byte[] payload) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(MAGIC);
        dos.writeByte(type);
        dos.writeInt(payload != null ? payload.length : 0);
        if (payload != null && payload.length > 0) {
            dos.write(payload);
        }
        dos.flush();
    }

    public static RaftPacket readPacket(InputStream in) throws IOException {
        DataInputStream dis = new DataInputStream(in);
        int magic = dis.readInt();
        if (magic != MAGIC) {
            throw new IOException("Invalid Raft packet magic: 0x" + Integer.toHexString(magic));
        }
        byte type = dis.readByte();
        int length = dis.readInt();
        if (length < 0 || length > 16 * 1024 * 1024) {
            throw new IOException("Invalid Raft packet payload length: " + length);
        }
        byte[] payload = new byte[length];
        dis.readFully(payload);
        return new RaftPacket(type, payload);
    }

    // --- Serializers ---

    public static byte[] encodeRequestVote(RequestVote req) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeUTF(req.candidateId());
        dos.writeLong(req.term());
        dos.writeLong(req.lastLogIndex());
        dos.writeLong(req.lastLogTerm());
        dos.flush();
        return baos.toByteArray();
    }

    public static RequestVote decodeRequestVote(byte[] bytes) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        String candidateId = dis.readUTF();
        long term = dis.readLong();
        long lastLogIndex = dis.readLong();
        long lastLogTerm = dis.readLong();
        return new RequestVote(candidateId, term, lastLogIndex, lastLogTerm);
    }

    public static byte[] encodeRequestVoteResponse(RequestVoteResponse resp) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeUTF(resp.fromNodeId());
        dos.writeLong(resp.term());
        dos.writeBoolean(resp.voteGranted());
        dos.flush();
        return baos.toByteArray();
    }

    public static RequestVoteResponse decodeRequestVoteResponse(byte[] bytes) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        String fromNodeId = dis.readUTF();
        long term = dis.readLong();
        boolean voteGranted = dis.readBoolean();
        return new RequestVoteResponse(fromNodeId, term, voteGranted);
    }

    public static byte[] encodeAppendEntries(AppendEntries ae) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeUTF(ae.leaderId());
        dos.writeLong(ae.term());
        dos.writeLong(ae.prevLogIndex());
        dos.writeLong(ae.prevLogTerm());
        dos.writeLong(ae.leaderCommit());

        List<StdRaftLogEntry> entries = ae.entries() != null ? ae.entries() : List.of();
        dos.writeInt(entries.size());
        for (StdRaftLogEntry e : entries) {
            dos.writeLong(e.index());
            dos.writeLong(e.term());
            dos.writeUTF(e.command() != null ? e.command() : "");
            dos.writeUTF(e.data() != null ? e.data() : "");
        }

        dos.writeUTF(ae.leaseHolder() != null ? ae.leaseHolder() : "");
        dos.writeLong(ae.leaseExpiresAt());
        dos.flush();
        return baos.toByteArray();
    }

    public static AppendEntries decodeAppendEntries(byte[] bytes) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        String leaderId = dis.readUTF();
        long term = dis.readLong();
        long prevLogIndex = dis.readLong();
        long prevLogTerm = dis.readLong();
        long leaderCommit = dis.readLong();

        int size = dis.readInt();
        List<StdRaftLogEntry> entries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            long index = dis.readLong();
            long entryTerm = dis.readLong();
            String command = dis.readUTF();
            String data = dis.readUTF();
            entries.add(new StdRaftLogEntry(index, entryTerm, command, data));
        }

        String leaseHolder = dis.readUTF();
        long leaseExpiresAt = dis.readLong();
        return new AppendEntries(leaderId, term, prevLogIndex, prevLogTerm, leaderCommit, entries, leaseHolder, leaseExpiresAt);
    }

    public static byte[] encodeAppendEntriesResponse(AppendEntriesResponse resp) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeUTF(resp.fromNodeId());
        dos.writeLong(resp.term());
        dos.writeBoolean(resp.success());
        dos.writeLong(resp.matchIndex());
        dos.flush();
        return baos.toByteArray();
    }

    public static AppendEntriesResponse decodeAppendEntriesResponse(byte[] bytes) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        String fromNodeId = dis.readUTF();
        long term = dis.readLong();
        boolean success = dis.readBoolean();
        long matchIndex = dis.readLong();
        return new AppendEntriesResponse(fromNodeId, term, success, matchIndex);
    }

    public static byte[] encodeLeaseAcquire(LeaseAcquire req) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeUTF(req.holderId());
        dos.writeUTF(req.leaseKey());
        dos.writeLong(req.ttlMillis());
        dos.flush();
        return baos.toByteArray();
    }

    public static LeaseAcquire decodeLeaseAcquire(byte[] bytes) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        String holderId = dis.readUTF();
        String leaseKey = dis.readUTF();
        long ttlMillis = dis.readLong();
        return new LeaseAcquire(holderId, leaseKey, ttlMillis);
    }

    public static byte[] encodeLeaseAcquireResponse(LeaseAcquireResponse resp) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeBoolean(resp.granted());
        dos.writeLong(resp.term());
        dos.writeLong(resp.expiresAt());
        dos.writeUTF(resp.leaderId() != null ? resp.leaderId() : "");
        dos.flush();
        return baos.toByteArray();
    }

    public static LeaseAcquireResponse decodeLeaseAcquireResponse(byte[] bytes) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        boolean granted = dis.readBoolean();
        long term = dis.readLong();
        long expiresAt = dis.readLong();
        String leaderId = dis.readUTF();
        return new LeaseAcquireResponse(granted, term, expiresAt, leaderId);
    }

    public static byte[] encodeLeaseQuery(LeaseQuery req) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeUTF(req.leaseKey());
        dos.flush();
        return baos.toByteArray();
    }

    public static LeaseQuery decodeLeaseQuery(byte[] bytes) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        String leaseKey = dis.readUTF();
        return new LeaseQuery(leaseKey);
    }

    public static byte[] encodeLeaseQueryResponse(LeaseQueryResponse resp) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeBoolean(resp.active());
        dos.writeUTF(resp.holderId() != null ? resp.holderId() : "");
        dos.writeLong(resp.expiresAt());
        dos.writeUTF(resp.leaderId() != null ? resp.leaderId() : "");
        dos.flush();
        return baos.toByteArray();
    }

    public static LeaseQueryResponse decodeLeaseQueryResponse(byte[] bytes) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        boolean active = dis.readBoolean();
        String holderId = dis.readUTF();
        long expiresAt = dis.readLong();
        String leaderId = dis.readUTF();
        return new LeaseQueryResponse(active, holderId, expiresAt, leaderId);
    }
}
