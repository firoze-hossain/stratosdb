package com.stratosdb.testing.jepsen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Jepsen-style operation history recorder and linearizability / consistency verifier.
 * Tracks every client write, read, and cluster leader observation to enforce:
 *   1. Zero stale reads (monotonic reads / read-your-writes after commit ACK)
 *   2. Zero lost updates (every acknowledged write persists across failovers & restarts)
 *   3. Zero split-brain inconsistencies (never > 1 active leader simultaneously)
 */
public class JepsenHistoryVerifier {

    public enum OpType {
        WRITE,
        READ,
        TPCC_NEW_ORDER,
        TPCC_PAYMENT
    }

    public enum Outcome {
        OK,
        FAIL,
        INDETERMINATE
    }

    public record HistoryEvent(
        long opId,
        long timestampNanos,
        OpType opType,
        int key,
        long value,
        Outcome outcome,
        String nodeId,
        String detail
    ) {}

    private final AtomicLong opSeq = new AtomicLong(0);
    private final List<HistoryEvent> events = new CopyOnWriteArrayList<>();

    // Committed write tracking: key -> highest acknowledged monotonic value
    private final Map<Integer, AtomicLong> maxAckedByKey = new ConcurrentHashMap<>();
    // Every acknowledged unique order/write ID that must survive all chaos
    private final Set<Integer> ackedOrderIds = ConcurrentHashMap.newKeySet();

    private final AtomicInteger maxObservedLeaders = new AtomicInteger(0);
    private final List<String> violations = new CopyOnWriteArrayList<>();

    public long nextOpId() {
        return opSeq.incrementAndGet();
    }

    public void recordLeaderCount(int count, String context) {
        maxObservedLeaders.accumulateAndGet(count, Math::max);
        if (count > 1) {
            violations.add("SPLIT-BRAIN detected (" + context + "): " + count + " concurrent leaders");
        }
    }

    public void recordViolation(String message) {
        violations.add(message);
    }

    public void recordAckedWrite(long opId, int key, long value, String nodeId) {
        events.add(new HistoryEvent(opId, System.nanoTime(), OpType.WRITE, key, value, Outcome.OK, nodeId, "acked"));
        maxAckedByKey.computeIfAbsent(key, k -> new AtomicLong(0)).accumulateAndGet(value, Math::max);
        ackedOrderIds.add(key);
    }

    public void recordFailedWrite(long opId, int key, long value, String nodeId, String reason) {
        events.add(new HistoryEvent(opId, System.nanoTime(), OpType.WRITE, key, value, Outcome.FAIL, nodeId, reason));
    }

    public void recordAckedTpccTx(long opId, OpType type, int orderId, long amount, String nodeId) {
        events.add(new HistoryEvent(opId, System.nanoTime(), type, orderId, amount, Outcome.OK, nodeId, "committed"));
        if (type == OpType.TPCC_NEW_ORDER) {
            ackedOrderIds.add(orderId);
        }
    }

    public void recordRead(long opId, int key, long observedValue, String nodeId) {
        events.add(new HistoryEvent(opId, System.nanoTime(), OpType.READ, key, observedValue, Outcome.OK, nodeId, "read"));
        AtomicLong maxAcked = maxAckedByKey.get(key);
        if (maxAcked != null) {
            long expectedMin = maxAcked.get();
            if (observedValue < expectedMin) {
                violations.add(
                    "STALE READ on key=" + key + " node=" + nodeId
                    + ": observed=" + observedValue + " < acked=" + expectedMin
                );
            }
        }
    }

    public void verifyAllAckedKeysPresent(Set<Integer> presentKeys, String context) {
        for (Integer ackedKey : ackedOrderIds) {
            if (!presentKeys.contains(ackedKey)) {
                violations.add("LOST UPDATE (" + context + "): acknowledged key/order_id=" + ackedKey + " missing after chaos");
            }
        }
    }

    public List<String> getViolations() {
        return Collections.unmodifiableList(new ArrayList<>(violations));
    }

    public int getMaxObservedLeaders() {
        return maxObservedLeaders.get();
    }

    public int getAckedWriteCount() {
        return ackedOrderIds.size();
    }

    public List<HistoryEvent> getEvents() {
        return Collections.unmodifiableList(events);
    }
}
