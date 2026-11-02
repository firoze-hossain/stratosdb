package com.stratosdb.transaction;

import com.stratosdb.transaction.mvcc.Snapshot;

public class Transaction {
    private final long xid;
    private volatile Snapshot snapshot;
    private final long startTimeMillis;
    private State state = State.ACTIVE;
    private volatile IsolationLevel isolationLevel;

    public enum State { ACTIVE, COMMITTED, ABORTED }

    public Transaction(long xid, Snapshot snapshot) {
        this(xid, snapshot, IsolationLevel.READ_COMMITTED);
    }

    public Transaction(long xid, Snapshot snapshot, IsolationLevel isolationLevel) {
        this.xid = xid;
        this.snapshot = snapshot;
        this.isolationLevel = isolationLevel != null ? isolationLevel : IsolationLevel.READ_COMMITTED;
        this.startTimeMillis = System.currentTimeMillis();
    }

    public long getXID() { return xid; }
    public Snapshot getSnapshot() { return snapshot; }
    public void setSnapshot(Snapshot snapshot) { this.snapshot = snapshot; }
    public IsolationLevel getIsolationLevel() { return isolationLevel; }
    public void setIsolationLevel(IsolationLevel isolationLevel) { this.isolationLevel = isolationLevel; }
    public long getStartTimeMillis() { return startTimeMillis; }
    public State getState() { return state; }
    public void commit() { state = State.COMMITTED; }
    public void abort() { state = State.ABORTED; }
    public boolean isActive() { return state == State.ACTIVE; }
    public boolean isCommitted() { return state == State.COMMITTED; }
}
