package com.stratosdb.transaction;

import com.stratosdb.transaction.mvcc.Snapshot;

public class Transaction {
    private final long xid;
    private final Snapshot snapshot;
    private final long startTimeMillis;
    private State state = State.ACTIVE;
    
    public enum State { ACTIVE, COMMITTED, ABORTED }
    
    public Transaction(long xid, Snapshot snapshot) {
        this.xid = xid;
        this.snapshot = snapshot;
        this.startTimeMillis = System.currentTimeMillis();
    }
    
    public long getXID() { return xid; }
    public Snapshot getSnapshot() { return snapshot; }
    public long getStartTimeMillis() { return startTimeMillis; }
    public State getState() { return state; }
    public void commit() { state = State.COMMITTED; }
    public void abort() { state = State.ABORTED; }
    public boolean isActive() { return state == State.ACTIVE; }
    public boolean isCommitted() { return state == State.COMMITTED; }
}