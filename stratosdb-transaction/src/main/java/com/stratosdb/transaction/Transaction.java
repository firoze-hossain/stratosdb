package com.stratosdb.transaction;

public class Transaction {
    private final long xid;
    private State state = State.ACTIVE;
    
    public enum State { ACTIVE, COMMITTED, ABORTED }
    
    public Transaction(long xid) {
        this.xid = xid;
    }
    
    public long getXID() { return xid; }
    public State getState() { return state; }
    public void commit() { state = State.COMMITTED; }
    public void abort() { state = State.ABORTED; }
    public boolean isActive() { return state == State.ACTIVE; }
    public boolean isCommitted() { return state == State.COMMITTED; }
}