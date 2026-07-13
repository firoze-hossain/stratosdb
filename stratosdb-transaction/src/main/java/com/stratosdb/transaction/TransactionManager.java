package com.stratosdb.transaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {
    private final AtomicLong nextXID = new AtomicLong(1);
    private final ThreadLocal<Transaction> currentTransaction = new ThreadLocal<>();
    private final ConcurrentHashMap<Long, Transaction> activeTransactions = new ConcurrentHashMap<>();
    
    public Transaction begin() {
        Transaction tx = new Transaction(nextXID.getAndIncrement());
        currentTransaction.set(tx);
        activeTransactions.put(tx.getXID(), tx);
        return tx;
    }
    
    public void commit() {
        Transaction tx = currentTransaction.get();
        if (tx != null) {
            tx.commit();
            activeTransactions.remove(tx.getXID());
            currentTransaction.remove();
        }
    }
    
    public void rollback() {
        Transaction tx = currentTransaction.get();
        if (tx != null) {
            tx.abort();
            activeTransactions.remove(tx.getXID());
            currentTransaction.remove();
        }
    }
    
    public Transaction getCurrentTransaction() {
        return currentTransaction.get();
    }
    
    public boolean isActive(long xid) {
        return activeTransactions.containsKey(xid);
    }
}

