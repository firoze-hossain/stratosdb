package com.stratosdb.sql.ast;

import com.stratosdb.transaction.IsolationLevel;

/**
 * SET [LOCAL] TRANSACTION ISOLATION LEVEL <level> [READ ONLY | READ WRITE]
 */
public record SetTransactionIsolationStatement(IsolationLevel isolationLevel, boolean isLocal) implements Statement {}
