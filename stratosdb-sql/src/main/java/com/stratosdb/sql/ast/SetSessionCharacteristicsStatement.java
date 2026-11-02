package com.stratosdb.sql.ast;

import com.stratosdb.transaction.IsolationLevel;

/**
 * SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>
 */
public record SetSessionCharacteristicsStatement(IsolationLevel isolationLevel) implements Statement {}
