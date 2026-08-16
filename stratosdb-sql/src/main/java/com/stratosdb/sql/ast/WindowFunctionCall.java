package com.stratosdb.sql.ast;

import java.util.List;

/**
 * ROW_NUMBER()/RANK()/DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...).
 * partitionBy is empty when omitted (the whole result set is one partition);
 * orderBy is empty when omitted (rows are numbered/ranked in whatever order
 * they're otherwise produced - not a specially meaningful order, matching
 * real Postgres's own behavior for an OVER clause with no ORDER BY).
 */
public record WindowFunctionCall(String functionName, List<String> partitionBy, List<WindowOrderItem> orderBy, String alias) {}
