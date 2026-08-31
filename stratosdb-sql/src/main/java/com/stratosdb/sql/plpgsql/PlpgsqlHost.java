package com.stratosdb.sql.plpgsql;

import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.common.exceptions.DeadlockException;
import com.stratosdb.transaction.Transaction;

import java.util.List;

/**
 * The real, minimal surface PlpgsqlInterpreter needs from ExecutorEngine -
 * a small, deliberate interface rather than a direct, tight coupling in
 * either direction, since ExecutorEngine both implements this (to let the
 * interpreter call back into it) and itself calls into
 * PlpgsqlInterpreter/PlpgsqlParser (to run a procedural function/procedure's
 * own body) - a real, two-way relationship this interface exists to keep
 * clean and narrow.
 */
public interface PlpgsqlHost {
    /** Runs one real, already-variable-substituted SQL statement within the given, shared transaction - the exact same real mechanism a procedure's own multi-statement body already used before this round (see ExecutorEngine.runProcedure). */
    QueryResult executeEmbeddedSql(String sql, Transaction txn) throws DeadlockException;

    /** Invokes a real built-in or user-defined function by name (see ExecutorEngine.invokeFunction) within the given, shared transaction - used for a function call appearing inside a plpgsql expression itself (e.g. `x := upper(y)`). */
    Object invokeFunctionForPlpgsql(String functionName, List<Object> args, Transaction txn) throws DeadlockException;
}
