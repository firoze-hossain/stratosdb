package com.stratosdb.sql.executor;

import java.util.List;

/**
 * A real, minimal contract {@link ExecutorEngine} depends on for
 * {@code CREATE DATABASE}/{@code DROP DATABASE}/{@code SHOW DATABASES} -
 * implemented by {@code StratosCluster} (in {@code stratosdb-core}),
 * which owns the real, concrete multi-database registry.
 *
 * Defined here, in {@code stratosdb-sql}, rather than having
 * {@code ExecutorEngine} depend on {@code stratosdb-core} directly,
 * since {@code stratosdb-core} already depends on {@code stratosdb-sql}
 * ({@code StratosDB} constructs an {@code ExecutorEngine}) - a direct
 * dependency the other way would be a real, circular module dependency
 * no real, ordered multi-module build (Maven included) can resolve at
 * all. This is the standard, textbook way to break exactly that cycle:
 * the lower-level module declares the abstract contract it needs: the
 * higher-level module provides the concrete implementation.
 */
public interface DatabaseClusterHost {
    /** @throws IllegalStateException if a database by this name already exists, or the name is invalid */
    void createDatabase(String name);

    /** @throws IllegalStateException if the database doesn't exist, or is the caller's own currently-open database */
    void dropDatabase(String name, String currentDatabaseName);

    List<String> listDatabaseNames();
}
