package com.stratosdb.sql.catalog;

/**
 * Functional interface representing an undo action for a DDL change.
 * When a transaction executing DDL operations is aborted or rolled back to a savepoint,
 * the registered DdlUndoActions are invoked in reverse order to revert in-memory catalog
 * and relcache state.
 */
@FunctionalInterface
public interface DdlUndoAction {
    void undo();
}
