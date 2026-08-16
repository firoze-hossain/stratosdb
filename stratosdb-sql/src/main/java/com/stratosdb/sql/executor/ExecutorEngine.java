package com.stratosdb.sql.executor;

import com.stratosdb.common.exceptions.DeadlockException;
import com.stratosdb.index.btree.BTreeIndex;
import com.stratosdb.sql.ast.*;
import com.stratosdb.sql.parser.SqlParser;
import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.BTreePage;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.storage.wal.WALManager;
import com.stratosdb.transaction.Transaction;
import com.stratosdb.transaction.TransactionManager;
import com.stratosdb.transaction.mvcc.MVCCVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ExecutorEngine {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorEngine.class);

    private final SqlParser parser;
    private final Map<String, HeapTable> tables;
    private final BufferPool bufferPool;
    private final WALManager walManager;
    private final TransactionManager transactionManager;
    // Store column names for each table
    private final Map<String, List<String>> tableColumns;
    /** tableName -> columnName -> its raw default expression text (a literal, or once SERIAL/sequences exist, a "nextval('seqname')" marker) - null/absent means no default, so an omitted column gets SQL NULL. */
    private final Map<String, Map<String, String>> tableColumnDefaults = new ConcurrentHashMap<>();
    private final Map<String, Sequence> sequences = new ConcurrentHashMap<>();

    /**
     * CREATE VIEW just remembers the defining query - a view is never
     * materialized. SELECT-ing from a view re-runs that query fresh every
     * time (see executeSelectOverView), so it always reflects the current
     * data, the same tradeoff any non-materialized view makes.
     */
    private final Map<String, SelectStatement> views = new ConcurrentHashMap<>();

    /** One index (B+Tree or hash), and what it indexes. Only integer/long-valued columns are indexable (see toIndexKey). */
    private record IndexEntry(String indexName, String tableName, String columnName, com.stratosdb.index.KeyValueIndex index) {}

    private final Map<String, IndexEntry> indexesByName;
    private final Map<String, List<IndexEntry>> indexesByTable;

    /**
     * Per-column statistics: distinct-value count (for equality selectivity)
     * and numeric min/max (for range selectivity). Populated only by
     * ANALYZE - there is no automatic refresh on INSERT/UPDATE/DELETE (that
     * would be autovacuum's job, Phase E, not attempted here), so these can
     * go stale exactly the way Postgres's own statistics do without a
     * periodic ANALYZE/autovacuum. This is a real, named limitation, not
     * hidden: a table that grows substantially after ANALYZE was last run
     * will have the optimizer working from outdated row-count and
     * selectivity estimates until ANALYZE runs again.
     */
    private record ColumnStatistics(long distinctCount, Double min, Double max) {}
    private record TableStatistics(long rowCount, Map<String, ColumnStatistics> columnStats) {}

    private final Map<String, TableStatistics> statistics;

    /**
     * Where CREATE TABLE / CREATE INDEX / CREATE VIEW / their DROP
     * counterparts get durably recorded, so a restart - crash or clean
     * shutdown - can reconstruct which tables/indexes/views exist. Null
     * means "no catalog persistence" (schema is in-memory only, lost on
     * restart) - kept as a valid, explicit option for the 3-arg
     * constructor rather than silently requiring every caller to have a
     * real directory.
     *
     * This was a real, significant gap found while working on savepoints:
     * a table's heap file (t.dat) already survived a restart correctly -
     * DiskManager/HeapTable have always handled that - but nothing told a
     * freshly started ExecutorEngine that table "t" existed at all, so a
     * restarted engine reported "Table not found" for data that was
     * sitting right there on disk. Proven with a real kill -9 test before
     * being treated as real, not assumed from reading the code. See
     * PROGRESS.md for the full story.
     */
    private final String dataDirectory;

    /**
     * indexName -> the catalog line needed to reconstruct it structurally
     * (name/table/column/type), NOT by replaying "CREATE INDEX" as SQL -
     * that would rescan the table and re-insert every row into an index
     * whose file already has those exact entries from before the restart,
     * silently duplicating every entry. Tables and views don't have this
     * problem (recreating a HeapTable object for a name is idempotent
     * regardless of whether its file already has data - HeapTable already
     * seeds itself from the existing file's actual state), so those are
     * simply replayed as their original CREATE TABLE / CREATE VIEW SQL text.
     */
    private final LinkedHashMap<String, String> catalogLines = new LinkedHashMap<>();
    private boolean loadingCatalog = false;

    public ExecutorEngine(BufferPool bufferPool, WALManager walManager, TransactionManager transactionManager) {
        this(bufferPool, walManager, transactionManager, null);
    }

    public ExecutorEngine(BufferPool bufferPool, WALManager walManager, TransactionManager transactionManager, String dataDirectory) {
        this.parser = new SqlParser();
        this.tables = new ConcurrentHashMap<>();
        this.tableColumns = new ConcurrentHashMap<>();
        this.indexesByName = new ConcurrentHashMap<>();
        this.indexesByTable = new ConcurrentHashMap<>();
        this.statistics = new ConcurrentHashMap<>();
        this.bufferPool = bufferPool;
        this.walManager = walManager;
        this.transactionManager = transactionManager;
        this.dataDirectory = dataDirectory;
        loadCatalog();
    }

    /** Called after any successfully-dispatched statement; updates the catalog only for the schema-changing statement types. */
    private void recordCatalogChange(Statement stmt, String sql) {
        if (dataDirectory == null) return; // no persistence configured
        if (stmt instanceof CreateTableStatement s) {
            catalogLines.put("TABLE:" + s.tableName(), "TABLE|" + sql);
            saveCatalog();
        } else if (stmt instanceof DropTableStatement s) {
            catalogLines.remove("TABLE:" + s.tableName());
            saveCatalog();
        } else if (stmt instanceof CreateViewStatement s) {
            catalogLines.put("VIEW:" + s.viewName(), "VIEW|" + sql);
            saveCatalog();
        } else if (stmt instanceof DropViewStatement s) {
            catalogLines.remove("VIEW:" + s.viewName());
            saveCatalog();
        } else if (stmt instanceof CreateIndexStatement s) {
            catalogLines.put("INDEX:" + s.indexName(),
                "INDEX|" + s.indexName() + "|" + s.tableName() + "|" + s.columnName() + "|" + s.indexType());
            saveCatalog();
        } else if (stmt instanceof CreateSequenceStatement s) {
            catalogLines.put("SEQUENCE:" + s.name(), "SEQUENCE|" + sql);
            saveCatalog();
        } else if (stmt instanceof DropSequenceStatement s) {
            catalogLines.remove("SEQUENCE:" + s.name());
            saveCatalog();
        }
        // No DropIndexStatement exists yet in this grammar - nothing to remove for that case.
    }

    private java.io.File catalogFile() {
        return dataDirectory == null ? null : new java.io.File(dataDirectory, "catalog.txt");
    }

    private void saveCatalog() {
        if (loadingCatalog) return; // deferred to one final write at the end of loadCatalog - see its javadoc
        java.io.File file = catalogFile();
        if (file == null) return;
        try {
            java.nio.file.Files.write(file.toPath(), catalogLines.values());
        } catch (java.io.IOException e) {
            LOG.error("Failed to save schema catalog to {}", file, e);
        }
    }

    /**
     * Runs once at construction. Tables and views replay their original
     * DDL text through the normal execute() path - safe and idempotent
     * because HeapTable already correctly adapts to an existing data file
     * rather than assuming it's empty. Indexes reconstruct structurally
     * (see catalogLines' javadoc) to avoid re-scanning and duplicating
     * entries in an index file that already has them.
     *
     * The catalog file itself is NOT rewritten after each individual
     * entry replays (see the loadingCatalog guard in saveCatalog) - doing
     * that would mean an interruption partway through a large replay
     * leaves the catalog file holding only the entries processed so far,
     * silently losing every entry after that point. One save, after the
     * whole replay succeeds, avoids that.
     */
    private void loadCatalog() {
        java.io.File file = catalogFile();
        if (file == null || !file.exists()) return;
        loadingCatalog = true;
        try {
            for (String line : java.nio.file.Files.readAllLines(file.toPath())) {
                if (line.isBlank()) continue;
                String[] parts = line.split("\\|", 2);
                String kind = parts[0];
                if (kind.equals("INDEX")) {
                    String[] indexParts = parts[1].split("\\|");
                    String indexName = indexParts[0], tableName = indexParts[1], columnName = indexParts[2], type = indexParts[3];
                    reconstructIndex(indexName, tableName, columnName, type);
                    catalogLines.put("INDEX:" + indexName, line);
                } else {
                    // TABLE, VIEW, or SEQUENCE - parts[1] is the original raw
                    // SQL text. Safe to replay verbatim for all three: a
                    // table/sequence's constructor already correctly resumes
                    // from whatever persisted state (heap file, watermark
                    // file) already exists rather than assuming it's fresh.
                    // execute() already records this back into catalogLines
                    // via recordCatalogChange on success, so nothing extra
                    // needed here beyond checking it actually worked.
                    QueryResult result = execute(parts[1]);
                    if (!result.isSuccess()) {
                        LOG.error("Failed to replay catalog entry on startup: {} -> {}", parts[1], result.getError());
                    }
                }
            }
            LOG.info("Schema catalog loaded: {} table(s)/view(s)/index(es)", catalogLines.size());
        } catch (Exception e) {
            LOG.error("Failed to load schema catalog from {}", file, e);
        } finally {
            loadingCatalog = false;
        }
        saveCatalog(); // one real write, now that loadingCatalog is false, reflecting everything that just replayed
    }

    /** Recreates an index's in-memory registration and its BTreeIndex/HashIndex object WITHOUT rescanning the table - the index's own file already has every entry from before the restart. */
    private void reconstructIndex(String indexName, String tableName, String columnName, String type) {
        com.stratosdb.index.KeyValueIndex index = type.equals("HASH")
            ? new com.stratosdb.index.hash.HashIndex(indexName, bufferPool)
            : new BTreeIndex(indexName, bufferPool);
        IndexEntry entry = new IndexEntry(indexName, tableName, columnName, index);
        indexesByName.put(indexName, entry);
        indexesByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(entry);
    }

    /**
     * Per-calling-thread session state for explicit transactions. A thread
     * (in practice, one virtual thread per network connection - see
     * StratosServer) that sends BEGIN keeps its Transaction here across
     * however many subsequent execute() calls it makes, until COMMIT or
     * ROLLBACK. "poisoned" implements the same rule Postgres uses: once any
     * statement inside an explicit transaction fails, the whole transaction
     * is dead - every further statement is rejected until ROLLBACK, rather
     * than silently continuing on a transaction that's already partially
     * inconsistent from the caller's point of view.
     */
    private static final class SessionState {
        Transaction transaction;
        boolean poisoned;
        List<Savepoint> savepoints = new ArrayList<>(); // stack, most recently created last
        /**
         * CTE name -> its query, active only while executing the one
         * statement that defined it. Session-local (ThreadLocal-backed,
         * since SessionState itself is), NOT a shared map like `views` -
         * two concurrent connections both using a CTE named the same thing
         * must not interfere with each other, which a shared map mutated
         * around each execution would risk (one connection's cleanup
         * removing the other's still-in-use entry mid-execution).
         */
        Map<String, SelectStatement> activeCtes = new java.util.HashMap<>();
        /** sequenceName -> the last value nextval() returned FOR THIS SESSION - real Postgres's currval() is explicitly per-session, not "whatever the sequence's global value happens to be" (which could reflect a completely different connection's nextval() call). */
        Map<String, Long> lastNextvalBySequence = new java.util.HashMap<>();
        /** cteName -> its rows for THIS iteration of a recursive CTE's fixpoint (see executeRecursiveCteSelect) - resolved directly, bypassing query re-execution entirely, since a recursive CTE's self-reference must see specific, already-computed rows (the previous iteration's new ones), not a re-runnable query. */
        Map<String, List<Tuple>> materializedCteRows = new java.util.HashMap<>();
    }

    private static final class Savepoint {
        final String name;
        final List<UndoAction> actions = new ArrayList<>();
        Savepoint(String name) { this.name = name; }
    }

    /**
     * What ROLLBACK TO SAVEPOINT actually undoes. MVCC visibility already
     * gives exactly the primitive this needs for free: MVCCVisibility.
     * isVisible() treats a version whose xmax equals the CURRENT snapshot's
     * own xid as invisible "even to me" (see its javadoc) - so
     * self-tombstoning a row this same transaction created makes it vanish
     * for the rest of this transaction, permanently, without needing any
     * new visibility rule. Undoing an UPDATE or DELETE needs the reverse
     * primitive too - clearing a tombstone back to NO_XMAX to restore a
     * version this transaction had removed - which MVCCVisibility.withXmax
     * already supports (it was written generically as "set xmax to X," not
     * specifically "set xmax to a real xid").
     *
     * Recorded (and later undone) in exactly the same terms this class
     * already maintains indexes in: an undo doesn't just flip a tombstone
     * bit, it also runs maintainIndexesOnWrite/maintainIndexesOnDelete to
     * keep the index consistent with whatever the undo just did to
     * visibility - the same pairing the forward INSERT/UPDATE/DELETE path
     * already uses, just run in the opposite direction.
     */
    private sealed interface UndoAction {
        record UndoInsert(String tableName, long pageId, int slot) implements UndoAction {}
        record UndoDelete(String tableName, long pageId, int slot) implements UndoAction {}
        record UndoUpdate(String tableName, long oldPageId, int oldSlot, long newPageId, int newSlot) implements UndoAction {}
    }

    private final ThreadLocal<SessionState> session = ThreadLocal.withInitial(SessionState::new);

    /** Only tracks anything when at least one savepoint is currently active - ordinary statements outside a savepoint pay zero bookkeeping cost. */
    private void recordUndo(UndoAction action) {
        SessionState state = session.get();
        if (!state.savepoints.isEmpty()) {
            state.savepoints.get(state.savepoints.size() - 1).actions.add(action);
        }
    }

    /**
     * Every statement runs inside a transaction - either one this call
     * begins and commits/aborts itself (auto-commit, the default, one
     * statement per transaction), or an explicit one opened by a prior
     * BEGIN on this same session and held open across calls until COMMIT
     * or ROLLBACK. Real transaction lifecycle either way: INSERT/SELECT/
     * UPDATE/DELETE all go through MVCC snapshots and, for writers, real
     * row-level locking with deadlock detection.
     *
     * The WAL commit record is written and forced to disk BEFORE the
     * transaction is marked committed in memory - if the process dies
     * between those two lines, redo on restart will still replay this
     * transaction's operations, and no reader can have seen it as committed
     * before it truly was. Since Phase D, redo also checks that a commit
     * record actually exists for each operation's transaction before
     * replaying it (see WALManager.recover) - essential once a transaction
     * can span more than one statement, since a multi-statement transaction
     * that crashes before COMMIT must leave zero trace, not a partial one.
     */
    /**
     * Slow-query logging: 0 or negative disables it (the default, matching
     * this project's opt-in pattern for other operational features).
     * Timing wraps the entire statement - parse, dispatch, and the
     * commit/abort bookkeeping around it - not just the dispatch call, so
     * "slow" reflects what a caller actually experienced end to end.
     */
    private volatile long slowQueryThresholdMs = -1;

    public void setSlowQueryThresholdMs(long thresholdMs) {
        this.slowQueryThresholdMs = thresholdMs;
    }

    public QueryResult execute(String sql) {
        long startNanos = System.nanoTime();
        QueryResult result = executeInternal(sql);
        if (slowQueryThresholdMs >= 0) {
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            if (elapsedMs >= slowQueryThresholdMs) {
                LOG.warn("Slow query ({} ms, threshold {} ms): {}", elapsedMs, slowQueryThresholdMs, sql);
            }
        }
        return result;
    }

    private QueryResult executeInternal(String sql) {
        Statement stmt;
        try {
            stmt = parser.parse(sql);
        } catch (Exception e) {
            return QueryResult.error(e.getMessage());
        }

        if (stmt instanceof BeginStatement) {
            return executeBegin();
        }
        if (stmt instanceof CommitStatement) {
            return executeCommit();
        }
        if (stmt instanceof RollbackStatement) {
            return executeRollback();
        }
        if (stmt instanceof RollbackToSavepointStatement s) {
            return executeRollbackToSavepoint(s);
        }

        SessionState state = session.get();
        if (state.poisoned) {
            return QueryResult.error("Current transaction is aborted, commands ignored until ROLLBACK");
        }
        if (stmt instanceof SavepointStatement s) {
            return executeSavepoint(s, state);
        }
        if (stmt instanceof ReleaseSavepointStatement s) {
            return executeReleaseSavepoint(s, state);
        }

        boolean explicit = state.transaction != null;
        Transaction txn = explicit ? state.transaction : transactionManager.begin();
        try {
            QueryResult result = dispatch(stmt, txn);

            if (result.isSuccess()) {
                recordCatalogChange(stmt, sql);
                if (!explicit) {
                    walManager.logCommit(txn.getXID());
                    transactionManager.commit(txn);
                }
                // else: stays open, part of the caller's explicit transaction until COMMIT/ROLLBACK
            } else if (explicit) {
                state.poisoned = true;
            } else {
                transactionManager.abort(txn);
            }
            return result;
        } catch (DeadlockException e) {
            if (explicit) {
                state.poisoned = true;
            } else {
                transactionManager.abort(txn);
            }
            LOG.warn("Transaction {} aborted due to deadlock: {}", txn.getXID(), e.getMessage());
            return QueryResult.error("Deadlock detected, transaction aborted: " + e.getMessage());
        } catch (Exception e) {
            if (explicit) {
                state.poisoned = true;
            } else {
                transactionManager.abort(txn);
            }
            LOG.error("Execution failed: {}", sql, e);
            return QueryResult.error(e.getMessage());
        }
    }

    private QueryResult executeBegin() {
        SessionState state = session.get();
        if (state.transaction != null) {
            return QueryResult.error("Already in a transaction - nested BEGIN is not supported");
        }
        state.transaction = transactionManager.begin();
        state.poisoned = false;
        state.savepoints.clear();
        return QueryResult.success("BEGIN");
    }

    private QueryResult executeCommit() {
        SessionState state = session.get();
        if (state.transaction == null) {
            return QueryResult.error("No transaction in progress");
        }
        if (state.poisoned) {
            // Matches real Postgres: COMMIT on an already-aborted transaction
            // rolls it back instead - there is nothing valid left to commit.
            transactionManager.abort(state.transaction);
            state.transaction = null;
            state.poisoned = false;
            state.savepoints.clear();
            return QueryResult.error("Current transaction is aborted, rolled back instead of committed");
        }
        walManager.logCommit(state.transaction.getXID());
        transactionManager.commit(state.transaction);
        state.transaction = null;
        state.savepoints.clear();
        return QueryResult.success("COMMIT");
    }

    private QueryResult executeRollback() {
        SessionState state = session.get();
        if (state.transaction == null) {
            return QueryResult.error("No transaction in progress");
        }
        transactionManager.abort(state.transaction);
        state.transaction = null;
        state.poisoned = false;
        state.savepoints.clear();
        return QueryResult.success("ROLLBACK");
    }

    private QueryResult executeSavepoint(SavepointStatement stmt, SessionState state) {
        if (state.transaction == null) {
            return QueryResult.error("SAVEPOINT can only be used inside a transaction - BEGIN first");
        }
        state.savepoints.add(new Savepoint(stmt.name()));
        return QueryResult.success("SAVEPOINT " + stmt.name());
    }

    /**
     * Per the SQL standard: releasing a savepoint also releases every
     * savepoint established after it (they're nested inside it) - but
     * their combined changes are NOT undone, only forgotten as separate
     * rollback targets. If an enclosing savepoint (or the plain
     * transaction) is later rolled back further out, it still needs to
     * undo everything that happened in here - so the released savepoints'
     * undo actions are folded into the parent rather than discarded.
     */
    private QueryResult executeReleaseSavepoint(ReleaseSavepointStatement stmt, SessionState state) {
        if (state.transaction == null) {
            return QueryResult.error("RELEASE SAVEPOINT can only be used inside a transaction");
        }
        int idx = findSavepointIndex(state, stmt.name());
        if (idx == -1) {
            return QueryResult.error("No such savepoint: " + stmt.name());
        }
        List<UndoAction> merged = new ArrayList<>();
        while (state.savepoints.size() > idx) {
            merged.addAll(state.savepoints.remove(idx).actions);
        }
        if (idx > 0) {
            state.savepoints.get(idx - 1).actions.addAll(merged);
        }
        return QueryResult.success("RELEASE SAVEPOINT " + stmt.name());
    }

    /**
     * The one command allowed to run even on a poisoned (aborted-by-error)
     * transaction - see the dispatch order in execute(): this is exactly
     * how a real transaction recovers from a mid-transaction error without
     * losing everything committed before it, by rolling back to a
     * checkpoint taken earlier and continuing from there.
     */
    private QueryResult executeRollbackToSavepoint(RollbackToSavepointStatement stmt) {
        SessionState state = session.get();
        if (state.transaction == null) {
            return QueryResult.error("ROLLBACK TO SAVEPOINT can only be used inside a transaction");
        }
        int idx = findSavepointIndex(state, stmt.name());
        if (idx == -1) {
            return QueryResult.error("No such savepoint: " + stmt.name());
        }

        long myXid = state.transaction.getXID();
        // Undo every action from the most recently created savepoint back
        // through (and including) the target, newest action first overall -
        // standard undo-log order.
        for (int i = state.savepoints.size() - 1; i >= idx; i--) {
            List<UndoAction> actions = state.savepoints.get(i).actions;
            for (int j = actions.size() - 1; j >= 0; j--) {
                undoAction(actions.get(j), myXid);
            }
        }
        // Discard every savepoint created after the target; the target
        // itself survives (ROLLBACK TO SAVEPOINT does not remove it - a
        // second ROLLBACK TO the same savepoint is valid) with its action
        // list cleared, since those actions are now undone.
        while (state.savepoints.size() > idx + 1) {
            state.savepoints.remove(state.savepoints.size() - 1);
        }
        state.savepoints.get(idx).actions.clear();

        state.poisoned = false; // recovering from whatever error (if any) led here is the whole point
        return QueryResult.success("ROLLBACK TO SAVEPOINT " + stmt.name());
    }

    private int findSavepointIndex(SessionState state, String name) {
        // Search newest-first: a reused savepoint name refers to the most recently established one, matching standard SQL.
        for (int i = state.savepoints.size() - 1; i >= 0; i--) {
            if (state.savepoints.get(i).name.equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private void undoAction(UndoAction action, long myXid) {
        if (action instanceof UndoAction.UndoInsert u) {
            HeapTable table = tables.get(u.tableName());
            if (table == null) return; // table was dropped mid-transaction - nothing left to undo
            byte[] stored = table.readTuple(u.pageId(), u.slot());
            if (stored == null) return;
            Tuple tuple = Tuple.deserialize(MVCCVisibility.readPayload(stored));
            table.update(u.pageId(), u.slot(), MVCCVisibility.withXmax(stored, myXid));
            maintainIndexesOnDelete(u.tableName(), tuple, u.pageId(), u.slot());
        } else if (action instanceof UndoAction.UndoDelete u) {
            HeapTable table = tables.get(u.tableName());
            if (table == null) return;
            byte[] stored = table.readTuple(u.pageId(), u.slot());
            if (stored == null) return;
            byte[] restored = MVCCVisibility.withXmax(stored, MVCCVisibility.NO_XMAX);
            table.update(u.pageId(), u.slot(), restored);
            Tuple tuple = Tuple.deserialize(MVCCVisibility.readPayload(restored));
            maintainIndexesOnWrite(u.tableName(), tuple, u.pageId(), u.slot());
        } else if (action instanceof UndoAction.UndoUpdate u) {
            HeapTable table = tables.get(u.tableName());
            if (table == null) return;
            byte[] oldStored = table.readTuple(u.oldPageId(), u.oldSlot());
            if (oldStored != null) {
                byte[] restored = MVCCVisibility.withXmax(oldStored, MVCCVisibility.NO_XMAX);
                table.update(u.oldPageId(), u.oldSlot(), restored);
                Tuple oldTuple = Tuple.deserialize(MVCCVisibility.readPayload(restored));
                maintainIndexesOnWrite(u.tableName(), oldTuple, u.oldPageId(), u.oldSlot());
            }
            byte[] newStored = table.readTuple(u.newPageId(), u.newSlot());
            if (newStored != null) {
                Tuple newTuple = Tuple.deserialize(MVCCVisibility.readPayload(newStored));
                table.update(u.newPageId(), u.newSlot(), MVCCVisibility.withXmax(newStored, myXid));
                maintainIndexesOnDelete(u.tableName(), newTuple, u.newPageId(), u.newSlot());
            }
        }
    }

    /**
     * Call when a connection/session ends (see StratosServer/StratosDB),
     * not just between statements. Without this, a client that sends BEGIN
     * and then simply disconnects - never sending COMMIT or ROLLBACK -
     * would leave its transaction "active" in TransactionManager forever:
     * getOldestActiveXid() would permanently report that abandoned xid as
     * the horizon, and VACUUM would never be able to reclaim anything
     * created after it, for the life of the process. Rolling back
     * whatever's still open is the same thing a real database does when a
     * connection drops mid-transaction.
     */
    public void closeSession() {
        SessionState state = session.get();
        if (state.transaction != null) {
            LOG.warn("Session ending with an open transaction (xid={}) - rolling it back", state.transaction.getXID());
            transactionManager.abort(state.transaction);
        }
        session.remove();
    }

    /** A snapshot of current table names - views are not included. Used by autovacuum to know what to vacuum. */
    public java.util.Set<String> getTableNames() {
        return new java.util.HashSet<>(tables.keySet());
    }

    private QueryResult dispatch(Statement stmt, Transaction txn) throws DeadlockException {
        if (stmt instanceof CreateTableStatement s) return executeCreateTable(s);
        if (stmt instanceof CreateIndexStatement s) return executeCreateIndex(s, txn);
        if (stmt instanceof InsertStatement s) return executeInsert(s, txn);
        if (stmt instanceof SelectStatement s) return executeSelect(s, txn);
        if (stmt instanceof UpdateStatement s) return executeUpdate(s, txn);
        if (stmt instanceof DeleteStatement s) return executeDelete(s, txn);
        if (stmt instanceof DropTableStatement s) return executeDropTable(s);
        if (stmt instanceof ShowTablesStatement) return executeShowTables();
        if (stmt instanceof ShowStatsStatement) return executeShowStats();
        if (stmt instanceof ExplainStatement s) return executeExplain(s);
        if (stmt instanceof AnalyzeStatement s) return executeAnalyze(s, txn);
        if (stmt instanceof VacuumStatement s) return executeVacuum(s);
        if (stmt instanceof CreateViewStatement s) return executeCreateView(s);
        if (stmt instanceof DropViewStatement s) return executeDropView(s);
        if (stmt instanceof CteSelectStatement s) return executeCteSelect(s, txn);
        if (stmt instanceof RecursiveCteSelectStatement s) return executeRecursiveCteSelect(s, txn);
        if (stmt instanceof CreateSequenceStatement s) return executeCreateSequence(s);
        if (stmt instanceof DropSequenceStatement s) return executeDropSequence(s);
        return QueryResult.error("Unsupported statement");
    }

    /**
     * WITH cteName AS (cteQuery) outerQuery - registers the CTE as a
     * temporary, session-local, statement-scoped entry (see SessionState.
     * activeCtes' javadoc for why this isn't the shared `views` map) so
     * the outer query's normal table-name resolution finds it via
     * executeSelect's existing view-lookup fallback, then always removes
     * it afterward regardless of success or failure - a CTE must never
     * leak into any statement after the one that defined it.
     *
     * Known, named simplification: a single, non-recursive CTE only -
     * multiple CTEs in one WITH clause is real further work, not
     * attempted here. (WITH RECURSIVE is a separate statement type - see
     * executeRecursiveCteSelect - since a recursive CTE's execution model
     * is different enough to not fit this same code path.) Also unlike
     * real Postgres, a CTE here only takes effect when no real table of
     * the same name exists (true shadowing of an existing table is a
     * further refinement, not a correctness gap for the common case of
     * picking a name that doesn't collide with anything).
     */
    private QueryResult executeCteSelect(CteSelectStatement stmt, Transaction txn) {
        SessionState state = session.get();
        state.activeCtes.put(stmt.cteName(), stmt.cteQuery());
        try {
            return executeSelect(stmt.outerQuery(), txn);
        } finally {
            state.activeCtes.remove(stmt.cteName());
        }
    }

    /**
     * Real limit against a recursive query that never converges (e.g. a
     * cycle in the underlying data with no guard against revisiting a
     * node, or simply a WHERE clause that always matches) - fails with a
     * clear, actionable error rather than an infinite loop or an
     * out-of-memory crash. 1000 is a generous ceiling for legitimate
     * hierarchical data (a 1000-level-deep tree/chain is already an
     * unusual shape) while still bounding worst-case work to something
     * that fails fast.
     */
    private static final int RECURSIVE_CTE_MAX_ITERATIONS = 1000;

    /**
     * WITH RECURSIVE cteName AS (baseQuery UNION ALL recursiveQuery) outerQuery.
     *
     * Fixpoint iteration, the standard evaluation strategy for a recursive
     * CTE: run baseQuery once to seed both the accumulated result and the
     * "new rows" working set; then repeatedly run recursiveQuery with
     * cteName resolving to ONLY the previous iteration's new rows (not the
     * whole accumulated set so far - real "working table" semantics,
     * matching Postgres, and also what keeps each iteration's work
     * bounded to genuinely new data rather than reprocessing everything
     * accumulated so far), adding whatever comes out to the accumulated
     * result, until an iteration produces nothing new (the fixpoint) or
     * the safety iteration limit is hit.
     *
     * Uses materializedCteRows (SessionState), not activeCtes - the
     * recursive branch needs to resolve to a SPECIFIC, already-computed
     * row list each iteration, not a re-executable query definition (see
     * executeSelect's lookup chain and resolveJoinSource for where this is
     * actually consumed - critically, resolveJoinSource is what lets the
     * recursive branch be "FROM realTable JOIN cteName ON ..." - the
     * standard, valuable hierarchy/graph-traversal pattern - not just a
     * bare "FROM cteName").
     *
     * Known, honestly-stated limitation: no cycle detection beyond the
     * iteration cap - a graph with an actual cycle (not just a deep tree)
     * will keep finding "new" rows every iteration until the cap is hit,
     * since nothing here checks for a row's key having been seen before.
     * Real cycle-safe recursive CTEs (Postgres's own UNION, as opposed to
     * UNION ALL, does deduplicate) are further work, not attempted here.
     */
    private QueryResult executeRecursiveCteSelect(RecursiveCteSelectStatement stmt, Transaction txn) {
        QueryResult baseResult = executeSelect(stmt.baseQuery(), txn);
        if (!baseResult.isSuccess()) {
            return baseResult;
        }

        // UNION ALL matches columns by POSITION, not by name, across its two
        // branches - real SQL requires both sides to have the same column
        // count for exactly this reason. Every row from every iteration is
        // renamed to the base query's own column names, positionally,
        // before being used as the next iteration's working set. Without
        // this, a recursive branch that writes its own column names
        // explicitly (e.g. "SELECT employees.id, ..." rather than a bare
        // "id") produces differently-named columns each iteration, which
        // silently breaks the NEXT iteration's join resolution (a real bug
        // found this way: a real employee-hierarchy query stopped after one
        // recursive step, silently missing the deeper levels, because
        // "employees.id" from the recursive branch didn't match what a
        // later join condition like "org_chart.id" was actually looking
        // for once re-qualified).
        List<String> baseColumnNames = baseResult.getRows().isEmpty() ? List.of() : baseResult.getRows().get(0).getColumnNames();
        List<Tuple> accumulated = new ArrayList<>(alignColumnsToBaseSchema(baseResult.getRows(), baseColumnNames));
        List<Tuple> newRowsFromLastIteration = accumulated;

        SessionState state = session.get();
        int iterations = 0;
        while (!newRowsFromLastIteration.isEmpty()) {
            iterations++;
            if (iterations > RECURSIVE_CTE_MAX_ITERATIONS) {
                return QueryResult.error("Recursive CTE \"" + stmt.cteName() + "\" exceeded "
                    + RECURSIVE_CTE_MAX_ITERATIONS + " iterations without reaching a fixpoint - "
                    + "likely a non-terminating recursion (does the recursive branch actually converge toward a base case, "
                    + "or could there be a cycle in the underlying data?)");
            }

            state.materializedCteRows.put(stmt.cteName(), newRowsFromLastIteration);
            QueryResult stepResult;
            try {
                stepResult = executeSelect(stmt.recursiveQuery(), txn);
            } finally {
                state.materializedCteRows.remove(stmt.cteName());
            }
            if (!stepResult.isSuccess()) {
                return stepResult;
            }

            newRowsFromLastIteration = alignColumnsToBaseSchema(stepResult.getRows(), baseColumnNames);
            accumulated.addAll(newRowsFromLastIteration);
        }

        return applyOuterQueryToRows(stmt.outerQuery(), accumulated, txn);
    }

    /** Rebuilds each row with the base query's own column names, matched positionally - see executeRecursiveCteSelect's javadoc for why this matters. A no-op (values unchanged) when the names already match, which is the common case for a simply-written recursive branch. */
    private List<Tuple> alignColumnsToBaseSchema(List<Tuple> rows, List<String> baseColumnNames) {
        if (baseColumnNames.isEmpty()) {
            return rows;
        }
        List<Tuple> aligned = new ArrayList<>(rows.size());
        for (Tuple row : rows) {
            Tuple renamed = new Tuple();
            for (int i = 0; i < baseColumnNames.size() && i < row.getColumnNames().size(); i++) {
                renamed.addValue(baseColumnNames.get(i), row.getValue(i));
            }
            aligned.add(renamed);
        }
        return aligned;
    }

    private QueryResult executeCreateSequence(CreateSequenceStatement stmt) {
        if (sequences.containsKey(stmt.name())) {
            return QueryResult.error("Sequence already exists: " + stmt.name());
        }
        sequences.put(stmt.name(), new Sequence(stmt.name(), stmt.startValue(), stmt.incrementBy(), sequenceFile(stmt.name())));
        return QueryResult.success("Sequence created: " + stmt.name());
    }

    private QueryResult executeDropSequence(DropSequenceStatement stmt) {
        if (sequences.remove(stmt.name()) == null) {
            return QueryResult.error("Sequence not found: " + stmt.name());
        }
        return QueryResult.success("Sequence dropped: " + stmt.name());
    }

    /** Where a sequence's persisted watermark lives - null (no persistence) when this engine wasn't given a data directory, matching the same pattern the schema catalog and commit-status log already use. */
    private java.io.File sequenceFile(String sequenceName) {
        return dataDirectory == null ? null : new java.io.File(dataDirectory, "sequences/" + sequenceName + ".seq");
    }

    private QueryResult executeCreateTable(CreateTableStatement stmt) {
        if (tables.containsKey(stmt.tableName())) {
            return QueryResult.error("Table already exists: " + stmt.tableName());
        }
        if (views.containsKey(stmt.tableName())) {
            return QueryResult.error("A view already exists with that name: " + stmt.tableName());
        }

        // SERIAL/BIGSERIAL sugar: each such column gets its own backing
        // sequence, auto-named "{table}_{column}_seq" (matching real
        // Postgres's own naming convention) and wired as that column's
        // default. Checked and named up front, before creating anything,
        // so a collision with an already-existing sequence of that name
        // cleanly aborts the whole CREATE TABLE rather than leaving a
        // half-created table with some sequences made and others not.
        Map<String, String> autoSequenceNames = new java.util.HashMap<>(); // columnName -> its sequence's name
        for (ColumnDefinition col : stmt.columns()) {
            if (isSerialType(col.type())) {
                String seqName = stmt.tableName() + "_" + col.name() + "_seq";
                if (sequences.containsKey(seqName)) {
                    return QueryResult.error("Cannot create SERIAL column " + col.name()
                        + ": a sequence named " + seqName + " already exists");
                }
                autoSequenceNames.put(col.name(), seqName);
            }
        }

        HeapTable table = new HeapTable(stmt.tableName(), bufferPool);
        tables.put(stmt.tableName(), table);

        List<String> columns = new ArrayList<>();
        Map<String, String> defaults = new java.util.HashMap<>();
        for (ColumnDefinition col : stmt.columns()) {
            columns.add(col.name());
            String seqName = autoSequenceNames.get(col.name());
            if (seqName != null) {
                sequences.put(seqName, new Sequence(seqName, 1, 1, sequenceFile(seqName)));
                catalogLines.put("SEQUENCE:" + seqName, "SEQUENCE|CREATE SEQUENCE " + seqName + ";");
                defaults.put(col.name(), "nextval('" + seqName + "')");
            } else if (col.defaultValue() != null) {
                defaults.put(col.name(), col.defaultValue());
            }
        }
        tableColumns.put(stmt.tableName(), columns);
        tableColumnDefaults.put(stmt.tableName(), defaults);
        if (!autoSequenceNames.isEmpty()) {
            saveCatalog(); // one write covering every auto-created sequence's new catalog entry
        }

        return QueryResult.success("Table created: " + stmt.tableName());
    }

    private boolean isSerialType(String type) {
        return type.equalsIgnoreCase("SERIAL") || type.equalsIgnoreCase("BIGSERIAL");
    }

    /**
     * CREATE INDEX: builds a real B+Tree (stratosdb-index) over an existing
     * column and backfills it from every row currently visible to this
     * transaction. Only integer-valued columns can be indexed right now -
     * the B+Tree's keys are longs, and there's no string key encoding yet
     * (see toIndexKey). That's a real, named limitation, not a silent gap:
     * CREATE INDEX on a non-integer column succeeds but simply won't index
     * any rows, and later inserts into it won't be indexed either - the
     * planner will correctly never choose an index scan for it since
     * findIndex() only reports indexes that exist, but this is still worth
     * knowing before relying on it.
     */
    private QueryResult executeCreateIndex(CreateIndexStatement stmt, Transaction txn) {
        if (indexesByName.containsKey(stmt.indexName())) {
            return QueryResult.error("Index already exists: " + stmt.indexName());
        }
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }
        List<String> columns = tableColumns.get(stmt.tableName());
        boolean columnExists = columns != null && columns.stream().anyMatch(c -> c.equalsIgnoreCase(stmt.columnName()));
        if (!columnExists) {
            return QueryResult.error("Column not found: " + stmt.columnName() + " on table " + stmt.tableName());
        }

        com.stratosdb.index.KeyValueIndex index = stmt.indexType() == CreateIndexStatement.IndexType.HASH
            ? new com.stratosdb.index.hash.HashIndex(stmt.indexName(), bufferPool)
            : new BTreeIndex(stmt.indexName(), bufferPool);
        int indexed = 0;
        int skippedNonNumeric = 0;
        for (HeapTable.PositionedRow row : table.scanPositioned()) {
            if (!MVCCVisibility.isVisible(row.stored(), txn.getSnapshot(), transactionManager)) {
                continue;
            }
            Tuple tuple = Tuple.deserialize(MVCCVisibility.readPayload(row.stored()));
            Object value = findColumnValue(tuple, stmt.columnName());
            Long key = toIndexKey(value);
            if (key != null) {
                index.insert(key, new BTreePage.RID(row.pageId(), row.slot()));
                indexed++;
            } else {
                skippedNonNumeric++;
            }
        }

        IndexEntry entry = new IndexEntry(stmt.indexName(), stmt.tableName(), stmt.columnName(), index);
        indexesByName.put(stmt.indexName(), entry);
        indexesByTable.computeIfAbsent(stmt.tableName(), k -> new ArrayList<>()).add(entry);

        String message = "Index created: " + stmt.indexName() + " (" + stmt.indexType() + ") on " + stmt.tableName()
            + "(" + stmt.columnName() + "), indexed " + indexed + " row(s)";
        if (skippedNonNumeric > 0) {
            message += " (" + skippedNonNumeric + " row(s) skipped: non-integer column value)";
        }
        return QueryResult.success(message);
    }

    /**
     * ANALYZE: a real, if simple, statistics collector - one full scan of
     * the table computing row count and, per column, a distinct-value
     * count plus numeric min/max. Used by planScan (below) to make the
     * seq-scan-vs-index-scan choice genuinely cost-based rather than
     * rule-based, once this has been run.
     *
     * Known simplification, stated plainly: selectivity is estimated by
     * assuming a UNIFORM distribution across a column's distinct values
     * (rowCount / distinctCount for equality). Real Postgres tracks actual
     * most-common-value frequencies and histograms specifically because
     * real data is rarely uniform - a column with 2 distinct values split
     * 999-to-1 looks identical to this model as one split 500-to-500. That
     * gap is real further work (Phase B), not hidden here.
     */
    private QueryResult executeAnalyze(AnalyzeStatement stmt, Transaction txn) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        List<String> columnNames = tableColumns.get(stmt.tableName());
        Map<String, Set<Object>> distinctValues = new HashMap<>();
        Map<String, Double> minValues = new HashMap<>();
        Map<String, Double> maxValues = new HashMap<>();
        if (columnNames != null) {
            for (String col : columnNames) {
                distinctValues.put(col, new HashSet<>());
            }
        }

        long rowCount = 0;
        for (byte[] raw : table.scanMvcc(txn.getSnapshot(), transactionManager)) {
            Tuple tuple = Tuple.deserialize(raw);
            rowCount++;
            if (columnNames == null) {
                continue;
            }
            for (String col : columnNames) {
                Object value = findColumnValue(tuple, col);
                if (value == null) {
                    continue;
                }
                distinctValues.get(col).add(value);
                if (value instanceof Number n) {
                    double d = n.doubleValue();
                    minValues.merge(col, d, Math::min);
                    maxValues.merge(col, d, Math::max);
                }
            }
        }

        Map<String, ColumnStatistics> columnStats = new HashMap<>();
        if (columnNames != null) {
            for (String col : columnNames) {
                columnStats.put(col, new ColumnStatistics(
                    distinctValues.get(col).size(),
                    minValues.get(col),
                    maxValues.get(col)));
            }
        }

        statistics.put(stmt.tableName(), new TableStatistics(rowCount, columnStats));

        return QueryResult.success("Analyzed " + stmt.tableName() + ": " + rowCount + " row(s), "
            + (columnNames != null ? columnNames.size() : 0) + " column(s)");
    }

    /**
     * VACUUM: delegates the actual reclamation to HeapTable.vacuum() (see
     * its javadoc for the full explanation) - this method's only job is
     * computing the horizon and reporting the result as a QueryResult.
     *
     * Index entries are NOT touched here - they're already kept current in
     * real time by DELETE/UPDATE (see maintainIndexesOnDelete), a separate,
     * earlier piece of work. This is purely heap-level space reclamation.
     *
     * Known limitation, stated plainly: this is manual, not automatic -
     * there is no autovacuum background process deciding when to run this
     * on its own (Phase E, not attempted here). A table that's never
     * VACUUMed behaves exactly as it always has.
     */
    private QueryResult executeVacuum(VacuumStatement stmt) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        long horizon = transactionManager.getOldestActiveXid();
        HeapTable.VacuumResult result = table.vacuum(horizon, transactionManager);

        return QueryResult.success("Vacuumed " + stmt.tableName() + ": reclaimed "
            + result.reclaimedVersions() + " dead row version(s) across " + result.pagesCompacted() + " page(s)");
    }

    private QueryResult executeInsert(InsertStatement stmt, Transaction txn) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        List<String> allColumns = tableColumns.get(stmt.tableName());
        if (allColumns == null) {
            // Defensive fallback for a table somehow missing its column list -
            // shouldn't happen in practice, since executeCreateTable always
            // populates it, but keep the old col0/col1 behavior rather than
            // crashing if it ever does.
            Tuple fallback = new Tuple();
            for (int i = 0; i < stmt.values().size(); i++) {
                fallback.addValue("col" + i, resolveValue(stmt.values().get(i)));
            }
            return finishInsert(stmt, txn, fallback);
        }

        // The explicit (col1, col2, ...) list if the statement gave one;
        // otherwise every column, in the table's own declared order - the
        // two cases INSERT INTO t (a, b) VALUES (...) and INSERT INTO t
        // VALUES (...) are handled correctly and distinctly, unlike before
        // this fix (see buildInsert's javadoc for the bug this replaced).
        List<String> targetColumns = stmt.columns().isEmpty() ? allColumns : stmt.columns();
        if (targetColumns.size() != stmt.values().size()) {
            return QueryResult.error("INSERT has " + stmt.values().size() + " value(s) but "
                + targetColumns.size() + " column(s) were specified for table " + stmt.tableName());
        }

        Map<String, Object> givenValues = new java.util.LinkedHashMap<>();
        for (int i = 0; i < targetColumns.size(); i++) {
            String colName = targetColumns.get(i);
            if (!allColumns.contains(colName)) {
                return QueryResult.error("Column not found: " + colName + " on table " + stmt.tableName());
            }
            givenValues.put(colName, resolveValue(stmt.values().get(i)));
        }

        // Build the tuple in the TABLE's OWN column order (not the statement's
        // order - callers may list columns in any order), applying each
        // column's default for anything the statement didn't explicitly
        // provide, or SQL NULL if it has no default either.
        Map<String, String> defaults = tableColumnDefaults.getOrDefault(stmt.tableName(), Map.of());
        Tuple tuple = new Tuple();
        for (String col : allColumns) {
            if (givenValues.containsKey(col)) {
                tuple.addValue(col, givenValues.get(col));
            } else if (defaults.containsKey(col)) {
                tuple.addValue(col, resolveValue(defaults.get(col)));
            } else {
                tuple.addValue(col, null);
            }
        }

        return finishInsert(stmt, txn, tuple);
    }

    private QueryResult finishInsert(InsertStatement stmt, Transaction txn, Tuple tuple) {
        byte[] data = tuple.serialize();
        HeapTable table = tables.get(stmt.tableName());
        HeapTable.InsertResult result = table.insertMvcc(data, txn.getXID());

        walManager.logInsert(stmt.tableName(), txn.getXID(), result.pageId, result.slot, data);
        maintainIndexesOnWrite(stmt.tableName(), tuple, result.pageId, result.slot);
        recordUndo(new UndoAction.UndoInsert(stmt.tableName(), result.pageId, result.slot));

        return QueryResult.success("Inserted row at " + result.pageId + "/" + result.slot);
    }

    /**
     * Rule-based scan choice: use a B+Tree index when the WHERE clause is a
     * single numeric predicate on an indexed column, otherwise fall back to
     * a full MVCC scan. Not cost-based (there's no statistics collection to
     * base a cost on yet) - it's the honest "does an applicable index exist
     * at all" version, same spirit as the Week 3 plan called for.
     */
    private QueryResult executeSelect(SelectStatement stmt, Transaction txn) {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            List<Tuple> materializedRows = session.get().materializedCteRows.get(stmt.tableName());
            if (materializedRows != null) {
                // A recursive CTE's self-reference - already-computed rows from
                // the previous fixpoint iteration, not a query to re-execute.
                // Same join/aggregate/window-function restriction as views and
                // non-recursive CTEs below, for the same reason.
                boolean hasJoin = stmt.joins() != null && !stmt.joins().isEmpty();
                boolean hasAggregate = !stmt.aggregates().isEmpty() || !stmt.groupBy().isEmpty();
                boolean hasWindowFunction = !stmt.windowFunctions().isEmpty();
                if (hasJoin || hasAggregate || hasWindowFunction) {
                    return QueryResult.error("A recursive CTE's self-reference (" + stmt.tableName()
                        + ") combined with a JOIN, an aggregate/GROUP BY, or a window function isn't supported yet - "
                        + "a plain SELECT ... FROM " + stmt.tableName() + " [WHERE ...] works");
                }
                return applyOuterQueryToRows(stmt, materializedRows, txn);
            }

            SelectStatement viewQuery = views.get(stmt.tableName());
            if (viewQuery == null) {
                // Not a real table, not a persisted view - maybe it's an active CTE
                // (WITH cteName AS (...) ...), scoped to just this one statement.
                viewQuery = session.get().activeCtes.get(stmt.tableName());
            }
            if (viewQuery != null) {
                boolean hasJoin = stmt.joins() != null && !stmt.joins().isEmpty();
                boolean hasAggregate = !stmt.aggregates().isEmpty() || !stmt.groupBy().isEmpty();
                boolean hasWindowFunction = !stmt.windowFunctions().isEmpty();
                if (hasJoin || hasAggregate || hasWindowFunction) {
                    // executeSelectOverView only applies WHERE/projection/LIMIT to
                    // the view's rows - it doesn't know how to join, aggregate, or
                    // compute a window function. Silently falling through to it
                    // would silently ignore the outer query's real intent entirely
                    // (a real bug this check replaced, found by actually testing
                    // "SELECT COUNT(*) FROM aView" rather than assuming the
                    // join/aggregate code paths would reject it themselves - they
                    // never got the chance, since this views check runs first).
                    return QueryResult.error("Querying a view (" + stmt.tableName()
                        + ") together with a JOIN, an aggregate/GROUP BY, or a window function isn't supported yet - "
                        + "a plain SELECT ... FROM " + stmt.tableName() + " [WHERE ...] works");
                }
                return executeSelectOverView(stmt, viewQuery, txn);
            }
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        if (stmt.joins() != null && !stmt.joins().isEmpty()) {
            if (!stmt.windowFunctions().isEmpty()) {
                return QueryResult.error("Window functions combined with JOIN aren't supported yet");
            }
            return executeJoinedSelect(stmt, txn);
        }

        if (!stmt.aggregates().isEmpty() || !stmt.groupBy().isEmpty()) {
            if (!stmt.windowFunctions().isEmpty()) {
                return QueryResult.error("Window functions combined with GROUP BY/aggregates aren't supported yet");
            }
            return executeAggregateSelect(stmt, txn, table);
        }

        if (!stmt.windowFunctions().isEmpty()) {
            return executeWindowFunctionSelect(stmt, txn, table);
        }

        ScanPlan plan = planScan(stmt.tableName(), stmt.where());
        List<Tuple> tuples = new ArrayList<>();

        if (plan.useIndex()) {
            List<BTreePage.RID> rids = plan.loKey().equals(plan.hiKey())
                ? plan.index().index().searchAll(plan.loKey()) // equality: any KeyValueIndex (hash or btree) can serve this
                : ((BTreeIndex) plan.index().index()).rangeScan(plan.loKey(), plan.hiKey()); // range: planScan's findRangeCapableIndex guarantees this is always a BTreeIndex

            for (BTreePage.RID rid : rids) {
                byte[] stored = table.readTuple(rid.pageId(), rid.slot());
                if (stored == null || !MVCCVisibility.isVisible(stored, txn.getSnapshot(), transactionManager)) {
                    continue; // stale index entry (from an update/delete) or not visible to this snapshot
                }
                Tuple tuple = Tuple.deserialize(MVCCVisibility.readPayload(stored));
                if (!matchesWhere(tuple, stmt.where(), txn)) {
                    continue; // defensive re-check, keeps index-scan results identical to seq-scan results
                }
                tuples.add(project(tuple, stmt.columns()));
            }
        } else {
            List<byte[]> visibleRows = table.scanMvcc(txn.getSnapshot(), transactionManager);
            for (byte[] data : visibleRows) {
                Tuple tuple = Tuple.deserialize(data);
                if (!matchesWhere(tuple, stmt.where(), txn)) {
                    continue;
                }
                tuples.add(project(tuple, stmt.columns()));
            }
        }

        if (stmt.limit() != null) {
            try {
                int limit = Integer.parseInt(stmt.limit());
                if (tuples.size() > limit) {
                    tuples = tuples.subList(0, limit);
                }
            } catch (NumberFormatException e) {
                // Ignore invalid limit
            }
        }

        return QueryResult.success(tuples);
    }

    /**
     * A SELECT whose FROM clause names a view: run the view's own stored
     * query fresh (which may itself join, aggregate, filter, or reference
     * a subquery - whatever it was defined with), then apply THIS outer
     * query's WHERE/projection/LIMIT to the view's result rows. No
     * materialization or caching - a view always reflects current data,
     * the standard non-materialized-view tradeoff.
     *
     * Known, named scope limit: this handles "SELECT ... FROM aView"
     * directly. Joining a view against another table, or aggregating over
     * a view, isn't threaded through here - executeJoinedSelect and
     * executeAggregateSelect both look a FROM name up in the tables map
     * only, so a query trying either against a view name fails with
     * "table not found" rather than silently doing something wrong.
     */
    private QueryResult executeSelectOverView(SelectStatement outer, SelectStatement viewQuery, Transaction txn) {
        QueryResult viewResult = executeSelect(viewQuery, txn);
        if (!viewResult.isSuccess()) {
            return viewResult;
        }
        return applyOuterQueryToRows(outer, viewResult.getRows(), txn);
    }

    /**
     * Applies an outer query's WHERE/projection/LIMIT to an already-available
     * row list, rather than a table/view/CTE it would need to scan or
     * execute itself. Factored out of executeSelectOverView so recursive
     * CTEs (see executeRecursiveCteSelect) can reuse the exact same
     * WHERE/projection/LIMIT logic against a materialized row list (the
     * fixpoint iteration's accumulated result) without needing an
     * executable SelectStatement to run - there isn't one; the CTE's
     * "content" at that point is just a list of tuples already computed.
     */
    private QueryResult applyOuterQueryToRows(SelectStatement outer, List<Tuple> rows, Transaction txn) {
        List<Tuple> filtered = new ArrayList<>();
        for (Tuple row : rows) {
            if (matchesWhere(row, outer.where(), txn)) {
                filtered.add(project(row, outer.columns()));
            }
        }

        if (outer.limit() != null) {
            try {
                int limit = Integer.parseInt(outer.limit());
                if (filtered.size() > limit) {
                    filtered = filtered.subList(0, limit);
                }
            } catch (NumberFormatException e) {
                // Ignore invalid limit
            }
        }

        return QueryResult.success(filtered);
    }

    /**
     * GROUP BY / aggregates (COUNT/SUM/AVG/MIN/MAX) / HAVING.
     *
     * Always a full scan - there is no index acceleration for aggregate
     * queries yet (see PROJECT_PLAN.md Phase B: that needs a real
     * cost-based optimizer, which doesn't exist yet either). Not combined
     * with JOIN in this pass - a query with both joins and aggregates hits
     * executeJoinedSelect first and won't group; that combination is a
     * real, separate gap, not silently mishandled (executeJoinedSelect
     * doesn't consult stmt.aggregates()/groupBy() at all, so such a query
     * would just return per-row joined results, unaggregated).
     *
     * With no GROUP BY at all but an aggregate requested (e.g.
     * "SELECT COUNT(*) FROM t"), the whole filtered result set is treated
     * as a single implicit group - standard SQL behavior.
     */

    /**
     * ROW_NUMBER()/RANK()/DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...).
     *
     * Unlike GROUP BY, a window function never collapses rows - every row
     * that matched WHERE stays in the result, just with an extra computed
     * column added. So the shape here is deliberately different from
     * executeAggregateSelect: scan and filter once, compute each window
     * function's value per row by INDEX (not by Tuple identity/equality,
     * which would be fragile), then project.
     *
     * Known, honestly-stated limitations: no combining with JOIN or
     * GROUP BY/aggregates yet (rejected cleanly - see executeSelect), and
     * NULLs in a partition or order-by column always sort last regardless
     * of ASC/DESC, a real simplification of Postgres's own NULLS FIRST/LAST
     * rules.
     */
    private QueryResult executeWindowFunctionSelect(SelectStatement stmt, Transaction txn, HeapTable table) {
        List<Tuple> rows = new ArrayList<>();
        for (byte[] raw : table.scanMvcc(txn.getSnapshot(), transactionManager)) {
            Tuple tuple = Tuple.deserialize(raw);
            if (matchesWhere(tuple, stmt.where(), txn)) {
                rows.add(tuple);
            }
        }

        // alias -> this window function's value for each row, by index into `rows`.
        Map<String, long[]> windowValuesByAlias = new java.util.LinkedHashMap<>();
        for (WindowFunctionCall call : stmt.windowFunctions()) {
            windowValuesByAlias.put(call.alias(), computeWindowValues(rows, call));
        }

        List<Tuple> result = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            Tuple projected = project(rows.get(i), stmt.columns());
            for (Map.Entry<String, long[]> entry : windowValuesByAlias.entrySet()) {
                projected.addValue(entry.getKey(), entry.getValue()[i]);
            }
            result.add(projected);
        }

        if (stmt.limit() != null) {
            try {
                int limit = Integer.parseInt(stmt.limit());
                if (result.size() > limit) {
                    result = result.subList(0, limit);
                }
            } catch (NumberFormatException e) {
                // Ignore invalid limit, matching this engine's existing plain-select behavior.
            }
        }

        return QueryResult.success(result);
    }

    private long[] computeWindowValues(List<Tuple> rows, WindowFunctionCall call) {
        long[] result = new long[rows.size()];

        Map<Object, List<Integer>> partitions = new java.util.LinkedHashMap<>();
        for (int i = 0; i < rows.size(); i++) {
            partitions.computeIfAbsent(buildCompositeKey(rows.get(i), call.partitionBy()), k -> new ArrayList<>()).add(i);
        }

        for (List<Integer> partitionIndices : partitions.values()) {
            partitionIndices.sort((a, b) -> compareForWindowOrder(rows.get(a), rows.get(b), call.orderBy()));

            long rowNumber = 0;
            long rank = 0;
            long denseRank = 0;
            Object previousOrderKey = null;
            for (int idx : partitionIndices) {
                rowNumber++;
                Object currentOrderKey = buildOrderKey(rows.get(idx), call.orderBy());
                boolean tiedWithPrevious = previousOrderKey != null && previousOrderKey.equals(currentOrderKey);
                if (!tiedWithPrevious) {
                    rank = rowNumber; // RANK: ties share a rank, the next distinct value skips ahead (1,1,3 - not 1,1,2)
                    denseRank++;      // DENSE_RANK: ties share a rank, the next distinct value is just +1 (1,1,2)
                }
                result[idx] = switch (call.functionName()) {
                    case "ROW_NUMBER" -> rowNumber;
                    case "RANK" -> rank;
                    default -> denseRank;
                };
                previousOrderKey = currentOrderKey;
            }
        }
        return result;
    }

    /** A composite key built from possibly-multiple columns' values, safe to use directly as a Map key since List already implements content-based equals/hashCode. Empty column list means "everything is one partition/one order group" - the correct behavior for an omitted PARTITION BY or ORDER BY. */
    private Object buildCompositeKey(Tuple row, List<String> columns) {
        List<Object> key = new ArrayList<>();
        for (String col : columns) {
            key.add(normalizeJoinKey(findColumnValue(row, col)));
        }
        return key;
    }

    private Object buildOrderKey(Tuple row, List<WindowOrderItem> orderBy) {
        List<Object> key = new ArrayList<>();
        for (WindowOrderItem item : orderBy) {
            key.add(normalizeJoinKey(findColumnValue(row, item.column())));
        }
        return key;
    }

    private int compareForWindowOrder(Tuple a, Tuple b, List<WindowOrderItem> orderBy) {
        for (WindowOrderItem item : orderBy) {
            Object valA = normalizeJoinKey(findColumnValue(a, item.column()));
            Object valB = normalizeJoinKey(findColumnValue(b, item.column()));
            int cmp = compareNullsLast(valA, valB);
            if (cmp != 0) {
                return item.descending() ? -cmp : cmp;
            }
        }
        return 0;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compareNullsLast(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return 1;
        if (b == null) return -1;
        return ((Comparable) a).compareTo(b);
    }

    private QueryResult executeAggregateSelect(SelectStatement stmt, Transaction txn, HeapTable table) {

        List<Tuple> filteredRows = new ArrayList<>();
        for (byte[] raw : table.scanMvcc(txn.getSnapshot(), transactionManager)) {
            Tuple tuple = Tuple.deserialize(raw);
            if (matchesWhere(tuple, stmt.where(), txn)) {
                filteredRows.add(tuple);
            }
        }

        Map<List<Object>, List<Tuple>> groups = new LinkedHashMap<>();
        if (stmt.groupBy().isEmpty()) {
            groups.put(List.of(), filteredRows);
        } else {
            for (Tuple tuple : filteredRows) {
                List<Object> key = new ArrayList<>();
                for (String col : stmt.groupBy()) {
                    key.add(findColumnValue(tuple, col));
                }
                groups.computeIfAbsent(key, k -> new ArrayList<>()).add(tuple);
            }
        }

        List<Tuple> resultRows = new ArrayList<>();
        for (Map.Entry<List<Object>, List<Tuple>> entry : groups.entrySet()) {
            List<Object> groupKey = entry.getKey();
            List<Tuple> groupRows = entry.getValue();

            Tuple outputRow = new Tuple();
            for (int i = 0; i < stmt.groupBy().size(); i++) {
                outputRow.addValue(stmt.groupBy().get(i), groupKey.get(i));
            }

            Map<String, Object> aggregateValues = new LinkedHashMap<>();
            for (AggregateCall agg : stmt.aggregates()) {
                Object value = computeAggregate(agg, groupRows);
                aggregateValues.put(agg.canonicalForm(), value); // HAVING always references the FUNC(arg) form
                outputRow.addValue(agg.displayName(), value);
            }

            if (stmt.havingClause() != null && !matchesHaving(stmt.havingClause(), aggregateValues)) {
                continue;
            }

            resultRows.add(outputRow);
        }

        if (stmt.limit() != null) {
            try {
                int limit = Integer.parseInt(stmt.limit());
                if (resultRows.size() > limit) {
                    resultRows = resultRows.subList(0, limit);
                }
            } catch (NumberFormatException e) {
                // Ignore invalid limit
            }
        }

        return QueryResult.success(resultRows);
    }

    /** COUNT(col) excludes NULLs (standard SQL); COUNT(*) counts every row regardless. SUM/AVG of zero contributing rows is NULL, not zero - also standard. */
    private Object computeAggregate(AggregateCall agg, List<Tuple> rows) {
        switch (agg.function()) {
            case "COUNT": {
                if (agg.argument().equals("*")) {
                    return rows.size();
                }
                int count = 0;
                for (Tuple row : rows) {
                    if (findColumnValue(row, agg.argument()) != null) count++;
                }
                return count;
            }
            case "SUM": {
                double sum = 0;
                boolean any = false;
                boolean allIntegral = true;
                for (Tuple row : rows) {
                    Object v = findColumnValue(row, agg.argument());
                    if (v instanceof Integer i) { sum += i; any = true; }
                    else if (v instanceof Long l) { sum += l; any = true; }
                    else if (v instanceof Double d) { sum += d; any = true; allIntegral = false; }
                }
                if (!any) return null;
                return allIntegral ? (Object) (long) sum : (Object) sum;
            }
            case "AVG": {
                double sum = 0;
                int count = 0;
                for (Tuple row : rows) {
                    Object v = findColumnValue(row, agg.argument());
                    if (v instanceof Number n) { sum += n.doubleValue(); count++; }
                }
                return count > 0 ? sum / count : null;
            }
            case "MIN": {
                Object best = null;
                for (Tuple row : rows) {
                    Object v = findColumnValue(row, agg.argument());
                    if (v != null && (best == null || compareForMinMax(v, best) < 0)) best = v;
                }
                return best;
            }
            case "MAX": {
                Object best = null;
                for (Tuple row : rows) {
                    Object v = findColumnValue(row, agg.argument());
                    if (v != null && (best == null || compareForMinMax(v, best) > 0)) best = v;
                }
                return best;
            }
            default:
                throw new IllegalArgumentException("Unknown aggregate function: " + agg.function());
        }
    }

    private int compareForMinMax(Object a, Object b) {
        if (a instanceof Number na && b instanceof Number nb) {
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        return a.toString().compareTo(b.toString());
    }

    /** HAVING always references the aggregate's canonical "FUNC(arg)" form, regardless of any SELECT-list alias. */
    private boolean matchesHaving(String havingClause, Map<String, Object> aggregateValues) {
        WherePredicate pred = parseWhere(havingClause);
        if (pred == null) {
            return true;
        }
        Object value = aggregateValues.get(pred.column());
        if (value == null) {
            return false;
        }
        return evaluatePredicate(value.toString(), pred);
    }

    /**
     * Hash join - replaced the Week 3 nested-loop join (Phase B of
     * PROJECT_PLAN.md). This is correct as the *only* join algorithm here
     * (not merely one option among several) because every JOIN this
     * grammar allows is an equality join (`joinClause: ... ON columnName
     * ASSIGN columnName` - ASSIGN is `=`, nothing else is accepted), and
     * hash join is exactly the right tool for equality predicates: O(n+m)
     * instead of nested-loop's O(n*m).
     *
     * Not yet cost-based, stated plainly: there's no statistics collection
     * or optimizer to weigh join strategies against each other (that's the
     * next item after this in Phase B), so this always builds the hash
     * table on whichever side has fewer rows - a real, if simple, heuristic
     * (less memory, fewer hash computations, on the side that gets
     * hashed rather than scanned), not a genuine cost comparison. If a
     * future non-equality join predicate is ever added, it would need a
     * different algorithm (hash join fundamentally cannot support it) -
     * nested-loop would be the natural fallback for that case, not
     * resurrected here speculatively.
     *
     * Every column in the combined result is qualified as
     * "tableName.columnName" to avoid ambiguity when two joined tables
     * share a column name (both having "id" is the common case). Bare
     * column references in WHERE/SELECT still work when unambiguous, via
     * findColumnValue's suffix-match fallback.
     */
    private QueryResult executeJoinedSelect(SelectStatement stmt, Transaction txn) {
        List<Tuple> current = resolveJoinSource(stmt.tableName(), txn);
        if (current == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        for (JoinClause join : stmt.joins()) {
            List<Tuple> joinedRows = resolveJoinSource(join.tableName(), txn);
            if (joinedRows == null) {
                return QueryResult.error("Table not found: " + join.tableName());
            }

            current = chooseJoinAlgorithm(current, join.leftColumn(), joinedRows, join.rightColumn());
        }

        List<Tuple> tuples = new ArrayList<>();
        for (Tuple row : current) {
            if (!matchesWhere(row, stmt.where(), txn)) {
                continue;
            }
            tuples.add(project(row, stmt.columns()));
        }

        if (stmt.limit() != null) {
            try {
                int limit = Integer.parseInt(stmt.limit());
                if (tuples.size() > limit) {
                    tuples = tuples.subList(0, limit);
                }
            } catch (NumberFormatException e) {
                // Ignore invalid limit
            }
        }

        return QueryResult.success(tuples);
    }

    /**
     * Resolves one JOIN's source (or a query's own primary FROM table, used
     * the same way) to its qualified rows - either a real table (a plain
     * scan, as always) or a recursive CTE's own materialized self-reference
     * rows (this iteration's previous-iteration data - see
     * executeRecursiveCteSelect). This is specifically what lets a
     * recursive CTE's recursive branch do "FROM realTable JOIN cteName ON
     * ..." - the standard, most valuable recursive CTE pattern (hierarchy/
     * graph traversal) - rather than being limited to a bare "FROM
     * cteName" with no real table involved at all.
     *
     * Known, honestly-stated limitation: doesn't resolve a persisted VIEW
     * or a non-recursive CTE as a join target, only as a query's own
     * primary FROM table (see executeSelect) - a further generalization,
     * not attempted here since it wasn't needed for recursive CTEs
     * specifically. Returns null (rather than throwing) when the name
     * resolves to nothing at all, so the caller can produce its own
     * specific "table not found" error.
     */
    private List<Tuple> resolveJoinSource(String name, Transaction txn) {
        HeapTable table = tables.get(name);
        if (table != null) {
            List<Tuple> rows = new ArrayList<>();
            for (byte[] raw : table.scanMvcc(txn.getSnapshot(), transactionManager)) {
                rows.add(qualify(Tuple.deserialize(raw), name));
            }
            return rows;
        }

        List<Tuple> materialized = session.get().materializedCteRows.get(name);
        if (materialized != null) {
            List<Tuple> rows = new ArrayList<>();
            for (Tuple row : materialized) {
                rows.add(qualify(row, name));
            }
            return rows;
        }

        return null;
    }

    /**
     * Classic hash join: build a hash table on the smaller side (by row
     * count), keyed by the join column, then probe it with the other side.
     * Output column order is always left-then-right regardless of which
     * side ended up as the build side internally - that choice is a pure
     * implementation detail, invisible to the caller.
     *
     * NULLs never match, on either side (standard SQL equi-join semantics)
     * - a row whose join-column value is null contributes nothing whether
     * it's on the build or probe side.
     *
     * Join keys are normalized to a canonical numeric form (double) when
     * they're any kind of Number before hashing, so values that are equal
     * numerically but different boxed types still hash/match consistently -
     * the same "compare by value, not by exact type" philosophy already
     * used throughout this class (see valuesEqual, evaluatePredicate,
     * compareForMinMax).
     */
    /**
     * Real join-strategy choice, replacing "always hash join": hash join's
     * whole build side has to fit in memory as one hash table, which
     * becomes a real cost once both sides are large; merge join instead
     * needs to sort both sides (an O(n log n) cost with no single huge
     * in-memory structure), which pays off once both inputs are big enough
     * that the sort is cheaper than the risk/cost of a huge hash table.
     *
     * Deliberately simple and named as such: a row-count threshold using
     * the actual, already-scanned sizes in hand (not stale ANALYZE
     * statistics, not a real memory-cost model) - the same honest,
     * rule-based spirit as this engine's existing scan-choice planner,
     * not a full cost-based join optimizer.
     */
    private static final int MERGE_JOIN_ROW_THRESHOLD = 10_000;

    private List<Tuple> chooseJoinAlgorithm(List<Tuple> left, String leftColumn, List<Tuple> right, String rightColumn) {
        if (left.size() >= MERGE_JOIN_ROW_THRESHOLD && right.size() >= MERGE_JOIN_ROW_THRESHOLD) {
            return mergeJoin(left, leftColumn, right, rightColumn);
        }
        return hashJoin(left, leftColumn, right, rightColumn);
    }

    private List<Tuple> hashJoin(List<Tuple> left, String leftColumn, List<Tuple> right, String rightColumn) {
        boolean buildOnLeft = left.size() <= right.size();
        List<Tuple> buildSide = buildOnLeft ? left : right;
        List<Tuple> probeSide = buildOnLeft ? right : left;
        String buildColumn = buildOnLeft ? leftColumn : rightColumn;
        String probeColumn = buildOnLeft ? rightColumn : leftColumn;

        Map<Object, List<Tuple>> hashTable = new HashMap<>();
        for (Tuple row : buildSide) {
            Object key = normalizeJoinKey(findColumnValue(row, buildColumn));
            if (key != null) {
                hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
        }

        List<Tuple> result = new ArrayList<>();
        for (Tuple probeRow : probeSide) {
            Object key = normalizeJoinKey(findColumnValue(probeRow, probeColumn));
            if (key == null) {
                continue;
            }
            List<Tuple> matches = hashTable.get(key);
            if (matches == null) {
                continue;
            }
            for (Tuple buildRow : matches) {
                result.add(buildOnLeft ? merge(buildRow, probeRow) : merge(probeRow, buildRow));
            }
        }
        return result;
    }

    /**
     * Sort-merge join: sort both sides by their join column, then advance
     * through them in lockstep, matching keys as they line up. Same output
     * contract as hashJoin (left-then-right column order, NULLs on either
     * side never match) and produces the exact same SET of result rows -
     * verified directly by testing both algorithms against identical input
     * and comparing results, not just trusting the implementation.
     *
     * The one real complication sort-merge has that hash join doesn't:
     * duplicate keys on either side need a full cross-product of the two
     * matching groups, not a simple one-to-one advance - handled by finding
     * each side's full run of equal keys before pairing them up.
     *
     * Chosen by the caller when both inputs are large enough that hash
     * join's full in-memory hash table becomes a real memory concern (see
     * executeJoinedSelect) - a simple, honest, row-count-based rule using
     * the actual scanned sizes already in hand, not a full cost model with
     * real memory/IO estimates.
     */
    private List<Tuple> mergeJoin(List<Tuple> left, String leftColumn, List<Tuple> right, String rightColumn) {
        List<Tuple> sortedLeft = new ArrayList<>();
        for (Tuple row : left) {
            if (normalizeJoinKey(findColumnValue(row, leftColumn)) != null) sortedLeft.add(row);
        }
        List<Tuple> sortedRight = new ArrayList<>();
        for (Tuple row : right) {
            if (normalizeJoinKey(findColumnValue(row, rightColumn)) != null) sortedRight.add(row);
        }
        sortedLeft.sort((a, b) -> compareKeys(normalizeJoinKey(findColumnValue(a, leftColumn)), normalizeJoinKey(findColumnValue(b, leftColumn))));
        sortedRight.sort((a, b) -> compareKeys(normalizeJoinKey(findColumnValue(a, rightColumn)), normalizeJoinKey(findColumnValue(b, rightColumn))));

        List<Tuple> result = new ArrayList<>();
        int i = 0, j = 0;
        while (i < sortedLeft.size() && j < sortedRight.size()) {
            Object leftKey = normalizeJoinKey(findColumnValue(sortedLeft.get(i), leftColumn));
            Object rightKey = normalizeJoinKey(findColumnValue(sortedRight.get(j), rightColumn));
            int cmp = compareKeys(leftKey, rightKey);
            if (cmp < 0) {
                i++;
            } else if (cmp > 0) {
                j++;
            } else {
                int leftRunEnd = i;
                while (leftRunEnd < sortedLeft.size()
                    && compareKeys(normalizeJoinKey(findColumnValue(sortedLeft.get(leftRunEnd), leftColumn)), leftKey) == 0) {
                    leftRunEnd++;
                }
                int rightRunEnd = j;
                while (rightRunEnd < sortedRight.size()
                    && compareKeys(normalizeJoinKey(findColumnValue(sortedRight.get(rightRunEnd), rightColumn)), rightKey) == 0) {
                    rightRunEnd++;
                }
                for (int a = i; a < leftRunEnd; a++) {
                    for (int b = j; b < rightRunEnd; b++) {
                        result.add(merge(sortedLeft.get(a), sortedRight.get(b)));
                    }
                }
                i = leftRunEnd;
                j = rightRunEnd;
            }
        }
        return result;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compareKeys(Object a, Object b) {
        return ((Comparable) a).compareTo(b);
    }

    private Object normalizeJoinKey(Object value) {
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        return value;
    }

    private Tuple qualify(Tuple tuple, String tableName) {
        Tuple qualified = new Tuple();
        List<String> columnNames = tuple.getColumnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            qualified.addValue(tableName + "." + columnNames.get(i), tuple.getValue(i));
        }
        return qualified;
    }

    private Tuple merge(Tuple left, Tuple right) {
        Tuple merged = new Tuple();
        List<String> leftNames = left.getColumnNames();
        for (int i = 0; i < leftNames.size(); i++) {
            merged.addValue(leftNames.get(i), left.getValue(i));
        }
        List<String> rightNames = right.getColumnNames();
        for (int i = 0; i < rightNames.size(); i++) {
            merged.addValue(rightNames.get(i), right.getValue(i));
        }
        return merged;
    }

    /** Reports which strategy planScan would pick, without running the query. */
    private QueryResult executeExplain(ExplainStatement stmt) {
        SelectStatement select = stmt.select();
        if (!tables.containsKey(select.tableName())) {
            return QueryResult.error("Table not found: " + select.tableName());
        }

        if (select.joins() != null && !select.joins().isEmpty()) {
            StringBuilder sb = new StringBuilder("Hash Join: Seq Scan on ").append(select.tableName());
            for (JoinClause join : select.joins()) {
                if (!tables.containsKey(join.tableName())) {
                    return QueryResult.error("Table not found: " + join.tableName());
                }
                sb.append(" -> Seq Scan on ").append(join.tableName())
                  .append(" ON ").append(join.leftColumn()).append("=").append(join.rightColumn());
            }
            return QueryResult.success(sb.toString());
        }

        if (!select.aggregates().isEmpty() || !select.groupBy().isEmpty()) {
            String groupDesc = select.groupBy().isEmpty() ? "" : " GROUP BY " + String.join(", ", select.groupBy());
            return QueryResult.success("Aggregate" + groupDesc + ": Seq Scan on " + select.tableName()
                + " (no index acceleration for aggregate queries yet)");
        }

        ScanPlan plan = planScan(select.tableName(), select.where());
        String costSuffix = plan.hasStatistics()
            ? String.format(" (cost=%.1f)", plan.estimatedCost())
            : " (no statistics - run ANALYZE for a cost-based choice)";
        String description = plan.useIndex()
            ? String.format("Index Scan using %s on %s (column=%s, range=[%s, %s])%s",
                plan.index().indexName(), select.tableName(), plan.index().columnName(), plan.loKey(), plan.hiKey(), costSuffix)
            : "Seq Scan on " + select.tableName() + costSuffix;
        return QueryResult.success(description);
    }

    /**
     * Constants in the same spirit as Postgres's own seq_page_cost (1.0)
     * vs. random_page_cost (4.0 by default): an index probe costs more per
     * matching row than a sequential scan does, because it's a B+Tree
     * traversal plus a random heap access rather than the next sequential
     * block. This is what makes the optimizer correctly prefer a seq scan
     * over an index for a predicate that matches a large fraction of the
     * table, even when an applicable index exists - "an index exists" and
     * "the index is worth using" are different questions, and only the
     * cost model (not the old rule-based planner) can tell them apart.
     */
    private static final double SEQ_SCAN_ROW_COST = 1.0;
    private static final double INDEX_ROW_COST = 4.0;
    private static final double INDEX_STARTUP_COST = 2.0; // B+Tree root-to-leaf traversal, paid once regardless of match count

    private record ScanPlan(boolean useIndex, IndexEntry index, Long loKey, Long hiKey, double estimatedCost, boolean hasStatistics) {
        static ScanPlan seqScan(double cost, boolean hasStats) {
            return new ScanPlan(false, null, null, null, cost, hasStats);
        }

        static ScanPlan indexScan(IndexEntry index, long lo, long hi, double cost, boolean hasStats) {
            return new ScanPlan(true, index, lo, hi, cost, hasStats);
        }
    }

    /**
     * Chooses seq scan vs. index scan. Genuinely cost-based when ANALYZE
     * has been run for this table (compares estimated costs and picks the
     * cheaper one); falls back to the original rule-based heuristic
     * ("an applicable index exists, so use it") when it hasn't, since
     * guessing at a cost with zero data would be worse than the simple
     * heuristic, not better - the same reasoning Postgres itself uses
     * when statistics are unavailable.
     */
    private ScanPlan planScan(String tableName, WhereExpr where) {
        TableStatistics stats = statistics.get(tableName);
        boolean hasStats = stats != null;

        WhereExpr.Comparison cmp = extractSimpleComparison(where);
        if (cmp == null) {
            return ScanPlan.seqScan(hasStats ? costSeqScan(stats) : 0, hasStats);
        }
        WherePredicate pred = toPredicate(cmp.column(), cmp.operator(), cmp.literal());
        if (!pred.isNumeric()) {
            return ScanPlan.seqScan(hasStats ? costSeqScan(stats) : 0, hasStats);
        }
        boolean isEquality = pred.operator().equals("=");
        IndexEntry idx = isEquality
            ? findEqualityIndex(tableName, pred.column())   // hash preferred (O(1) vs O(log n)), btree acceptable
            : findRangeCapableIndex(tableName, pred.column()); // hashing destroys order - only btree can serve a range
        if (idx == null) {
            return ScanPlan.seqScan(hasStats ? costSeqScan(stats) : 0, hasStats);
        }
        long value;
        try {
            value = Long.parseLong(pred.value());
        } catch (NumberFormatException e) {
            return ScanPlan.seqScan(hasStats ? costSeqScan(stats) : 0, hasStats);
        }

        long lo, hi;
        switch (pred.operator()) {
            case "=" -> { lo = value; hi = value; }
            case ">" -> { lo = value + 1; hi = Long.MAX_VALUE; }
            case ">=" -> { lo = value; hi = Long.MAX_VALUE; }
            case "<" -> { lo = Long.MIN_VALUE; hi = value - 1; }
            case "<=" -> { lo = Long.MIN_VALUE; hi = value; }
            default -> { return ScanPlan.seqScan(hasStats ? costSeqScan(stats) : 0, hasStats); } // "!=" isn't a contiguous range
        }

        if (!hasStats) {
            return ScanPlan.indexScan(idx, lo, hi, 0, false);
        }

        double seqCost = costSeqScan(stats);
        double idxCost = costIndexScan(stats, idx.columnName(), pred.operator(), lo, hi);
        return idxCost < seqCost
            ? ScanPlan.indexScan(idx, lo, hi, idxCost, true)
            : ScanPlan.seqScan(seqCost, true);
    }

    /**
     * Extracts one top-level equality/range comparison usable for index
     * scan planning, if the WHERE clause is simple enough - just one
     * Comparison, or an AND containing one (the first one found; picking
     * a single driving comparison out of several AND'd conditions and
     * filtering the rest afterward via matchesWhere's defensive re-check
     * is standard practice, not a shortcut unique to this planner).
     * OR/NOT/LIKE/IN/subqueries aren't usable for this simple contiguous-
     * range mechanism and correctly fall back to a full scan.
     */
    private WhereExpr.Comparison extractSimpleComparison(WhereExpr where) {
        if (where instanceof WhereExpr.Comparison cmp) {
            return cmp;
        }
        if (where instanceof WhereExpr.And and) {
            WhereExpr.Comparison left = extractSimpleComparison(and.left());
            return left != null ? left : extractSimpleComparison(and.right());
        }
        return null;
    }

    private double costSeqScan(TableStatistics stats) {
        return stats.rowCount() * SEQ_SCAN_ROW_COST;
    }

    private double costIndexScan(TableStatistics stats, String column, String operator, long lo, long hi) {
        long estimatedRows = estimateMatchingRows(stats, column, operator, lo, hi);
        return INDEX_STARTUP_COST + estimatedRows * INDEX_ROW_COST;
    }

    private long estimateMatchingRows(TableStatistics stats, String column, String operator, long lo, long hi) {
        if (stats.rowCount() == 0) {
            return 0;
        }
        ColumnStatistics colStats = stats.columnStats().get(column);
        if (colStats == null) {
            return stats.rowCount(); // no per-column stats - assume worst case (matches everything)
        }
        if (operator.equals("=")) {
            long distinct = Math.max(1, colStats.distinctCount());
            return Math.max(1, stats.rowCount() / distinct);
        }
        if (colStats.min() == null || colStats.max() == null || colStats.max() <= colStats.min()) {
            return stats.rowCount(); // no usable numeric range - assume worst case
        }
        double totalWidth = colStats.max() - colStats.min();
        double rangeLo = Math.max(colStats.min(), lo == Long.MIN_VALUE ? colStats.min() : lo);
        double rangeHi = Math.min(colStats.max(), hi == Long.MAX_VALUE ? colStats.max() : hi);
        double rangeWidth = Math.max(0, rangeHi - rangeLo);
        double selectivity = Math.min(1.0, rangeWidth / totalWidth);
        return Math.max(1, (long) (stats.rowCount() * selectivity));
    }

    /** For an equality predicate: prefers a hash index (O(1) vs O(log n)) when one exists on this column, else a B+Tree one. */
    private IndexEntry findEqualityIndex(String tableName, String columnName) {
        List<IndexEntry> tableIndexes = indexesByTable.get(tableName);
        if (tableIndexes == null) return null;
        IndexEntry btreeFallback = null;
        for (IndexEntry idx : tableIndexes) {
            if (!idx.columnName().equalsIgnoreCase(columnName)) continue;
            if (idx.index() instanceof com.stratosdb.index.hash.HashIndex) {
                return idx; // hash is strictly cheaper for equality when available - no need to keep looking
            }
            if (btreeFallback == null) {
                btreeFallback = idx;
            }
        }
        return btreeFallback;
    }

    /** For a range predicate (&gt;, &lt;, &gt;=, &lt;=): only a B+Tree index can serve it - hashing destroys key order on purpose. */
    private IndexEntry findRangeCapableIndex(String tableName, String columnName) {
        List<IndexEntry> tableIndexes = indexesByTable.get(tableName);
        if (tableIndexes == null) return null;
        for (IndexEntry idx : tableIndexes) {
            if (idx.columnName().equalsIgnoreCase(columnName) && idx.index() instanceof BTreeIndex) {
                return idx;
            }
        }
        return null;
    }

    private void maintainIndexesOnWrite(String tableName, Tuple tuple, long pageId, int slot) {
        List<IndexEntry> tableIndexes = indexesByTable.get(tableName);
        if (tableIndexes == null) return;
        for (IndexEntry idx : tableIndexes) {
            Object value = findColumnValue(tuple, idx.columnName());
            Long key = toIndexKey(value);
            if (key != null) {
                idx.index().insert(key, new BTreePage.RID(pageId, slot));
            } else {
                LOG.debug("Not indexing row in {} for index {}: column {} value {} isn't an integer key",
                    tableName, idx.indexName(), idx.columnName(), value);
            }
        }
    }

    /**
     * Removes the index entry pointing at (pageId, slot) - the physical
     * location a row is about to stop occupying, whether because it was
     * DELETEd outright or is the old version being replaced by an UPDATE.
     * Uses the row's CURRENT (pre-write) values, since that's what's
     * actually stored in the index right now - an UPDATE that changes the
     * indexed column itself must remove the entry keyed by the OLD value,
     * not the new one, or it would silently leave the real stale entry
     * behind while removing nothing.
     */
    private void maintainIndexesOnDelete(String tableName, Tuple tuple, long pageId, int slot) {
        List<IndexEntry> tableIndexes = indexesByTable.get(tableName);
        if (tableIndexes == null) return;
        for (IndexEntry idx : tableIndexes) {
            Object value = findColumnValue(tuple, idx.columnName());
            Long key = toIndexKey(value);
            if (key != null) {
                idx.index().delete(key, new BTreePage.RID(pageId, slot));
            }
        }
    }

    /** Only Integer/Long column values can be B+Tree keys right now - see executeCreateIndex's javadoc. */
    private Long toIndexKey(Object value) {
        if (value instanceof Integer i) return i.longValue();
        if (value instanceof Long l) return l;
        return null;
    }

    /**
     * Real UPDATE, replacing the previous hardcoded "Updated 0 rows" stub.
     * Scans with positions so each matching visible row can be targeted at
     * its exact (pageId, slot) for the MVCC update (tombstone + reinsert).
     * The old version's index entry is now genuinely removed (BTreeIndex
     * gained real delete - see PROJECT_PLAN.md Phase A), not just left as
     * MVCC-filtered dead weight the way it was before.
     */
    private QueryResult executeUpdate(UpdateStatement stmt, Transaction txn) throws DeadlockException {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        int updated = 0;
        for (HeapTable.PositionedRow row : table.scanPositioned()) {
            if (!MVCCVisibility.isVisible(row.stored(), txn.getSnapshot(), transactionManager)) {
                continue;
            }
            byte[] oldPayload = MVCCVisibility.readPayload(row.stored());
            Tuple tuple = Tuple.deserialize(oldPayload);
            if (!matchesWhere(tuple, stmt.where(), txn)) {
                continue;
            }

            // Snapshot the old values BEFORE applying assignments - needed to
            // remove the old index entry by its actual current key, in case
            // an assignment changes the very column that's indexed.
            Tuple oldTuple = Tuple.deserialize(oldPayload);

            for (Assignment assignment : stmt.assignments()) {
                setColumnValue(tuple, assignment.column(), parseLiteral(assignment.value()));
            }
            byte[] newPayload = tuple.serialize();

            HeapTable.InsertResult newVersion = table.updateMvcc(row.pageId(), row.slot(), newPayload, txn.getXID(),
                txn.getSnapshot(), transactionManager, transactionManager.getLockManager());
            walManager.logUpdate(stmt.tableName(), txn.getXID(), row.pageId(), row.slot(), oldPayload, newPayload);
            maintainIndexesOnDelete(stmt.tableName(), oldTuple, row.pageId(), row.slot());
            maintainIndexesOnWrite(stmt.tableName(), tuple, newVersion.pageId, newVersion.slot);
            recordUndo(new UndoAction.UndoUpdate(stmt.tableName(), row.pageId(), row.slot(), newVersion.pageId, newVersion.slot));
            updated++;
        }

        return QueryResult.success("Updated " + updated + " row(s)");
    }

    /**
     * Real DELETE, replacing the previous hardcoded "Deleted 0 rows" stub.
     * The row's index entries are now genuinely removed (BTreeIndex gained
     * real delete - see PROJECT_PLAN.md Phase A), not left to accumulate
     * as MVCC-filtered dead weight forever.
     */
    private QueryResult executeDelete(DeleteStatement stmt, Transaction txn) throws DeadlockException {
        HeapTable table = tables.get(stmt.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        int deleted = 0;
        for (HeapTable.PositionedRow row : table.scanPositioned()) {
            if (!MVCCVisibility.isVisible(row.stored(), txn.getSnapshot(), transactionManager)) {
                continue;
            }
            Tuple tuple = Tuple.deserialize(MVCCVisibility.readPayload(row.stored()));
            if (!matchesWhere(tuple, stmt.where(), txn)) {
                continue;
            }

            boolean removed = table.deleteMvcc(row.pageId(), row.slot(), txn.getXID(),
                txn.getSnapshot(), transactionManager, transactionManager.getLockManager());
            if (removed) {
                walManager.logDelete(stmt.tableName(), txn.getXID(), row.pageId(), row.slot());
                maintainIndexesOnDelete(stmt.tableName(), tuple, row.pageId(), row.slot());
                recordUndo(new UndoAction.UndoDelete(stmt.tableName(), row.pageId(), row.slot()));
                deleted++;
            }
        }

        return QueryResult.success("Deleted " + deleted + " row(s)");
    }

    private QueryResult executeDropTable(DropTableStatement stmt) {
        if (!tables.containsKey(stmt.tableName())) {
            return QueryResult.error("Table not found: " + stmt.tableName());
        }

        tables.remove(stmt.tableName());
        tableColumns.remove(stmt.tableName());
        return QueryResult.success("Table dropped: " + stmt.tableName());
    }

    /**
     * A view is never materialized - just its defining query, remembered
     * under a name. See executeSelectOverView for how a SELECT against a
     * view actually runs. No circular-view-definition check: a view that
     * (directly or transitively) selects from itself would recurse until
     * a StackOverflowError, a real, named gap rather than a silent
     * infinite loop with no diagnostic.
     */
    private QueryResult executeCreateView(CreateViewStatement stmt) {
        if (tables.containsKey(stmt.viewName())) {
            return QueryResult.error("A table already exists with that name: " + stmt.viewName());
        }
        if (views.containsKey(stmt.viewName())) {
            return QueryResult.error("View already exists: " + stmt.viewName());
        }
        views.put(stmt.viewName(), stmt.query());
        return QueryResult.success("View created: " + stmt.viewName());
    }

    private QueryResult executeDropView(DropViewStatement stmt) {
        if (!views.containsKey(stmt.viewName())) {
            return QueryResult.error("View not found: " + stmt.viewName());
        }
        views.remove(stmt.viewName());
        return QueryResult.success("View dropped: " + stmt.viewName());
    }

    /**
     * Returns one row per table, with a single "table_name" column - not a
     * message string. This used to return QueryResult.success("Tables: a, b")
     * or QueryResult.success("No tables found"), which worked fine for the
     * in-process CLI (which printed QueryResult.toString() directly) but
     * would have been silently useless over standard JDBC: Statement.execute()
     * only exposes a boolean ("was there a ResultSet") and an int row count,
     * not an arbitrary message string, so the actual table names would never
     * have reached a real JDBC caller. A proper rows-based result is both
     * more correct (this is fundamentally a query, not a command) and the
     * only way it can flow through a ResultSet.
     */
    private QueryResult executeShowTables() {
        List<Tuple> rows = new ArrayList<>();
        for (String tableName : tables.keySet()) {
            Tuple row = new Tuple();
            row.addValue("table_name", tableName);
            rows.add(row);
        }
        return QueryResult.success(rows);
    }

    /**
     * A real, queryable snapshot of engine internals - the metrics
     * themselves already existed (buffer pool hit ratio, WAL LSN, and so
     * on were already tracked and shown by the CLI's own \status
     * command), just not reachable via SQL, meaning nothing but that one
     * specific CLI command could ever see them. SHOW STATS exposes the
     * same numbers as an ordinary query result, so any client (psql, a
     * monitoring script, a BI tool) that can run SQL can read them too -
     * a genuine step toward a real pg_stat-style interface, though a
     * proper pg_stat_activity/pg_stat_user_tables-shaped SQL surface with
     * per-table and per-connection breakdowns is real further work, not
     * attempted here.
     */
    private QueryResult executeShowStats() {
        List<Tuple> rows = new ArrayList<>();
        addStat(rows, "buffer_pool_hit_ratio", String.format("%.4f", bufferPool.getCacheHitRatio()));
        addStat(rows, "buffer_pool_cache_size", String.valueOf(bufferPool.getCacheSize()));
        addStat(rows, "wal_current_lsn", String.valueOf(walManager.getCurrentLSN()));
        addStat(rows, "table_count", String.valueOf(tables.size()));
        addStat(rows, "view_count", String.valueOf(views.size()));
        addStat(rows, "index_count", String.valueOf(indexesByName.size()));
        addStat(rows, "oldest_active_xid", String.valueOf(transactionManager.getOldestActiveXid()));
        return QueryResult.success(rows);
    }

    private void addStat(List<Tuple> rows, String metricName, String value) {
        Tuple row = new Tuple();
        row.addValue("metric", metricName);
        row.addValue("value", value);
        rows.add(row);
    }

    /**
     * Column projection, factored out of executeSelect so it can stay
     * unchanged while the surrounding method switched to MVCC scanning.
     * Delegates to findColumnValue so joined (qualified-column) tuples are
     * projected correctly too, using the same resolution rules WHERE uses.
     */
    private Tuple project(Tuple tuple, List<String> requestedColumns) {
        if (requestedColumns.isEmpty() || requestedColumns.get(0).equals("*")) {
            return tuple;
        }
        Tuple projected = new Tuple();
        for (String colName : requestedColumns) {
            projected.addValue(colName, findColumnValue(tuple, colName));
        }
        return projected;
    }

    private record WherePredicate(String column, String operator, String value, boolean isNumeric) {}

    /**
     * Parses a single-predicate WHERE clause into (column, operator, value).
     *
     * Fixes a real bug found while building the planner: the previous
     * operator-detection order was {"=", ">", "<", ">=", "<=", "!="} and
     * picked the FIRST operator whose characters appeared anywhere in the
     * clause. Since "age>=30" contains "=" as a substring of ">=", it always
     * matched "=" first and split on it, producing column="age>" (with the
     * ">" wrongly stuck to the column name) and value="30". Longer operators
     * must be checked before their single-character prefixes.
     */
    private WherePredicate parseWhere(String whereClause) {
        if (whereClause == null || whereClause.isEmpty()) {
            return null;
        }

        String[] operatorsLongestFirst = {">=", "<=", "!=", "=", ">", "<"};
        String operator = null;
        for (String op : operatorsLongestFirst) {
            if (whereClause.contains(op)) {
                operator = op;
                break;
            }
        }
        if (operator == null) {
            return null;
        }

        String[] parts = whereClause.split(operator, 2);
        if (parts.length != 2) {
            return null;
        }

        String column = parts[0].trim();
        String value = parts[1].trim();
        if (value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length() - 1);
        }

        boolean isNumeric;
        try {
            Integer.parseInt(value);
            isNumeric = true;
        } catch (NumberFormatException e) {
            isNumeric = false;
        }

        return new WherePredicate(column, operator, value, isNumeric);
    }

    /**
     * WHERE-clause evaluation over a real expression tree - replaces the
     * previous design, which captured the whole clause as raw text and
     * treated it as a single flat "column op literal" predicate no matter
     * what it actually contained. AND/OR/NOT/LIKE/IN were all accepted by
     * the grammar but silently misevaluated: a compound AND condition like
     * "(age>25) AND status='active'" returned WRONG rows rather than an
     * error, because the old code just grabbed whichever comparison
     * operator it found anywhere in the raw text and ignored the rest.
     * Found by testing this specific case, not by inspection - see
     * PROGRESS.md.
     *
     * outerRow is non-null only when evaluating a correlated subquery's
     * own WHERE clause (see runSubquerySelect) - consulted as a fallback
     * whenever a column reference isn't found in the row actually being
     * tested, which is exactly what correlation means: "not a column of
     * my own table."
     */
    private boolean matchesWhere(Tuple tuple, WhereExpr where, Transaction txn) {
        return matchesWhere(tuple, where, txn, null);
    }

    private boolean matchesWhere(Tuple tuple, WhereExpr where, Transaction txn, Tuple outerRow) {
        if (where == null) {
            return true;
        }
        return evaluateWhereExpr(tuple, where, txn, outerRow);
    }

    private boolean evaluateWhereExpr(Tuple row, WhereExpr expr, Transaction txn, Tuple outerRow) {
        if (expr instanceof WhereExpr.And and) {
            return evaluateWhereExpr(row, and.left(), txn, outerRow) && evaluateWhereExpr(row, and.right(), txn, outerRow);
        }
        if (expr instanceof WhereExpr.Or or) {
            return evaluateWhereExpr(row, or.left(), txn, outerRow) || evaluateWhereExpr(row, or.right(), txn, outerRow);
        }
        if (expr instanceof WhereExpr.Not not) {
            return !evaluateWhereExpr(row, not.inner(), txn, outerRow);
        }
        if (expr instanceof WhereExpr.Comparison cmp) {
            return evaluateComparison(row, cmp, outerRow);
        }
        if (expr instanceof WhereExpr.ColumnComparison colCmp) {
            Object left = resolveColumnValue(row, colCmp.leftColumn(), outerRow);
            Object right = resolveColumnValue(row, colCmp.rightColumn(), outerRow);
            return left != null && right != null && compareValues(left, colCmp.operator(), right);
        }
        if (expr instanceof WhereExpr.Like like) {
            Object value = resolveColumnValue(row, like.column(), outerRow);
            return value != null && matchesLikePattern(value.toString(), stripQuotes(like.pattern()));
        }
        if (expr instanceof WhereExpr.InList inList) {
            Object value = resolveColumnValue(row, inList.column(), outerRow);
            if (value == null) {
                return false;
            }
            boolean found = false;
            for (String literalText : inList.values()) {
                if (compareValues(value, "=", stripQuotes(literalText))) {
                    found = true;
                    break;
                }
            }
            return inList.negated() != found;
        }
        if (expr instanceof WhereExpr.InSubquery inSub) {
            Object value = resolveColumnValue(row, inSub.column(), outerRow);
            if (value == null) {
                return false;
            }
            List<Object> subValues = evaluateSubqueryValues(inSub.subquery(), txn, row);
            boolean found = subValues.stream().anyMatch(sv -> sv != null && compareValues(value, "=", sv));
            return inSub.negated() != found;
        }
        if (expr instanceof WhereExpr.ScalarSubqueryComparison scalarCmp) {
            Object value = resolveColumnValue(row, scalarCmp.column(), outerRow);
            if (value == null) {
                return false;
            }
            Object subValue = evaluateScalarSubqueryValue(scalarCmp.subquery(), txn, row);
            return subValue != null && compareValues(value, scalarCmp.operator(), subValue);
        }
        if (expr instanceof WhereExpr.ExistsSubquery existsSub) {
            boolean hasRows = evaluateSubqueryHasRows(existsSub.subquery(), txn, row);
            return existsSub.negated() != hasRows;
        }
        throw new IllegalStateException("Unhandled WhereExpr type: " + expr.getClass());
    }

    /** Preserves a pre-existing, non-standard fallback: if the named column isn't found anywhere, an equality comparison still matches if ANY column in the row equals the literal. Ordering comparisons have no sensible version of this and simply don't match. */
    private boolean evaluateComparison(Tuple row, WhereExpr.Comparison cmp, Tuple outerRow) {
        Object value = resolveColumnValue(row, cmp.column(), outerRow);
        WherePredicate pred = toPredicate(cmp.column(), cmp.operator(), cmp.literal());
        if (value != null) {
            return evaluatePredicate(value.toString(), pred);
        }
        if (!pred.operator().equals("=")) {
            return false;
        }
        for (int i = 0; i < row.size(); i++) {
            Object anyValue = row.getValue(i);
            if (anyValue != null && evaluatePredicate(anyValue.toString(), pred)) {
                return true;
            }
        }
        return false;
    }

    private Object resolveColumnValue(Tuple row, String column, Tuple outerRow) {
        Object value = findColumnValue(row, column);
        if (value == null && outerRow != null) {
            value = findColumnValue(outerRow, column);
        }
        return value;
    }

    private WherePredicate toPredicate(String column, String operator, String literalText) {
        String value = stripQuotes(literalText);
        boolean isNumeric;
        try {
            Double.parseDouble(value);
            isNumeric = true;
        } catch (NumberFormatException e) {
            isNumeric = false;
        }
        return new WherePredicate(column, operator, value, isNumeric);
    }

    private String stripQuotes(String literalText) {
        if (literalText.startsWith("'") && literalText.endsWith("'")) {
            return literalText.substring(1, literalText.length() - 1);
        }
        return literalText;
    }

    /** Numeric-aware equality/ordering between two already-resolved values (as opposed to evaluatePredicate, which compares a resolved value against raw literal text). */
    private boolean compareValues(Object a, String operator, Object b) {
        Double da = asNumberOrNull(a);
        Double db = asNumberOrNull(b);
        if (da != null && db != null) {
            return switch (operator) {
                case "=" -> da.doubleValue() == db.doubleValue();
                case "!=" -> da.doubleValue() != db.doubleValue();
                case ">" -> da > db;
                case ">=" -> da >= db;
                case "<" -> da < db;
                case "<=" -> da <= db;
                default -> false;
            };
        }
        String sa = a.toString();
        String sb = b.toString();
        return switch (operator) {
            case "=" -> sa.equals(sb);
            case "!=" -> !sa.equals(sb);
            default -> false; // ordering comparisons on non-numeric values aren't supported
        };
    }

    private Double asNumberOrNull(Object value) {
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** SQL LIKE semantics: % matches any sequence (including empty), _ matches exactly one character. */
    private boolean matchesLikePattern(String value, String pattern) {
        StringBuilder regex = new StringBuilder();
        for (char c : pattern.toCharArray()) {
            switch (c) {
                case '%' -> regex.append(".*");
                case '_' -> regex.append(".");
                case '.', '*', '+', '?', '(', ')', '[', ']', '{', '}', '^', '$', '|', '\\' -> regex.append('\\').append(c);
                default -> regex.append(c);
            }
        }
        return value.matches(regex.toString());
    }

    // --- subqueries ---

    /**
     * Runs a subquery for WHERE-clause evaluation (IN / EXISTS / scalar
     * comparison). Supports correlation: if outerRow is non-null, any
     * column in the subquery's own WHERE clause not found in the row
     * currently being tested falls back to outerRow - the same mechanism
     * findColumnValue already uses for qualified-column fallback in
     * joins, just reaching one level further out.
     *
     * Scope, stated plainly: this handles the common case (a plain scan
     * with a WHERE clause). JOINs or GROUP BY/aggregates inside a
     * subquery still work, but not combined with correlation - a
     * correlated reference inside a joined or aggregated subquery isn't
     * threaded through here. That's a real, further limitation, not
     * silently miscalculated: such a reference just wouldn't resolve,
     * behaving as if the column doesn't exist (the same well-defined
     * "not found" behavior as any other unresolvable column reference).
     */
    private QueryResult runSubquerySelect(SelectStatement subquery, Transaction txn, Tuple outerRow) {
        if (subquery.joins() != null && !subquery.joins().isEmpty()) {
            return executeJoinedSelect(subquery, txn);
        }
        HeapTable table = tables.get(subquery.tableName());
        if (table == null) {
            return QueryResult.error("Table not found: " + subquery.tableName());
        }
        if (!subquery.aggregates().isEmpty() || !subquery.groupBy().isEmpty()) {
            return executeAggregateSelect(subquery, txn, table);
        }

        List<Tuple> tuples = new ArrayList<>();
        for (byte[] raw : table.scanMvcc(txn.getSnapshot(), transactionManager)) {
            // Qualifying the inner row (the same mechanism JOIN uses) is what
            // makes correlation safe: "orders.customer_id = customers.id"
            // then resolves "orders.customer_id" by an exact match against
            // this row, and "customers.id" correctly finds no match here at
            // all (falling through to outerRow) instead of risking an
            // accidental match against some unrelated same-named column of
            // this table (both "orders" and "customers" happen to have an
            // "id" column - without qualifying, a naive bare-name fallback
            // could resolve "customers.id" against orders' OWN id by mistake).
            Tuple tuple = qualify(Tuple.deserialize(raw), subquery.tableName());
            if (matchesWhere(tuple, subquery.where(), txn, outerRow)) {
                tuples.add(project(tuple, subquery.columns()));
            }
        }
        return QueryResult.success(tuples);
    }

    /** Every value from the subquery's (single) result column - for IN (SELECT ...). */
    private List<Object> evaluateSubqueryValues(SelectStatement subquery, Transaction txn, Tuple outerRow) {
        QueryResult result = runSubquerySelect(subquery, txn, outerRow);
        if (!result.isSuccess()) {
            // Must propagate, not silently treat as "no matching values" - a
            // subquery that fails (e.g. references an unsupported view
            // combination) would otherwise make its enclosing WHERE clause
            // silently evaluate as if nothing matched, with no error at all.
            throw new IllegalStateException("Subquery failed: " + result.getError());
        }
        if (result.getRows() == null) {
            return List.of();
        }
        List<Object> values = new ArrayList<>();
        for (Tuple row : result.getRows()) {
            if (row.size() > 0) {
                values.add(row.getValue(0));
            }
        }
        return values;
    }

    /** For a scalar comparison, e.g. "salary > (SELECT AVG(salary) FROM employees)" - errors if the subquery returns more than one row, matching standard SQL scalar-subquery semantics. */
    private Object evaluateScalarSubqueryValue(SelectStatement subquery, Transaction txn, Tuple outerRow) {
        List<Object> values = evaluateSubqueryValues(subquery, txn, outerRow);
        if (values.size() > 1) {
            throw new IllegalStateException("Scalar subquery returned more than one row");
        }
        return values.isEmpty() ? null : values.get(0);
    }

    private boolean evaluateSubqueryHasRows(SelectStatement subquery, Transaction txn, Tuple outerRow) {
        QueryResult result = runSubquerySelect(subquery, txn, outerRow);
        if (!result.isSuccess()) {
            throw new IllegalStateException("Subquery failed: " + result.getError());
        }
        return result.getRows() != null && !result.getRows().isEmpty();
    }

    private Object findColumnValue(Tuple tuple, String columnName) {
        List<String> columnNames = tuple.getColumnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(columnName)) {
                return tuple.getValue(i);
            }
        }
        // Convenience fallback for joined tuples, whose columns are all
        // qualified as "table.column": a bare "amount" resolves to
        // "orders.amount" if some qualified column ends with ".amount".
        // This is a simplification, stated plainly: if two joined tables
        // both have a column with that bare name, whichever appears first
        // in the merged tuple wins silently rather than raising a real
        // SQL-style "ambiguous column reference" error. Use the qualified
        // form (table.column) to be unambiguous.
        if (!columnName.contains(".")) {
            String suffix = "." + columnName.toLowerCase();
            for (int i = 0; i < columnNames.size(); i++) {
                if (columnNames.get(i).toLowerCase().endsWith(suffix)) {
                    return tuple.getValue(i);
                }
            }
        } else {
            // The reverse direction: a QUALIFIED request ("orders.customer_id")
            // against a PLAIN, unqualified tuple (customer_id) - the shape a
            // correlated subquery predicate takes when compared against its
            // own (non-joined) FROM table's scan, since only JOIN's qualify()
            // actually prefixes column names; a plain table scan never does.
            String unqualified = columnName.substring(columnName.lastIndexOf('.') + 1);
            for (int i = 0; i < columnNames.size(); i++) {
                if (columnNames.get(i).equalsIgnoreCase(unqualified)) {
                    return tuple.getValue(i);
                }
            }
        }
        return null;
    }

    private boolean evaluatePredicate(String actualStr, WherePredicate pred) {
        if (pred.isNumeric()) {
            try {
                double actual = Double.parseDouble(actualStr);
                double expected = Double.parseDouble(pred.value());
                return switch (pred.operator()) {
                    case "=" -> actual == expected;
                    case "!=" -> actual != expected;
                    case ">" -> actual > expected;
                    case ">=" -> actual >= expected;
                    case "<" -> actual < expected;
                    case "<=" -> actual <= expected;
                    default -> false;
                };
            } catch (NumberFormatException e) {
                // actualStr wasn't numeric after all - fall through to string comparison
            }
        }
        return switch (pred.operator()) {
            case "=" -> actualStr.equals(pred.value());
            case "!=" -> !actualStr.equals(pred.value());
            default -> false; // ordering comparisons on non-numeric values aren't supported
        };
    }

    /** Mutates tuple in place - Tuple.getValues()/getColumnNames() return live backing lists. */
    private void setColumnValue(Tuple tuple, String column, Object newValue) {
        List<String> columnNames = tuple.getColumnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(column)) {
                tuple.getValues().set(i, newValue);
                return;
            }
        }
    }

    private static final java.util.regex.Pattern NEXTVAL_PATTERN = java.util.regex.Pattern.compile("(?i)nextval\\('([^']+)'\\)");
    private static final java.util.regex.Pattern CURRVAL_PATTERN = java.util.regex.Pattern.compile("(?i)currval\\('([^']+)'\\)");

    /** Resolves a raw value string that might be a plain literal, or a nextval('seq')/currval('seq') call - used for both column defaults and explicit values in an INSERT's VALUES list, since both need the exact same resolution logic. */
    private Object resolveValue(String raw) {
        java.util.regex.Matcher nextvalMatch = NEXTVAL_PATTERN.matcher(raw);
        if (nextvalMatch.matches()) {
            return callNextval(nextvalMatch.group(1));
        }
        java.util.regex.Matcher currvalMatch = CURRVAL_PATTERN.matcher(raw);
        if (currvalMatch.matches()) {
            return callCurrval(currvalMatch.group(1));
        }
        return parseLiteral(raw);
    }

    private long callNextval(String sequenceName) {
        Sequence seq = sequences.get(sequenceName);
        if (seq == null) {
            throw new IllegalStateException("Sequence not found: " + sequenceName);
        }
        long value = seq.nextValue();
        session.get().lastNextvalBySequence.put(sequenceName, value);
        return value;
    }

    private long callCurrval(String sequenceName) {
        Long value = session.get().lastNextvalBySequence.get(sequenceName);
        if (value == null) {
            throw new IllegalStateException("currval(\"" + sequenceName + "\") called before nextval() was called for that sequence in this session");
        }
        return value;
    }

    private Object parseLiteral(String value) {
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(value);
        }
        if (value.equalsIgnoreCase("null")) {
            return null;
        }
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            }
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return value;
        }
    }
}
