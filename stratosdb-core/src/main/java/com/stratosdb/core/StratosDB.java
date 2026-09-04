package com.stratosdb.core;

import com.stratosdb.sql.executor.ExecutorEngine;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.wal.WALManager;
import com.stratosdb.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class StratosDB {
    private static final Logger LOG = LoggerFactory.getLogger(StratosDB.class);

    private final DatabaseConfig config;
    private final DiskManager diskManager;
    private final BufferPoolManager bufferPool;
    private final WALManager walManager;
    private final TransactionManager transactionManager;
    private final ExecutorEngine executor;

    private boolean running = false;
    private ScheduledExecutorService autovacuumExecutor;
    private ScheduledFuture<?> autovacuumTask;

    public StratosDB(DatabaseConfig config) {
        this.config = config;

        // Initialize components
        this.diskManager = new DiskManager(config.getDataDirectory());
        this.bufferPool = new BufferPoolManager(config.getBufferPoolSize(), diskManager);
        this.walManager = new WALManager(config.getDataDirectory());
        this.transactionManager = new TransactionManager(config.getDataDirectory());
        this.executor = new ExecutorEngine(bufferPool, walManager, transactionManager, config.getDataDirectory());
        if (config.getSlowQueryThresholdMs() >= 0) {
            this.executor.setSlowQueryThresholdMs(config.getSlowQueryThresholdMs());
        }

        // Recover from WAL
        this.walManager.recover(diskManager);

        if (config.getAutovacuumIntervalMs() > 0) {
            startAutovacuum(config.getAutovacuumIntervalMs());
        }

        LOG.info("StratosDB initialized at {}", config.getDataDirectory());
    }

    public QueryResult execute(String sql) {
        LOG.debug("Executing: {}", sql);
        return executor.execute(sql);
    }

    /**
     * Call once per connection, right after real authentication succeeds -
     * see ExecutorEngine.setCurrentUser's own javadoc for the real
     * backward-compatibility guarantee this gives every existing caller
     * that never calls this at all.
     */
    public void setCurrentUser(String username) {
        executor.setCurrentUser(username);
    }

    /** See ExecutorEngine.RoleCredentialSink's own javadoc for why this bridge exists and what it's for. */
    public void setRoleCredentialSink(ExecutorEngine.RoleCredentialSink sink) {
        executor.setRoleCredentialSink(sink);
    }

    /** Called once, by {@code StratosCluster}, immediately after constructing this instance - see {@code DatabaseClusterHost}'s own javadoc for the full account of why {@code CREATE DATABASE}/{@code DROP DATABASE}/{@code SHOW DATABASES} need this at all. Never called at all for a plain, standalone instance constructed directly (every existing test and internal tool) - those three statements then honestly refuse instead. */
    public void setCluster(com.stratosdb.sql.executor.DatabaseClusterHost clusterHost, String currentDatabaseName) {
        executor.setClusterHost(clusterHost, currentDatabaseName);
    }


    /**
     * Call when a connection/session ends (not between individual
     * statements) - see ExecutorEngine.closeSession's javadoc for why this
     * matters: a client that sends BEGIN and then just disconnects,
     * without COMMIT or ROLLBACK, would otherwise leave that transaction
     * "active" forever, permanently blocking VACUUM's horizon.
     */
    public void closeSession() {
        executor.closeSession();
    }

    /**
     * Runs one autovacuum pass: VACUUM on every current table, right now,
     * synchronously. Exposed directly (not just via the scheduled timer)
     * so it can be tested deterministically without waiting on a clock,
     * and so a caller wanting to force an immediate pass doesn't have to
     * stop and restart the scheduled one to do it.
     *
     * Each table's VACUUM runs through execute("VACUUM tableName") - the
     * exact same path a person typing that command would use. That's a
     * deliberate choice, not a shortcut: it means autovacuum carries
     * exactly the same concurrency characteristics manual VACUUM already
     * had (see PROGRESS.md for the full reasoning) rather than introducing
     * a new, separately-reasoned-about code path that bypasses the normal
     * statement-execution machinery.
     */
    public void runAutovacuumPass() {
        for (String tableName : executor.getTableNames()) {
            QueryResult result = execute("VACUUM " + tableName);
            if (result.isSuccess()) {
                LOG.debug("Autovacuum: {}", result.getMessage());
            } else {
                // One table failing (e.g. dropped mid-pass) must not stop the
                // rest of the pass from covering every other table.
                LOG.warn("Autovacuum failed for table {}: {}", tableName, result.getError());
            }
        }
    }

    /**
     * Starts a background daemon thread that calls runAutovacuumPass every
     * intervalMs. Safe to call again after stopAutovacuum; calling it while
     * already running replaces the previous schedule rather than running
     * two overlapping ones.
     */
    public synchronized void startAutovacuum(long intervalMs) {
        stopAutovacuum();
        autovacuumExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "stratos-autovacuum");
            t.setDaemon(true); // must never keep the JVM alive on its own
            return t;
        });
        autovacuumTask = autovacuumExecutor.scheduleWithFixedDelay(() -> {
            try {
                runAutovacuumPass();
            } catch (Exception e) {
                // The scheduler silently stops future runs if a task throws -
                // catching here keeps autovacuum running across a bad pass
                // instead of quietly dying after the first exception.
                LOG.error("Autovacuum pass failed unexpectedly", e);
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        LOG.info("Autovacuum started, running every {} ms", intervalMs);
    }

    public synchronized void stopAutovacuum() {
        if (autovacuumTask != null) {
            autovacuumTask.cancel(false);
            autovacuumTask = null;
        }
        if (autovacuumExecutor != null) {
            autovacuumExecutor.shutdownNow();
            autovacuumExecutor = null;
        }
    }

    /**
     * Marks this instance as "running" - a state flag for callers that want
     * to track it, nothing more. This does NOT open a network listener:
     * stratosdb-core has no networking dependency by design, so embedding
     * StratosDB as a plain library loads no socket code at all. To actually
     * accept network connections, wrap this instance in a
     * com.stratosdb.network.stdwire.StdWireServer from the stratosdb-network
     * module (which depends on core, not the other way around, so core
     * itself cannot start that server without a circular module
     * dependency): {@code new StdWireServer(config.getPort(), this).start();}
     * The previous version of this method logged "server started on port
     * X" without starting anything - fixed to say what actually happens.
     */
    public void startServer() {
        running = true;
        LOG.info("StratosDB marked as running (port {} configured - use stratosdb-network's "
            + "StratosServer to actually accept connections on it)", config.getPort());
    }

    public void shutdown() {
        LOG.info("Shutting down StratosDB...");
        running = false;
        stopAutovacuum();
        bufferPool.close(); // flushes every heap-table page and closes DiskManager's file handles
        walManager.close(); // checkpoints internally, then closes its own separate wal.log file
                             // handle - bufferPool.close() never touched that handle, so it used
                             // to stay open for the life of the process (invisible on Linux, a
                             // locked file on Windows - the bug this project's test suite hit
                             // before this method was fixed to call walManager.close() at all).
        transactionManager.close(); // closes the persisted commit log's own separate file handle
        LOG.info("StratosDB shutdown complete");
    }

    // Getters
    public DatabaseConfig getConfig() { return config; }
    public DiskManager getDiskManager() { return diskManager; }
    public BufferPoolManager getBufferPool() { return bufferPool; }
    public WALManager getWalManager() { return walManager; }
    public TransactionManager getTransactionManager() { return transactionManager; }
    public ExecutorEngine getExecutor() { return executor; }

    /** Puts this instance into (or out of) real, enforced read-only replica mode - see ExecutorEngine's own READ_ONLY_SAFE_STATEMENTS javadoc. The real mechanism PROMOTE (see StdWireServer.tryHandlePromoteStatement) flips off when this instance stops following a primary and starts accepting writes as one. */
    public void setReadOnly(boolean readOnly) {
        executor.setReadOnly(readOnly);
    }

    public boolean isReadOnly() {
        return executor.isReadOnly();
    }
}