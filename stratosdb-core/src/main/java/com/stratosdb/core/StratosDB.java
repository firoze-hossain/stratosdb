package com.stratosdb.core;

import com.stratosdb.sql.executor.ExecutorEngine;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.buffer.BufferPoolManager;
import com.stratosdb.storage.disk.DiskManager;
import com.stratosdb.storage.wal.WALManager;
import com.stratosdb.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StratosDB {
    private static final Logger LOG = LoggerFactory.getLogger(StratosDB.class);

    private final DatabaseConfig config;
    private final DiskManager diskManager;
    private final BufferPoolManager bufferPool;
    private final WALManager walManager;
    private final TransactionManager transactionManager;
    private final ExecutorEngine executor;

    private boolean running = false;

    public StratosDB(DatabaseConfig config) {
        this.config = config;

        // Initialize components
        this.diskManager = new DiskManager(config.getDataDirectory());
        this.bufferPool = new BufferPoolManager(config.getBufferPoolSize(), diskManager);
        this.walManager = new WALManager(config.getDataDirectory());
        this.transactionManager = new TransactionManager();
        this.executor = new ExecutorEngine(bufferPool, walManager, transactionManager);

        // Recover from WAL
        this.walManager.recover(diskManager);

        LOG.info("StratosDB initialized at {}", config.getDataDirectory());
    }

    public QueryResult execute(String sql) {
        LOG.debug("Executing: {}", sql);
        return executor.execute(sql);
    }

    /**
     * Marks this instance as "running" - a state flag for callers that want
     * to track it, nothing more. This does NOT open a network listener:
     * stratosdb-core has no networking dependency by design, so embedding
     * StratosDB as a plain library loads no socket code at all. To actually
     * accept network connections, wrap this instance in a
     * com.stratosdb.network.server.StratosServer from the stratosdb-network
     * module (which depends on core, not the other way around, so core
     * itself cannot start that server without a circular module
     * dependency): {@code new StratosServer(config.getPort(), this).start();}
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
        walManager.checkpoint();
        bufferPool.close(); // flushes every heap-table page and closes DiskManager's file handles
        walManager.close(); // WALManager owns a separate file handle for wal.log - bufferPool.close()
                             // never touched it, so it stayed open for the life of the process.
                             // Invisible on Linux (an open file can still be deleted/reused); on
                             // Windows the handle stays locked, which is exactly the class of bug
                             // this project's test suite hit before this file was ever wired in.
        LOG.info("StratosDB shutdown complete");
    }

    // Getters
    public DatabaseConfig getConfig() { return config; }
    public BufferPoolManager getBufferPool() { return bufferPool; }
    public WALManager getWalManager() { return walManager; }
    public TransactionManager getTransactionManager() { return transactionManager; }
    public ExecutorEngine getExecutor() { return executor; }
}