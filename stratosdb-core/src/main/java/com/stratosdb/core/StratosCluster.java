package com.stratosdb.core;

import com.stratosdb.sql.executor.DatabaseClusterHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A real, multi-database StratosDB deployment - many independently-named,
 * independently-isolated databases (each with its own real WAL, buffer
 * pool, and table catalog - a genuinely separate {@link StratosDB}
 * instance under the hood, not a shared catalog with a name-prefix hack)
 * served from one cluster data directory and, once wired into
 * {@code StdWireServer}, one listening port. This is the same real shape
 * PostgreSQL's own "cluster" has (one postmaster process, many databases
 * under one {@code PGDATA} directory) - and a genuinely different thing
 * from a single, standalone {@link StratosDB} instance, which remains
 * fully, unconditionally supported for every existing caller: this class
 * is purely additive, built entirely on top of {@link StratosDB}'s own
 * real, already-existing, unmodified constructor.
 *
 * Each database gets its own real subdirectory under the cluster's own
 * data directory (e.g. {@code clusterDataDir/mydb/}), matching the same
 * familiar shape PostgreSQL's own real {@code base/<oid>/} per-database
 * layout has, just with a real, readable directory name instead of an
 * OID.
 *
 * A fresh cluster (an empty or not-yet-existing data directory) always
 * starts with one real, already-created, connectable database named
 * {@value #DEFAULT_DATABASE} - matching PostgreSQL's own real convention
 * of always having a "postgres" database ready immediately after a fresh
 * install, so a client can always connect to *something* without first
 * needing an existing session to run {@code CREATE DATABASE} from.
 */
public class StratosCluster implements DatabaseClusterHost {
    private static final Logger LOG = LoggerFactory.getLogger(StratosCluster.class);

    /** PostgreSQL's own real default/administrative database is "postgres" - this project's own, already-established real convention (see StratosDriver's own javadoc on the JDBC URL's default database) is "stratos". */
    public static final String DEFAULT_DATABASE = "stratos";

    private final File clusterDataDirectory;
    private final DatabaseConfig templateConfig;
    private final Map<String, StratosDB> databases = new ConcurrentHashMap<>();

    public StratosCluster(DatabaseConfig templateConfig) {
        this.templateConfig = templateConfig;
        this.clusterDataDirectory = new File(templateConfig.getDataDirectory());
        loadExistingDatabases();
        if (databases.isEmpty()) {
            createDatabase(DEFAULT_DATABASE);
        }
        LOG.info("StratosCluster initialized at {} with database(s): {}",
            clusterDataDirectory.getPath(), listDatabaseNames());
    }

    /** Real recovery, not just discovery: each existing database subdirectory gets a real {@link StratosDB} instance constructed for it, which itself runs real WAL recovery exactly as it always has for a single, standalone instance. */
    private void loadExistingDatabases() {
        if (!clusterDataDirectory.isDirectory()) {
            return; // brand-new cluster - nothing to load yet; the constructor's own caller below creates the real default database
        }
        File[] subdirs = clusterDataDirectory.listFiles(File::isDirectory);
        if (subdirs == null) {
            return;
        }
        for (File subdir : subdirs) {
            String name = subdir.getName();
            LOG.info("StratosCluster: found existing database '{}', recovering", name);
            databases.put(name, openDatabase(name));
        }
    }

    private StratosDB openDatabase(String name) {
        DatabaseConfig perDbConfig = templateConfig.copyWithDataDirectory(
            new File(clusterDataDirectory, name).getPath());
        StratosDB db = new StratosDB(perDbConfig);
        db.setCluster(this, name);
        return db;
    }

    /**
     * Real {@code CREATE DATABASE}: makes the real subdirectory, opens a
     * real, new, independent {@link StratosDB} instance rooted there
     * (a real, fresh WAL/buffer-pool/catalog of its own, not merely a
     * name registered somewhere), and registers it. Synchronized against
     * {@link #dropDatabase} - both mutate the same shared registry and
     * the filesystem beneath it, and must never interleave. Returns
     * nothing (matching the {@link DatabaseClusterHost} interface
     * exactly) - a caller that needs the resulting {@link StratosDB}
     * instance itself (as {@code StdWireServer} does, to route a fresh
     * connection to it) calls {@link #getDatabase} right afterward,
     * since this method has already registered it by the time it
     * returns.
     */
    @Override
    public synchronized void createDatabase(String name) {
        validateDatabaseName(name);
        if (databases.containsKey(name)) {
            throw new IllegalStateException("database \"" + name + "\" already exists");
        }
        File dbDir = new File(clusterDataDirectory, name);
        if (!dbDir.exists() && !dbDir.mkdirs()) {
            throw new IllegalStateException("could not create data directory for database \"" + name + "\"");
        }
        StratosDB db = openDatabase(name);
        databases.put(name, db);
        LOG.info("StratosCluster: created database '{}'", name);
    }

    /**
     * Real {@code DROP DATABASE}: a real, genuine safety rule mirrors
     * PostgreSQL's own real behavior exactly - you cannot drop the
     * database a connection is currently using (PostgreSQL's own real
     * error: "cannot drop the currently open database"). The caller
     * (see {@code ExecutorEngine}'s own handling) supplies the requesting
     * connection's own current database name for this check - there is
     * no implicit, global "current database" at the cluster level, since
     * different connections can each be using a different one
     * simultaneously.
     */
    @Override
    public synchronized void dropDatabase(String name, String currentDatabaseName) {
        if (name.equals(currentDatabaseName)) {
            throw new IllegalStateException(
                "cannot drop the currently open database \"" + name + "\" - connect to a different database first");
        }
        StratosDB db = databases.get(name);
        if (db == null) {
            throw new IllegalStateException("database \"" + name + "\" does not exist");
        }
        db.shutdown();
        databases.remove(name);
        deleteRecursively(new File(clusterDataDirectory, name));
        LOG.info("StratosCluster: dropped database '{}'", name);
    }

    public StratosDB getDatabase(String name) {
        return databases.get(name);
    }

    public boolean hasDatabase(String name) {
        return databases.containsKey(name);
    }

    /** Real, clean shutdown of every database this cluster manages - each one's own real WAL/buffer-pool close, exactly as a plain, standalone StratosDB.shutdown() already does. */
    public void shutdown() {
        for (StratosDB database : databases.values()) {
            database.shutdown();
        }
    }

    @Override
    public List<String> listDatabaseNames() {
        return databases.keySet().stream().sorted().toList();
    }

    /** Matches this engine's own real identifier rules elsewhere (a plain IDENTIFIER token) - not full SQL-identifier generality, but enough to safely double as a real filesystem directory name with no escaping concerns at all. */
    private static void validateDatabaseName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("database name cannot be blank");
        }
        if (!name.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
            throw new IllegalArgumentException(
                "invalid database name \"" + name + "\" - must start with a letter or underscore "
                    + "and contain only letters, digits, and underscores");
        }
    }

    private static void deleteRecursively(File file) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }
}
