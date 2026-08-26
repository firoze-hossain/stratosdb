package com.stratosdb.sql.ast;

/**
 * CHECKPOINT - forces every dirty page to disk, then archives (if WAL
 * archiving is enabled - see WALManager.setWalArchiveDirectory's own
 * javadoc) and truncates the active WAL. The real, remote-triggerable
 * hook PitrBackup uses: a base backup needs this exact "everything is
 * now durably on disk, and the WAL segment covering everything up to
 * this point is safely archived" guarantee before it's safe to copy
 * the data directory's own files.
 */
public record CheckpointStatement() implements Statement {}
