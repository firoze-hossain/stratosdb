package com.stratosdb.sql.ast;

/**
 * PROMOTE - stops this replica from following its own primary and
 * begins accepting writes directly as a new primary itself. Parsed as
 * a real SQL statement so it goes through the normal parsing path, but
 * intercepted by StdWireServer before ever reaching ExecutorEngine's
 * own execute() - the actual work (stopping a ReplicationClient) lives
 * entirely outside ExecutorEngine's own knowledge, in stratosdb-network
 * (see StdWireServer.setReplicationClient's own javadoc). If this
 * statement somehow does reach ExecutorEngine directly (e.g. called
 * via db.execute() with no StdWireServer involved at all), it reports
 * a clear error rather than silently doing nothing.
 */
public record PromoteStatement() implements Statement {}
