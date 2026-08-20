package com.stratosdb.common.constants;

public class ProtocolConstants {
    /** StratosDB's own custom-protocol server default port. Deliberately not 5432 (real PostgreSQL's default) - this project is fully independent and doesn't want to imply otherwise by squatting on the same well-known port. */
    public static final int DEFAULT_PORT = 6582;
    /** The standard-wire (PostgreSQL wire protocol v3 compatible) server's default port when enabled alongside the custom protocol - one higher than DEFAULT_PORT, matching how the two servers' actual startup code picks a default. */
    public static final int DEFAULT_STDWIRE_PORT = DEFAULT_PORT + 1;
    /** Real physical (WAL-shipping) replication's default port, when a primary is started with --replication-port and no explicit port is given - one higher than DEFAULT_STDWIRE_PORT, matching the same "next port up" convention. */
    public static final int DEFAULT_REPLICATION_PORT = DEFAULT_STDWIRE_PORT + 1;
    public static final int MAX_MESSAGE_SIZE = 1024 * 1024; // 1MB
    public static final String PROTOCOL_VERSION = "3.0";
}
