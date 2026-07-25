package com.stratosdb.jdbc;

import java.sql.SQLFeatureNotSupportedException;

/** Shared by the proxy-backed JDBC classes (Connection/Statement/ResultSet) for their fallback case. */
final class JdbcSupport {
    private JdbcSupport() {}

    static SQLFeatureNotSupportedException notSupported(String interfaceName, String methodName) {
        return new SQLFeatureNotSupportedException(
            interfaceName + "." + methodName + "() is not implemented by StratosDB's JDBC driver. "
            + "This is a minimal driver (see stratosdb-jdbc's module javadoc for what is supported).");
    }
}
