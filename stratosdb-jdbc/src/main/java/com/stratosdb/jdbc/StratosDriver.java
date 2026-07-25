package com.stratosdb.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC driver for StratosDB. URL format: jdbc:stratos://host:port/
 * (a trailing path segment is accepted and ignored - StratosDB has no
 * multiple-databases-per-server concept yet, just one data directory per
 * server process).
 *
 * Registers itself with DriverManager on class load, the standard JDBC
 * pattern: {@code Class.forName("com.stratosdb.jdbc.StratosDriver")} (or
 * just referencing the class) followed by
 * {@code DriverManager.getConnection("jdbc:stratos://host:port/")} works
 * without any other setup.
 */
public class StratosDriver implements Driver {
    private static final String URL_PREFIX = "jdbc:stratos://";

    static {
        try {
            DriverManager.registerDriver(new StratosDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null; // per Driver contract: null (not an exception) for a URL this driver doesn't handle
        }
        String hostPort = url.substring(URL_PREFIX.length());
        int slash = hostPort.indexOf('/');
        if (slash >= 0) {
            hostPort = hostPort.substring(0, slash);
        }
        int colon = hostPort.indexOf(':');
        if (colon < 0) {
            throw new SQLException("Invalid StratosDB JDBC URL - expected jdbc:stratos://host:port/, got: " + url);
        }
        String host = hostPort.substring(0, colon);
        int port;
        try {
            port = Integer.parseInt(hostPort.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new SQLException("Invalid port in StratosDB JDBC URL: " + url, e);
        }
        return StratosConnection.connect(host, port);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false; // honest: this is a minimal driver, not a fully JDBC-compliant one
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("StratosDB's JDBC driver does not use java.util.logging");
    }
}
