package com.stratosdb.jdbc;

import com.stratosdb.network.tls.TlsSupport;

import javax.net.ssl.SSLContext;
import java.security.GeneralSecurityException;
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

        // Standard JDBC convention: DriverManager.getConnection(url, username, password)
        // populates these two properties for the driver to read.
        String username = info != null ? info.getProperty("user") : null;
        String password = info != null ? info.getProperty("password") : null;

        // Non-standard but common convention for "extra" driver options: ssl=true
        // enables TLS. There is no client-side certificate verification yet (see
        // TlsSupport's javadoc) - this encrypts the connection, it does not
        // authenticate the server, so it does not defend against an active
        // man-in-the-middle. Stated here rather than left to be discovered.
        boolean ssl = info != null && "true".equalsIgnoreCase(info.getProperty("ssl"));
        SSLContext sslContext = null;
        if (ssl) {
            try {
                sslContext = TlsSupport.insecureTrustAllClientContext();
            } catch (GeneralSecurityException e) {
                throw new SQLException("Failed to set up TLS for StratosDB connection", e);
            }
        }

        return StratosConnection.connect(host, port, username, password, sslContext);
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
