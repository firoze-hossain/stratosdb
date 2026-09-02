package com.stratosdb.jdbc;

import com.stratosdb.network.tls.TlsSupport;

import javax.net.ssl.SSLContext;
import java.security.GeneralSecurityException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC driver for StratosDB. URL format: jdbc:stratos://host:port/database
 * (the database segment is sent as the real StartupMessage's own
 * "database" parameter - see StratosConnection's own javadoc for the
 * real, current wire protocol this now speaks. StratosDB itself still
 * has no real multiple-databases-per-server concept, so any value here
 * is accepted without being meaningfully validated server-side - defaults
 * to "stratos" if the path segment is omitted).
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
        String database = "stratos";
        int slash = hostPort.indexOf('/');
        if (slash >= 0) {
            String pathSegment = hostPort.substring(slash + 1);
            if (!pathSegment.isEmpty()) {
                database = pathSegment;
            }
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
        // enables TLS. See StratosConnection's own javadoc for why this currently
        // always throws a clear, honest error - the real, current server has no
        // TLS support at all yet.
        boolean ssl = info != null && "true".equalsIgnoreCase(info.getProperty("ssl"));
        SSLContext sslContext = null;
        if (ssl) {
            try {
                sslContext = TlsSupport.insecureTrustAllClientContext();
            } catch (GeneralSecurityException e) {
                throw new SQLException("Failed to set up TLS for StratosDB connection", e);
            }
        }

        return StratosConnection.connect(host, port, username, password, database, sslContext);
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
