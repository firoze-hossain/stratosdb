package com.stratosdb.jdbc;

import com.stratosdb.network.tls.TlsSupport;

import javax.net.ssl.SSLContext;
import java.security.GeneralSecurityException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC driver for StratosDB. URL format:
 * {@code jdbc:stratos://host:port/database?key=value&key2=value2}
 * (the database segment is sent as the real StartupMessage's own
 * "database" parameter - see StratosConnection's own javadoc for the
 * real, current wire protocol this now speaks. StratosDB itself still
 * has no real multiple-databases-per-server concept, so any value here
 * is accepted without being meaningfully validated server-side - defaults
 * to "stratos" if the path segment is omitted).
 *
 * A real {@code ?key=value} query string is parsed too, the same real
 * convention every other mainstream JDBC driver's own URL already
 * supports (e.g. {@code jdbc:postgresql://host:port/db?ssl=true}) - not
 * invented here, since a real tool building a connection URL for this
 * driver (rather than calling {@link #connect} with a pre-built
 * {@link Properties} object directly) needs this to actually work.
 * Values explicitly passed via the {@code info} parameter (e.g. through
 * {@code DriverManager.getConnection(url, user, password)}) take
 * precedence over the same key parsed from the URL's own query string,
 * matching that same real convention.
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
            Properties urlProperties = new Properties();
            int question = pathSegment.indexOf('?');
            if (question >= 0) {
                parseQueryString(pathSegment.substring(question + 1), urlProperties);
                pathSegment = pathSegment.substring(0, question);
            }
            if (!pathSegment.isEmpty()) {
                database = pathSegment;
            }
            hostPort = hostPort.substring(0, slash);
            // Real precedence, matching every other mainstream JDBC driver's own
            // convention: whatever the caller explicitly passed in info wins over
            // the same key merely embedded in the URL's own query string.
            for (String key : urlProperties.stringPropertyNames()) {
                if (info == null || info.getProperty(key) == null) {
                    if (info == null) info = new Properties();
                    info.setProperty(key, urlProperties.getProperty(key));
                }
            }
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

    /** Parses a real {@code key=value&key2=value2} query string (real percent-decoding included, since a real value might legitimately need to carry a reserved character) into props. */
    private static void parseQueryString(String query, Properties props) throws SQLException {
        if (query.isEmpty()) return;
        for (String pair : query.split("&")) {
            if (pair.isEmpty()) continue;
            int eq = pair.indexOf('=');
            String key = eq >= 0 ? pair.substring(0, eq) : pair;
            String value = eq >= 0 ? pair.substring(eq + 1) : "";
            try {
                key = java.net.URLDecoder.decode(key, "UTF-8");
                value = java.net.URLDecoder.decode(value, "UTF-8");
            } catch (java.io.UnsupportedEncodingException e) {
                throw new SQLException("Failed to decode URL query string", e);
            }
            props.setProperty(key, value);
        }
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
