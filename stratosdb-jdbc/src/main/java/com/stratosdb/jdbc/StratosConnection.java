package com.stratosdb.jdbc;

import com.stratosdb.network.protocol.WireProtocol;
import com.stratosdb.sql.executor.QueryResult;

import javax.net.ssl.SSLContext;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;

import static com.stratosdb.jdbc.JdbcSupport.notSupported;

/**
 * Real behavior: connect/close/isClosed, createStatement, isValid,
 * getCatalog/getSchema (both null - no such concept here), and now real
 * multi-statement transactions: setAutoCommit(false) sends BEGIN,
 * commit()/rollback() send COMMIT/ROLLBACK (each immediately followed by
 * a fresh BEGIN while still in manual-commit mode - standard JDBC
 * semantics: setAutoCommit(false) opens an ongoing sequence of
 * transactions, not just one), and setAutoCommit(true) while a manual
 * transaction is open commits it first, matching the JDBC spec. This
 * works correctly over the wire with no server-side protocol changes
 * needed: StratosServer runs one thread per connection for that
 * connection's whole lifetime, and ExecutorEngine's transaction session
 * state is thread-local - so a connection's BEGIN/statements/COMMIT
 * naturally share the same session simply by being on the same thread.
 *
 * Every connection performs the AUTH handshake (see WireProtocol) as soon
 * as the socket opens, sending whatever username/password were passed to
 * connect() (empty strings if none) - a server with no authentication
 * configured accepts this unconditionally, so unauthenticated use keeps
 * working exactly as before auth existed.
 *
 * TLS is opt-in via an SSLContext passed to connect(); when present, the
 * socket is created through sslContext.getSocketFactory() instead of a
 * plain Socket. See com.stratosdb.network.tls.TlsSupport for exactly what
 * "TLS support" does and does not mean on the client side here (server
 * certificate verification is not wired up - trust-all only).
 *
 * Everything else required by java.sql.Connection (there are 63 methods on
 * that interface) throws SQLFeatureNotSupportedException with a clear
 * message, via the shared fallback in invoke(). Implemented as a dynamic
 * proxy rather than ~60 hand-written stub methods - same end result for
 * callers (they just see a Connection), far less boilerplate to maintain,
 * and the "not supported" behavior lives in one place instead of being
 * copy-pasted 50 times.
 */
class StratosConnection implements InvocationHandler {
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private volatile boolean closed = false;
    private volatile boolean autoCommit = true;
    private Connection proxy;

    private StratosConnection(Socket socket, String username, String password) throws IOException, SQLException {
        this.socket = socket;
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        performAuthHandshake(username, password);
    }

    private void performAuthHandshake(String username, String password) throws IOException, SQLException {
        WireProtocol.writeAuth(out, username, password);
        int type = WireProtocol.readMessageType(in);
        if (type != WireProtocol.MSG_AUTH_RESULT) {
            throw new SQLException("Unexpected response during authentication handshake: message type " + type);
        }
        WireProtocol.AuthResult result = WireProtocol.readAuthResultBody(in);
        if (!result.success()) {
            socket.close();
            throw new SQLException("Authentication failed: " + result.message());
        }
    }

    static Connection connect(String host, int port, String username, String password, SSLContext sslContext) throws SQLException {
        try {
            Socket socket = sslContext != null
                ? sslContext.getSocketFactory().createSocket(host, port)
                : new Socket(host, port);
            StratosConnection handler = new StratosConnection(socket, username, password);
            Connection proxy = (Connection) Proxy.newProxyInstance(
                StratosConnection.class.getClassLoader(), new Class[]{Connection.class}, handler);
            handler.proxy = proxy;
            return proxy;
        } catch (IOException e) {
            throw new SQLException("Failed to connect to StratosDB at " + host + ":" + port, e);
        }
    }

    /** Package-visible so StratosStatement can send queries over this same connection. */
    QueryResult execute(String sql) throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
        try {
            WireProtocol.writeQuery(out, sql);
            int type = WireProtocol.readMessageType(in);
            if (type != WireProtocol.MSG_RESULT) {
                throw new SQLException("Unexpected response type " + type + " from StratosDB server");
            }
            return WireProtocol.readResultBody(in);
        } catch (IOException e) {
            throw new SQLException("Communication with StratosDB server failed", e);
        }
    }

    Connection asProxy() {
        return proxy;
    }

    @Override
    public Object invoke(Object p, Method method, Object[] args) throws Throwable {
        String name = method.getName();
        switch (name) {
            case "createStatement":
                checkOpen();
                return StratosStatement.create(this);
            case "close":
                if (!closed) {
                    closed = true;
                    if (!autoCommit) {
                        // Standard driver behavior: closing a connection with an open
                        // manual transaction rolls it back - there's no way to commit
                        // work the caller never explicitly committed. Best-effort: if
                        // the socket is already in a bad state, closing it is more
                        // important than this cleanup succeeding.
                        try {
                            execute("ROLLBACK");
                        } catch (SQLException ignored) {
                        }
                    }
                    socket.close();
                }
                return null;
            case "isClosed":
                return closed;
            case "isValid":
                return !closed;
            case "setAutoCommit": {
                boolean requested = (Boolean) args[0];
                if (!requested && autoCommit) {
                    requireSuccess(execute("BEGIN"), "start a transaction");
                    autoCommit = false;
                } else if (requested && !autoCommit) {
                    // Per the JDBC spec: switching back to auto-commit while a
                    // transaction is open commits that transaction first.
                    requireSuccess(execute("COMMIT"), "commit the current transaction before switching to auto-commit");
                    autoCommit = true;
                }
                return null;
            }
            case "getAutoCommit":
                return autoCommit;
            case "commit":
                if (autoCommit) {
                    return null; // nothing to commit - matches JDBC drivers that tolerate a defensive commit() call
                }
                requireSuccess(execute("COMMIT"), "commit");
                // Manual-commit mode is an ongoing sequence of transactions, not just
                // one - immediately open the next one, same as every real driver does.
                requireSuccess(execute("BEGIN"), "start the next transaction after commit");
                return null;
            case "rollback":
                if (autoCommit) {
                    throw new java.sql.SQLFeatureNotSupportedException(
                        "Nothing to roll back - not currently in a transaction (autoCommit is true)");
                }
                requireSuccess(execute("ROLLBACK"), "roll back");
                requireSuccess(execute("BEGIN"), "start the next transaction after rollback");
                return null;
            case "getCatalog":
            case "getSchema":
                return null;
            case "getWarnings":
                return null;
            case "clearWarnings":
                return null;
            case "toString":
                return "StratosConnection[" + socket.getRemoteSocketAddress() + ", closed=" + closed + "]";
            case "equals":
                return p == args[0];
            case "hashCode":
                return System.identityHashCode(p);
            default:
                throw notSupported("Connection", name);
        }
    }

    private void requireSuccess(QueryResult result, String action) throws SQLException {
        if (!result.isSuccess()) {
            throw new SQLException("Failed to " + action + ": " + result.getError());
        }
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }
}
