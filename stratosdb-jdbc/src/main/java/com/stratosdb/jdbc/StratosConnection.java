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
 * Real behavior: connect/close/isClosed, createStatement, commit (a no-op -
 * every statement already auto-commits per Week 2's design),
 * setAutoCommit(true) (setAutoCommit(false) throws - there is no
 * multi-statement transaction protocol over the wire yet), isValid,
 * getCatalog/getSchema (both null - no such concept here).
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
                    socket.close();
                }
                return null;
            case "isClosed":
                return closed;
            case "isValid":
                return !closed;
            case "setAutoCommit":
                if (!((Boolean) args[0])) {
                    throw new java.sql.SQLFeatureNotSupportedException(
                        "StratosDB auto-commits every statement (see Week 2); "
                        + "multi-statement transactions over JDBC are not implemented yet");
                }
                return null;
            case "getAutoCommit":
                return true;
            case "commit":
                return null; // no-op: whatever was executed already committed when its result arrived
            case "rollback":
                throw new java.sql.SQLFeatureNotSupportedException("Nothing to roll back - every statement auto-commits");
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

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }
}
