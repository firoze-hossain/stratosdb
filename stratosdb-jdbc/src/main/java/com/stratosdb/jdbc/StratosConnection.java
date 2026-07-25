package com.stratosdb.jdbc;

import com.stratosdb.network.protocol.WireProtocol;
import com.stratosdb.sql.executor.QueryResult;

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

    private StratosConnection(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    static Connection connect(String host, int port) throws SQLException {
        try {
            StratosConnection handler = new StratosConnection(new Socket(host, port));
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
