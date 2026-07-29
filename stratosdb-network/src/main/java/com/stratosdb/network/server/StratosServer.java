package com.stratosdb.network.server;

import com.stratosdb.core.StratosDB;
import com.stratosdb.network.auth.UserStore;
import com.stratosdb.network.protocol.WireProtocol;
import com.stratosdb.sql.executor.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A real TCP server for StratosDB: accepts connections, reads QUERY
 * messages, executes them against a single shared StratosDB instance
 * (one process, one data directory, many connections - the same model
 * PostgreSQL and most single-node databases use), and writes back RESULT
 * messages. One virtual thread per connection (Java 21 has these - the
 * reason this project pinned to 21 LTS back in Week 1), so a slow or idle
 * client costs a lightweight virtual thread, not a full platform thread.
 *
 * Deliberately not in stratosdb-core: core has zero networking
 * dependencies by design, so embedding StratosDB as a plain library pulls
 * in no socket code at all. This class is what turns that embeddable
 * engine into a server - stratosdb-network depends on stratosdb-core, not
 * the other way around, so core itself cannot start this directly without
 * a circular module dependency.
 *
 * Authentication: every connection sends an AUTH message first, always -
 * even against a server started with no UserStore, which just accepts it
 * unconditionally (open access, the same as before auth existed). If a
 * UserStore IS provided, AUTH must carry valid credentials or the
 * connection is closed before any query is accepted.
 *
 * TLS: an optional SSLContext wraps the server socket. See
 * com.stratosdb.network.tls.TlsSupport for what "TLS support" does and
 * does not mean here - server-side certificate handling is real, client-
 * side verification is not (trust-all only), and that gap is documented
 * there rather than papered over.
 *
 * This is not compatible with the PostgreSQL wire protocol - it's
 * StratosDB's own small protocol (see WireProtocol), not an
 * implementation of libpq's format.
 */
public class StratosServer {
    private static final Logger LOG = LoggerFactory.getLogger(StratosServer.class);

    private final int port;
    private final StratosDB db;
    private final UserStore userStore; // null = no authentication required
    private final SSLContext sslContext; // null = plain TCP

    private ServerSocket serverSocket;
    private ExecutorService connectionExecutor;
    private Thread acceptThread;
    private volatile boolean running = false;

    public StratosServer(int port, StratosDB db) {
        this(port, db, null, null);
    }

    public StratosServer(int port, StratosDB db, UserStore userStore, SSLContext sslContext) {
        this.port = port;
        this.db = db;
        this.userStore = userStore;
        this.sslContext = sslContext;
    }

    public synchronized void start() throws IOException {
        if (running) {
            throw new IllegalStateException("Server already running on port " + port);
        }
        serverSocket = sslContext != null
            ? sslContext.getServerSocketFactory().createServerSocket(port)
            : new ServerSocket(port);
        connectionExecutor = Executors.newVirtualThreadPerTaskExecutor();
        running = true;

        acceptThread = new Thread(this::acceptLoop, "stratos-server-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();

        LOG.info("StratosDB network server listening on port {} (tls={}, auth={})",
            port, sslContext != null, userStore != null);
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                connectionExecutor.submit(() -> handleConnection(client));
            } catch (IOException e) {
                if (running) {
                    LOG.warn("Accept failed: {}", e.getMessage());
                }
                // else: expected - stop() closed serverSocket to break out of accept()
            }
        }
    }

    private void handleConnection(Socket socket) {
        String remote;
        try {
            remote = socket.getRemoteSocketAddress().toString();
        } catch (Exception e) {
            remote = "<unknown>";
        }
        LOG.debug("Client connected: {}", remote);

        try {
            try (Socket s = socket;
                 DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                 DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()))) {

                if (!authenticate(in, out, remote)) {
                    return;
                }

                while (running) {
                    int type;
                    try {
                        type = WireProtocol.readMessageType(in);
                    } catch (EOFException e) {
                        break; // client closed the connection cleanly between messages
                    }

                    if (type != WireProtocol.MSG_QUERY) {
                        LOG.warn("Unexpected message type {} from {}, closing connection", type, remote);
                        break;
                    }

                    String sql = WireProtocol.readQueryBody(in);
                    QueryResult result;
                    try {
                        result = db.execute(sql);
                    } catch (Exception e) {
                        // A query that throws instead of returning QueryResult.error(...) must
                        // still get a RESULT message back - otherwise the client hangs waiting
                        // for a reply that will never come.
                        result = QueryResult.error(e.getMessage());
                    }
                    WireProtocol.writeResult(out, result);
                }
            } catch (IOException e) {
                LOG.debug("Connection {} closed: {}", remote, e.getMessage());
            }
        } finally {
            // Regardless of how the connection ended (clean disconnect, a
            // protocol error, or an exception) - if this client left a
            // transaction open (BEGIN with no matching COMMIT/ROLLBACK), it
            // must be rolled back here. Without this, an abandoned session's
            // transaction stays in TransactionManager's active set forever,
            // permanently pinning VACUUM's horizon and blocking reclamation
            // of anything newer than the abandoned transaction, for every
            // table, indefinitely - a real resource leak, not a cosmetic one.
            db.closeSession();
        }
        LOG.debug("Client disconnected: {}", remote);
    }

    /** Returns true iff the connection is authenticated and should proceed to the query loop. */
    private boolean authenticate(DataInputStream in, DataOutputStream out, String remote) throws IOException {
        int type;
        try {
            type = WireProtocol.readMessageType(in);
        } catch (EOFException e) {
            return false; // client disconnected before completing the handshake
        }
        if (type != WireProtocol.MSG_AUTH) {
            LOG.warn("Expected AUTH as the first message from {}, got type {} - closing connection", remote, type);
            return false;
        }

        String[] credentials = WireProtocol.readAuthBody(in);
        String username = credentials[0];
        String password = credentials[1];

        boolean ok = (userStore == null) || userStore.verify(username, password);
        String message = userStore == null
            ? "No authentication required"
            : (ok ? "Authenticated" : "Invalid username or password");

        WireProtocol.writeAuthResult(out, ok, message);

        if (!ok) {
            LOG.warn("Authentication failed for user '{}' from {}", username, remote);
            return false;
        }
        if (userStore != null) {
            LOG.info("User '{}' authenticated from {}", username, remote);
        }
        return true;
    }

    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        try {
            serverSocket.close(); // unblocks the accept() call in acceptLoop()
        } catch (IOException e) {
            LOG.warn("Error closing server socket: {}", e.getMessage());
        }
        connectionExecutor.shutdownNow();
        LOG.info("StratosDB network server on port {} stopped", port);
    }

    public boolean isRunning() {
        return running;
    }

    public int getPort() {
        return port;
    }
}
