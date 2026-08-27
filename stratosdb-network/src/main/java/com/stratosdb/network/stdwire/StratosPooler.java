package com.stratosdb.network.stdwire;

import com.stratosdb.network.auth.ScramClient;
import com.stratosdb.network.auth.ScramSha256;
import com.stratosdb.network.auth.UserStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * StratosDB's own real connection pooler - this project's own honestly
 * -named "every client gets a full, heavyweight session, a real
 * bottleneck past a handful of concurrent connections" gap, and the
 * actual role PgBouncer plays for real Postgres.
 *
 * A real, architectural question worth answering directly rather than
 * assuming the analogy holds: what does pooling actually save for
 * StratosDB specifically, given it already handles each client
 * connection on its own JVM virtual thread (see StdWireServer's own
 * connectionExecutor), not the expensive, OS-limited thread-per-
 * connection model - or the even more expensive separate OS *process*
 * per connection real multi-process Postgres uses, which is the actual,
 * original reason PgBouncer exists at all? Two real costs remain even
 * with virtual threads, and this class exists to address exactly those,
 * not an imagined one:
 *   1. SCRAM authentication is deliberately CPU-expensive (many PBKDF2
 *      iterations) - repeating that full handshake for every short-lived
 *      client connection (a common, real pattern: a web app opening a
 *      fresh DB connection per HTTP request) is genuinely wasteful, even
 *      though the connection itself is cheap to hold open.
 *   2. An unbounded number of real backend sessions - each with its own
 *      transaction, snapshot, and potential locks - is a real risk
 *      regardless of how cheap the thread holding it is: a runaway or
 *      poorly-behaved client application opening thousands of
 *      connections (some possibly left idle-in-transaction, holding back
 *      VACUUM's own horizon - see HeapTable.vacuum's own reasoning) can
 *      still degrade the underlying engine. Pooling bounds the real
 *      number of backend connections/transactions in flight at once,
 *      independent of how many clients connect to the pooler itself.
 *
 * Real transaction-mode pooling (not just session pooling): each real
 * backend connection is authenticated once, then reused across many
 * different client connections' own separate round-trips - returned to
 * the pool the moment a real ReadyForQuery message reports the backend
 * has gone back to idle ('I', not 'T'/'E'), the same real signal a
 * transaction has genuinely ended that PgBouncer itself watches for.
 * Between BEGIN and COMMIT/ROLLBACK, the SAME backend stays dedicated to
 * one client (a transaction's own locks/snapshot live on that one real
 * session, not something that can be handed to a different client
 * mid-transaction).
 *
 * Real, honestly-stated scope: proxies the simple query protocol only
 * (Query 'Q' messages and their own responses, including the real COPY
 * sub-protocol this engine already has, forwarded transparently as raw
 * message bytes with no special handling needed at all) - a client
 * needing the extended query protocol (Parse/Bind/Describe/Execute/
 * Sync) should connect directly to the real server, bypassing the
 * pooler, for now. A real, separate, further piece of work, not
 * attempted here given the scope already covered this round.
 */
public class StratosPooler {
    private static final Logger LOG = LoggerFactory.getLogger(StratosPooler.class);

    public enum PoolMode { SESSION, TRANSACTION }

    private final int listenPort;
    private final String backendHost;
    private final int backendPort;
    private final String backendUser;
    private final String backendPassword;
    private final String backendDatabase;
    private final int maxPoolSize;
    private final PoolMode poolMode;
    private final UserStore clientUserStore; // null = trust auth for clients, matching StdWireServer's own established convention

    private final BlockingQueue<BackendConnection> availableBackends = new LinkedBlockingQueue<>();
    private final AtomicInteger totalBackends = new AtomicInteger(0);
    private final java.util.concurrent.locks.ReentrantLock backendCreationLock = new java.util.concurrent.locks.ReentrantLock();

    private ServerSocket serverSocket;
    private ExecutorService connectionExecutor;
    private volatile boolean running = false;

    public StratosPooler(int listenPort, String backendHost, int backendPort, String backendUser,
                          String backendPassword, String backendDatabase, int maxPoolSize,
                          PoolMode poolMode, UserStore clientUserStore) {
        this.listenPort = listenPort;
        this.backendHost = backendHost;
        this.backendPort = backendPort;
        this.backendUser = backendUser;
        this.backendPassword = backendPassword;
        this.backendDatabase = backendDatabase;
        this.maxPoolSize = maxPoolSize;
        this.poolMode = poolMode;
        this.clientUserStore = clientUserStore;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(listenPort);
        connectionExecutor = Executors.newVirtualThreadPerTaskExecutor();
        running = true;
        Thread.ofVirtual().start(() -> {
            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    connectionExecutor.submit(() -> handleClient(client));
                } catch (IOException e) {
                    if (running) LOG.warn("Error accepting pooler client connection", e);
                }
            }
        });
        LOG.info("StratosPooler listening on port {} (mode={}, maxPoolSize={}, backend={}:{})",
            listenPort, poolMode, maxPoolSize, backendHost, backendPort);
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {
        }
        if (connectionExecutor != null) connectionExecutor.shutdownNow();
        BackendConnection conn;
        while ((conn = availableBackends.poll()) != null) {
            conn.close();
        }
    }

    // --- Backend connection pool ---

    private record BackendConnection(Socket socket, DataInputStream in, DataOutputStream out) {
        void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * Borrows an already-authenticated backend connection - an existing,
     * idle one if available, a freshly-created one if under
     * maxPoolSize, or blocks until one is returned otherwise. This is
     * the real, actual bound on concurrent backend sessions PgBouncer
     * itself provides.
     *
     * A real bug found only by actually testing this within a single
     * JVM process (every earlier, real, separate-process test passed
     * cleanly, hiding it): a plain `synchronized` block here, wrapping
     * createBackendConnection()'s own real, blocking socket I/O, is a
     * genuine virtual-thread pinning hazard - `synchronized` can pin a
     * virtual thread to its carrier thread for the block's entire
     * duration, and a real network handshake blocking inside one, with
     * only a small, CPU-core-sized pool of real carrier threads
     * backing every virtual thread in the JVM, was enough to starve the
     * backend's own accept/handling threads entirely, hanging the whole
     * pooler forever with no error at all. A `ReentrantLock` instead
     * never pins a virtual thread, since the JVM's own virtual-thread
     * scheduler knows how to unmount and reschedule around it.
     */
    private BackendConnection borrowBackend() throws IOException, InterruptedException {
        BackendConnection existing = availableBackends.poll();
        if (existing != null) {
            return existing;
        }
        backendCreationLock.lock();
        try {
            if (totalBackends.get() < maxPoolSize) {
                totalBackends.incrementAndGet();
                try {
                    return createBackendConnection();
                } catch (IOException e) {
                    totalBackends.decrementAndGet(); // creation failed - don't permanently lose this slot from the pool's own capacity
                    throw e;
                }
            }
        } finally {
            backendCreationLock.unlock();
        }
        return availableBackends.take(); // pool is at capacity - wait for another client's own connection to be returned
    }

    private void returnBackend(BackendConnection conn) {
        availableBackends.offer(conn);
    }

    /** A real backend connection to the actual server, authenticated once via real SCRAM (or trust, if the backend has none configured) using this pooler's own configured backend credentials - reused across many different client connections' own separate round-trips for as long as this pooler runs. */
    private BackendConnection createBackendConnection() throws IOException {
        Socket socket = new Socket(backendHost, backendPort);
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

        StdWireMessages.writeStartupMessage(out, backendUser, backendDatabase);
        out.flush();
        while (true) {
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
            switch (msg.type()) {
                case 'R' -> {
                    int authCode = readInt32(msg.body(), 0);
                    if (authCode == 10) {
                        performClientSideScram(backendUser, backendPassword, in, out);
                    }
                }
                case 'S', 'K' -> { }
                case 'Z' -> {
                    return new BackendConnection(socket, in, out);
                }
                case 'E' -> throw new IOException("Backend rejected pooler's own startup: " + extractErrorMessage(msg));
                default -> { }
            }
        }
    }

    private void performClientSideScram(String username, String password, DataInputStream in, DataOutputStream out) throws IOException {
        ScramClient scram = new ScramClient(username, password);
        String clientFirstMessage = scram.buildClientFirstMessage();
        byte[] mechanismBytes = ScramSha256.MECHANISM_NAME.getBytes(StandardCharsets.UTF_8);
        byte[] dataBytes = clientFirstMessage.getBytes(StandardCharsets.UTF_8);
        out.writeByte('p');
        out.writeInt(mechanismBytes.length + 1 + 4 + dataBytes.length + 4);
        out.write(mechanismBytes);
        out.writeByte(0);
        out.writeInt(dataBytes.length);
        out.write(dataBytes);
        out.flush();

        StdWireMessages.TypedMessage continueMsg = StdWireMessages.readTypedMessage(in);
        if (continueMsg.type() != 'R') {
            throw new IOException("Expected AuthenticationSASLContinue from backend during pooler's own SCRAM handshake");
        }
        String serverFirstMessage = new String(continueMsg.body(), 4, continueMsg.body().length - 4, StandardCharsets.UTF_8);
        String clientFinalMessage = scram.buildClientFinalMessage(serverFirstMessage);
        byte[] cfBytes = clientFinalMessage.getBytes(StandardCharsets.UTF_8);
        out.writeByte('p');
        out.writeInt(cfBytes.length + 4);
        out.write(cfBytes);
        out.flush();

        StdWireMessages.TypedMessage finalMsg = StdWireMessages.readTypedMessage(in);
        if (finalMsg.type() == 'E') {
            throw new IOException("Backend authentication failed for pooler's own configured backend user: " + extractErrorMessage(finalMsg));
        }
    }

    // --- Client-facing connection handling ---

    private void handleClient(Socket clientSocket) {
        BackendConnection heldBackend = null;
        try (Socket s = clientSocket;
             DataInputStream clientIn = new DataInputStream(new BufferedInputStream(s.getInputStream()));
             DataOutputStream clientOut = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()))) {

            if (!performClientStartup(clientIn, clientOut)) {
                return;
            }

            while (true) {
                StdWireMessages.TypedMessage clientMsg;
                try {
                    clientMsg = StdWireMessages.readTypedMessage(clientIn);
                } catch (IOException e) {
                    break; // client disconnected without a clean Terminate
                }
                if (clientMsg.type() == 'X') {
                    break;
                }

                if (heldBackend == null) {
                    heldBackend = borrowBackend();
                }

                StdWireMessages.writeRawMessage(heldBackend.out(), clientMsg);
                heldBackend.out().flush();

                boolean backendNowIdle = forwardBackendResponseUntilReadyForQuery(heldBackend, clientIn, clientOut);
                clientOut.flush();

                if (backendNowIdle && poolMode == PoolMode.TRANSACTION) {
                    returnBackend(heldBackend);
                    heldBackend = null;
                }
            }
        } catch (Exception e) {
            LOG.warn("Error handling pooler client connection", e);
        } finally {
            if (heldBackend != null) {
                returnBackend(heldBackend);
            }
        }
    }

    /**
     * Forwards every message from the backend to the client verbatim,
     * as raw, unparsed bytes - this pooler proxies at the message-
     * boundary level only, so a RowDescription, a DataRow, an
     * ErrorResponse, whatever it is, all forward identically with no
     * special handling needed - until ReadyForQuery, at which point its
     * own real transaction-status byte ('I'/'T'/'E' - see
     * StdWireServer's own writeReadyForQuery call sites) is inspected
     * specifically to decide whether this backend is now safe to
     * return to the pool.
     *
     * One real exception, found by actually testing COPY FROM STDIN
     * through this pooler, not by inspection: a naive "forward backend
     * messages until ReadyForQuery" loop hangs forever the moment the
     * backend sends a CopyInResponse ('G'), since the backend then
     * genuinely waits for the CLIENT to send its own CopyData messages
     * next, not for anything more from this pooler's own backend-facing
     * read loop - a real, different flow direction the simple query
     * protocol's own request/response shape never has. On seeing 'G',
     * this method pauses reading from the backend and instead relays
     * CopyData/CopyDone/CopyFail messages FROM the client TO the
     * backend until the client signals it's done, only then resuming
     * the normal backend-to-client direction. COPY ... TO STDOUT needs
     * no such handling - CopyData there already flows backend-to-client,
     * the same direction this loop already forwards.
     */
    private boolean forwardBackendResponseUntilReadyForQuery(BackendConnection backend, DataInputStream clientIn, DataOutputStream clientOut) throws IOException {
        while (true) {
            StdWireMessages.TypedMessage backendMsg = StdWireMessages.readTypedMessage(backend.in());
            StdWireMessages.writeRawMessage(clientOut, backendMsg);
            clientOut.flush(); // the client must actually see CopyInResponse before it will send any CopyData back
            if (backendMsg.type() == 'G') {
                forwardClientCopyDataUntilDone(clientIn, backend.out());
            } else if (backendMsg.type() == 'Z') {
                char status = (char) backendMsg.body()[0];
                return status == 'I';
            }
        }
    }

    /** Relays CopyData ('d') messages from the client to the backend during a COPY ... FROM STDIN in progress, stopping once the client sends CopyDone ('c') or CopyFail ('f') - see forwardBackendResponseUntilReadyForQuery's own javadoc for why this exists. */
    private void forwardClientCopyDataUntilDone(DataInputStream clientIn, DataOutputStream backendOut) throws IOException {
        while (true) {
            StdWireMessages.TypedMessage clientMsg = StdWireMessages.readTypedMessage(clientIn);
            StdWireMessages.writeRawMessage(backendOut, clientMsg);
            backendOut.flush();
            if (clientMsg.type() == 'c' || clientMsg.type() == 'f') {
                return;
            }
        }
    }

    /**
     * Real, client-facing authentication (trust or SCRAM, using this
     * pooler's own separately-configured clientUserStore) - decoupled
     * from whatever credentials the pooler itself uses to talk to the
     * real backend (see createBackendConnection), matching PgBouncer's
     * own real design: many different client identities can share a
     * small, fixed set of already-authenticated backend sessions.
     *
     * The initial startup packet has no leading type byte at all,
     * unlike every other message on this connection (see
     * StdWireMessages.readUntypedPacket's own purpose) - the same real
     * SSL-then-plaintext-fallback sequence StdWireServer.performStartup
     * itself already handles is replicated here rather than shared,
     * since this pooler's own startup response (ParameterStatus/
     * BackendKeyData/ReadyForQuery) still needs to be sent regardless of
     * which real backend session ends up handling this client's first
     * actual query later.
     */
    private boolean performClientStartup(DataInputStream in, DataOutputStream out) throws IOException {
        byte[] body = StdWireMessages.readUntypedPacket(in);
        int code = readInt32(body, 0);

        if (code == StdWireMessages.SSL_REQUEST_CODE) {
            StdWireMessages.writeSslDecline(out);
            out.flush();
            body = StdWireMessages.readUntypedPacket(in);
            code = readInt32(body, 0);
        }

        if (code != StdWireMessages.PROTOCOL_VERSION_3) {
            StdWireMessages.writeErrorResponse(out, "Only protocol version 3.0 is supported");
            out.flush();
            return false;
        }

        Map<String, String> params = StdWireMessages.parseStartupParams(body, 4);
        String username = params.get("user");
        LOG.info("Pooler client startup: user={} database={}", username, params.get("database"));

        if (clientUserStore != null) {
            if (!performServerSideScram(username, in, out)) {
                return false;
            }
        } else {
            StdWireMessages.writeAuthenticationOk(out);
        }

        StdWireMessages.writeParameterStatus(out, "server_version", "16.0 (StratosDB pooler)");
        StdWireMessages.writeParameterStatus(out, "client_encoding", "UTF8");
        StdWireMessages.writeBackendKeyData(out, 0, 0);
        StdWireMessages.writeReadyForQuery(out, 'I');
        out.flush();
        return true;
    }

    /** Real, server-side SCRAM (this pooler authenticating a client, using its own clientUserStore) - a real, separate, self-contained copy of StdWireServer.performScramAuthentication's own proven logic, not a shared refactor of it (see e.g. PitrBackup's own javadoc for why this project consistently prefers a separate copy here over touching an already-working, already-tested class). */
    private boolean performServerSideScram(String username, DataInputStream in, DataOutputStream out) throws IOException {
        StdWireMessages.writeAuthenticationSasl(out, ScramSha256.MECHANISM_NAME);
        out.flush();

        StdWireMessages.TypedMessage initialMsg = StdWireMessages.readTypedMessage(in);
        if (initialMsg.type() != 'p') {
            StdWireMessages.writeErrorResponse(out, "Expected SASLInitialResponse");
            out.flush();
            return false;
        }
        StdWireMessages.SaslInitialResponse initial = StdWireMessages.readSaslInitialResponse(initialMsg);
        if (!ScramSha256.MECHANISM_NAME.equals(initial.mechanism())) {
            StdWireMessages.writeErrorResponse(out, "Unsupported SASL mechanism: " + initial.mechanism());
            out.flush();
            return false;
        }

        UserStore.ScramCredential credential = clientUserStore.getScramCredential(username);
        ScramSha256.Handshake handshake = new ScramSha256.Handshake(username, credential);

        String serverFirstMessage;
        try {
            serverFirstMessage = handshake.clientFirst(initial.initialResponseData());
        } catch (ScramSha256.ScramAuthenticationException e) {
            StdWireMessages.writeErrorResponse(out, "Authentication failed: " + e.getMessage());
            out.flush();
            return false;
        }
        StdWireMessages.writeAuthenticationSaslContinue(out, serverFirstMessage);
        out.flush();

        StdWireMessages.TypedMessage finalMsg = StdWireMessages.readTypedMessage(in);
        if (finalMsg.type() != 'p') {
            StdWireMessages.writeErrorResponse(out, "Expected SASLResponse");
            out.flush();
            return false;
        }
        String clientFinalMessage = StdWireMessages.readSaslResponse(finalMsg);

        String serverFinalMessage;
        try {
            serverFinalMessage = handshake.clientFinal(clientFinalMessage);
        } catch (ScramSha256.ScramAuthenticationException e) {
            LOG.warn("Pooler: SCRAM authentication failed for client user {}: {}", username, e.getMessage());
            StdWireMessages.writeErrorResponse(out, "password authentication failed for user \"" + username + "\"");
            out.flush();
            return false;
        }
        StdWireMessages.writeAuthenticationSaslFinal(out, serverFinalMessage);
        StdWireMessages.writeAuthenticationOk(out);
        return true;
    }

    private static int readInt32(byte[] b, int offset) {
        return ((b[offset] & 0xFF) << 24) | ((b[offset + 1] & 0xFF) << 16) | ((b[offset + 2] & 0xFF) << 8) | (b[offset + 3] & 0xFF);
    }

    private static String extractErrorMessage(StdWireMessages.TypedMessage msg) {
        byte[] body = msg.body();
        int pos = 0;
        while (pos < body.length && body[pos] != 0) {
            char field = (char) body[pos];
            pos++;
            int start = pos;
            while (body[pos] != 0) pos++;
            String value = new String(body, start, pos - start, StandardCharsets.UTF_8);
            pos++;
            if (field == 'M') return value;
        }
        return "unknown error";
    }

    public static void main(String[] args) throws Exception {
        int listenPort = 6432; // matching real PgBouncer's own default port, by convention
        String backendHost = "localhost";
        int backendPort = 5432;
        String backendUser = "stratos";
        String backendPassword;
        String backendDatabase = "stratos";
        int maxPoolSize = 10;
        PoolMode mode = PoolMode.TRANSACTION;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--listen-port" -> listenPort = Integer.parseInt(args[++i]);
                case "--backend-host" -> backendHost = args[++i];
                case "--backend-port" -> backendPort = Integer.parseInt(args[++i]);
                case "--backend-user" -> backendUser = args[++i];
                case "--backend-database" -> backendDatabase = args[++i];
                case "--max-pool-size" -> maxPoolSize = Integer.parseInt(args[++i]);
                case "--mode" -> mode = PoolMode.valueOf(args[++i].toUpperCase(java.util.Locale.ROOT));
                default -> {
                    System.err.println("Unrecognized argument: " + args[i]);
                    printUsage();
                    return;
                }
            }
        }
        backendPassword = System.getenv("STRATOSDB_BACKEND_PASSWORD");

        StratosPooler pooler = new StratosPooler(listenPort, backendHost, backendPort, backendUser,
            backendPassword, backendDatabase, maxPoolSize, mode, null);
        pooler.start();
        Thread.currentThread().join();
    }

    private static void printUsage() {
        System.err.println("Usage: StratosPooler --listen-port <port> --backend-host <host> --backend-port <port> "
            + "--backend-user <user> --backend-database <db> --max-pool-size <n> --mode {session|transaction}");
        System.err.println("Backend password is read from the STRATOSDB_BACKEND_PASSWORD environment variable.");
    }
}
