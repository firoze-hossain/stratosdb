package com.stratosdb.network.stratoswire;

import com.stratosdb.core.StratosCluster;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.auth.UserStore;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stratos Native Protocol (SWP v1.0) Server.
 *
 * Implements the 100% custom, independent StratosDB wire protocol.
 * Listens for StratosWire clients, performs binary handshakes, executes
 * SQL commands, and streams strongly-typed binary rows.
 */
public class StratosWireServer {
    private static final Logger LOG = LoggerFactory.getLogger(StratosWireServer.class);
    private static final AtomicLong SESSION_COUNTER = new AtomicLong(1);

    private final int port;
    private final StratosDB singleDb;
    private final StratosCluster cluster;
    private final UserStore userStore;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private ServerSocket serverSocket;
    private ExecutorService threadPool;

    public StratosWireServer(int port, StratosDB db) {
        this(port, db, null, null);
    }

    public StratosWireServer(int port, StratosCluster cluster) {
        this(port, null, cluster, null);
    }

    public StratosWireServer(int port, StratosDB db, UserStore userStore) {
        this(port, db, null, userStore);
    }

    public StratosWireServer(int port, StratosCluster cluster, UserStore userStore) {
        this(port, null, cluster, userStore);
    }

    private StratosWireServer(int port, StratosDB singleDb, StratosCluster cluster, UserStore userStore) {
        this.port = port;
        this.singleDb = singleDb;
        this.cluster = cluster;
        this.userStore = userStore;
    }

    public int getPort() {
        return serverSocket != null ? serverSocket.getLocalPort() : port;
    }

    public boolean isRunning() {
        return running.get();
    }

    public synchronized void start() throws IOException {
        if (running.get()) return;
        serverSocket = new ServerSocket(port);
        running.set(true);
        threadPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "stratoswire-client-" + SESSION_COUNTER.getAndIncrement());
            t.setDaemon(true);
            return t;
        });

        Thread acceptThread = new Thread(this::acceptLoop, "stratoswire-accept-" + port);
        acceptThread.setDaemon(true);
        acceptThread.start();
        LOG.info("StratosWireServer (Stratos Native Protocol) started on port {}", getPort());
    }

    public synchronized void stop() {
        if (!running.get()) return;
        running.set(false);
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        LOG.info("StratosWireServer stopped");
    }

    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);
                threadPool.submit(() -> handleClient(clientSocket));
            } catch (IOException e) {
                if (!running.get()) break;
                LOG.error("Error accepting StratosWire connection: {}", e.getMessage());
            }
        }
    }

    private void handleClient(Socket socket) {
        String remote = socket.getRemoteSocketAddress().toString();
        LOG.debug("StratosWire client connected from {}", remote);

        StratosDB sessionDb = singleDb;

        try (socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

            // 1. Initial Handshake
            StratosWireProtocol.Frame initialFrame = StratosWireProtocol.readFrame(in);
            if (initialFrame.type() != StratosWireProtocol.MSG_CONNECT_REQ) {
                sendError(out, StratosWireProtocol.ERR_GENERIC, "Expected MSG_CONNECT_REQ");
                return;
            }

            DataInputStream connectIn = initialFrame.dataStream();
            int clientVersion = connectIn.readInt();
            String username = connectIn.readUTF();
            String dbName = connectIn.readUTF();
            String authToken = connectIn.readUTF();

            // Resolve target database
            if (cluster != null) {
                String targetDb = (dbName == null || dbName.isBlank()) ? StratosCluster.DEFAULT_DATABASE : dbName;
                sessionDb = cluster.getDatabase(targetDb);
                if (sessionDb == null) {
                    sendError(out, StratosWireProtocol.ERR_TABLE_NOT_FOUND, "Database '" + targetDb + "' does not exist");
                    return;
                }
            }

            // Authentication verification
            if (userStore != null) {
                if (!userStore.verify(username, authToken)) {
                    sendError(out, StratosWireProtocol.ERR_AUTH_FAILED, "Authentication failed for user: " + username);
                    return;
                }
            }

            // Send Connect OK
            ByteArrayOutputStream okBuffer = new ByteArrayOutputStream();
            DataOutputStream okOut = new DataOutputStream(okBuffer);
            okOut.writeInt(StratosWireProtocol.PROTOCOL_VERSION);
            okOut.writeUTF("StratosDB 1.0.0");
            okOut.writeLong(SESSION_COUNTER.get());
            okOut.writeUTF(dbName != null ? dbName : "stratos");
            StratosWireProtocol.writeFrame(out, StratosWireProtocol.MSG_CONNECT_OK, okBuffer.toByteArray());

            // 2. Command / Query Loop
            while (running.get()) {
                StratosWireProtocol.Frame frame;
                try {
                    frame = StratosWireProtocol.readFrame(in);
                } catch (EOFException e) {
                    break; // Client closed connection
                }

                switch (frame.type()) {
                    case StratosWireProtocol.MSG_QUERY -> handleQuery(frame, sessionDb, out);
                    case StratosWireProtocol.MSG_PING -> StratosWireProtocol.writeFrame(out, StratosWireProtocol.MSG_PONG, new byte[0]);
                    case StratosWireProtocol.MSG_DISCONNECT -> {
                        return;
                    }
                    default -> sendError(out, StratosWireProtocol.ERR_UNSUPPORTED, "Unknown message type: " + frame.type());
                }
            }

        } catch (IOException e) {
            LOG.debug("StratosWire connection {} closed: {}", remote, e.getMessage());
        } catch (Throwable t) {
            LOG.error("Unexpected error in StratosWire session {}: {}", remote, t.getMessage(), t);
        } finally {
            if (sessionDb != null) {
                sessionDb.closeSession();
            }
            LOG.debug("StratosWire client disconnected: {}", remote);
        }
    }

    private void handleQuery(StratosWireProtocol.Frame frame, StratosDB db, DataOutputStream out) throws IOException {
        DataInputStream qIn = frame.dataStream();
        String sql = qIn.readUTF();

        long start = System.currentTimeMillis();
        QueryResult result;
        try {
            result = db.execute(sql);
        } catch (Exception e) {
            sendError(out, StratosWireProtocol.ERR_SYNTAX, e.getMessage());
            return;
        }

        if (!result.isSuccess()) {
            sendError(out, StratosWireProtocol.ERR_SYNTAX, result.getError());
            return;
        }

        List<Tuple> rows = result.getRows();
        if (rows != null) {
            // Row-returning Query (SELECT / SHOW / EXPLAIN)
            List<String> colNames = result.getColumnNames();
            if (colNames == null && !rows.isEmpty()) {
                colNames = rows.get(0).getColumnNames();
            }
            if (colNames == null) colNames = List.of();

            // Send MSG_QUERY_HEADER
            ByteArrayOutputStream headerBuf = new ByteArrayOutputStream();
            DataOutputStream headerOut = new DataOutputStream(headerBuf);
            headerOut.writeInt(colNames.size());
            for (int i = 0; i < colNames.size(); i++) {
                headerOut.writeUTF(colNames.get(i));
                byte tag = StratosWireProtocol.TYPE_STRING;
                if (!rows.isEmpty() && rows.get(0).size() > i) {
                    tag = inferTypeTag(rows.get(0).getValue(i));
                }
                headerOut.writeByte(tag);
            }
            StratosWireProtocol.writeFrame(out, StratosWireProtocol.MSG_QUERY_HEADER, headerBuf.toByteArray());

            // Send MSG_ROW for each row
            for (Tuple row : rows) {
                ByteArrayOutputStream rowBuf = new ByteArrayOutputStream();
                DataOutputStream rowOut = new DataOutputStream(rowBuf);
                rowOut.writeInt(row.size());
                for (int i = 0; i < row.size(); i++) {
                    StratosWireProtocol.writeTypedValue(rowOut, row.getValue(i));
                }
                StratosWireProtocol.writeFrame(out, StratosWireProtocol.MSG_ROW, rowBuf.toByteArray());
            }

            // Send MSG_QUERY_END
            long elapsed = System.currentTimeMillis() - start;
            ByteArrayOutputStream endBuf = new ByteArrayOutputStream();
            DataOutputStream endOut = new DataOutputStream(endBuf);
            endOut.writeInt(rows.size()); // Row count
            endOut.writeLong(elapsed);   // Execution time in ms
            endOut.writeUTF("OK");
            StratosWireProtocol.writeFrame(out, StratosWireProtocol.MSG_QUERY_END, endBuf.toByteArray());

        } else {
            // Command (INSERT / UPDATE / DELETE / DDL)
            long elapsed = System.currentTimeMillis() - start;
            ByteArrayOutputStream endBuf = new ByteArrayOutputStream();
            DataOutputStream endOut = new DataOutputStream(endBuf);
            endOut.writeInt(0); // 0 rows returned
            endOut.writeLong(elapsed);
            endOut.writeUTF(result.getMessage() != null ? result.getMessage() : "OK");
            StratosWireProtocol.writeFrame(out, StratosWireProtocol.MSG_QUERY_END, endBuf.toByteArray());
        }
    }

    private byte inferTypeTag(Object val) {
        if (val == null) return StratosWireProtocol.TYPE_NULL;
        if (val instanceof Integer) return StratosWireProtocol.TYPE_INT32;
        if (val instanceof Long) return StratosWireProtocol.TYPE_INT64;
        if (val instanceof Double || val instanceof Float) return StratosWireProtocol.TYPE_DOUBLE;
        if (val instanceof Boolean) return StratosWireProtocol.TYPE_BOOLEAN;
        if (val instanceof byte[]) return StratosWireProtocol.TYPE_BYTES;
        return StratosWireProtocol.TYPE_STRING;
    }

    private void sendError(DataOutputStream out, int code, String message) throws IOException {
        ByteArrayOutputStream errBuf = new ByteArrayOutputStream();
        DataOutputStream errOut = new DataOutputStream(errBuf);
        errOut.writeInt(code);
        errOut.writeUTF(message != null ? message : "Unknown error");
        StratosWireProtocol.writeFrame(out, StratosWireProtocol.MSG_ERROR, errBuf.toByteArray());
    }
}
