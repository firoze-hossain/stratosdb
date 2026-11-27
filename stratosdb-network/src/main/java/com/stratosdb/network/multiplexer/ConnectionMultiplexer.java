package com.stratosdb.network.multiplexer;

import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireMessages;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-throughput NIO {@link Selector}-based Connection Multiplexer that decouples
 * client network sockets from SQL execution threads.
 *
 * <p>Idle and active client {@link SocketChannel}s are multiplexed across a small
 * fixed set of NIO reactor threads (no thread-per-connection), while complete
 * SQL requests are dispatched to a bounded worker pool. Shared per-reactor
 * read scratch buffers keep per-connection memory overhead minimal so 10,000+
 * concurrent connections fit comfortably within &lt; 500 MB RAM with zero thread
 * starvation.
 */
public class ConnectionMultiplexer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionMultiplexer.class);
    private static final int READ_SCRATCH_BYTES = 16 * 1024;
    private static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;

    private final int port;
    private final StratosDB db;
    private final int reactorCount;
    private final int workerCount;

    private ServerSocketChannel serverChannel;
    private Reactor[] reactors;
    private Thread acceptorThread;
    private ExecutorService workerPool;
    private volatile boolean running = false;

    private final AtomicInteger nextReactor = new AtomicInteger(0);
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicLong totalQueriesExecuted = new AtomicLong(0);
    private final AtomicInteger nextPid = new AtomicInteger(1000);

    public ConnectionMultiplexer(int port, StratosDB db) {
        this(port, db, Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
             Math.max(4, Runtime.getRuntime().availableProcessors()));
    }

    public ConnectionMultiplexer(int port, StratosDB db, int reactorCount, int workerCount) {
        this.port = port;
        this.db = db;
        this.reactorCount = Math.max(1, reactorCount);
        this.workerCount = Math.max(1, workerCount);
    }

    public void start() throws IOException {
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(true);
        serverChannel.bind(new InetSocketAddress(port), 4096);

        running = true;
        workerPool = Executors.newFixedThreadPool(workerCount, r -> {
            Thread t = new Thread(r, "mux-worker");
            t.setDaemon(true);
            return t;
        });

        reactors = new Reactor[reactorCount];
        for (int i = 0; i < reactorCount; i++) {
            reactors[i] = new Reactor("mux-reactor-" + i);
            reactors[i].start();
        }

        acceptorThread = new Thread(this::acceptLoop, "mux-acceptor");
        acceptorThread.setDaemon(true);
        acceptorThread.start();
        LOG.info("ConnectionMultiplexer listening on port {} (reactors={}, workers={})", port, reactorCount, workerCount);
    }

    private void acceptLoop() {
        while (running) {
            try {
                SocketChannel client = serverChannel.accept();
                client.configureBlocking(false);
                client.socket().setTcpNoDelay(true);
                int idx = Math.floorMod(nextReactor.getAndIncrement(), reactorCount);
                reactors[idx].register(client);
            } catch (IOException e) {
                if (running) {
                    LOG.debug("Multiplexer accept error: {}", e.getMessage());
                }
            }
        }
    }

    public void stop() {
        running = false;
        try {
            if (serverChannel != null) serverChannel.close();
        } catch (IOException ignored) {
        }
        if (reactors != null) {
            for (Reactor r : reactors) {
                if (r != null) r.shutdown();
            }
        }
        if (workerPool != null) {
            workerPool.shutdownNow();
        }
    }

    @Override
    public void close() {
        stop();
    }

    public int getPort() {
        return serverChannel != null ? serverChannel.socket().getLocalPort() : port;
    }

    public int getActiveConnectionCount() {
        return activeConnections.get();
    }

    public long getTotalQueriesExecuted() {
        return totalQueriesExecuted.get();
    }

    public int getReactorThreadCount() {
        return reactorCount;
    }

    public int getWorkerThreadCount() {
        return workerCount;
    }

    private final class ClientSession {
        final SocketChannel channel;
        final Reactor reactor;
        SelectionKey key;
        boolean startupDone = false;
        String user = "stratos";
        ByteArrayOutputStream partialFrame;
        final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();

        ClientSession(SocketChannel channel, Reactor reactor) {
            this.channel = channel;
            this.reactor = reactor;
        }

        void enqueueWrite(byte[] bytes) {
            writeQueue.add(ByteBuffer.wrap(bytes));
            reactor.scheduleInterest(this, SelectionKey.OP_WRITE | SelectionKey.OP_READ);
        }

        void closeQuietly() {
            if (key != null) {
                key.cancel();
            }
            try {
                if (channel.isOpen()) {
                    channel.close();
                    activeConnections.decrementAndGet();
                }
            } catch (IOException ignored) {
            }
            partialFrame = null;
            writeQueue.clear();
        }
    }

    private final class Reactor extends Thread {
        private final Selector selector;
        private final Queue<SocketChannel> pendingRegistrations = new ConcurrentLinkedQueue<>();
        private final Queue<Runnable> pendingTasks = new ConcurrentLinkedQueue<>();
        private final ByteBuffer readScratch = ByteBuffer.allocateDirect(READ_SCRATCH_BYTES);

        Reactor(String name) throws IOException {
            super(name);
            setDaemon(true);
            this.selector = Selector.open();
        }

        void register(SocketChannel ch) {
            pendingRegistrations.add(ch);
            selector.wakeup();
        }

        void scheduleInterest(ClientSession session, int ops) {
            pendingTasks.add(() -> {
                if (session.key != null && session.key.isValid()) {
                    session.key.interestOps(ops);
                }
            });
            selector.wakeup();
        }

        void shutdown() {
            try {
                selector.close();
            } catch (IOException ignored) {
            }
            interrupt();
        }

        @Override
        public void run() {
            while (running && selector.isOpen()) {
                try {
                    processPendingRegistrations();
                    processPendingTasks();
                    selector.select(50);
                    Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                    while (it.hasNext()) {
                        SelectionKey key = it.next();
                        it.remove();
                        if (!key.isValid()) continue;
                        ClientSession session = (ClientSession) key.attachment();
                        if (key.isReadable()) {
                            handleRead(session);
                        }
                        if (key.isValid() && key.isWritable()) {
                            handleWrite(session);
                        }
                    }
                } catch (Exception e) {
                    if (running) {
                        LOG.debug("Reactor loop exception: {}", e.getMessage());
                    }
                }
            }
            for (SelectionKey k : selector.keys()) {
                Object att = k.attachment();
                if (att instanceof ClientSession s) {
                    s.closeQuietly();
                }
            }
        }

        private void processPendingRegistrations() {
            SocketChannel ch;
            while ((ch = pendingRegistrations.poll()) != null) {
                try {
                    ClientSession session = new ClientSession(ch, this);
                    session.key = ch.register(selector, SelectionKey.OP_READ, session);
                    activeConnections.incrementAndGet();
                } catch (IOException e) {
                    try { ch.close(); } catch (IOException ignored) {}
                }
            }
        }

        private void processPendingTasks() {
            Runnable task;
            while ((task = pendingTasks.poll()) != null) {
                task.run();
            }
        }

        private void handleRead(ClientSession session) {
            readScratch.clear();
            int bytesRead;
            try {
                bytesRead = session.channel.read(readScratch);
            } catch (IOException e) {
                session.closeQuietly();
                return;
            }
            if (bytesRead < 0) {
                session.closeQuietly();
                return;
            }
            if (bytesRead == 0) {
                return;
            }
            readScratch.flip();
            byte[] chunk = new byte[readScratch.remaining()];
            readScratch.get(chunk);
            if (session.partialFrame == null) {
                session.partialFrame = new ByteArrayOutputStream(chunk.length);
            }
            session.partialFrame.write(chunk, 0, chunk.length);
            tryDrainFrames(session);
        }

        private void tryDrainFrames(ClientSession session) {
            byte[] buf = session.partialFrame.toByteArray();
            int pos = 0;

            while (true) {
                if (!session.startupDone) {
                    if (buf.length - pos < 4) break;
                    int len = readIntBE(buf, pos);
                    if (len < 8 || len > MAX_FRAME_BYTES) {
                        session.closeQuietly();
                        return;
                    }
                    if (buf.length - pos < len) break;

                    int code = readIntBE(buf, pos + 4);
                    if (code == StdWireMessages.SSL_REQUEST_CODE) {
                        session.enqueueWrite(new byte[]{'N'});
                        pos += len;
                        continue;
                    }
                    byte[] startupPayload = java.util.Arrays.copyOfRange(buf, pos + 8, pos + len);
                    pos += len;
                    session.startupDone = true;
                    handleStartup(session, startupPayload);
                } else {
                    if (buf.length - pos < 5) break;
                    byte type = buf[pos];
                    int len = readIntBE(buf, pos + 1);
                    if (len < 4 || len > MAX_FRAME_BYTES) {
                        session.closeQuietly();
                        return;
                    }
                    if (buf.length - pos < 1 + len) break;

                    byte[] body = java.util.Arrays.copyOfRange(buf, pos + 5, pos + 1 + len);
                    pos += 1 + len;

                    if (type == 'X') {
                        session.closeQuietly();
                        return;
                    }
                    if (type == 'Q') {
                        // Pause OP_READ while worker executes query to preserve per-connection ordering
                        if (session.key != null && session.key.isValid()) {
                            session.key.interestOps(session.key.interestOps() & ~SelectionKey.OP_READ);
                        }
                        String sql = extractCString(body);
                        workerPool.execute(() -> executeQuery(session, sql));
                    }
                }
            }

            if (pos >= buf.length) {
                session.partialFrame = null; // Release buffer when idle!
            } else if (pos > 0) {
                session.partialFrame = new ByteArrayOutputStream(buf.length - pos);
                session.partialFrame.write(buf, pos, buf.length - pos);
            }
        }

        private void handleWrite(ClientSession session) {
            while (!session.writeQueue.isEmpty()) {
                ByteBuffer buf = session.writeQueue.peek();
                try {
                    session.channel.write(buf);
                } catch (IOException e) {
                    session.closeQuietly();
                    return;
                }
                if (buf.hasRemaining()) {
                    return; // Socket send buffer full; wait for next OP_WRITE
                }
                session.writeQueue.poll();
            }
            if (session.key != null && session.key.isValid()) {
                session.key.interestOps((session.key.interestOps() & ~SelectionKey.OP_WRITE) | SelectionKey.OP_READ);
            }
        }
    }

    private void handleStartup(ClientSession session, byte[] startupPayload) {
        // Extract user parameter if present
        String paramsText = new String(startupPayload, StandardCharsets.UTF_8);
        String[] parts = paramsText.split("\0");
        for (int i = 0; i + 1 < parts.length; i += 2) {
            if ("user".equals(parts[i]) && !parts[i + 1].isBlank()) {
                session.user = parts[i + 1];
            }
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
            DataOutputStream out = new DataOutputStream(baos);
            StdWireMessages.writeAuthenticationOk(out);
            StdWireMessages.writeParameterStatus(out, "server_version", "16.0 (StratosDB Multiplexer)");
            StdWireMessages.writeParameterStatus(out, "client_encoding", "UTF8");
            StdWireMessages.writeBackendKeyData(out, nextPid.getAndIncrement(), 12345);
            StdWireMessages.writeReadyForQuery(out, 'I');
            out.flush();
            session.enqueueWrite(baos.toByteArray());
        } catch (IOException e) {
            session.closeQuietly();
        }
    }

    private void executeQuery(ClientSession session, String sql) {
        try {
            db.setCurrentUser(session.user);
            QueryResult result = db.execute(sql);
            totalQueriesExecuted.incrementAndGet();

            ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
            DataOutputStream out = new DataOutputStream(baos);
            if (!result.isSuccess()) {
                StdWireMessages.writeErrorResponse(out, result.getError());
            } else {
                List<Tuple> rows = result.getRows();
                if (rows != null && !rows.isEmpty()) {
                    List<String> colNames = rows.get(0).getColumnNames();
                    List<StdWireMessages.Column> cols = new ArrayList<>(colNames.size());
                    for (String name : colNames) {
                        cols.add(new StdWireMessages.Column(name, 25, (short) -1));
                    }
                    StdWireMessages.writeRowDescription(out, cols);
                    for (Tuple row : rows) {
                        List<String> vals = new ArrayList<>(colNames.size());
                        for (int i = 0; i < colNames.size(); i++) {
                            Object v = row.getValue(i);
                            vals.add(v != null ? v.toString() : null);
                        }
                        StdWireMessages.writeDataRow(out, vals);
                    }
                    StdWireMessages.writeCommandComplete(out, "SELECT " + rows.size());
                } else {
                    StdWireMessages.writeCommandComplete(out, result.getMessage() != null ? result.getMessage() : "OK");
                }
            }
            StdWireMessages.writeReadyForQuery(out, 'I');
            out.flush();
            session.enqueueWrite(baos.toByteArray());
        } catch (Exception e) {
            session.closeQuietly();
        }
    }

    private static int readIntBE(byte[] b, int pos) {
        return ((b[pos] & 0xFF) << 24) | ((b[pos + 1] & 0xFF) << 16)
            | ((b[pos + 2] & 0xFF) << 8) | (b[pos + 3] & 0xFF);
    }

    private static String extractCString(byte[] body) {
        int len = 0;
        while (len < body.length && body[len] != 0) len++;
        return new String(body, 0, len, StandardCharsets.UTF_8);
    }
}
