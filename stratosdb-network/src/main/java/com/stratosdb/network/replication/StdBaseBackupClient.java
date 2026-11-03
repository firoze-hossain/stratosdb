package com.stratosdb.network.replication;

import com.stratosdb.network.auth.ScramClient;
import com.stratosdb.network.auth.ScramSha256;
import com.stratosdb.network.stdwire.StdWireMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Network client for PostgreSQL wire protocol replication commands ({@code IDENTIFY_SYSTEM}
 * and {@code BASE_BACKUP}).
 *
 * Connects to a primary {@link com.stratosdb.network.stdwire.StdWireServer} with replication
 * mode enabled, executes {@code BASE_BACKUP}, and streams the primary's data directory tarball
 * directly over the TCP connection into a local directory, enabling fully automated, zero-copy
 * replica bootstrapping over the network.
 */
public class StdBaseBackupClient implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(StdBaseBackupClient.class);

    private final String host;
    private final int port;
    private final String user;
    private final String database;
    private final String password;

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private boolean connected = false;

    public record IdentifySystemResult(String systemId, long timeline, String xlogpos, String dbname) {}

    public record StdBaseBackupResult(
        Path targetDir,
        long startEpoch,
        long startLsn,
        long endEpoch,
        long endLsn,
        String label,
        int filesExtracted
    ) {}

    public StdBaseBackupClient(String host, int port) {
        this(host, port, "stratos", null, null);
    }

    public StdBaseBackupClient(String host, int port, String user, String database) {
        this(host, port, user, database, null);
    }

    public StdBaseBackupClient(String host, int port, String user, String database, String password) {
        this.host = host;
        this.port = port;
        this.user = (user != null && !user.isBlank()) ? user : "stratos";
        this.database = database;
        this.password = password;
    }

    public synchronized void connect() throws IOException {
        if (connected) return;

        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), 10000);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

        Map<String, String> params = new LinkedHashMap<>();
        params.put("user", user);
        if (database != null && !database.isBlank()) {
            params.put("database", database);
        }
        params.put("replication", "database");
        params.put("application_name", "std_basebackup");

        StdWireMessages.writeStartupMessage(out, params);

        while (true) {
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
            if (msg.type() == 'R') {
                int authType = ByteBuffer.wrap(msg.body(), 0, 4).getInt();
                if (authType == 0) {
                    // AuthenticationOk
                    continue;
                } else if (authType == 10) { // SASL request
                    handleSaslAuth(msg);
                } else {
                    throw new IOException("Unsupported authentication request type: " + authType);
                }
            } else if (msg.type() == 'K') { // BackendKeyData
                continue;
            } else if (msg.type() == 'S') { // ParameterStatus
                continue;
            } else if (msg.type() == 'Z') { // ReadyForQuery
                break;
            } else if (msg.type() == 'E') { // ErrorResponse
                String err = new String(msg.body(), StandardCharsets.UTF_8);
                throw new IOException("Connection failed: " + err);
            }
        }

        connected = true;
        LOG.info("StdBaseBackupClient connected to primary at {}:{}", host, port);
    }

    private void handleSaslAuth(StdWireMessages.TypedMessage msg) throws IOException {
        if (password == null) {
            throw new IOException("Server requested SASL authentication, but no password was provided");
        }
        ScramClient scram = new ScramClient(user, password);
        String clientFirstBare = scram.buildClientFirstMessage();
        byte[] firstMsgBytes = clientFirstBare.getBytes(StandardCharsets.UTF_8);

        // Send SASLInitialResponse ('p')
        StdWireMessages.writeSaslInitialResponse(out, ScramSha256.MECHANISM_NAME, firstMsgBytes);
        out.flush();

        // Read SASLContinue
        StdWireMessages.TypedMessage continueMsg = StdWireMessages.readTypedMessage(in);
        if (continueMsg.type() != 'R') {
            throw new IOException("Expected SASLContinue ('R'), got: " + continueMsg.type());
        }
        int code = ByteBuffer.wrap(continueMsg.body(), 0, 4).getInt();
        if (code != 11) {
            throw new IOException("Expected SASLContinue code 11, got: " + code);
        }
        String serverFirst = new String(continueMsg.body(), 4, continueMsg.body().length - 4, StandardCharsets.UTF_8);

        try {
            String clientFinal = scram.buildClientFinalMessage(serverFirst);
            byte[] finalBytes = clientFinal.getBytes(StandardCharsets.UTF_8);

            // Send SASLResponse ('p')
            out.writeByte('p');
            out.writeInt(finalBytes.length + 4);
            out.write(finalBytes);
            out.flush();

            // Read SASLFinal ('R' code 12)
            StdWireMessages.TypedMessage finalMsg = StdWireMessages.readTypedMessage(in);
            if (finalMsg.type() != 'R') {
                throw new IOException("Expected SASLFinal ('R'), got: " + finalMsg.type());
            }
            int finalCode = ByteBuffer.wrap(finalMsg.body(), 0, 4).getInt();
            if (finalCode == 12) {
                String serverFinal = new String(finalMsg.body(), 4, finalMsg.body().length - 4, StandardCharsets.UTF_8);
                scram.verifyServerFinalMessage(serverFinal);
                // Read following AuthenticationOk (code 0)
                StdWireMessages.TypedMessage okMsg = StdWireMessages.readTypedMessage(in);
                if (okMsg.type() != 'R' || ByteBuffer.wrap(okMsg.body(), 0, 4).getInt() != 0) {
                    throw new IOException("Authentication did not complete with AuthenticationOk");
                }
            } else if (finalCode != 0) {
                throw new IOException("SASL authentication failed with code: " + finalCode);
            }
        } catch (Exception e) {
            throw new IOException("SCRAM-SHA-256 handshake failed: " + e.getMessage(), e);
        }
    }

    /**
     * Executes {@code IDENTIFY_SYSTEM} replication command on the connected primary.
     */
    public synchronized IdentifySystemResult identifySystem() throws IOException {
        ensureConnected();
        StdWireMessages.writeQuery(out, "IDENTIFY_SYSTEM");

        List<String> values = null;
        while (true) {
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
            if (msg.type() == 'T') {
                // RowDescription
                continue;
            } else if (msg.type() == 'D') {
                // DataRow: systemid, timeline, xlogpos, dbname
                values = parseDataRow(msg);
            } else if (msg.type() == 'C') {
                // CommandComplete
                continue;
            } else if (msg.type() == 'Z') {
                // ReadyForQuery
                break;
            } else if (msg.type() == 'E') {
                throw new IOException("IDENTIFY_SYSTEM failed: " + new String(msg.body(), StandardCharsets.UTF_8));
            }
        }

        if (values == null || values.size() < 4) {
            throw new IOException("IDENTIFY_SYSTEM returned invalid row shape");
        }
        return new IdentifySystemResult(
            values.get(0),
            Long.parseLong(values.get(1)),
            values.get(2),
            values.get(3)
        );
    }

    /**
     * Executes {@code BASE_BACKUP} replication command on the connected primary,
     * streams the tarball directly over the wire, and unpacks it into {@code targetDir}.
     *
     * @param targetDir destination directory where backup contents are extracted
     * @param options base backup options
     * @return result containing backup locations and extracted file count
     */
    public synchronized StdBaseBackupResult executeBaseBackup(Path targetDir, StdBaseBackupOptions options) throws IOException {
        ensureConnected();

        StringBuilder query = new StringBuilder("BASE_BACKUP (");
        query.append("LABEL '").append(options.label()).append("'");
        if (options.fast()) query.append(", FAST");
        if (options.wal()) query.append(", WAL");
        if (options.progress()) query.append(", PROGRESS");
        if (options.noWait()) query.append(", NOWAIT");
        if (options.maxRateBytesPerSec() > 0) query.append(", MAX_RATE ").append(options.maxRateBytesPerSec() / 1024L);
        query.append(")");

        LOG.info("Sending replication command: {}", query);
        StdWireMessages.writeQuery(out, query.toString());

        // 1. Read first result set (starting LSN & timeline)
        List<String> startRow = null;
        while (true) {
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
            if (msg.type() == 'T') {
                continue;
            } else if (msg.type() == 'D') {
                startRow = parseDataRow(msg);
            } else if (msg.type() == 'C') {
                break;
            } else if (msg.type() == 'E') {
                throw new IOException("BASE_BACKUP start failed: " + new String(msg.body(), StandardCharsets.UTF_8));
            }
        }

        if (startRow == null || startRow.size() < 2) {
            throw new IOException("BASE_BACKUP did not return start position row");
        }
        long[] startPos = parsePos(startRow.get(0), startRow.get(1));

        // 2. Read CopyOutResponse ('H')
        StdWireMessages.TypedMessage copyOutMsg = StdWireMessages.readTypedMessage(in);
        if (copyOutMsg.type() != 'H') {
            throw new IOException("Expected CopyOutResponse ('H'), got: '" + copyOutMsg.type() + "'");
        }

        // 3. Extract tar archive stream directly into targetDir
        StdCopyDataInputStream copyIn = new StdCopyDataInputStream(in);
        int filesExtracted = StdTarArchive.extract(copyIn, targetDir);
        LOG.info("BASE_BACKUP streamed and extracted {} file(s) into {}", filesExtracted, targetDir);

        // 4. Read second result set (ending LSN & timeline)
        List<String> endRow = null;
        while (true) {
            StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
            if (msg.type() == 'T') {
                continue;
            } else if (msg.type() == 'D') {
                endRow = parseDataRow(msg);
            } else if (msg.type() == 'C') {
                // CommandComplete
                continue;
            } else if (msg.type() == 'Z') {
                // ReadyForQuery
                break;
            } else if (msg.type() == 'E') {
                throw new IOException("BASE_BACKUP ending phase failed: " + new String(msg.body(), StandardCharsets.UTF_8));
            }
        }

        if (endRow == null || endRow.size() < 2) {
            throw new IOException("BASE_BACKUP did not return end position row");
        }
        long[] endPos = parsePos(endRow.get(0), endRow.get(1));

        return new StdBaseBackupResult(
            targetDir,
            startPos[0],
            startPos[1],
            endPos[0],
            endPos[1],
            options.label(),
            filesExtracted
        );
    }

    private static List<String> parseDataRow(StdWireMessages.TypedMessage msg) {
        ByteBuffer buf = ByteBuffer.wrap(msg.body());
        int colCount = buf.getShort() & 0xFFFF;
        List<String> values = new ArrayList<>(colCount);
        for (int i = 0; i < colCount; i++) {
            int len = buf.getInt();
            if (len == -1) {
                values.add(null);
            } else {
                byte[] b = new byte[len];
                buf.get(b);
                values.add(new String(b, StandardCharsets.UTF_8));
            }
        }
        return values;
    }

    private static long[] parsePos(String recptr, String tli) {
        long epoch = 0;
        long lsn = 0;
        if (tli != null && !tli.isBlank()) {
            try {
                epoch = Long.parseLong(tli.trim());
            } catch (NumberFormatException ignored) {}
        }
        if (recptr != null && !recptr.isBlank()) {
            if (recptr.contains("/")) {
                String[] parts = recptr.trim().split("/");
                try {
                    epoch = Long.parseLong(parts[0], 16);
                    lsn = Long.parseLong(parts[1], 16);
                } catch (NumberFormatException ignored) {}
            } else {
                try {
                    lsn = Long.parseLong(recptr.trim());
                } catch (NumberFormatException ignored) {}
            }
        }
        return new long[]{epoch, lsn};
    }

    private void ensureConnected() throws IOException {
        if (!connected) connect();
    }

    @Override
    public synchronized void close() {
        if (connected) {
            try {
                StdWireMessages.writeTerminate(out);
            } catch (Exception ignored) {}
            connected = false;
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (Exception ignored) {}
        }
    }
}
