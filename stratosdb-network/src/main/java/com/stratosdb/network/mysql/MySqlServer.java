package com.stratosdb.network.mysql;

import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MySQL Client/Server Protocol v10 Gateway (default port 3306).
 *
 * <p>Supports MySQL HandshakeV10, {@code caching_sha2_password} and
 * {@code mysql_native_password} authentication, {@code COM_QUERY},
 * {@code COM_PING}, {@code COM_INIT_DB}, and {@code COM_QUIT}.
 */
public class MySqlServer {
    public static final int DEFAULT_MYSQL_PORT = 3306;
    private static final Logger LOG = LoggerFactory.getLogger(MySqlServer.class);

    private final int port;
    private final StratosDB db;
    private final Map<String, String> users = new ConcurrentHashMap<>();
    private final AtomicInteger connectionIds = new AtomicInteger(1);
    private final String defaultAuthPlugin;

    private volatile boolean running = false;
    private ServerSocket serverSocket;
    private ExecutorService executor;

    public MySqlServer(StratosDB db) {
        this(DEFAULT_MYSQL_PORT, db, MySqlAuth.PLUGIN_CACHING_SHA2);
    }

    public MySqlServer(int port, StratosDB db) {
        this(port, db, MySqlAuth.PLUGIN_CACHING_SHA2);
    }

    public MySqlServer(int port, StratosDB db, String defaultAuthPlugin) {
        this.port = port;
        this.db = db;
        this.defaultAuthPlugin = defaultAuthPlugin != null ? defaultAuthPlugin : MySqlAuth.PLUGIN_CACHING_SHA2;
    }

    public void addUser(String username, String password) {
        users.put(username, password);
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        executor = Executors.newVirtualThreadPerTaskExecutor();
        Thread acceptThread = new Thread(() -> {
            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    executor.submit(() -> handleClient(client));
                } catch (IOException e) {
                    if (running) LOG.error("MySqlServer accept failed", e);
                }
            }
        }, "mysql-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
        LOG.info("MySqlServer (Protocol v10) listening on port {}", port);
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {
        }
        if (executor != null) executor.shutdownNow();
    }

    public int getPort() {
        return serverSocket != null ? serverSocket.getLocalPort() : port;
    }

    private void handleClient(Socket socket) {
        int connId = connectionIds.getAndIncrement();
        try (Socket s = socket;
             DataInputStream in = new DataInputStream(s.getInputStream());
             OutputStream out = new BufferedOutputStream(s.getOutputStream())) {
            s.setTcpNoDelay(true);

            byte[] scramble = MySqlAuth.generateScramble20();
            MySqlPacketCodec.writePacket(out, 0, MySqlPacketCodec.buildHandshakeV10(connId, scramble, defaultAuthPlugin));
            out.flush();

            MySqlPacketCodec.Packet resp = MySqlPacketCodec.readPacket(in);
            ClientHandshake hs = parseHandshakeResponse41(resp.payload());
            boolean deprecateEof = (hs.capabilityFlags & MySqlPacketCodec.CLIENT_DEPRECATE_EOF) != 0;

            int seq = resp.sequenceId() + 1;
            int authResultSeq = authenticate(hs, scramble, in, out, seq);
            if (authResultSeq < 0) {
                db.getAuditLogger().logEvent("AUTH", hs.username, "FAILED_LOGIN", "MySQL v10 authentication failed");
                return;
            }

            db.setCurrentUser(hs.username);
            MySqlPacketCodec.writePacket(out, authResultSeq, MySqlPacketCodec.buildOkPacket(0, 0, deprecateEof));
            out.flush();

            while (running && !s.isClosed()) {
                MySqlPacketCodec.Packet cmd;
                try {
                    cmd = MySqlPacketCodec.readPacket(in);
                } catch (EOFException e) {
                    break;
                }
                byte[] payload = cmd.payload();
                if (payload.length == 0) continue;
                int command = payload[0] & 0xFF;
                int outSeq = cmd.sequenceId() + 1;

                switch (command) {
                    case MySqlPacketCodec.COM_QUIT -> {
                        return;
                    }
                    case MySqlPacketCodec.COM_PING, MySqlPacketCodec.COM_INIT_DB -> {
                        MySqlPacketCodec.writePacket(out, outSeq, MySqlPacketCodec.buildOkPacket(0, 0, deprecateEof));
                        out.flush();
                    }
                    case MySqlPacketCodec.COM_QUERY -> {
                        String sql = new String(payload, 1, payload.length - 1, StandardCharsets.UTF_8).trim();
                        handleComQuery(sql, out, outSeq, deprecateEof);
                        out.flush();
                    }
                    default -> {
                        MySqlPacketCodec.writePacket(out, outSeq,
                            MySqlPacketCodec.buildErrPacket(1047, "08S01", "Unsupported MySQL command: " + command));
                        out.flush();
                    }
                }
            }
        } catch (IOException e) {
            LOG.debug("MySQL client {} disconnected: {}", connId, e.getMessage());
        }
    }

    private int authenticate(ClientHandshake hs, byte[] scramble, DataInputStream in, OutputStream out, int seq) throws IOException {
        if (users.isEmpty()) {
            // Trust mode when no explicit users are configured
            return seq;
        }
        String expectedPassword = users.get(hs.username);
        if (expectedPassword == null) {
            MySqlPacketCodec.writePacket(out, seq,
                MySqlPacketCodec.buildErrPacket(1045, "28000", "Access denied for user '" + hs.username + "'"));
            out.flush();
            return -1;
        }

        String plugin = hs.authPlugin != null ? hs.authPlugin : defaultAuthPlugin;
        boolean ok;
        if (MySqlAuth.PLUGIN_NATIVE_PASSWORD.equals(plugin)) {
            ok = MySqlAuth.verifyNativePassword(expectedPassword, scramble, hs.authResponse);
        } else {
            ok = MySqlAuth.verifyCachingSha2(expectedPassword, scramble, hs.authResponse);
            if (ok) {
                // Send fast-auth success marker (AuthMoreData: 0x01, 0x03) before OK_Packet
                MySqlPacketCodec.writePacket(out, seq, new byte[]{0x01, 0x03});
                out.flush();
                return seq + 1;
            }
        }

        if (!ok) {
            MySqlPacketCodec.writePacket(out, seq,
                MySqlPacketCodec.buildErrPacket(1045, "28000", "Access denied for user '" + hs.username + "' (using password: YES)"));
            out.flush();
            return -1;
        }
        return seq;
    }

    private void handleComQuery(String sql, OutputStream out, int seq, boolean deprecateEof) throws IOException {
        String upper = sql.toUpperCase();
        // Handle standard MySQL CLI / Workbench session initialization & metadata probes
        if (upper.startsWith("SET NAMES") || upper.startsWith("SET CHARACTER_SET")
                || upper.startsWith("SET SQL_MODE") || upper.startsWith("SET AUTOCOMMIT")) {
            MySqlPacketCodec.writePacket(out, seq, MySqlPacketCodec.buildOkPacket(0, 0, deprecateEof));
            return;
        }
        if (upper.equals("SELECT @@VERSION_COMMENT LIMIT 1") || upper.equals("SELECT @@VERSION_COMMENT")) {
            sendSingleValueResult(out, seq, deprecateEof, "@@version_comment", "StratosDB MySQL Gateway");
            return;
        }
        if (upper.equals("SELECT @@VERSION") || upper.equals("SELECT VERSION()")) {
            sendSingleValueResult(out, seq, deprecateEof, "VERSION()", MySqlPacketCodec.SERVER_VERSION);
            return;
        }
        if (upper.equals("SELECT DATABASE()")) {
            sendSingleValueResult(out, seq, deprecateEof, "DATABASE()", "stratos");
            return;
        }

        QueryResult result = db.execute(sql);
        if (!result.isSuccess()) {
            MySqlPacketCodec.writePacket(out, seq, MySqlPacketCodec.buildErrPacket(1064, "42000", result.getError()));
            return;
        }
        List<Tuple> rows = result.getRows();
        if (rows == null || rows.isEmpty()) {
            // Check if it was a SELECT returning 0 rows vs DDL/DML
            if (upper.startsWith("SELECT") || upper.startsWith("SHOW") || upper.startsWith("WITH")) {
                List<String> cols = List.of("result");
                sendResultSet(out, seq, deprecateEof, cols, List.of());
            } else {
                MySqlPacketCodec.writePacket(out, seq, MySqlPacketCodec.buildOkPacket(0, 0, deprecateEof));
            }
            return;
        }

        List<String> colNames = rows.get(0).getColumnNames();
        sendResultSet(out, seq, deprecateEof, colNames, rows);
    }

    private void sendSingleValueResult(OutputStream out, int seq, boolean deprecateEof, String colName, String value) throws IOException {
        Tuple t = new Tuple();
        t.addValue(colName, value);
        sendResultSet(out, seq, deprecateEof, List.of(colName), List.of(t));
    }

    private void sendResultSet(OutputStream out, int seq, boolean deprecateEof, List<String> colNames, List<Tuple> rows) throws IOException {
        MySqlPacketCodec.writePacket(out, seq++, MySqlPacketCodec.buildColumnCountPacket(colNames.size()));
        for (int i = 0; i < colNames.size(); i++) {
            int mysqlType = MySqlPacketCodec.TYPE_VAR_STRING;
            if (!rows.isEmpty()) {
                Object sample = rows.get(0).getValue(i);
                if (sample instanceof Integer || sample instanceof Short) mysqlType = MySqlPacketCodec.TYPE_LONG;
                else if (sample instanceof Long) mysqlType = MySqlPacketCodec.TYPE_LONGLONG;
                else if (sample instanceof Double || sample instanceof Float) mysqlType = MySqlPacketCodec.TYPE_DOUBLE;
            }
            MySqlPacketCodec.writePacket(out, seq++, MySqlPacketCodec.buildColumnDef41("stratos", "", colNames.get(i), mysqlType));
        }
        if (!deprecateEof) {
            MySqlPacketCodec.writePacket(out, seq++, MySqlPacketCodec.buildEofPacket());
        }
        for (Tuple row : rows) {
            List<String> textVals = new ArrayList<>(colNames.size());
            for (int i = 0; i < colNames.size(); i++) {
                Object val = row.getValue(i);
                textVals.add(val != null ? val.toString() : null);
            }
            MySqlPacketCodec.writePacket(out, seq++, MySqlPacketCodec.buildTextRowPacket(textVals));
        }
        if (deprecateEof) {
            MySqlPacketCodec.writePacket(out, seq, MySqlPacketCodec.buildOkPacket(0, 0, true));
        } else {
            MySqlPacketCodec.writePacket(out, seq, MySqlPacketCodec.buildEofPacket());
        }
    }

    private record ClientHandshake(int capabilityFlags, String username, byte[] authResponse, String database, String authPlugin) {}

    private static ClientHandshake parseHandshakeResponse41(byte[] buf) {
        int caps = MySqlPacketCodec.readInt4LE(buf, 0);
        int pos = 32; // caps(4) + maxPacket(4) + charset(1) + reserved(23)

        int userStart = pos;
        while (pos < buf.length && buf[pos] != 0) pos++;
        String username = new String(buf, userStart, pos - userStart, StandardCharsets.UTF_8);
        pos++; // skip NUL

        byte[] authResp = new byte[0];
        if ((caps & MySqlPacketCodec.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            int len = buf[pos++] & 0xFF;
            authResp = new byte[len];
            System.arraycopy(buf, pos, authResp, 0, len);
            pos += len;
        } else if ((caps & MySqlPacketCodec.CLIENT_SECURE_CONNECTION) != 0) {
            int len = buf[pos++] & 0xFF;
            authResp = new byte[len];
            System.arraycopy(buf, pos, authResp, 0, len);
            pos += len;
        } else {
            int authStart = pos;
            while (pos < buf.length && buf[pos] != 0) pos++;
            authResp = java.util.Arrays.copyOfRange(buf, authStart, pos);
            pos++;
        }

        String database = null;
        if ((caps & MySqlPacketCodec.CLIENT_CONNECT_WITH_DB) != 0 && pos < buf.length) {
            int dbStart = pos;
            while (pos < buf.length && buf[pos] != 0) pos++;
            database = new String(buf, dbStart, pos - dbStart, StandardCharsets.UTF_8);
            pos++;
        }

        String authPlugin = null;
        if ((caps & MySqlPacketCodec.CLIENT_PLUGIN_AUTH) != 0 && pos < buf.length) {
            int plugStart = pos;
            while (pos < buf.length && buf[pos] != 0) pos++;
            authPlugin = new String(buf, plugStart, pos - plugStart, StandardCharsets.UTF_8);
        }
        return new ClientHandshake(caps, username, authResp, database, authPlugin);
    }
}
