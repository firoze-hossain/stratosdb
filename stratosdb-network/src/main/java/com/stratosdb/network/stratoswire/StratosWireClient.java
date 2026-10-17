package com.stratosdb.network.stratoswire;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Stratos Native Binary Wire Protocol Client.
 *
 * Provides a clean, lightweight, high-performance client connector
 * to connect directly to any StratosDB server speaking the Stratos Native Protocol.
 */
public class StratosWireClient implements AutoCloseable {

    private final String host;
    private final int port;
    private final String username;
    private final String database;
    private final String password;

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private boolean connected = false;

    private String serverVersion;
    private long sessionId;

    public StratosWireClient(String host, int port, String username, String database, String password) {
        this.host = host;
        this.port = port;
        this.username = username != null ? username : "stratos";
        this.database = database != null ? database : "stratos";
        this.password = password != null ? password : "";
    }

    public synchronized void connect() throws IOException {
        if (connected) return;
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

        // Send MSG_CONNECT_REQ
        ByteArrayOutputStream connectBuf = new ByteArrayOutputStream();
        DataOutputStream connectOut = new DataOutputStream(connectBuf);
        connectOut.writeInt(StratosWireProtocol.PROTOCOL_VERSION);
        connectOut.writeUTF(username);
        connectOut.writeUTF(database);
        connectOut.writeUTF(password);
        StratosWireProtocol.writeFrame(out, StratosWireProtocol.MSG_CONNECT_REQ, connectBuf.toByteArray());

        // Read Server Response
        StratosWireProtocol.Frame resp = StratosWireProtocol.readFrame(in);
        if (resp.type() == StratosWireProtocol.MSG_ERROR) {
            DataInputStream errIn = resp.dataStream();
            int code = errIn.readInt();
            String msg = errIn.readUTF();
            throw new IOException("Connection failed [" + code + "]: " + msg);
        }

        if (resp.type() != StratosWireProtocol.MSG_CONNECT_OK) {
            throw new IOException("Unexpected handshake response type: " + resp.type());
        }

        DataInputStream okIn = resp.dataStream();
        int protocolVersion = okIn.readInt();
        this.serverVersion = okIn.readUTF();
        this.sessionId = okIn.readLong();
        this.connected = true;
    }

    public synchronized Response executeQuery(String sql) throws IOException {
        ensureConnected();

        ByteArrayOutputStream queryBuf = new ByteArrayOutputStream();
        DataOutputStream queryOut = new DataOutputStream(queryBuf);
        queryOut.writeUTF(sql);
        StratosWireProtocol.writeFrame(out, StratosWireProtocol.MSG_QUERY, queryBuf.toByteArray());

        List<StratosWireProtocol.ColumnMeta> columns = new ArrayList<>();
        List<Object[]> rows = new ArrayList<>();
        int affectedRows = 0;
        long executionTimeMs = 0;
        String message = "";

        while (true) {
            StratosWireProtocol.Frame frame = StratosWireProtocol.readFrame(in);

            if (frame.type() == StratosWireProtocol.MSG_ERROR) {
                DataInputStream errIn = frame.dataStream();
                int code = errIn.readInt();
                String errMessage = errIn.readUTF();
                throw new IOException("Query error [" + code + "]: " + errMessage);
            }

            if (frame.type() == StratosWireProtocol.MSG_QUERY_HEADER) {
                DataInputStream hIn = frame.dataStream();
                int colCount = hIn.readInt();
                for (int i = 0; i < colCount; i++) {
                    String colName = hIn.readUTF();
                    byte typeTag = hIn.readByte();
                    columns.add(new StratosWireProtocol.ColumnMeta(colName, typeTag));
                }
            } else if (frame.type() == StratosWireProtocol.MSG_ROW) {
                DataInputStream rIn = frame.dataStream();
                int valCount = rIn.readInt();
                Object[] values = new Object[valCount];
                for (int i = 0; i < valCount; i++) {
                    values[i] = StratosWireProtocol.readTypedValue(rIn);
                }
                rows.add(values);
            } else if (frame.type() == StratosWireProtocol.MSG_QUERY_END) {
                DataInputStream endIn = frame.dataStream();
                affectedRows = endIn.readInt();
                executionTimeMs = endIn.readLong();
                message = endIn.readUTF();
                break;
            } else {
                throw new IOException("Unexpected frame type during query: " + frame.type());
            }
        }

        return new Response(columns, rows, affectedRows, executionTimeMs, message);
    }

    public synchronized boolean ping() {
        if (!connected) return false;
        try {
            StratosWireProtocol.writeFrame(out, StratosWireProtocol.MSG_PING, new byte[0]);
            StratosWireProtocol.Frame frame = StratosWireProtocol.readFrame(in);
            return frame.type() == StratosWireProtocol.MSG_PONG;
        } catch (IOException e) {
            return false;
        }
    }

    private void ensureConnected() throws IOException {
        if (!connected || socket == null || socket.isClosed()) {
            connect();
        }
    }

    @Override
    public synchronized void close() {
        if (!connected) return;
        connected = false;
        try {
            if (out != null) {
                StratosWireProtocol.writeFrame(out, StratosWireProtocol.MSG_DISCONNECT, new byte[0]);
            }
        } catch (IOException ignored) {}
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException ignored) {}
    }

    public String getServerVersion() { return serverVersion; }
    public long getSessionId() { return sessionId; }

    public record Response(
        List<StratosWireProtocol.ColumnMeta> columns,
        List<Object[]> rows,
        int affectedRows,
        long executionTimeMs,
        String message
    ) {
        public boolean isQuery() {
            return !columns.isEmpty() || !rows.isEmpty();
        }
    }
}
