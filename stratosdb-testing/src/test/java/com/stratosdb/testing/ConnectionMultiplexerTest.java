package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.multiplexer.ConnectionMultiplexer;
import com.stratosdb.network.stdwire.StdWireMessages;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Sprint 11 Week 22: Server-side NIO Connection Multiplexer
 * handling 10,000+ concurrent idle and active connections with < 500MB RAM
 * and zero thread starvation.
 */
class ConnectionMultiplexerTest {

    private static int findFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testTenThousandConnectionsUnder500MbAndZeroThreadStarvation(@TempDir Path tempDir) throws Exception {
        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(cfg);
        int port = findFreePort();

        ConnectionMultiplexer mux = new ConnectionMultiplexer(port, db, 4, 8);
        mux.start();

        final int targetConnections = 10_000;
        List<SocketChannel> idleChannels = new ArrayList<>(targetConnections);

        try {
            InetSocketAddress addr = new InetSocketAddress("127.0.0.1", port);
            for (int i = 0; i < targetConnections; i++) {
                SocketChannel ch = SocketChannel.open(addr);
                idleChannels.add(ch);
            }

            long deadline = System.currentTimeMillis() + 10_000;
            while (mux.getActiveConnectionCount() < targetConnections && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(targetConnections, mux.getActiveConnectionCount(),
                "Multiplexer must hold 10,000 simultaneous client connections");

            // Verify decoupled threading (no thread-per-connection!)
            int liveThreads = Thread.activeCount();
            assertTrue(liveThreads < 150,
                () -> "Multiplexer must decouple sockets from threads (expected < 150 threads for 10k connections, got " + liveThreads + ")");

            // Verify heap memory footprint < 500 MB
            System.gc();
            Runtime rt = Runtime.getRuntime();
            long usedMb = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
            assertTrue(usedMb < 500,
                () -> "10,000 connections must use < 500MB RAM, used " + usedMb + " MB");

            // Execute active SQL queries over multiplexed wire connections while 10,000 idle sockets remain open
            try (Socket activeClient = new Socket("127.0.0.1", port)) {
                DataOutputStream out = new DataOutputStream(activeClient.getOutputStream());
                DataInputStream in = new DataInputStream(activeClient.getInputStream());

                StdWireMessages.writeStartupMessage(out, "stratos", "stratos");
                out.flush();
                // Read AuthOk, ParameterStatus x2, BackendKeyData, ReadyForQuery
                while (true) {
                    StdWireMessages.TypedMessage m = StdWireMessages.readTypedMessage(in);
                    if (m.type() == 'Z') break;
                }

                StdWireMessages.writeQuery(out, "CREATE TABLE mux_test (id INT, label VARCHAR)");
                out.flush();
                while (StdWireMessages.readTypedMessage(in).type() != 'Z') {}

                StdWireMessages.writeQuery(out, "INSERT INTO mux_test VALUES (42, 'multiplexed')");
                out.flush();
                while (StdWireMessages.readTypedMessage(in).type() != 'Z') {}

                StdWireMessages.writeQuery(out, "SELECT id, label FROM mux_test WHERE id = 42");
                out.flush();
                List<String> rowValues = new ArrayList<>();
                while (true) {
                    StdWireMessages.TypedMessage m = StdWireMessages.readTypedMessage(in);
                    if (m.type() == 'D') {
                        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(m.body());
                        int cols = bb.getShort();
                        rowValues = new ArrayList<>(cols);
                        for (int c = 0; c < cols; c++) {
                            int len = bb.getInt();
                            if (len < 0) {
                                rowValues.add(null);
                            } else {
                                byte[] valBytes = new byte[len];
                                bb.get(valBytes);
                                rowValues.add(new String(valBytes, java.nio.charset.StandardCharsets.UTF_8));
                            }
                        }
                    } else if (m.type() == 'Z') {
                        break;
                    }
                }
                assertEquals(List.of("42", "multiplexed"), rowValues);

                StdWireMessages.writeTerminate(out);
                out.flush();
            }

            assertTrue(mux.getTotalQueriesExecuted() >= 3);
        } finally {
            for (SocketChannel ch : idleChannels) {
                try { ch.close(); } catch (Exception ignored) {}
            }
            mux.stop();
            db.shutdown();
        }
    }
}
