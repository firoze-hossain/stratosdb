package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.PrometheusExporter;
import com.stratosdb.network.stdwire.StdWireMessages;
import com.stratosdb.network.stdwire.StdWireServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, end-to-end proof that SHOW ACTIVITY and the real Prometheus
 * exporter both work - real, separate connections and a real HTTP GET,
 * not a simulation of either. See StratosDBTest for SHOW STATEMENTS and
 * SHOW TABLE STATS, which need no separate connection and are tested
 * in-process there instead.
 */
public class ObservabilityEndToEndTest {

    private StratosDB db;
    private StdWireServer server;
    private PrometheusExporter exporter;

    @AfterEach
    void tearDown() {
        if (exporter != null) exporter.stop();
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void showActivityReflectsARealSecondConnectionsRealLiveState(@TempDir Path tempDir) throws Exception {
        int port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        server = new StdWireServer(port, db);
        server.start();
        Thread.sleep(200);

        RawClient setup = new RawClient("localhost", port, "alice");
        setup.sendQuery("CREATE TABLE t (id INT)");
        setup.close();

        // A second, real, separate connection - held open and idle deliberately,
        // so a third connection's own SHOW ACTIVITY can observe its real state.
        RawClient idleClient = new RawClient("localhost", port, "bob");
        idleClient.sendQuery("SELECT * FROM t");

        RawClient observer = new RawClient("localhost", port, "carol");
        String result = observer.sendQuery("SHOW ACTIVITY");
        assertNull(result);
        List<String> activityLines = observer.lastResultLines;

        assertTrue(activityLines.stream().anyMatch(l -> l.contains("bob") && l.contains("idle")),
            () -> "bob's own real, separate connection must appear as idle after its own query completed: " + activityLines);
        assertTrue(activityLines.stream().anyMatch(l -> l.contains("carol")),
            () -> "the observing connection itself must also appear, since it's a real, currently-registered session too: " + activityLines);

        idleClient.close();
        observer.close();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void prometheusExporterServesRealLiveMetricsOverRealHttp(@TempDir Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        db.execute("CREATE TABLE t (id INT)");
        db.execute("INSERT INTO t VALUES (1)");
        db.execute("INSERT INTO t VALUES (2)");
        db.execute("SELECT * FROM t WHERE id = 1");

        int port = freePort();
        exporter = new PrometheusExporter(port, db);
        exporter.start();
        Thread.sleep(200);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/metrics")).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").contains("text/plain"),
            "a real Prometheus scrape expects text/plain, matching the real exposition format");

        String body = response.body();
        assertTrue(body.contains("# TYPE stratosdb_table_rows_inserted_total counter"), "must declare a real TYPE line for every real metric");
        assertTrue(body.contains("stratosdb_table_rows_inserted_total{table=\"t\"} 2"), "must reflect the real, current insert count: " + body);
        assertTrue(body.contains("stratosdb_query_calls_total{query=\"SELECT * FROM t WHERE id = ?\"} 1"), "must reflect the real, normalized query's own call count: " + body);
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** A minimal, real, trust-authenticated wire-protocol client, tracking the last query's own result rows as plain text lines for simple substring assertions. */
    private static class RawClient {
        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;
        List<String> lastResultLines = new java.util.ArrayList<>();

        RawClient(String host, int port, String user) throws Exception {
            socket = new Socket(host, port);
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            StdWireMessages.writeStartupMessage(out, user, "anydb");
            out.flush();
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                if (msg.type() == 'Z') break;
            }
        }

        String sendQuery(String sql) throws Exception {
            lastResultLines = new java.util.ArrayList<>();
            StdWireMessages.writeQuery(out, sql);
            out.flush();
            String error = null;
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                if (msg.type() == 'D') {
                    lastResultLines.add(decodeDataRow(msg));
                } else if (msg.type() == 'E') {
                    error = extractError(msg);
                } else if (msg.type() == 'Z') {
                    return error;
                }
            }
        }

        private String decodeDataRow(StdWireMessages.TypedMessage msg) {
            byte[] b = msg.body();
            StringBuilder sb = new StringBuilder();
            int pos = 0;
            int columnCount = ((b[pos] & 0xFF) << 8) | (b[pos + 1] & 0xFF);
            pos += 2;
            for (int i = 0; i < columnCount; i++) {
                int len = ((b[pos] & 0xFF) << 24) | ((b[pos + 1] & 0xFF) << 16) | ((b[pos + 2] & 0xFF) << 8) | (b[pos + 3] & 0xFF);
                pos += 4;
                if (len >= 0) {
                    sb.append(new String(b, pos, len, java.nio.charset.StandardCharsets.UTF_8)).append(" | ");
                    pos += len;
                } else {
                    sb.append("NULL | ");
                }
            }
            return sb.toString();
        }

        private String extractError(StdWireMessages.TypedMessage msg) {
            byte[] b = msg.body();
            int pos = 0;
            while (pos < b.length && b[pos] != 0) {
                char field = (char) b[pos]; pos++;
                int start = pos;
                while (b[pos] != 0) pos++;
                String value = new String(b, start, pos - start, java.nio.charset.StandardCharsets.UTF_8);
                pos++;
                if (field == 'M') return value;
            }
            return "unknown";
        }

        void close() throws Exception {
            socket.close();
        }
    }
}
