package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.auth.ScramClient;
import com.stratosdb.network.auth.ScramSha256;
import com.stratosdb.network.auth.UserStore;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, end-to-end proof that COPY ... FROM/TO STDIN/STDOUT actually
 * works over a real connection - the real CopyInResponse/CopyOutResponse/
 * CopyData/CopyDone wire messages, driven by a real client, not just
 * ExecutorEngine's own internal per-row methods called directly (see
 * StratosDBTest's own file-based COPY tests for that coverage, and its
 * own real, separate finding: bare boolean literals never worked at all
 * anywhere in this SQL dialect, fixed as part of building this feature).
 *
 * This is the more practically valuable COPY variant - real client-
 * driven bulk load/export with no need for the data to already be a
 * file on the server's own filesystem, matching how psql's own `\copy`
 * meta-command actually works against real Postgres.
 */
public class CopyStdioEndToEndTest {

    private StratosDB db;
    private StdWireServer server;

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void copyFromStdinAndToStdoutRoundTripRealDataOverARealConnection(@TempDir Path tempDir) throws Exception {
        int port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        UserStore userStore = new UserStore();
        userStore.addUser("admin", "adminpass");
        server = new StdWireServer(port, db, userStore);
        server.start();
        Thread.sleep(200);

        TestClient client = new TestClient("localhost", port, "admin", "anydb", "adminpass");
        try {
            client.sendQuery("CREATE TABLE employees (id INT, name VARCHAR, department VARCHAR)");
            assertNull(client.readUntilReadyForQuery());

            client.sendQuery("COPY employees FROM STDIN");
            String copyInError = client.doCopyFromStdin(List.of(
                "1\tAlice\tEngineering",
                "2\tBob\tSales",
                "3\tCarol\t\\N"
            ));
            assertNull(copyInError, () -> "COPY FROM STDIN must succeed: " + copyInError);

            client.sendQuery("COPY employees TO STDOUT");
            List<String> lines = client.doCopyToStdout();
            assertEquals(3, lines.size(), () -> "COPY TO STDOUT must return exactly the 3 rows just loaded: " + lines);
            assertTrue(lines.stream().anyMatch(l -> l.contains("Alice") && l.contains("Engineering")));
            assertTrue(lines.stream().anyMatch(l -> l.contains("Carol") && l.contains("\\N")),
                () -> "NULL must be represented as \\N in the real wire output: " + lines);
        } finally {
            client.close();
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void copyFromStdinAbortsTheWholeTransactionOnABadRow(@TempDir Path tempDir) throws Exception {
        int port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        UserStore userStore = new UserStore();
        userStore.addUser("admin", "adminpass");
        server = new StdWireServer(port, db, userStore);
        server.start();
        Thread.sleep(200);

        TestClient client = new TestClient("localhost", port, "admin", "anydb", "adminpass");
        try {
            client.sendQuery("CREATE TABLE strict_types (id INT, score INT)");
            assertNull(client.readUntilReadyForQuery());

            client.sendQuery("COPY strict_types FROM STDIN");
            String error = client.doCopyFromStdin(List.of(
                "1\t100",
                "2\tnot-a-number",
                "3\t300"
            ));
            assertNotNull(error, "an unconvertible value in one row must fail the whole COPY");

            client.sendQuery("COPY strict_types TO STDOUT");
            List<String> lines = client.doCopyToStdout();
            assertTrue(lines.isEmpty(), () -> "the whole COPY transaction must have been aborted - zero rows committed, not even the valid ones: " + lines);
        } finally {
            client.close();
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void copyStdioRespectsCsvFormat(@TempDir Path tempDir) throws Exception {
        int port = freePort();
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        UserStore userStore = new UserStore();
        userStore.addUser("admin", "adminpass");
        server = new StdWireServer(port, db, userStore);
        server.start();
        Thread.sleep(200);

        TestClient client = new TestClient("localhost", port, "admin", "anydb", "adminpass");
        try {
            client.sendQuery("CREATE TABLE csv_test (id INT, note VARCHAR)");
            assertNull(client.readUntilReadyForQuery());

            client.sendQuery("COPY csv_test FROM STDIN WITH (FORMAT CSV)");
            String error = client.doCopyFromStdin(List.of("1,\"has, a comma\""));
            assertNull(error, () -> "COPY FROM STDIN with CSV format must succeed: " + error);

            client.sendQuery("COPY csv_test TO STDOUT WITH (FORMAT CSV)");
            List<String> lines = client.doCopyToStdout();
            assertEquals(1, lines.size());
            assertTrue(lines.get(0).contains("\"has, a comma\""), () -> "CSV format must round-trip correctly over STDIN/STDOUT: " + lines);
        } finally {
            client.close();
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** A minimal, real wire-protocol client speaking the real COPY sub-protocol (CopyInResponse/CopyData/CopyDone), for this test file's own use. */
    private static class TestClient {
        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;

        TestClient(String host, int port, String user, String database, String password) throws Exception {
            this.socket = new Socket(host, port);
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            StdWireMessages.writeStartupMessage(out, user, database);
            readStartupResponses(user, password);
        }

        private void readStartupResponses(String user, String password) throws Exception {
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                switch (msg.type()) {
                    case 'R' -> {
                        int authCode = readAuthCode(msg);
                        if (authCode == 10) performScram(user, password);
                    }
                    case 'S', 'K' -> { }
                    case 'Z' -> { return; }
                    case 'E' -> throw new RuntimeException("Startup failed: " + extractError(msg));
                    default -> { }
                }
            }
        }

        private void performScram(String username, String password) throws Exception {
            ScramClient scram = new ScramClient(username, password);
            String clientFirst = scram.buildClientFirstMessage();
            byte[] mech = ScramSha256.MECHANISM_NAME.getBytes(StandardCharsets.UTF_8);
            byte[] data = clientFirst.getBytes(StandardCharsets.UTF_8);
            out.writeByte('p');
            out.writeInt(mech.length + 1 + 4 + data.length + 4);
            out.write(mech);
            out.writeByte(0);
            out.writeInt(data.length);
            out.write(data);
            out.flush();

            StdWireMessages.TypedMessage cont = StdWireMessages.readTypedMessage(in);
            String serverFirst = new String(cont.body(), 4, cont.body().length - 4, StandardCharsets.UTF_8);
            String clientFinal = scram.buildClientFinalMessage(serverFirst);
            byte[] cf = clientFinal.getBytes(StandardCharsets.UTF_8);
            out.writeByte('p');
            out.writeInt(cf.length + 4);
            out.write(cf);
            out.flush();

            StdWireMessages.TypedMessage fin = StdWireMessages.readTypedMessage(in);
            if (fin.type() == 'E') throw new RuntimeException("Auth failed: " + extractError(fin));
        }

        private int readAuthCode(StdWireMessages.TypedMessage msg) {
            byte[] b = msg.body();
            return ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
        }

        private String extractError(StdWireMessages.TypedMessage msg) {
            byte[] b = msg.body();
            int pos = 0;
            while (pos < b.length && b[pos] != 0) {
                char field = (char) b[pos]; pos++;
                int start = pos;
                while (b[pos] != 0) pos++;
                String value = new String(b, start, pos - start, StandardCharsets.UTF_8);
                pos++;
                if (field == 'M') return value;
            }
            return "unknown";
        }

        void sendQuery(String sql) throws Exception {
            StdWireMessages.writeQuery(out, sql);
            out.flush();
        }

        String doCopyFromStdin(List<String> lines) throws Exception {
            StdWireMessages.TypedMessage first = StdWireMessages.readTypedMessage(in);
            if (first.type() == 'E') return extractError(first);
            if (first.type() != 'G') return "expected CopyInResponse, got " + first.type();

            for (String line : lines) {
                byte[] bytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
                out.writeByte('d');
                out.writeInt(bytes.length + 4);
                out.write(bytes);
            }
            out.writeByte('c');
            out.writeInt(4);
            out.flush();

            return readUntilReadyForQuery();
        }

        List<String> doCopyToStdout() throws Exception {
            List<String> lines = new ArrayList<>();
            StdWireMessages.TypedMessage first = StdWireMessages.readTypedMessage(in);
            if (first.type() == 'E') throw new RuntimeException(extractError(first));
            if (first.type() != 'H') throw new RuntimeException("expected CopyOutResponse, got " + first.type());

            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                if (msg.type() == 'd') {
                    String text = new String(msg.body(), StandardCharsets.UTF_8);
                    for (String line : text.split("\n")) {
                        if (!line.isEmpty()) lines.add(line);
                    }
                } else if (msg.type() == 'c') {
                    break;
                } else {
                    throw new RuntimeException("unexpected message during COPY TO STDOUT: " + msg.type());
                }
            }
            readUntilReadyForQuery();
            return lines;
        }

        String readUntilReadyForQuery() throws Exception {
            String error = null;
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                if (msg.type() == 'E') {
                    error = extractError(msg);
                } else if (msg.type() == 'Z') {
                    return error;
                }
            }
        }

        void close() throws Exception {
            StdWireMessages.writeTerminate(out);
            socket.close();
        }
    }
}
