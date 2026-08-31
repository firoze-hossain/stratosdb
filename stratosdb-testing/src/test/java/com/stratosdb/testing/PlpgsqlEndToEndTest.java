package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireMessages;
import com.stratosdb.network.stdwire.StdWireServer;
import com.stratosdb.sql.executor.QueryResult;
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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, end-to-end proof that this engine's own real procedural language
 * ("LANGUAGE plpgsql") actually works - this project's own honestly-named
 * "LANGUAGE SQL functions/procedures exist, but there's no actual control
 * flow at all - no loops, no IF/ELSE, no local variables" gap, a real
 * gap from real PL/pgSQL, not just a naming difference. Every construct
 * here is run against a real, live StratosDB instance and checked
 * against its own real, computed result - not just "it parsed".
 */
public class PlpgsqlEndToEndTest {

    private StratosDB db;

    @AfterEach
    void tearDown() {
        if (db != null) db.shutdown();
    }

    private StratosDB freshDb(Path tempDir) throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
        return db;
    }

    @Test
    void forLoopComputesRealSum(@TempDir Path tempDir) throws Exception {
        StratosDB database = freshDb(tempDir);
        assertTrue(database.execute(
            "CREATE FUNCTION sum_to_n(n INT) RETURNS INT AS $$ DECLARE total INT := 0; i INT; " +
            "BEGIN FOR i IN 1..n LOOP total := total + i; END LOOP; RETURN total; END; $$ LANGUAGE plpgsql")
            .isSuccess());

        assertEquals(15, scalarResult(database.execute("SELECT sum_to_n(5)")));
        assertEquals(55, scalarResult(database.execute("SELECT sum_to_n(10)")));
    }

    @Test
    void ifElsifElseTakesTheCorrectRealBranch(@TempDir Path tempDir) throws Exception {
        StratosDB database = freshDb(tempDir);
        assertTrue(database.execute(
            "CREATE FUNCTION classify(n INT) RETURNS VARCHAR AS $$ DECLARE result VARCHAR; " +
            "BEGIN IF n > 100 THEN result := 'big'; ELSIF n > 10 THEN result := 'medium'; ELSE result := 'small'; END IF; " +
            "RETURN result; END; $$ LANGUAGE plpgsql").isSuccess());

        assertEquals("big", scalarResult(database.execute("SELECT classify(200)")));
        assertEquals("medium", scalarResult(database.execute("SELECT classify(50)")));
        assertEquals("small", scalarResult(database.execute("SELECT classify(1)")));
    }

    @Test
    void whileLoopComputesRealSum(@TempDir Path tempDir) throws Exception {
        StratosDB database = freshDb(tempDir);
        assertTrue(database.execute(
            "CREATE FUNCTION while_sum(n INT) RETURNS INT AS $$ DECLARE total INT := 0; i INT := 0; " +
            "BEGIN WHILE i < n LOOP i := i + 1; total := total + i; END LOOP; RETURN total; END; $$ LANGUAGE plpgsql")
            .isSuccess());

        assertEquals(15, scalarResult(database.execute("SELECT while_sum(5)")));
    }

    @Test
    void plainLoopWithExitWhenStopsAtTheRightPoint(@TempDir Path tempDir) throws Exception {
        StratosDB database = freshDb(tempDir);
        assertTrue(database.execute(
            "CREATE FUNCTION loop_exit_when(n INT) RETURNS INT AS $$ DECLARE i INT := 0; " +
            "BEGIN LOOP i := i + 1; EXIT WHEN i >= n; END LOOP; RETURN i; END; $$ LANGUAGE plpgsql")
            .isSuccess());

        assertEquals(7, scalarResult(database.execute("SELECT loop_exit_when(7)")));
    }

    @Test
    void callOnARealProcedureWithSelectIntoArithmeticAndRaiseException(@TempDir Path tempDir) throws Exception {
        StratosDB database = freshDb(tempDir);
        database.execute("CREATE TABLE accounts (id INT, balance INT)");
        database.execute("INSERT INTO accounts VALUES (1, 100)");
        database.execute("INSERT INTO accounts VALUES (2, 50)");

        assertTrue(database.execute(
            "CREATE PROCEDURE transfer(from_id INT, to_id INT, amount INT) AS $$ " +
            "DECLARE from_balance INT; to_balance INT; new_from INT; new_to INT; " +
            "BEGIN " +
            "SELECT balance INTO from_balance FROM accounts WHERE id = from_id; " +
            "SELECT balance INTO to_balance FROM accounts WHERE id = to_id; " +
            "IF from_balance < amount THEN RAISE EXCEPTION 'insufficient funds'; END IF; " +
            "new_from := from_balance - amount; new_to := to_balance + amount; " +
            "UPDATE accounts SET balance = new_from WHERE id = from_id; " +
            "UPDATE accounts SET balance = new_to WHERE id = to_id; " +
            "END; $$ LANGUAGE plpgsql").isSuccess());

        assertTrue(database.execute("CALL transfer(1, 2, 30)").isSuccess());
        var afterTransfer = database.execute("SELECT * FROM accounts WHERE id = 1");
        assertEquals(70, afterTransfer.getRows().get(0).getValue("balance"));
        var account2 = database.execute("SELECT * FROM accounts WHERE id = 2");
        assertEquals(80, account2.getRows().get(0).getValue("balance"));

        // RAISE EXCEPTION must genuinely abort - neither account's own real balance
        // may change at all when the procedure fails partway through.
        QueryResult failed = database.execute("CALL transfer(1, 2, 1000)");
        assertFalse(failed.isSuccess());
        assertTrue(failed.getError().contains("insufficient funds"));
        var unchangedAccount1 = database.execute("SELECT * FROM accounts WHERE id = 1");
        assertEquals(70, unchangedAccount1.getRows().get(0).getValue("balance"));
    }

    @Test
    void malformedBodyIsRejectedAtCreateTimeNotSilentlyAcceptedThenFailingLaterAtCall(@TempDir Path tempDir) throws Exception {
        StratosDB database = freshDb(tempDir);
        // A real, genuine syntax error found by testing: a real bug this test itself
        // guards against regressing - a missing END IF used to be silently accepted
        // as generic, opaque SQL text at CREATE time, only failing confusingly at
        // CALL time instead. This must now fail immediately, at CREATE time.
        QueryResult result = database.execute(
            "CREATE FUNCTION broken() RETURNS INT AS $$ DECLARE x INT; BEGIN IF x > 5 THEN x := 1; END; $$ LANGUAGE plpgsql");
        assertFalse(result.isSuccess(), "a genuinely malformed plpgsql body (missing END IF) must be rejected at CREATE time");
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void multiStatementBodyWithInternalSemicolonsSurvivesTheRealWireProtocol(@TempDir Path tempDir) throws Exception {
        // A real, separate, critical bug found while building this feature: the
        // wire protocol's own splitStatements used to naively split on EVERY
        // semicolon, including ones INSIDE a dollar-quoted body - a real,
        // multi-statement procedural body sent as a single real Query message
        // (the way a real client always sends it) would be silently shredded.
        // This test proves the fix by sending exactly that shape over a real,
        // separate TCP connection - not the in-process db.execute() path every
        // other test in this file uses, since that path never goes through
        // StdWireServer's own splitStatements at all.
        StratosDB database = freshDb(tempDir);
        int port = freePort();
        StdWireServer server = new StdWireServer(port, database);
        server.start();
        Thread.sleep(200);
        try {
            RawClient client = new RawClient("localhost", port);
            String createError = client.sendQuery(
                "CREATE FUNCTION sum_to_n2(n INT) RETURNS INT AS $$ DECLARE total INT := 0; i INT; " +
                "BEGIN FOR i IN 1..n LOOP total := total + i; END LOOP; RETURN total; END; $$ LANGUAGE plpgsql");
            assertNull(createError, () -> "CREATE FUNCTION over a real connection must succeed: " + createError);

            String selectError = client.sendQuery("SELECT sum_to_n2(5)");
            assertNull(selectError, () -> "calling the function over a real connection must succeed: " + selectError);
            client.close();
        } finally {
            server.stop();
        }
    }

    private static Object scalarResult(QueryResult result) {
        assertTrue(result.isSuccess(), () -> "expected success but got: " + result.getError());
        assertFalse(result.getRows().isEmpty());
        return result.getRows().get(0).getValue(0);
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** A minimal, real, trust-authenticated wire-protocol client for sending a real Query and reading its own real response - see ObservabilityEndToEndTest/PromoteEndToEndTest for the same, established pattern. */
    private static class RawClient {
        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;

        RawClient(String host, int port) throws Exception {
            socket = new Socket(host, port);
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            StdWireMessages.writeStartupMessage(out, "anyuser", "anydb");
            out.flush();
            while (true) {
                StdWireMessages.TypedMessage msg = StdWireMessages.readTypedMessage(in);
                if (msg.type() == 'Z') break;
            }
        }

        String sendQuery(String sql) throws Exception {
            StdWireMessages.writeQuery(out, sql);
            out.flush();
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

        void close() throws Exception {
            socket.close();
        }
    }
}
