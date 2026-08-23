package com.stratosdb.testing;

import com.stratosdb.cli.StratosDump;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real, end-to-end proof that stratosdump actually works: a real source
 * StratosDB instance (with a real StdWireServer, the same one any real
 * client connects to), a real StratosDump client connecting over the
 * actual wire protocol (not reading server internals directly - this
 * tool has no more access than psql would), and a completely separate,
 * fresh target StratosDB instance the dump output is fed back into via
 * ordinary SQL - exactly how a real restore works
 * (`stdsql ... < dump.sql`, no separate restore tool needed).
 */
public class StratosDumpEndToEndTest {

    private StratosDB source;
    private StratosDB target;
    private StdWireServer sourceServer;
    private StdWireServer targetServer;

    @AfterEach
    void tearDown() {
        if (sourceServer != null) sourceServer.stop();
        if (targetServer != null) targetServer.stop();
        if (source != null) source.shutdown();
        if (target != null) target.shutdown();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void dumpAndRestoreRoundTripsSchemaAndDataCorrectly(@TempDir Path tempDir) throws Exception {
        int sourcePort = freePort();
        DatabaseConfig sourceConfig = new DatabaseConfig();
        sourceConfig.setDataDirectory(tempDir.resolve("source").toString());
        source = new StratosDB(sourceConfig);
        sourceServer = new StdWireServer(sourcePort, source);
        sourceServer.start();
        Thread.sleep(200);

        source.execute("CREATE SEQUENCE emp_seq");
        source.execute("CREATE TABLE employees (id INT, name VARCHAR, department VARCHAR, salary INT)");
        source.execute("CREATE INDEX idx_dept ON employees (department)");
        source.execute("CREATE VIEW high_earners AS SELECT id, name FROM employees WHERE salary > 90000");
        source.execute("CREATE PROCEDURE give_raise(emp_id INT, new_salary INT) AS $$ UPDATE employees SET salary = new_salary WHERE id = emp_id $$ LANGUAGE SQL");
        source.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 95000)");
        source.execute("INSERT INTO employees VALUES (2, 'Bob', 'Sales', 72000)");
        source.execute("INSERT INTO employees VALUES (3, 'Carol''s Team Lead', 'Engineering', 110000)");
        source.execute("INSERT INTO employees VALUES (4, 'Dave', 'Marketing', 68000)");

        // The actual tool under test - a real client, connecting over the real wire
        // protocol, exactly the way a user would run it from the command line.
        StratosDump dumper = new StratosDump("localhost", sourcePort, "testuser", "testdb", "");
        StringWriter dumpBuffer = new StringWriter();
        dumper.dump(new PrintWriter(dumpBuffer));
        String dumpSql = dumpBuffer.toString();

        assertTrue(dumpSql.contains("CREATE TABLE employees"), "dump must contain the table's own DDL");
        assertTrue(dumpSql.contains("CREATE SEQUENCE emp_seq"), "dump must contain the sequence's own DDL");
        assertTrue(dumpSql.contains("CREATE INDEX idx_dept"), "dump must contain the index's own DDL");
        assertTrue(dumpSql.contains("CREATE VIEW high_earners"), "dump must contain the view's own DDL");
        assertTrue(dumpSql.contains("CREATE PROCEDURE give_raise"), "dump must contain the procedure's own DDL");
        assertTrue(dumpSql.contains("Carol''s Team Lead"), "an embedded apostrophe in real data must be correctly escaped in the generated INSERT");

        // Restore into a completely separate, fresh instance - proving the dump is
        // genuinely self-contained, not relying on anything already present.
        int targetPort = freePort();
        DatabaseConfig targetConfig = new DatabaseConfig();
        targetConfig.setDataDirectory(tempDir.resolve("target").toString());
        target = new StratosDB(targetConfig);
        targetServer = new StdWireServer(targetPort, target);
        targetServer.start();
        Thread.sleep(200);

        for (String line : dumpSql.split("\n")) {
            if (!line.isBlank()) {
                com.stratosdb.sql.executor.QueryResult result = target.execute(line);
                assertTrue(result.isSuccess() || line.trim().startsWith("--"),
                    () -> "restoring line failed: " + line + " -> " + result.getError());
            }
        }

        // The restored data must match the original exactly.
        com.stratosdb.sql.executor.QueryResult employees = target.execute("SELECT id, name, department, salary FROM employees");
        assertTrue(employees.isSuccess());
        assertEquals(4, employees.getRows().size());
        assertEquals("Carol's Team Lead", employees.getRows().stream()
            .filter(r -> r.getValue("id").equals(3))
            .findFirst().orElseThrow().getValue("name"),
            "the restored row's apostrophe must round-trip back to its original, unescaped form");

        // The restored view must actually work, not just exist.
        com.stratosdb.sql.executor.QueryResult highEarners = target.execute("SELECT id FROM high_earners");
        assertTrue(highEarners.isSuccess());
        assertEquals(2, highEarners.getRows().size(), "the restored view must correctly filter to the 2 employees over 90000 salary");

        // The restored procedure must actually run, not just exist.
        com.stratosdb.sql.executor.QueryResult callResult = target.execute("CALL give_raise(2, 80000)");
        assertTrue(callResult.isSuccess(), () -> "the restored procedure must execute correctly: " + callResult.getError());
        com.stratosdb.sql.executor.QueryResult updatedSalary = target.execute("SELECT salary FROM employees WHERE id = 2");
        assertEquals(80000, updatedSalary.getRows().get(0).getValue("salary"));
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void dumpOrdersObjectsSoARestoreNeverHitsAMissingDependency(@TempDir Path tempDir) throws Exception {
        int sourcePort = freePort();
        DatabaseConfig sourceConfig = new DatabaseConfig();
        sourceConfig.setDataDirectory(tempDir.resolve("source2").toString());
        source = new StratosDB(sourceConfig);
        sourceServer = new StdWireServer(sourcePort, source);
        sourceServer.start();
        Thread.sleep(200);

        source.execute("CREATE TABLE t (id INT, status VARCHAR)");
        source.execute("CREATE PROCEDURE mark_done(row_id INT) AS $$ UPDATE t SET status = 'done' WHERE id = row_id $$ LANGUAGE SQL");
        source.execute("CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW EXECUTE PROCEDURE mark_done()");

        StratosDump dumper = new StratosDump("localhost", sourcePort, "testuser", "testdb", "");
        StringWriter dumpBuffer = new StringWriter();
        dumper.dump(new PrintWriter(dumpBuffer));
        String dumpSql = dumpBuffer.toString();

        int tablePos = dumpSql.indexOf("CREATE TABLE t ");
        int procPos = dumpSql.indexOf("CREATE PROCEDURE mark_done");
        int triggerPos = dumpSql.indexOf("CREATE TRIGGER trg");

        assertTrue(tablePos >= 0 && procPos >= 0 && triggerPos >= 0, "all three objects must be present in the dump");
        assertTrue(tablePos < triggerPos, "the table must be dumped before the trigger that depends on it");
        assertTrue(procPos < triggerPos, "the procedure must be dumped before the trigger that names it as its own handler");
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
