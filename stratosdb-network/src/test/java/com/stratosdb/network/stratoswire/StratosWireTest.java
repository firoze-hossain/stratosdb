package com.stratosdb.network.stratoswire;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StratosWireTest {

    @TempDir
    Path tempDir;

    private int port;
    private StratosDB db;
    private StratosWireServer server;

    @BeforeEach
    void setUp() throws Exception {
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);

        server = new StratosWireServer(port, db);
        server.start();
        Thread.sleep(100);
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    @Test
    void testStratosNativeProtocolEndToEnd() throws Exception {
        try (StratosWireClient client = new StratosWireClient("localhost", port, "stratos_admin", "stratos", "")) {
            client.connect();
            assertEquals("StratosDB 1.0.0", client.getServerVersion());
            assertTrue(client.getSessionId() > 0);
            assertTrue(client.ping());

            // 1. Create Table (DDL)
            StratosWireClient.Response ddlResp = client.executeQuery("CREATE TABLE employees (id INT, name VARCHAR, salary DOUBLE, active BOOLEAN)");
            assertFalse(ddlResp.isQuery());
            assertTrue(ddlResp.message().contains("Table created: employees") || ddlResp.message().contains("OK"));

            // 2. Insert Data (DML)
            StratosWireClient.Response ins1 = client.executeQuery("INSERT INTO employees VALUES (1, 'Alice', 75000.50, true)");
            assertFalse(ins1.isQuery());

            StratosWireClient.Response ins2 = client.executeQuery("INSERT INTO employees VALUES (2, 'Bob', 62000.00, false)");
            assertFalse(ins2.isQuery());

            StratosWireClient.Response ins3 = client.executeQuery("INSERT INTO employees VALUES (3, 'Charlie', NULL, true)");
            assertFalse(ins3.isQuery());

            // 3. Query Data (SELECT)
            StratosWireClient.Response queryResp = client.executeQuery("SELECT * FROM employees ORDER BY id ASC");
            assertTrue(queryResp.isQuery());
            assertEquals(3, queryResp.rows().size());
            assertEquals(4, queryResp.columns().size());

            // Verify Alice
            Object[] row1 = queryResp.rows().get(0);
            assertEquals(1, row1[0]);
            assertEquals("Alice", row1[1]);
            assertEquals(75000.50, (Double) row1[2], 0.001);
            assertEquals(true, row1[3]);

            // Verify Charlie (with NULL)
            Object[] row3 = queryResp.rows().get(2);
            assertEquals(3, row3[0]);
            assertEquals("Charlie", row3[1]);
            assertNull(row3[2]); // Strongly-typed NULL roundtrip!
            assertEquals(true, row3[3]);
        }
    }
}
