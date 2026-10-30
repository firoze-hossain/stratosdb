package com.stratosdb.testing;

import com.stratosdb.common.exceptions.CheckViolationException;
import com.stratosdb.common.exceptions.ForeignKeyViolationException;
import com.stratosdb.common.exceptions.NotNullViolationException;
import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireServer;
import com.stratosdb.sql.executor.QueryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ForeignKeyAndCheckConstraintTest {

    @TempDir
    Path tempDir;

    private StratosDB db;
    private StdWireServer server;
    private int port;

    @BeforeEach
    void setUp() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setBufferPoolSize(64);
        db = new StratosDB(config);
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception ignored) {}
        }
        if (db != null) {
            db.shutdown();
        }
    }

    @Test
    void testNotNullConstraintOnInsert() {
        QueryResult create = db.execute("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR NOT NULL, salary DOUBLE)");
        assertTrue(create.isSuccess(), "CREATE TABLE should succeed");

        // Null value in NOT NULL column rejected with 23502
        QueryResult res = db.execute("INSERT INTO employees VALUES (1, NULL, 50000.0)");
        assertFalse(res.isSuccess());
        assertEquals("23502", res.getSqlState());
        assertTrue(res.getException() instanceof NotNullViolationException);

        assertThrows(NotNullViolationException.class, () ->
            db.executeOrThrow("INSERT INTO employees VALUES (2, NULL, 60000.0)"));

        // Valid insert succeeds
        QueryResult ok = db.execute("INSERT INTO employees VALUES (1, 'Alice', 50000.0)");
        assertTrue(ok.isSuccess());
    }

    @Test
    void testNotNullConstraintOnUpdate() {
        db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, owner VARCHAR NOT NULL)");
        assertTrue(db.execute("INSERT INTO accounts VALUES (1, 'Bob')").isSuccess());

        QueryResult upd = db.execute("UPDATE accounts SET owner = NULL WHERE id = 1");
        assertFalse(upd.isSuccess());
        assertEquals("23502", upd.getSqlState());
        assertTrue(upd.getException() instanceof NotNullViolationException);

        // Verify owner did not change
        QueryResult sel = db.execute("SELECT owner FROM accounts WHERE id = 1");
        assertEquals("Bob", sel.getRows().get(0).getValue("owner"));
    }

    @Test
    void testCheckConstraintOnInsert() {
        QueryResult create = db.execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR, price DOUBLE CHECK (price > 0))");
        assertTrue(create.isSuccess());

        // Negative price rejected with 23514
        QueryResult resNegative = db.execute("INSERT INTO products VALUES (1, 'Apple', -5.0)");
        assertFalse(resNegative.isSuccess());
        assertEquals("23514", resNegative.getSqlState());
        assertTrue(resNegative.getException() instanceof CheckViolationException);

        // Zero price rejected with 23514
        QueryResult resZero = db.execute("INSERT INTO products VALUES (2, 'ZeroItem', 0.0)");
        assertFalse(resZero.isSuccess());
        assertEquals("23514", resZero.getSqlState());

        // Positive price succeeds
        QueryResult ok = db.execute("INSERT INTO products VALUES (3, 'Banana', 1.5)");
        assertTrue(ok.isSuccess());
    }

    @Test
    void testCheckConstraintAllowsNullWhenNullable() {
        db.execute("CREATE TABLE items (id INT PRIMARY KEY, score INT CHECK (score >= 0))");

        // In standard SQL, CHECK constraints pass on NULL (three-valued logic evaluates to UNKNOWN)
        QueryResult resNull = db.execute("INSERT INTO items VALUES (1, NULL)");
        assertTrue(resNull.isSuccess(), "CHECK constraint should pass on NULL value when column is nullable");

        QueryResult resInvalid = db.execute("INSERT INTO items VALUES (2, -10)");
        assertFalse(resInvalid.isSuccess());
        assertEquals("23514", resInvalid.getSqlState());
    }

    @Test
    void testCheckConstraintOnUpdate() {
        db.execute("CREATE TABLE widgets (id INT PRIMARY KEY, qty INT CHECK (qty >= 0))");
        db.execute("INSERT INTO widgets VALUES (1, 10)");

        QueryResult updInvalid = db.execute("UPDATE widgets SET qty = -1 WHERE id = 1");
        assertFalse(updInvalid.isSuccess());
        assertEquals("23514", updInvalid.getSqlState());
        assertTrue(updInvalid.getException() instanceof CheckViolationException);

        QueryResult updValid = db.execute("UPDATE widgets SET qty = 20 WHERE id = 1");
        assertTrue(updValid.isSuccess());
    }

    @Test
    void testForeignKeyOrphanInsertBlocked() {
        db.execute("CREATE TABLE depts (id INT PRIMARY KEY, dname VARCHAR)");
        db.execute("CREATE TABLE emps (id INT PRIMARY KEY, ename VARCHAR, dept_id INT REFERENCES depts(id))");

        // Orphan insert rejected with 23503
        QueryResult orphan = db.execute("INSERT INTO emps VALUES (101, 'Alice', 999)");
        assertFalse(orphan.isSuccess());
        assertEquals("23503", orphan.getSqlState());
        assertTrue(orphan.getException() instanceof ForeignKeyViolationException);

        assertThrows(ForeignKeyViolationException.class, () ->
            db.executeOrThrow("INSERT INTO emps VALUES (102, 'Bob', 999)"));

        // Insert valid parent row first
        assertTrue(db.execute("INSERT INTO depts VALUES (10, 'Engineering')").isSuccess());

        // Referencing existing parent succeeds
        QueryResult ok = db.execute("INSERT INTO emps VALUES (101, 'Alice', 10)");
        assertTrue(ok.isSuccess());
    }

    @Test
    void testForeignKeyNullableChildAllowed() {
        db.execute("CREATE TABLE projects (id INT PRIMARY KEY, pname VARCHAR)");
        db.execute("CREATE TABLE tasks (id INT PRIMARY KEY, tname VARCHAR, project_id INT REFERENCES projects(id))");

        // Nullable foreign key value is allowed
        QueryResult nullFk = db.execute("INSERT INTO tasks VALUES (1, 'Unassigned Task', NULL)");
        assertTrue(nullFk.isSuccess(), "NULL foreign key column value should be accepted");
    }

    @Test
    void testForeignKeyOrphanUpdateBlocked() {
        db.execute("CREATE TABLE depts (id INT PRIMARY KEY, dname VARCHAR)");
        db.execute("CREATE TABLE emps (id INT PRIMARY KEY, ename VARCHAR, dept_id INT REFERENCES depts(id))");
        db.execute("INSERT INTO depts VALUES (1, 'Sales')");
        db.execute("INSERT INTO emps VALUES (10, 'Charlie', 1)");

        // Updating child row to point to non-existent parent key fails with 23503
        QueryResult upd = db.execute("UPDATE emps SET dept_id = 999 WHERE id = 10");
        assertFalse(upd.isSuccess());
        assertEquals("23503", upd.getSqlState());
        assertTrue(upd.getException() instanceof ForeignKeyViolationException);
    }

    @Test
    void testForeignKeyOnDeleteRestrictByDefault() {
        db.execute("CREATE TABLE authors (id INT PRIMARY KEY, name VARCHAR)");
        db.execute("CREATE TABLE books (id INT PRIMARY KEY, title VARCHAR, author_id INT REFERENCES authors(id))");

        db.execute("INSERT INTO authors VALUES (1, 'Tolkien')");
        db.execute("INSERT INTO books VALUES (101, 'The Hobbit', 1)");

        // Deleting author 1 while book 101 references it is rejected with 23503
        QueryResult del = db.execute("DELETE FROM authors WHERE id = 1");
        assertFalse(del.isSuccess());
        assertEquals("23503", del.getSqlState());
        assertTrue(del.getException() instanceof ForeignKeyViolationException);

        // Delete child row first, then parent delete succeeds
        assertTrue(db.execute("DELETE FROM books WHERE id = 101").isSuccess());
        assertTrue(db.execute("DELETE FROM authors WHERE id = 1").isSuccess());
    }

    @Test
    void testForeignKeyOnDeleteCascade() {
        db.execute("CREATE TABLE departments (id INT PRIMARY KEY, name VARCHAR)");
        db.execute("CREATE TABLE staff (id INT PRIMARY KEY, name VARCHAR, dept_id INT REFERENCES departments(id) ON DELETE CASCADE)");

        db.execute("INSERT INTO departments VALUES (1, 'HR')");
        db.execute("INSERT INTO departments VALUES (2, 'Finance')");
        db.execute("INSERT INTO staff VALUES (101, 'Alice', 1)");
        db.execute("INSERT INTO staff VALUES (102, 'Bob', 1)");
        db.execute("INSERT INTO staff VALUES (103, 'Charlie', 2)");

        // Deleting dept 1 cascades to staff 101 and 102 atomically
        QueryResult del = db.execute("DELETE FROM departments WHERE id = 1");
        assertTrue(del.isSuccess());

        QueryResult selStaff = db.execute("SELECT id FROM staff");
        assertEquals(1, selStaff.getRows().size());
        assertEquals(103, ((Number) selStaff.getRows().get(0).getValue("id")).intValue());
    }

    @Test
    void testForeignKeyMultiLevelCascade() {
        db.execute("CREATE TABLE countries (id INT PRIMARY KEY, name VARCHAR)");
        db.execute("CREATE TABLE states (id INT PRIMARY KEY, name VARCHAR, country_id INT REFERENCES countries(id) ON DELETE CASCADE)");
        db.execute("CREATE TABLE cities (id INT PRIMARY KEY, name VARCHAR, state_id INT REFERENCES states(id) ON DELETE CASCADE)");

        db.execute("INSERT INTO countries VALUES (1, 'USA')");
        db.execute("INSERT INTO states VALUES (10, 'California', 1)");
        db.execute("INSERT INTO cities VALUES (100, 'San Francisco', 10)");
        db.execute("INSERT INTO cities VALUES (101, 'Los Angeles', 10)");

        // Deleting country 1 cascades to state 10 and in turn cascades to cities 100 & 101
        QueryResult del = db.execute("DELETE FROM countries WHERE id = 1");
        assertTrue(del.isSuccess());

        assertEquals(0, db.execute("SELECT * FROM countries").getRows().size());
        assertEquals(0, db.execute("SELECT * FROM states").getRows().size());
        assertEquals(0, db.execute("SELECT * FROM cities").getRows().size());
    }

    @Test
    void testForeignKeyOnDeleteSetNull() {
        db.execute("CREATE TABLE categories (id INT PRIMARY KEY, name VARCHAR)");
        db.execute("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR, cat_id INT REFERENCES categories(id) ON DELETE SET NULL)");

        db.execute("INSERT INTO categories VALUES (1, 'Electronics')");
        db.execute("INSERT INTO items VALUES (10, 'Laptop', 1)");

        // Deleting category 1 sets child item cat_id to NULL
        QueryResult del = db.execute("DELETE FROM categories WHERE id = 1");
        assertTrue(del.isSuccess());

        QueryResult sel = db.execute("SELECT cat_id FROM items WHERE id = 10");
        assertEquals(1, sel.getRows().size());
        assertNull(sel.getRows().get(0).getValue("cat_id"));
    }

    @Test
    void testForeignKeyOnUpdateCascade() {
        db.execute("CREATE TABLE parent_cat (id INT PRIMARY KEY, name VARCHAR)");
        db.execute("CREATE TABLE child_prod (id INT PRIMARY KEY, title VARCHAR, cat_id INT REFERENCES parent_cat(id) ON UPDATE CASCADE)");

        db.execute("INSERT INTO parent_cat VALUES (100, 'Books')");
        db.execute("INSERT INTO child_prod VALUES (1, 'Novel', 100)");

        // Updating parent PK from 100 to 200 cascades to child cat_id
        QueryResult upd = db.execute("UPDATE parent_cat SET id = 200 WHERE id = 100");
        assertTrue(upd.isSuccess());

        QueryResult selChild = db.execute("SELECT cat_id FROM child_prod WHERE id = 1");
        assertEquals(200, ((Number) selChild.getRows().get(0).getValue("cat_id")).intValue());
    }

    @Test
    void testForeignKeyOnUpdateSetNull() {
        db.execute("CREATE TABLE parent_org (id INT PRIMARY KEY, name VARCHAR)");
        db.execute("CREATE TABLE child_member (id INT PRIMARY KEY, name VARCHAR, org_id INT REFERENCES parent_org(id) ON UPDATE SET NULL)");

        db.execute("INSERT INTO parent_org VALUES (50, 'Acme')");
        db.execute("INSERT INTO child_member VALUES (1, 'Alice', 50)");

        QueryResult upd = db.execute("UPDATE parent_org SET id = 60 WHERE id = 50");
        assertTrue(upd.isSuccess());

        QueryResult selChild = db.execute("SELECT org_id FROM child_member WHERE id = 1");
        assertNull(selChild.getRows().get(0).getValue("org_id"));
    }

    @Test
    void testForeignKeyOnUpdateRestrict() {
        db.execute("CREATE TABLE p_table (id INT PRIMARY KEY)");
        db.execute("CREATE TABLE c_table (id INT PRIMARY KEY, pid INT REFERENCES p_table(id) ON UPDATE RESTRICT)");

        db.execute("INSERT INTO p_table VALUES (1)");
        db.execute("INSERT INTO c_table VALUES (10, 1)");

        QueryResult upd = db.execute("UPDATE p_table SET id = 2 WHERE id = 1");
        assertFalse(upd.isSuccess());
        assertEquals("23503", upd.getSqlState());
        assertTrue(upd.getException() instanceof ForeignKeyViolationException);
    }

    @Test
    void testForeignKeyTransactionRollback() {
        db.execute("CREATE TABLE depts_rb (id INT PRIMARY KEY, name VARCHAR)");
        db.execute("CREATE TABLE staff_rb (id INT PRIMARY KEY, name VARCHAR, dept_id INT REFERENCES depts_rb(id) ON DELETE CASCADE)");

        db.execute("INSERT INTO depts_rb VALUES (1, 'R&D')");
        db.execute("INSERT INTO staff_rb VALUES (10, 'Dave', 1)");

        // In a transaction, delete parent then rollback
        db.execute("BEGIN");
        QueryResult del = db.execute("DELETE FROM depts_rb WHERE id = 1");
        assertTrue(del.isSuccess());
        assertEquals(0, db.execute("SELECT * FROM staff_rb").getRows().size());

        db.execute("ROLLBACK");

        // Both parent and child rows must still exist after rollback
        assertEquals(1, db.execute("SELECT * FROM depts_rb").getRows().size());
        assertEquals(1, db.execute("SELECT * FROM staff_rb").getRows().size());
    }

    @Test
    void testDropTableBlockedByForeignKey() {
        db.execute("CREATE TABLE parent_drop (id INT PRIMARY KEY)");
        db.execute("CREATE TABLE child_drop (id INT PRIMARY KEY, p_id INT REFERENCES parent_drop(id))");

        QueryResult drop = db.execute("DROP TABLE parent_drop");
        assertFalse(drop.isSuccess(), "DROP TABLE parent should fail when dependent foreign key exists");
        assertTrue(drop.getError().contains("cannot drop table parent_drop because other objects depend on it"));

        // Dropping child table first allows parent table to be dropped
        assertTrue(db.execute("DROP TABLE child_drop").isSuccess());
        assertTrue(db.execute("DROP TABLE parent_drop").isSuccess());
    }

    @Test
    void testTableLevelForeignKeySyntax() {
        QueryResult createParent = db.execute("CREATE TABLE suppliers (sid INT PRIMARY KEY, sname VARCHAR)");
        assertTrue(createParent.isSuccess());

        QueryResult createChild = db.execute(
            "CREATE TABLE parts ("
            + "pid INT PRIMARY KEY, "
            + "pname VARCHAR NOT NULL, "
            + "supplier_id INT, "
            + "price DOUBLE CHECK (price > 0), "
            + "CONSTRAINT fk_parts_supplier FOREIGN KEY (supplier_id) REFERENCES suppliers(sid) ON DELETE CASCADE ON UPDATE CASCADE"
            + ")"
        );
        assertTrue(createChild.isSuccess(), "Table-level foreign key and check syntax should parse and create successfully");

        // Test FK validation
        QueryResult badFk = db.execute("INSERT INTO parts VALUES (1, 'Gear', 99, 10.0)");
        assertFalse(badFk.isSuccess());
        assertEquals("23503", badFk.getSqlState());

        // Test CHECK validation
        db.execute("INSERT INTO suppliers VALUES (5, 'Acme Corp')");
        QueryResult badCheck = db.execute("INSERT INTO parts VALUES (1, 'Gear', 5, -2.0)");
        assertFalse(badCheck.isSuccess());
        assertEquals("23514", badCheck.getSqlState());

        // Test valid insertion
        QueryResult ok = db.execute("INSERT INTO parts VALUES (1, 'Gear', 5, 12.5)");
        assertTrue(ok.isSuccess());

        // Test cascading delete
        db.execute("DELETE FROM suppliers WHERE sid = 5");
        assertEquals(0, db.execute("SELECT * FROM parts").getRows().size());
    }

    @Test
    void testSystemCatalogRestoresConstraintsOnRestart() {
        db.execute("CREATE TABLE offices (id INT PRIMARY KEY, city VARCHAR)");
        db.execute("CREATE TABLE workers (id INT PRIMARY KEY, name VARCHAR NOT NULL, office_id INT REFERENCES offices(id), age INT CHECK (age >= 18))");
        db.execute("INSERT INTO offices VALUES (1, 'London')");
        db.execute("INSERT INTO workers VALUES (10, 'Eve', 1, 25)");

        // Shutdown database and restart from same directory
        db.shutdown();

        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setBufferPoolSize(64);
        db = new StratosDB(config);

        // Verify NOT NULL is still enforced
        QueryResult badNull = db.execute("INSERT INTO workers VALUES (11, NULL, 1, 30)");
        assertFalse(badNull.isSuccess());
        assertEquals("23502", badNull.getSqlState());

        // Verify CHECK is still enforced
        QueryResult badCheck = db.execute("INSERT INTO workers VALUES (12, 'Frank', 1, 16)");
        assertFalse(badCheck.isSuccess());
        assertEquals("23514", badCheck.getSqlState());

        // Verify FOREIGN KEY is still enforced
        QueryResult badFk = db.execute("INSERT INTO workers VALUES (13, 'Grace', 999, 22)");
        assertFalse(badFk.isSuccess());
        assertEquals("23503", badFk.getSqlState());
    }

    @Test
    void testWireProtocolConstraintErrorCodes() throws Exception {
        port = freePort();
        server = new StdWireServer(port, db);
        server.start();

        db.execute("CREATE TABLE wire_parents (id INT PRIMARY KEY)");
        db.execute("CREATE TABLE wire_children ("
            + "id INT PRIMARY KEY, "
            + "code VARCHAR NOT NULL, "
            + "parent_id INT REFERENCES wire_parents(id), "
            + "amount INT CHECK (amount > 0)"
            + ")");
        db.execute("INSERT INTO wire_parents VALUES (1)");

        try (Socket socket = new Socket("localhost", port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            // SSLRequest (80877103) -> 'N'
            out.writeInt(8);
            out.writeInt(80877103);
            out.flush();
            assertEquals('N', in.readByte());

            // StartupMessage
            String params = "user\0testuser\0database\0testdb\0\0";
            byte[] pBytes = params.getBytes(StandardCharsets.UTF_8);
            out.writeInt(4 + 4 + pBytes.length);
            out.writeInt(196608); // protocol 3.0
            out.write(pBytes);
            out.flush();

            drainUntilReady(in);

            // 1. Violate NOT NULL constraint -> verify SQLSTATE 23502
            sendQuery(out, "INSERT INTO wire_children VALUES (1, NULL, 1, 50)");
            byte msgType = in.readByte();
            assertEquals('E', msgType, "Expected ErrorResponse for NOT NULL violation");
            int len = in.readInt();
            byte[] errBytes = in.readNBytes(len - 4);
            assertEquals("23502", extractErrorField(errBytes, (byte) 'C'), "Wire protocol error code must be 23502");
            drainUntilReady(in);

            // 2. Violate CHECK constraint -> verify SQLSTATE 23514
            sendQuery(out, "INSERT INTO wire_children VALUES (2, 'CODE1', 1, -10)");
            msgType = in.readByte();
            assertEquals('E', msgType, "Expected ErrorResponse for CHECK violation");
            len = in.readInt();
            errBytes = in.readNBytes(len - 4);
            assertEquals("23514", extractErrorField(errBytes, (byte) 'C'), "Wire protocol error code must be 23514");
            drainUntilReady(in);

            // 3. Violate FOREIGN KEY constraint -> verify SQLSTATE 23503
            sendQuery(out, "INSERT INTO wire_children VALUES (3, 'CODE2', 999, 100)");
            msgType = in.readByte();
            assertEquals('E', msgType, "Expected ErrorResponse for FOREIGN KEY violation");
            len = in.readInt();
            errBytes = in.readNBytes(len - 4);
            assertEquals("23503", extractErrorField(errBytes, (byte) 'C'), "Wire protocol error code must be 23503");
            drainUntilReady(in);
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    private void sendQuery(DataOutputStream out, String sql) throws IOException {
        byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
        out.writeByte('Q');
        out.writeInt(sqlBytes.length + 5);
        out.write(sqlBytes);
        out.writeByte(0);
        out.flush();
    }

    private void drainUntilReady(DataInputStream in) throws IOException {
        while (true) {
            byte type = in.readByte();
            int len = in.readInt();
            in.readNBytes(len - 4);
            if (type == 'Z') {
                break;
            }
        }
    }

    private String extractErrorField(byte[] errBytes, byte fieldCode) {
        int i = 0;
        while (i < errBytes.length) {
            byte code = errBytes[i++];
            if (code == 0) break;
            int start = i;
            while (i < errBytes.length && errBytes[i] != 0) {
                i++;
            }
            String val = new String(errBytes, start, i - start, StandardCharsets.UTF_8);
            i++; // skip null terminator
            if (code == fieldCode) {
                return val;
            }
        }
        return null;
    }
}
