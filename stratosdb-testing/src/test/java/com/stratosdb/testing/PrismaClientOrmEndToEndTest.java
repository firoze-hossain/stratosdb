package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sprint 12 Week 23: Full ORM & Client Ecosystem Certification.
 *
 * Prisma Client & Prisma Migrate ORM End-to-End Test Suite.
 *
 * Verifies:
 *   1. Quoted identifier syntax: "User", "Post", "id", "authorId" across DDL and DML.
 *   2. Prisma interactive transactions via SAVEPOINT, RELEASE SAVEPOINT, and ROLLBACK TO SAVEPOINT.
 *   3. Prisma batch and create operations with RETURNING clause.
 *   4. Prisma schema introspection against information_schema and pg_catalog views.
 */
public class PrismaClientOrmEndToEndTest {

    @TempDir
    Path tempDir;

    private StratosDB db;

    @BeforeEach
    void setUp() throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.shutdown();
        }
    }

    @Test
    void testPrismaQuotedIdentifiersAndDdlLifecycle() throws Exception {
        // 1. DDL with quoted identifiers
        QueryResult qUser = db.execute("""
            CREATE TABLE "User" (
                "id" INT NOT NULL,
                "email" VARCHAR NOT NULL,
                "name" VARCHAR,
                PRIMARY KEY ("id")
            )
            """);
        assertTrue(qUser.isSuccess(), "CREATE TABLE \"User\" failed: " + qUser.getError());

        QueryResult qPost = db.execute("""
            CREATE TABLE "Post" (
                "id" INT NOT NULL,
                "title" VARCHAR NOT NULL,
                "published" BOOLEAN DEFAULT false,
                "authorId" INT,
                PRIMARY KEY ("id")
            )
            """);
        assertTrue(qPost.isSuccess(), "CREATE TABLE \"Post\" failed: " + qPost.getError());

        QueryResult qFk = db.execute("""
            ALTER TABLE "Post" ADD CONSTRAINT "fk_post_author"
            FOREIGN KEY ("authorId") REFERENCES "User" ("id")
            """);
        assertTrue(qFk.isSuccess(), "ALTER TABLE \"Post\" ADD CONSTRAINT failed: " + qFk.getError());

        // 2. Insert with quoted identifiers
        QueryResult insUser = db.execute("INSERT INTO \"User\" (\"id\", \"email\", \"name\") VALUES (1, 'prisma@stratosdb.org', 'Alice Prisma')");
        assertTrue(insUser.isSuccess(), "INSERT into \"User\" failed: " + insUser.getError());

        QueryResult insPost = db.execute("INSERT INTO \"Post\" (\"id\", \"title\", \"published\", \"authorId\") VALUES (101, 'Prisma with StratosDB', true, 1)");
        assertTrue(insPost.isSuccess(), "INSERT into \"Post\" failed: " + insPost.getError());

        // 3. Select with quoted identifiers
        QueryResult qSel = db.execute("SELECT u.\"id\", u.\"email\", p.\"title\" FROM \"User\" u JOIN \"Post\" p ON (u.\"id\" = p.\"authorId\") WHERE u.\"id\" = 1");
        assertTrue(qSel.isSuccess(), "SELECT join failed: " + qSel.getError());
        assertEquals(1, qSel.getRows().size());
        assertEquals("prisma@stratosdb.org", qSel.getRows().get(0).getValue("email"));
        assertEquals("Prisma with StratosDB", qSel.getRows().get(0).getValue("title"));
    }

    @Test
    void testPrismaSavepointTransactions() throws Exception {
        db.execute("CREATE TABLE \"User\" (\"id\" INT NOT NULL, \"email\" VARCHAR, PRIMARY KEY (\"id\"))");

        // Transaction 1: Successful savepoint commit
        assertTrue(db.execute("BEGIN").isSuccess());
        assertTrue(db.execute("SAVEPOINT \"prisma_sp_1\"").isSuccess());
        assertTrue(db.execute("INSERT INTO \"User\" (\"id\", \"email\") VALUES (1, 'committed@prisma.io')").isSuccess());
        assertTrue(db.execute("RELEASE SAVEPOINT \"prisma_sp_1\"").isSuccess());
        assertTrue(db.execute("COMMIT").isSuccess());

        QueryResult q1 = db.execute("SELECT * FROM \"User\" WHERE \"id\" = 1");
        assertTrue(q1.isSuccess());
        assertEquals(1, q1.getRows().size(), "Committed savepoint row must exist");

        // Transaction 2: Rollback to savepoint
        assertTrue(db.execute("BEGIN").isSuccess());
        assertTrue(db.execute("INSERT INTO \"User\" (\"id\", \"email\") VALUES (2, 'before_sp@prisma.io')").isSuccess());
        assertTrue(db.execute("SAVEPOINT \"prisma_sp_2\"").isSuccess());
        assertTrue(db.execute("INSERT INTO \"User\" (\"id\", \"email\") VALUES (3, 'rolled_back@prisma.io')").isSuccess());
        assertTrue(db.execute("ROLLBACK TO SAVEPOINT \"prisma_sp_2\"").isSuccess());
        assertTrue(db.execute("COMMIT").isSuccess());

        QueryResult q2 = db.execute("SELECT * FROM \"User\" WHERE \"id\" = 2");
        assertTrue(q2.isSuccess());
        assertEquals(1, q2.getRows().size(), "Row before savepoint must be preserved");

        QueryResult q3 = db.execute("SELECT * FROM \"User\" WHERE \"id\" = 3");
        assertTrue(q3.isSuccess());
        assertEquals(0, q3.getRows().size(), "Row inserted after savepoint must be rolled back");
    }

    @Test
    void testPrismaInsertReturning() throws Exception {
        db.execute("CREATE TABLE \"User\" (\"id\" INT NOT NULL, \"name\" VARCHAR, \"role\" VARCHAR, PRIMARY KEY (\"id\"))");

        QueryResult res = db.execute("INSERT INTO \"User\" VALUES (10, 'Charlie', 'ADMIN') RETURNING \"id\", \"name\"");
        assertTrue(res.isSuccess(), "INSERT ... RETURNING failed: " + res.getError());
        assertEquals(1, res.getRows().size());
        assertEquals(10, ((Number) res.getRows().get(0).getValue("id")).intValue());
        assertEquals("Charlie", res.getRows().get(0).getValue("name"));
    }

    @Test
    void testPrismaSchemaIntrospection() throws Exception {
        db.execute("CREATE TABLE \"Product\" (\"id\" INT NOT NULL, \"sku\" VARCHAR NOT NULL, \"price\" FLOAT, PRIMARY KEY (\"id\"))");
        db.execute("CREATE TABLE \"Order\" (\"id\" INT NOT NULL, \"productId\" INT, PRIMARY KEY (\"id\"))");
        db.execute("ALTER TABLE \"Order\" ADD CONSTRAINT \"fk_order_product\" FOREIGN KEY (\"productId\") REFERENCES \"Product\" (\"id\")");

        // (a) Introspect tables
        QueryResult qTables = db.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public'");
        assertTrue(qTables.isSuccess(), "information_schema.tables query failed: " + qTables.getError());
        List<String> tables = qTables.getRows().stream()
            .map(r -> (String) r.getValue("table_name"))
            .toList();
        assertTrue(tables.contains("Product"), "Introspection must list Product table");
        assertTrue(tables.contains("Order"), "Introspection must list Order table");

        // (b) Introspect columns
        QueryResult qCols = db.execute("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'Product'");
        assertTrue(qCols.isSuccess(), "information_schema.columns query failed: " + qCols.getError());
        List<String> cols = qCols.getRows().stream()
            .map(r -> (String) r.getValue("column_name"))
            .toList();
        assertTrue(cols.contains("id"));
        assertTrue(cols.contains("sku"));
        assertTrue(cols.contains("price"));

        // (c) Introspect foreign key constraints
        QueryResult qFk = db.execute("SELECT conname, contype FROM pg_catalog.pg_constraint WHERE contype = 'f'");
        assertTrue(qFk.isSuccess(), "pg_constraint query failed: " + qFk.getError());
        List<String> fkNames = qFk.getRows().stream()
            .map(r -> (String) r.getValue("conname"))
            .toList();
        assertTrue(fkNames.contains("fk_order_product"), "pg_constraint must include fk_order_product");
    }
}
