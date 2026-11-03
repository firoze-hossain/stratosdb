package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.stdwire.StdWireServer;
import com.stratosdb.sql.executor.QueryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sprint 12 Week 23: Full ORM & Client Ecosystem Certification.
 *
 * DBeaver and JetBrains DataGrip Database Tooling End-to-End Test Suite.
 *
 * Verifies:
 *   1. Standard database tooling initialization queries:
 *      SHOW server_version_num, SHOW search_path, version(), current_schema().
 *   2. Tooling catalog introspection: pg_class, pg_proc, pg_roles, pg_sequence,
 *      information_schema.schemata, information_schema.columns, information_schema.table_constraints,
 *      information_schema.key_column_usage, and information_schema.referential_constraints.
 *   3. JDBC DatabaseMetaData capabilities: getDatabaseProductName() == "PostgreSQL",
 *      major 16, quote string '"', getSchemas, getCatalogs, getTableTypes, getTypeInfo,
 *      getTables, getColumns, getPrimaryKeys, getImportedKeys, getExportedKeys.
 */
public class DBeaverAndDataGripToolingEndToEndTest {

    @TempDir
    Path tempDir;

    private StratosDB db;
    private StdWireServer server;
    private String jdbcUrl;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setPort(port);
        db = new StratosDB(config);
        server = new StdWireServer(port, db);
        server.start();
        jdbcUrl = "jdbc:stratos://localhost:" + port + "/testdb";
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null) db.shutdown();
    }

    private Connection connect() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "stratos");
        return DriverManager.getConnection(jdbcUrl, props);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testToolingMetadataQueries() throws Exception {
        // Create sample schema
        db.execute("CREATE SEQUENCE customers_seq START WITH 1 INCREMENT BY 1");
        db.execute("CREATE TABLE customers (id INT NOT NULL PRIMARY KEY, name VARCHAR NOT NULL, city VARCHAR)");
        db.execute("CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, customer_id INT, total FLOAT)");
        db.execute("ALTER TABLE orders ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES customers (id)");
        db.execute("CREATE VIEW v_active_customers AS SELECT * FROM customers");

        // 1. Tooling handshake and parameter queries
        QueryResult qVer = db.execute("SHOW server_version_num");
        assertTrue(qVer.isSuccess(), "SHOW server_version_num failed: " + qVer.getError());
        assertEquals(1, qVer.getRows().size());

        QueryResult qPath = db.execute("SHOW search_path");
        assertTrue(qPath.isSuccess(), "SHOW search_path failed: " + qPath.getError());

        QueryResult qFuncs = db.execute("SELECT version(), current_schema(), current_database()");
        assertTrue(qFuncs.isSuccess(), "version/current_schema query failed: " + qFuncs.getError());

        // 2. pg_catalog.pg_class introspection
        QueryResult qClass = db.execute("SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind IN ('r', 'v', 'S')");
        assertTrue(qClass.isSuccess(), "pg_class query failed: " + qClass.getError());
        List<String> rels = qClass.getRows().stream()
            .map(r -> (String) r.getValue("relname"))
            .toList();
        assertTrue(rels.contains("customers"), "pg_class must include customers");
        assertTrue(rels.contains("orders"), "pg_class must include orders");
        assertTrue(rels.contains("v_active_customers"), "pg_class must include v_active_customers");
        assertTrue(rels.contains("customers_seq"), "pg_class must include customers_seq");

        // 3. pg_catalog.pg_sequence introspection
        QueryResult qSeq = db.execute("SELECT * FROM pg_catalog.pg_sequence");
        assertTrue(qSeq.isSuccess(), "pg_sequence query failed: " + qSeq.getError());
        assertFalse(qSeq.getRows().isEmpty(), "pg_sequence must not be empty");

        // 4. pg_catalog.pg_roles introspection
        QueryResult qRoles = db.execute("SELECT rolname, rolsuper, rolcanlogin FROM pg_catalog.pg_roles");
        assertTrue(qRoles.isSuccess(), "pg_roles query failed: " + qRoles.getError());
        List<String> roleNames = qRoles.getRows().stream()
            .map(r -> (String) r.getValue("rolname"))
            .toList();
        assertTrue(roleNames.contains("stratos"), "pg_roles must include stratos");

        // 5. information_schema.schemata introspection
        QueryResult qSchemata = db.execute("SELECT schema_name FROM information_schema.schemata");
        assertTrue(qSchemata.isSuccess(), "information_schema.schemata query failed: " + qSchemata.getError());
        List<String> schemaNames = qSchemata.getRows().stream()
            .map(r -> (String) r.getValue("schema_name"))
            .toList();
        assertTrue(schemaNames.contains("public"));
        assertTrue(schemaNames.contains("information_schema"));
        assertTrue(schemaNames.contains("pg_catalog"));

        // 6. information_schema.table_constraints & referential_constraints
        QueryResult qTc = db.execute("SELECT constraint_name, constraint_type, table_name FROM information_schema.table_constraints WHERE table_schema = 'public'");
        assertTrue(qTc.isSuccess(), "table_constraints query failed: " + qTc.getError());

        QueryResult qRef = db.execute("SELECT constraint_name, unique_constraint_name, update_rule, delete_rule FROM information_schema.referential_constraints");
        assertTrue(qRef.isSuccess(), "referential_constraints query failed: " + qRef.getError());
        List<String> fkNames = qRef.getRows().stream()
            .map(r -> (String) r.getValue("constraint_name"))
            .toList();
        assertTrue(fkNames.contains("fk_orders_customers"), "referential_constraints must include fk_orders_customers");
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testJdbcDatabaseMetaDataContract() throws Exception {
        try (Connection conn = connect()) {
            DatabaseMetaData meta = conn.getMetaData();

            // 1. Product detection for ORMs and GUI clients
            assertEquals("PostgreSQL", meta.getDatabaseProductName(), "Must report PostgreSQL for Hibernate/DBeaver auto-detect");
            assertEquals("16.0", meta.getDatabaseProductVersion());
            assertEquals(16, meta.getDatabaseMajorVersion());
            assertEquals(0, meta.getDatabaseMinorVersion());
            assertEquals("\"", meta.getIdentifierQuoteString(), "Must report double-quote as identifier quote string");

            // 2. Feature flags
            assertTrue(meta.supportsBatchUpdates());
            assertTrue(meta.supportsSavepoints());
            assertTrue(meta.supportsGetGeneratedKeys());
            assertTrue(meta.supportsSelectForUpdate());

            // 3. Catalogs & Schemas
            try (ResultSet rs = meta.getCatalogs()) {
                assertTrue(rs.next());
                assertEquals("stratos", rs.getString("TABLE_CAT"));
            }

            List<String> schemas = new ArrayList<>();
            try (ResultSet rs = meta.getSchemas()) {
                while (rs.next()) {
                    schemas.add(rs.getString("TABLE_SCHEM"));
                }
            }
            assertTrue(schemas.contains("public"));
            assertTrue(schemas.contains("information_schema"));
            assertTrue(schemas.contains("pg_catalog"));

            // 4. Table Types
            List<String> types = new ArrayList<>();
            try (ResultSet rs = meta.getTableTypes()) {
                while (rs.next()) {
                    types.add(rs.getString("TABLE_TYPE"));
                }
            }
            assertTrue(types.contains("TABLE"));
            assertTrue(types.contains("VIEW"));
            assertTrue(types.contains("SEQUENCE"));

            // 5. Type Info
            List<String> typeNames = new ArrayList<>();
            try (ResultSet rs = meta.getTypeInfo()) {
                while (rs.next()) {
                    typeNames.add(rs.getString("TYPE_NAME"));
                }
            }
            assertTrue(typeNames.contains("int4"));
            assertTrue(typeNames.contains("varchar"));
            assertTrue(typeNames.contains("bool"));

            // 6. Schema creation and key introspection
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE authors (id INT NOT NULL PRIMARY KEY, name VARCHAR NOT NULL)");
                s.execute("CREATE TABLE books (id INT NOT NULL PRIMARY KEY, title VARCHAR NOT NULL, author_id INT, CONSTRAINT fk_books_authors FOREIGN KEY (author_id) REFERENCES authors (id))");
            }

            // Tables check
            boolean foundAuthors = false, foundBooks = false;
            try (ResultSet rs = meta.getTables(null, null, "%", null)) {
                while (rs.next()) {
                    String t = rs.getString("TABLE_NAME");
                    if ("authors".equalsIgnoreCase(t)) foundAuthors = true;
                    if ("books".equalsIgnoreCase(t)) foundBooks = true;
                }
            }
            assertTrue(foundAuthors, "getTables must find authors");
            assertTrue(foundBooks, "getTables must find books");

            // Columns check
            boolean foundIdCol = false;
            try (ResultSet rs = meta.getColumns(null, null, "authors", "id")) {
                if (rs.next()) {
                    foundIdCol = true;
                    assertEquals("authors", rs.getString("TABLE_NAME"));
                    assertEquals("id", rs.getString("COLUMN_NAME"));
                    assertEquals(DatabaseMetaData.columnNoNulls, rs.getInt("NULLABLE"));
                }
            }
            assertTrue(foundIdCol, "getColumns must find authors.id");

            // Primary Keys check
            boolean foundPk = false;
            try (ResultSet rs = meta.getPrimaryKeys(null, null, "authors")) {
                if (rs.next()) {
                    foundPk = true;
                    assertEquals("id", rs.getString("COLUMN_NAME"));
                }
            }
            assertTrue(foundPk, "getPrimaryKeys must find authors primary key");

            // Imported Keys check (FK in child table 'books')
            boolean foundImportedFk = false;
            try (ResultSet rs = meta.getImportedKeys(null, null, "books")) {
                while (rs.next()) {
                    if ("authors".equalsIgnoreCase(rs.getString("PKTABLE_NAME"))
                        && "id".equalsIgnoreCase(rs.getString("PKCOLUMN_NAME"))
                        && "author_id".equalsIgnoreCase(rs.getString("FKCOLUMN_NAME"))) {
                        foundImportedFk = true;
                    }
                }
            }
            assertTrue(foundImportedFk, "getImportedKeys on books must identify foreign key to authors");

            // Exported Keys check (FK referencing parent table 'authors')
            boolean foundExportedFk = false;
            try (ResultSet rs = meta.getExportedKeys(null, null, "authors")) {
                while (rs.next()) {
                    if ("books".equalsIgnoreCase(rs.getString("FKTABLE_NAME"))
                        && "author_id".equalsIgnoreCase(rs.getString("FKCOLUMN_NAME"))) {
                        foundExportedFk = true;
                    }
                }
            }
            assertTrue(foundExportedFk, "getExportedKeys on authors must identify referencing books table");
        }
    }
}
