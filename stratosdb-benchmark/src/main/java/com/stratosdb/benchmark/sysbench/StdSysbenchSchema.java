package com.stratosdb.benchmark.sysbench;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standard Sysbench schema and DDL generator for StratosDB, PostgreSQL 16, and MySQL 8.0.
 */
public class StdSysbenchSchema {

    private static final Logger LOG = LoggerFactory.getLogger(StdSysbenchSchema.class);

    public static List<String> getDropTableStatements(int numTables) {
        List<String> list = new ArrayList<>();
        for (int i = 1; i <= numTables; i++) {
            list.add("DROP TABLE IF EXISTS sbtest" + i);
        }
        return list;
    }

    public static List<String> getCreateTableStatements(int numTables, String dialect) {
        List<String> list = new ArrayList<>();
        for (int i = 1; i <= numTables; i++) {
            String tableName = "sbtest" + i;
            String ddl = """
                CREATE TABLE %s (
                    id INT PRIMARY KEY,
                    k INT,
                    c VARCHAR(120),
                    pad VARCHAR(60)
                )
                """.formatted(tableName).trim();
            list.add(ddl);
            list.add("CREATE INDEX idx_k_" + i + " ON " + tableName + " (k)");
        }
        return list;
    }

    public static void initializeSchema(Connection conn, int numTables, String dialect) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            for (String dropSql : getDropTableStatements(numTables)) {
                try {
                    stmt.execute(dropSql);
                } catch (SQLException ignored) {
                }
            }
            for (String createSql : getCreateTableStatements(numTables, dialect)) {
                stmt.execute(createSql);
            }
        }
        LOG.info("Initialized Sysbench schema for {} table(s) under dialect '{}'", numTables, dialect);
    }
}
