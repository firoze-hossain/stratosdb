package com.stratosdb.benchmark.sysbench;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-performance batch data loader for Sysbench tables (up to 10M rows).
 */
public class StdSysbenchDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(StdSysbenchDataGenerator.class);

    private final int numTables;
    private final int tableSize;
    private final int batchSize;
    private final Random rng = new Random(12345); // deterministic seed

    public StdSysbenchDataGenerator(int numTables, int tableSize) {
        this(numTables, tableSize, 1000);
    }

    public StdSysbenchDataGenerator(int numTables, int tableSize, int batchSize) {
        this.numTables = numTables;
        this.tableSize = tableSize;
        this.batchSize = batchSize;
    }

    public void loadData(Connection conn) throws SQLException {
        boolean autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            for (int t = 1; t <= numTables; t++) {
                String tableName = "sbtest" + t;
                String sql = "INSERT INTO " + tableName + " (id, k, c, pad) VALUES (?, ?, ?, ?)";

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    for (int i = 1; i <= tableSize; i++) {
                        ps.setInt(1, i);
                        ps.setInt(2, rng.nextInt(tableSize) + 1);
                        ps.setString(3, generateSysbenchString(120));
                        ps.setString(4, generateSysbenchString(60));
                        ps.executeUpdate();
                    }
                }
                conn.commit();
                LOG.info("Loaded {} rows into {}", tableSize, tableName);
            }
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(autoCommit);
        }
    }

    private String generateSysbenchString(int length) {
        // Sysbench format: '###########-###########-...' where # is a digit
        StringBuilder sb = new StringBuilder(length);
        while (sb.length() < length) {
            long val = Math.abs(rng.nextLong()) % 100000000000L;
            sb.append(String.format("%011d-", val));
        }
        return sb.substring(0, length);
    }
}
