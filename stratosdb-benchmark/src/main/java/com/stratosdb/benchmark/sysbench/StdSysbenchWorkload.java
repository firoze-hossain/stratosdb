package com.stratosdb.benchmark.sysbench;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

/**
 * Standard implementation of the Sysbench OLTP workload benchmarks:
 *   - oltp_read_write: 14 reads (10 point, 1 simple range, 1 sum range, 1 order range, 1 distinct range) + 4 writes (index update, non-index update, delete, insert)
 *   - oltp_read_only:  14 reads
 *   - oltp_point_select: 10 point selects
 *   - oltp_write_only: 4 writes
 */
public class StdSysbenchWorkload {

    public enum WorkloadType {
        OLTP_READ_WRITE,
        OLTP_READ_ONLY,
        OLTP_POINT_SELECT,
        OLTP_WRITE_ONLY
    }

    private final int numTables;
    private final int tableSize;
    private final int rangeSize;

    public StdSysbenchWorkload(int numTables, int tableSize) {
        this(numTables, tableSize, 100);
    }

    public StdSysbenchWorkload(int numTables, int tableSize, int rangeSize) {
        this.numTables = numTables;
        this.tableSize = tableSize;
        this.rangeSize = rangeSize;
    }

    public int executeTransaction(Connection conn, WorkloadType type, Random rng) throws SQLException {
        boolean autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        int queriesExecuted = 0;
        try {
            int tableIdx = rng.nextInt(numTables) + 1;
            String tableName = "sbtest" + tableIdx;

            switch (type) {
                case OLTP_READ_WRITE -> {
                    queriesExecuted += executePointSelects(conn, tableName, 10, rng);
                    queriesExecuted += executeSimpleRange(conn, tableName, rng);
                    queriesExecuted += executeSumRange(conn, tableName, rng);
                    queriesExecuted += executeOrderRange(conn, tableName, rng);
                    queriesExecuted += executeDistinctRange(conn, tableName, rng);
                    queriesExecuted += executeIndexUpdate(conn, tableName, rng);
                    queriesExecuted += executeNonIndexUpdate(conn, tableName, rng);
                    queriesExecuted += executeDeleteAndInsert(conn, tableName, rng);
                }
                case OLTP_READ_ONLY -> {
                    queriesExecuted += executePointSelects(conn, tableName, 10, rng);
                    queriesExecuted += executeSimpleRange(conn, tableName, rng);
                    queriesExecuted += executeSumRange(conn, tableName, rng);
                    queriesExecuted += executeOrderRange(conn, tableName, rng);
                    queriesExecuted += executeDistinctRange(conn, tableName, rng);
                }
                case OLTP_POINT_SELECT -> {
                    queriesExecuted += executePointSelects(conn, tableName, 10, rng);
                }
                case OLTP_WRITE_ONLY -> {
                    queriesExecuted += executeIndexUpdate(conn, tableName, rng);
                    queriesExecuted += executeNonIndexUpdate(conn, tableName, rng);
                    queriesExecuted += executeDeleteAndInsert(conn, tableName, rng);
                }
            }

            conn.commit();
            return queriesExecuted;
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(autoCommit);
        }
    }

    private int executePointSelects(Connection conn, String tableName, int count, Random rng) throws SQLException {
        String sql = "SELECT c FROM " + tableName + " WHERE id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < count; i++) {
                int id = rng.nextInt(tableSize) + 1;
                ps.setInt(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) rs.getString(1);
                }
            }
        }
        return count;
    }

    private int executeSimpleRange(Connection conn, String tableName, Random rng) throws SQLException {
        int low = rng.nextInt(Math.max(1, tableSize - rangeSize)) + 1;
        int high = low + rangeSize;
        String sql = "SELECT c FROM " + tableName + " WHERE id BETWEEN ? AND ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, low);
            ps.setInt(2, high);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) { /* drain */ }
            }
        }
        return 1;
    }

    private int executeSumRange(Connection conn, String tableName, Random rng) throws SQLException {
        int low = rng.nextInt(Math.max(1, tableSize - rangeSize)) + 1;
        int high = low + rangeSize;
        String sql = "SELECT SUM(k) FROM " + tableName + " WHERE id BETWEEN ? AND ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, low);
            ps.setInt(2, high);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) rs.getLong(1);
            }
        }
        return 1;
    }

    private int executeOrderRange(Connection conn, String tableName, Random rng) throws SQLException {
        int low = rng.nextInt(Math.max(1, tableSize - rangeSize)) + 1;
        int high = low + rangeSize;
        String sql = "SELECT c FROM " + tableName + " WHERE id BETWEEN ? AND ? ORDER BY c";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, low);
            ps.setInt(2, high);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) { /* drain */ }
            }
        }
        return 1;
    }

    private int executeDistinctRange(Connection conn, String tableName, Random rng) throws SQLException {
        int low = rng.nextInt(Math.max(1, tableSize - rangeSize)) + 1;
        int high = low + rangeSize;
        String sql = "SELECT DISTINCT c FROM " + tableName + " WHERE id BETWEEN ? AND ? ORDER BY c";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, low);
            ps.setInt(2, high);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) { /* drain */ }
            }
        }
        return 1;
    }

    private int executeIndexUpdate(Connection conn, String tableName, Random rng) throws SQLException {
        int id = rng.nextInt(tableSize) + 1;
        int newK = rng.nextInt(tableSize) + 1;
        String sql = "UPDATE " + tableName + " SET k = ? WHERE id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, newK);
            ps.setInt(2, id);
            ps.executeUpdate();
        }
        return 1;
    }

    private int executeNonIndexUpdate(Connection conn, String tableName, Random rng) throws SQLException {
        int id = rng.nextInt(tableSize) + 1;
        String newC = "updated-" + rng.nextInt(1000000);
        String sql = "UPDATE " + tableName + " SET c = ? WHERE id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, newC);
            ps.setInt(2, id);
            ps.executeUpdate();
        }
        return 1;
    }

    private int executeDeleteAndInsert(Connection conn, String tableName, Random rng) throws SQLException {
        int id = rng.nextInt(tableSize) + 1;
        int k = rng.nextInt(tableSize) + 1;
        String c = "reinserted-" + id;
        String pad = "pad-" + id;

        String delSql = "DELETE FROM " + tableName + " WHERE id = ?";
        try (PreparedStatement ps = conn.prepareStatement(delSql)) {
            ps.setInt(1, id);
            ps.executeUpdate();
        }

        String insSql = "INSERT INTO " + tableName + " (id, k, c, pad) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(insSql)) {
            ps.setInt(1, id);
            ps.setInt(2, k);
            ps.setString(3, c);
            ps.setString(4, pad);
            ps.executeUpdate();
        } catch (SQLException e) {
            // Concurrent transaction inserted/holds this id - expected under multi-client OLTP
            return 1;
        }
        return 2;
    }
}
