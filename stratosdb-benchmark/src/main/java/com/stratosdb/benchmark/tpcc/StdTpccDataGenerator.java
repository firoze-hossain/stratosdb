package com.stratosdb.benchmark.tpcc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data loader for the standard TPC-C benchmark suite.
 * Supports configurable warehouse scale (up to standard 50 warehouses).
 */
public class StdTpccDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(StdTpccDataGenerator.class);

    private static final String[] SYLLABLES = {
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
    };

    private final int numWarehouses;
    private final int itemsCount;
    private final int customersPerDistrict;
    private final Random rng = new Random(42); // deterministic seed for reproducibility

    public StdTpccDataGenerator(int numWarehouses) {
        this(numWarehouses, 100_000, 3_000);
    }

    public StdTpccDataGenerator(int numWarehouses, int itemsCount, int customersPerDistrict) {
        this.numWarehouses = numWarehouses;
        this.itemsCount = itemsCount;
        this.customersPerDistrict = customersPerDistrict;
    }

    public void loadData(Connection conn) throws SQLException {
        boolean autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            loadItems(conn);
            for (int w = 1; w <= numWarehouses; w++) {
                loadWarehouse(conn, w);
                loadStock(conn, w);
                for (int d = 1; d <= 10; d++) {
                    loadDistrict(conn, w, d);
                    loadCustomers(conn, w, d);
                    loadOrders(conn, w, d);
                }
                conn.commit();
                LOG.info("Loaded TPC-C warehouse {}/{}", w, numWarehouses);
            }
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(autoCommit);
        }
    }

    private void loadItems(Connection conn) throws SQLException {
        String sql = "INSERT INTO item VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 1; i <= itemsCount; i++) {
                ps.setInt(1, i);
                ps.setInt(2, rng.nextInt(10000) + 1);
                ps.setString(3, "Item-" + i);
                ps.setDouble(4, (rng.nextInt(9900) + 100) / 100.0);
                ps.setString(5, randomAString(26, 50));
                ps.executeUpdate();
            }
        }
        conn.commit();
        LOG.info("Loaded {} TPC-C items", itemsCount);
    }

    private void loadWarehouse(Connection conn, int w_id) throws SQLException {
        String sql = "INSERT INTO warehouse VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, w_id);
            ps.setString(2, "WName-" + w_id);
            ps.setString(3, randomAString(10, 20));
            ps.setString(4, randomAString(10, 20));
            ps.setString(5, randomAString(10, 20));
            ps.setString(6, "CA");
            ps.setString(7, "94000-001");
            ps.setDouble(8, (rng.nextInt(2000)) / 10000.0);
            ps.setDouble(9, 300000.0);
            ps.executeUpdate();
        }
    }

    private void loadDistrict(Connection conn, int w_id, int d_id) throws SQLException {
        String sql = "INSERT INTO district VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, d_id);
            ps.setInt(2, w_id);
            ps.setString(3, "DName-" + d_id);
            ps.setString(4, randomAString(10, 20));
            ps.setString(5, randomAString(10, 20));
            ps.setString(6, randomAString(10, 20));
            ps.setString(7, "CA");
            ps.setString(8, "94000-002");
            ps.setDouble(9, (rng.nextInt(2000)) / 10000.0);
            ps.setDouble(10, 30000.0);
            ps.setInt(11, customersPerDistrict + 1);
            ps.executeUpdate();
        }
    }

    private void loadCustomers(Connection conn, int w_id, int d_id) throws SQLException {
        String sql = "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String histSql = "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        String now = Instant.now().toString();

        try (PreparedStatement ps = conn.prepareStatement(sql);
             PreparedStatement hps = conn.prepareStatement(histSql)) {

            for (int c = 1; c <= customersPerDistrict; c++) {
                ps.setInt(1, c);
                ps.setInt(2, d_id);
                ps.setInt(3, w_id);
                ps.setString(4, randomAString(8, 16));
                ps.setString(5, "OE");
                ps.setString(6, randomLastName(c));
                ps.setString(7, randomAString(10, 20));
                ps.setString(8, randomAString(10, 20));
                ps.setString(9, randomAString(10, 20));
                ps.setString(10, "CA");
                ps.setString(11, "94000-003");
                ps.setString(12, "555-010-0001");
                ps.setString(13, now);
                ps.setString(14, (rng.nextDouble() < 0.1) ? "BC" : "GC");
                ps.setDouble(15, 50000.0);
                ps.setDouble(16, (rng.nextInt(5000)) / 10000.0);
                ps.setDouble(17, -10.0);
                ps.setDouble(18, 10.0);
                ps.setInt(19, 1);
                ps.setInt(20, 0);
                ps.setString(21, randomAString(300, 500));
                ps.executeUpdate();

                // History record for customer
                hps.setInt(1, c);
                hps.setInt(2, d_id);
                hps.setInt(3, w_id);
                hps.setInt(4, d_id);
                hps.setInt(5, w_id);
                hps.setString(6, now);
                hps.setDouble(7, 10.0);
                hps.setString(8, randomAString(12, 24));
                hps.executeUpdate();
            }
        }
    }

    private void loadOrders(Connection conn, int w_id, int d_id) throws SQLException {
        String ordSql = "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        String newOrdSql = "INSERT INTO new_orders VALUES (?, ?, ?)";
        String olSql = "INSERT INTO order_line VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String now = Instant.now().toString();

        try (PreparedStatement ops = conn.prepareStatement(ordSql);
             PreparedStatement nops = conn.prepareStatement(newOrdSql);
             PreparedStatement olps = conn.prepareStatement(olSql)) {

            for (int o = 1; o <= customersPerDistrict; o++) {
                int olCnt = rng.nextInt(10) + 5;
                ops.setInt(1, o);
                ops.setInt(2, d_id);
                ops.setInt(3, w_id);
                ops.setInt(4, o);
                ops.setString(5, now);
                ops.setInt(6, (o < customersPerDistrict * 0.7) ? (rng.nextInt(10) + 1) : 0);
                ops.setInt(7, olCnt);
                ops.setInt(8, 1);
                ops.executeUpdate();

                // 30% of orders are new orders awaiting delivery
                if (o >= customersPerDistrict * 0.7) {
                    nops.setInt(1, o);
                    nops.setInt(2, d_id);
                    nops.setInt(3, w_id);
                    nops.executeUpdate();
                }

                for (int l = 1; l <= olCnt; l++) {
                    olps.setInt(1, o);
                    olps.setInt(2, d_id);
                    olps.setInt(3, w_id);
                    olps.setInt(4, l);
                    olps.setInt(5, rng.nextInt(itemsCount) + 1);
                    olps.setInt(6, w_id);
                    olps.setString(7, now);
                    olps.setInt(8, 5);
                    olps.setDouble(9, (o < customersPerDistrict * 0.7) ? (rng.nextInt(9900) + 100) / 100.0 : 0.0);
                    olps.setString(10, randomAString(24, 24));
                    olps.executeUpdate();
                }
            }
        }
    }

    private void loadStock(Connection conn, int w_id) throws SQLException {
        String sql = "INSERT INTO stock VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 1; i <= itemsCount; i++) {
                ps.setInt(1, i);
                ps.setInt(2, w_id);
                ps.setInt(3, rng.nextInt(90) + 10);
                for (int d = 4; d <= 13; d++) {
                    ps.setString(d, randomAString(24, 24));
                }
                ps.setDouble(14, 0.0);
                ps.setInt(15, 0);
                ps.setInt(16, 0);
                ps.setString(17, randomAString(26, 50));
                ps.executeUpdate();
            }
        }
    }

    public static String randomLastName(int num) {
        StringBuilder sb = new StringBuilder();
        int d1 = (num / 100) % 10;
        int d2 = (num / 10) % 10;
        int d3 = num % 10;
        sb.append(SYLLABLES[d1]);
        sb.append(SYLLABLES[d2]);
        sb.append(SYLLABLES[d3]);
        return sb.toString();
    }

    private String randomAString(int min, int max) {
        int len = min + rng.nextInt(max - min + 1);
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = (char) ('a' + rng.nextInt(26));
        }
        return new String(chars);
    }
}
