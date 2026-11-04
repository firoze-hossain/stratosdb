package com.stratosdb.benchmark.tpcc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Random;

/**
 * Standard implementation of the 5 TPC-C transaction profiles:
 *   1. New-Order   (45%)
 *   2. Payment     (43%)
 *   3. Order-Status (4%)
 *   4. Delivery     (4%)
 *   5. Stock-Level  (4%)
 */
public class StdTpccWorkload {

    public enum TxType {
        NEW_ORDER,
        PAYMENT,
        ORDER_STATUS,
        DELIVERY,
        STOCK_LEVEL
    }

    private final int numWarehouses;
    private final int itemsCount;
    private final int customersPerDistrict;

    public StdTpccWorkload(int numWarehouses) {
        this(numWarehouses, 100_000, 3_000);
    }

    public StdTpccWorkload(int numWarehouses, int itemsCount, int customersPerDistrict) {
        this.numWarehouses = numWarehouses;
        this.itemsCount = itemsCount;
        this.customersPerDistrict = customersPerDistrict;
    }

    public TxType pickRandomTx(Random rng) {
        int x = rng.nextInt(100);
        if (x < 45) return TxType.NEW_ORDER;
        if (x < 88) return TxType.PAYMENT;
        if (x < 92) return TxType.ORDER_STATUS;
        if (x < 96) return TxType.DELIVERY;
        return TxType.STOCK_LEVEL;
    }

    public boolean executeTransaction(Connection conn, TxType type, Random rng) throws SQLException {
        boolean autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            boolean success = switch (type) {
                case NEW_ORDER -> runNewOrder(conn, rng);
                case PAYMENT -> runPayment(conn, rng);
                case ORDER_STATUS -> runOrderStatus(conn, rng);
                case DELIVERY -> runDelivery(conn, rng);
                case STOCK_LEVEL -> runStockLevel(conn, rng);
            };
            if (success) {
                conn.commit();
            } else {
                conn.rollback();
            }
            return success;
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(autoCommit);
        }
    }

    // 1. New-Order Transaction
    public boolean runNewOrder(Connection conn, Random rng) throws SQLException {
        int w_id = rng.nextInt(numWarehouses) + 1;
        int d_id = rng.nextInt(10) + 1;
        int c_id = rng.nextInt(customersPerDistrict) + 1;
        int ol_cnt = rng.nextInt(10) + 5;
        String now = Instant.now().toString();

        // 1. Query warehouse tax
        double w_tax = 0.08;
        try (PreparedStatement ps = conn.prepareStatement("SELECT w_tax FROM warehouse WHERE w_id = ?")) {
            ps.setInt(1, w_id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) w_tax = rs.getDouble(1);
            }
        }

        // 2. Query & update district next_o_id
        int d_next_o_id = 3001;
        double d_tax = 0.05;
        try (PreparedStatement ps = conn.prepareStatement("SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?")) {
            ps.setInt(1, w_id);
            ps.setInt(2, d_id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    d_tax = rs.getDouble(1);
                    d_next_o_id = rs.getInt(2);
                }
            }
        }

        try (PreparedStatement ps = conn.prepareStatement("UPDATE district SET d_next_o_id = ? WHERE d_w_id = ? AND d_id = ?")) {
            ps.setInt(1, d_next_o_id + 1);
            ps.setInt(2, w_id);
            ps.setInt(3, d_id);
            ps.executeUpdate();
        }

        // 3. Insert order
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
            ps.setInt(1, d_next_o_id);
            ps.setInt(2, d_id);
            ps.setInt(3, w_id);
            ps.setInt(4, c_id);
            ps.setString(5, now);
            ps.setInt(6, 0);
            ps.setInt(7, ol_cnt);
            ps.setInt(8, 1);
            ps.executeUpdate();
        }

        // 4. Insert new_orders
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO new_orders VALUES (?, ?, ?)")) {
            ps.setInt(1, d_next_o_id);
            ps.setInt(2, d_id);
            ps.setInt(3, w_id);
            ps.executeUpdate();
        }

        // 5. Order Lines & Stock Updates
        for (int i = 1; i <= ol_cnt; i++) {
            int i_id = rng.nextInt(itemsCount) + 1;
            int s_quantity = 50;

            try (PreparedStatement ps = conn.prepareStatement("SELECT s_quantity FROM stock WHERE s_i_id = ? AND s_w_id = ?")) {
                ps.setInt(1, i_id);
                ps.setInt(2, w_id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) s_quantity = rs.getInt(1);
                }
            }

            int newQuantity = (s_quantity >= 10) ? (s_quantity - 5) : (s_quantity + 91 - 5);
            try (PreparedStatement ps = conn.prepareStatement("UPDATE stock SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?")) {
                ps.setInt(1, newQuantity);
                ps.setInt(2, i_id);
                ps.setInt(3, w_id);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO order_line VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                ps.setInt(1, d_next_o_id);
                ps.setInt(2, d_id);
                ps.setInt(3, w_id);
                ps.setInt(4, i);
                ps.setInt(5, i_id);
                ps.setInt(6, w_id);
                ps.setString(7, now);
                ps.setInt(8, 5);
                ps.setDouble(9, 45.0);
                ps.setString(10, "dist_info");
                ps.executeUpdate();
            }
        }
        return true;
    }

    // 2. Payment Transaction
    public boolean runPayment(Connection conn, Random rng) throws SQLException {
        int w_id = rng.nextInt(numWarehouses) + 1;
        int d_id = rng.nextInt(10) + 1;
        int c_id = rng.nextInt(customersPerDistrict) + 1;
        double amount = (rng.nextInt(4900) + 100) / 100.0;
        String now = Instant.now().toString();

        // 1. Update warehouse YTD
        double w_ytd = 300000.0;
        try (PreparedStatement ps = conn.prepareStatement("SELECT w_ytd FROM warehouse WHERE w_id = ?")) {
            ps.setInt(1, w_id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) w_ytd = rs.getDouble(1);
            }
        }
        try (PreparedStatement ps = conn.prepareStatement("UPDATE warehouse SET w_ytd = ? WHERE w_id = ?")) {
            ps.setDouble(1, w_ytd + amount);
            ps.setInt(2, w_id);
            ps.executeUpdate();
        }

        // 2. Update district YTD
        double d_ytd = 30000.0;
        try (PreparedStatement ps = conn.prepareStatement("SELECT d_ytd FROM district WHERE d_w_id = ? AND d_id = ?")) {
            ps.setInt(1, w_id);
            ps.setInt(2, d_id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) d_ytd = rs.getDouble(1);
            }
        }
        try (PreparedStatement ps = conn.prepareStatement("UPDATE district SET d_ytd = ? WHERE d_w_id = ? AND d_id = ?")) {
            ps.setDouble(1, d_ytd + amount);
            ps.setInt(2, w_id);
            ps.setInt(3, d_id);
            ps.executeUpdate();
        }

        // 3. Update customer balance
        double c_balance = 0.0;
        try (PreparedStatement ps = conn.prepareStatement("SELECT c_balance FROM customer WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?")) {
            ps.setInt(1, c_id);
            ps.setInt(2, d_id);
            ps.setInt(3, w_id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) c_balance = rs.getDouble(1);
            }
        }
        try (PreparedStatement ps = conn.prepareStatement("UPDATE customer SET c_balance = ? WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?")) {
            ps.setDouble(1, c_balance - amount);
            ps.setInt(2, c_id);
            ps.setInt(3, d_id);
            ps.setInt(4, w_id);
            ps.executeUpdate();
        }

        // 4. Insert history record
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
            ps.setInt(1, c_id);
            ps.setInt(2, d_id);
            ps.setInt(3, w_id);
            ps.setInt(4, d_id);
            ps.setInt(5, w_id);
            ps.setString(6, now);
            ps.setDouble(7, amount);
            ps.setString(8, "pay_hist");
            ps.executeUpdate();
        }
        return true;
    }

    // 3. Order-Status Transaction
    public boolean runOrderStatus(Connection conn, Random rng) throws SQLException {
        int w_id = rng.nextInt(numWarehouses) + 1;
        int d_id = rng.nextInt(10) + 1;
        int c_id = rng.nextInt(customersPerDistrict) + 1;

        // Query customer details
        try (PreparedStatement ps = conn.prepareStatement("SELECT c_first, c_last, c_balance FROM customer WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?")) {
            ps.setInt(1, c_id);
            ps.setInt(2, d_id);
            ps.setInt(3, w_id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    c_id = 1;
                }
            }
        }

        // Query customer's order
        int o_id = c_id;
        try (PreparedStatement ps = conn.prepareStatement("SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_c_id = ? AND o_d_id = ? AND o_w_id = ?")) {
            ps.setInt(1, c_id);
            ps.setInt(2, d_id);
            ps.setInt(3, w_id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) o_id = rs.getInt(1);
            }
        }

        // Query order lines
        try (PreparedStatement ps = conn.prepareStatement("SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount FROM order_line WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?")) {
            ps.setInt(1, o_id);
            ps.setInt(2, d_id);
            ps.setInt(3, w_id);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // drain row
                }
            }
        }
        return true;
    }

    // 4. Delivery Transaction
    public boolean runDelivery(Connection conn, Random rng) throws SQLException {
        int w_id = rng.nextInt(numWarehouses) + 1;
        int carrier_id = rng.nextInt(10) + 1;
        String now = Instant.now().toString();

        for (int d = 1; d <= 10; d++) {
            int no_o_id = -1;
            try (PreparedStatement ps = conn.prepareStatement("SELECT no_o_id FROM new_orders WHERE no_w_id = ? AND no_d_id = ?")) {
                ps.setInt(1, w_id);
                ps.setInt(2, d);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) no_o_id = rs.getInt(1);
                }
            }
            if (no_o_id > 0) {
                try (PreparedStatement ps = conn.prepareStatement("DELETE FROM new_orders WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?")) {
                    ps.setInt(1, w_id);
                    ps.setInt(2, d);
                    ps.setInt(3, no_o_id);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement("UPDATE orders SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?")) {
                    ps.setInt(1, carrier_id);
                    ps.setInt(2, w_id);
                    ps.setInt(3, d);
                    ps.setInt(4, no_o_id);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement("UPDATE order_line SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?")) {
                    ps.setString(1, now);
                    ps.setInt(2, w_id);
                    ps.setInt(3, d);
                    ps.setInt(4, no_o_id);
                    ps.executeUpdate();
                }
            }
        }
        return true;
    }

    // 5. Stock-Level Transaction
    public boolean runStockLevel(Connection conn, Random rng) throws SQLException {
        int w_id = rng.nextInt(numWarehouses) + 1;
        int d_id = rng.nextInt(10) + 1;
        int threshold = rng.nextInt(10) + 10;

        int d_next_o_id = 3000;
        try (PreparedStatement ps = conn.prepareStatement("SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?")) {
            ps.setInt(1, w_id);
            ps.setInt(2, d_id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) d_next_o_id = rs.getInt(1);
            }
        }

        // Query stock count under threshold
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(s_i_id) FROM stock WHERE s_w_id = ? AND s_quantity < ?")) {
            ps.setInt(1, w_id);
            ps.setInt(2, threshold);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    rs.getLong(1);
                }
            }
        }
        return true;
    }
}
