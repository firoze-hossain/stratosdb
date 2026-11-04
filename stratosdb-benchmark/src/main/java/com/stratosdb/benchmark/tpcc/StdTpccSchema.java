package com.stratosdb.benchmark.tpcc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standard TPC-C schema definitions and DDL generation supporting StratosDB,
 * PostgreSQL 16, and MySQL 8.0 engines.
 *
 * The 9 standard TPC-C tables:
 *   1. warehouse
 *   2. district
 *   3. customer
 *   4. history
 *   5. new_orders
 *   6. orders
 *   7. order_line
 *   8. item
 *   9. stock
 */
public class StdTpccSchema {

    private static final Logger LOG = LoggerFactory.getLogger(StdTpccSchema.class);

    public static List<String> getDropTableStatements(String dialect) {
        String[] tables = {
            "order_line", "new_orders", "orders", "history",
            "stock", "item", "customer", "district", "warehouse"
        };
        List<String> list = new ArrayList<>();
        for (String tbl : tables) {
            list.add("DROP TABLE IF EXISTS " + tbl);
        }
        return list;
    }

    public static List<String> getCreateTableStatements(String dialect) {
        List<String> ddl = new ArrayList<>();
        boolean isMysql = "mysql".equalsIgnoreCase(dialect);
        String floatType = isMysql ? "DOUBLE" : "FLOAT8";

        // 1. warehouse
        ddl.add("""
            CREATE TABLE warehouse (
                w_id INT PRIMARY KEY,
                w_name VARCHAR(10),
                w_street_1 VARCHAR(20),
                w_street_2 VARCHAR(20),
                w_city VARCHAR(20),
                w_state VARCHAR(2),
                w_zip VARCHAR(9),
                w_tax %s,
                w_ytd %s
            )
            """.formatted(floatType, floatType).trim());

        // 2. district
        ddl.add("""
            CREATE TABLE district (
                d_id INT,
                d_w_id INT,
                d_name VARCHAR(10),
                d_street_1 VARCHAR(20),
                d_street_2 VARCHAR(20),
                d_city VARCHAR(20),
                d_state VARCHAR(2),
                d_zip VARCHAR(9),
                d_tax %s,
                d_ytd %s,
                d_next_o_id INT
            )
            """.formatted(floatType, floatType).trim());

        // 3. customer
        ddl.add("""
            CREATE TABLE customer (
                c_id INT,
                c_d_id INT,
                c_w_id INT,
                c_first VARCHAR(16),
                c_middle VARCHAR(2),
                c_last VARCHAR(16),
                c_street_1 VARCHAR(20),
                c_street_2 VARCHAR(20),
                c_city VARCHAR(20),
                c_state VARCHAR(2),
                c_zip VARCHAR(9),
                c_phone VARCHAR(16),
                c_since VARCHAR(30),
                c_credit VARCHAR(2),
                c_credit_lim %s,
                c_discount %s,
                c_balance %s,
                c_ytd_payment %s,
                c_payment_cnt INT,
                c_delivery_cnt INT,
                c_data VARCHAR(500)
            )
            """.formatted(floatType, floatType, floatType, floatType).trim());

        // 4. history
        ddl.add("""
            CREATE TABLE history (
                h_c_id INT,
                h_c_d_id INT,
                h_c_w_id INT,
                h_d_id INT,
                h_w_id INT,
                h_date VARCHAR(30),
                h_amount %s,
                h_data VARCHAR(24)
            )
            """.formatted(floatType).trim());

        // 5. new_orders
        ddl.add("""
            CREATE TABLE new_orders (
                no_o_id INT,
                no_d_id INT,
                no_w_id INT
            )
            """.trim());

        // 6. orders
        ddl.add("""
            CREATE TABLE orders (
                o_id INT,
                o_d_id INT,
                o_w_id INT,
                o_c_id INT,
                o_entry_d VARCHAR(30),
                o_carrier_id INT,
                o_ol_cnt INT,
                o_all_local INT
            )
            """.trim());

        // 7. order_line
        ddl.add("""
            CREATE TABLE order_line (
                ol_o_id INT,
                ol_d_id INT,
                ol_w_id INT,
                ol_number INT,
                ol_i_id INT,
                ol_supply_w_id INT,
                ol_delivery_d VARCHAR(30),
                ol_quantity INT,
                ol_amount %s,
                ol_dist_info VARCHAR(24)
            )
            """.formatted(floatType).trim());

        // 8. item
        ddl.add("""
            CREATE TABLE item (
                i_id INT PRIMARY KEY,
                i_im_id INT,
                i_name VARCHAR(24),
                i_price %s,
                i_data VARCHAR(50)
            )
            """.formatted(floatType).trim());

        // 9. stock
        ddl.add("""
            CREATE TABLE stock (
                s_i_id INT,
                s_w_id INT,
                s_quantity INT,
                s_dist_01 VARCHAR(24),
                s_dist_02 VARCHAR(24),
                s_dist_03 VARCHAR(24),
                s_dist_04 VARCHAR(24),
                s_dist_05 VARCHAR(24),
                s_dist_06 VARCHAR(24),
                s_dist_07 VARCHAR(24),
                s_dist_08 VARCHAR(24),
                s_dist_09 VARCHAR(24),
                s_dist_10 VARCHAR(24),
                s_ytd %s,
                s_order_cnt INT,
                s_remote_cnt INT,
                s_data VARCHAR(50)
            )
            """.formatted(floatType).trim());

        // Secondary Indexes
        ddl.add("CREATE INDEX idx_district_w ON district (d_w_id)");
        ddl.add("CREATE INDEX idx_customer_w ON customer (c_w_id)");
        ddl.add("CREATE INDEX idx_customer_id ON customer (c_id)");
        ddl.add("CREATE INDEX idx_orders_w ON orders (o_w_id)");
        ddl.add("CREATE INDEX idx_orders_c ON orders (o_c_id)");
        ddl.add("CREATE INDEX idx_orders_id ON orders (o_id)");
        ddl.add("CREATE INDEX idx_new_orders_w ON new_orders (no_w_id)");
        ddl.add("CREATE INDEX idx_order_line_w ON order_line (ol_w_id)");
        ddl.add("CREATE INDEX idx_order_line_o ON order_line (ol_o_id)");
        ddl.add("CREATE INDEX idx_stock_w ON stock (s_w_id)");
        ddl.add("CREATE INDEX idx_stock_i ON stock (s_i_id)");

        return ddl;
    }

    public static void initializeSchema(Connection conn, String dialect) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            for (String dropSql : getDropTableStatements(dialect)) {
                try {
                    stmt.execute(dropSql);
                } catch (SQLException ignored) {
                    // Ignore table not found on drop
                }
            }
            for (String createSql : getCreateTableStatements(dialect)) {
                stmt.execute(createSql);
            }
        }
        LOG.info("Initialized TPC-C schema for dialect '{}'", dialect);
    }
}
