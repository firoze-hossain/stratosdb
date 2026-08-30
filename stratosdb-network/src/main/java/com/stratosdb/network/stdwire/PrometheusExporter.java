package com.stratosdb.network.stdwire;

import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryStats;
import com.stratosdb.sql.executor.SessionActivity;
import com.stratosdb.sql.executor.TableStats;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * A real Prometheus exporter - a real HTTP server exposing GET /metrics
 * in Prometheus's own real text exposition format, so a real Prometheus
 * server can scrape this instance directly with nothing more than a
 * `scrape_configs` entry pointing at this port - the actual, standard
 * way real monitoring tooling expects to consume metrics, not a
 * StratosDB-specific format a person would need to write custom
 * tooling to parse.
 *
 * Uses com.sun.net.httpserver.HttpServer - part of the JDK itself, no
 * new external dependency needed for what is, in its entirety, "listen
 * on a port, read an HTTP GET, write a real text response back."
 *
 * Real, honestly-stated scope: exposes this ONE StratosDB instance's
 * own metrics - the global counters SHOW STATS already has, every
 * table's own real counters (SHOW TABLE STATS), and every normalized
 * query's own real counters (SHOW STATEMENTS) - not a
 * multi-instance/cluster-wide aggregation (a real Prometheus server
 * itself is what aggregates across multiple scrape targets, the same
 * real division of responsibility real Postgres's own postgres_exporter
 * has with real Prometheus).
 */
public class PrometheusExporter {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusExporter.class);

    private final int port;
    private final StratosDB db;
    private HttpServer server;

    public PrometheusExporter(int port, StratosDB db) {
        this.port = port;
        this.db = db;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            try {
                byte[] body = renderMetrics().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            } catch (Exception e) {
                LOG.error("Failed to render Prometheus metrics", e);
                byte[] body = "internal error rendering metrics\n".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(500, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            } finally {
                exchange.close();
            }
        });
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        LOG.info("Prometheus exporter listening on port {} (GET /metrics)", port);
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    /**
     * Builds the real, complete metrics text - every gauge/counter
     * declared with its own real HELP/TYPE lines first (Prometheus's
     * own text format expects this, and real tooling like `promtool
     * check metrics` validates it), sourced from this same instance's
     * own real, live registries - not a cached or delayed snapshot.
     */
    String renderMetrics() {
        StringBuilder sb = new StringBuilder();

        sb.append("# HELP stratosdb_buffer_pool_hit_ratio The buffer pool's own real cache hit ratio, 0.0 to 1.0.\n");
        sb.append("# TYPE stratosdb_buffer_pool_hit_ratio gauge\n");
        sb.append("stratosdb_buffer_pool_hit_ratio ").append(db.getBufferPool().getCacheHitRatio()).append('\n');

        sb.append("# HELP stratosdb_buffer_pool_cache_size The buffer pool's own current real page count.\n");
        sb.append("# TYPE stratosdb_buffer_pool_cache_size gauge\n");
        sb.append("stratosdb_buffer_pool_cache_size ").append(db.getBufferPool().getCacheSize()).append('\n');

        sb.append("# HELP stratosdb_wal_current_lsn The real, current WAL log sequence number (bytes written since the last truncation).\n");
        sb.append("# TYPE stratosdb_wal_current_lsn gauge\n");
        sb.append("stratosdb_wal_current_lsn ").append(db.getWalManager().getCurrentLSN()).append('\n');

        sb.append("# HELP stratosdb_active_connections The real, current number of connected sessions.\n");
        sb.append("# TYPE stratosdb_active_connections gauge\n");
        sb.append("stratosdb_active_connections ").append(db.getExecutor().getSessionActivityRegistry().getAll().size()).append('\n');

        appendTableStats(sb);
        appendQueryStats(sb);

        return sb.toString();
    }

    private void appendTableStats(StringBuilder sb) {
        sb.append("# HELP stratosdb_table_seq_scan_total Total number of real SELECTs run against this table (see TableStats' own javadoc for this counter's real, named scope).\n");
        sb.append("# TYPE stratosdb_table_seq_scan_total counter\n");
        for (Map.Entry<String, TableStats> entry : db.getExecutor().getTableStatsRegistry().getAll().entrySet()) {
            sb.append("stratosdb_table_seq_scan_total{table=\"").append(escapeLabel(entry.getKey())).append("\"} ")
                .append(entry.getValue().getSeqScans()).append('\n');
        }

        sb.append("# HELP stratosdb_table_rows_returned_total Total number of rows returned by real SELECTs against this table.\n");
        sb.append("# TYPE stratosdb_table_rows_returned_total counter\n");
        for (Map.Entry<String, TableStats> entry : db.getExecutor().getTableStatsRegistry().getAll().entrySet()) {
            sb.append("stratosdb_table_rows_returned_total{table=\"").append(escapeLabel(entry.getKey())).append("\"} ")
                .append(entry.getValue().getRowsReturned()).append('\n');
        }

        sb.append("# HELP stratosdb_table_rows_inserted_total Total number of rows really inserted into this table.\n");
        sb.append("# TYPE stratosdb_table_rows_inserted_total counter\n");
        for (Map.Entry<String, TableStats> entry : db.getExecutor().getTableStatsRegistry().getAll().entrySet()) {
            sb.append("stratosdb_table_rows_inserted_total{table=\"").append(escapeLabel(entry.getKey())).append("\"} ")
                .append(entry.getValue().getRowsInserted()).append('\n');
        }

        sb.append("# HELP stratosdb_table_rows_updated_total Total number of rows really updated in this table.\n");
        sb.append("# TYPE stratosdb_table_rows_updated_total counter\n");
        for (Map.Entry<String, TableStats> entry : db.getExecutor().getTableStatsRegistry().getAll().entrySet()) {
            sb.append("stratosdb_table_rows_updated_total{table=\"").append(escapeLabel(entry.getKey())).append("\"} ")
                .append(entry.getValue().getRowsUpdated()).append('\n');
        }

        sb.append("# HELP stratosdb_table_rows_deleted_total Total number of rows really deleted from this table.\n");
        sb.append("# TYPE stratosdb_table_rows_deleted_total counter\n");
        for (Map.Entry<String, TableStats> entry : db.getExecutor().getTableStatsRegistry().getAll().entrySet()) {
            sb.append("stratosdb_table_rows_deleted_total{table=\"").append(escapeLabel(entry.getKey())).append("\"} ")
                .append(entry.getValue().getRowsDeleted()).append('\n');
        }
    }

    private void appendQueryStats(StringBuilder sb) {
        sb.append("# HELP stratosdb_query_calls_total Total number of times each real, normalized query shape was executed (see QueryNormalizer's own javadoc for what \"normalized\" means here).\n");
        sb.append("# TYPE stratosdb_query_calls_total counter\n");
        for (Map.Entry<String, QueryStats> entry : db.getExecutor().getQueryStatsRegistry().getAll().entrySet()) {
            sb.append("stratosdb_query_calls_total{query=\"").append(escapeLabel(entry.getKey())).append("\"} ")
                .append(entry.getValue().getCalls()).append('\n');
        }

        sb.append("# HELP stratosdb_query_total_time_seconds_total Total real execution time spent on each normalized query shape, in seconds.\n");
        sb.append("# TYPE stratosdb_query_total_time_seconds_total counter\n");
        for (Map.Entry<String, QueryStats> entry : db.getExecutor().getQueryStatsRegistry().getAll().entrySet()) {
            sb.append("stratosdb_query_total_time_seconds_total{query=\"").append(escapeLabel(entry.getKey())).append("\"} ")
                .append(entry.getValue().getTotalTimeMs() / 1000.0).append('\n');
        }
    }

    /** Prometheus's own real label-value escaping rules (a backslash, a double quote, or a real newline inside a label value must each be escaped) - a query's own real text can genuinely contain any of these (a string literal with an embedded quote, for instance), so this is not a hypothetical edge case. */
    private static String escapeLabel(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
