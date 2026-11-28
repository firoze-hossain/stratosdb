package com.stratosdb.benchmark.comparison;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/**
 * Generates publication-ready SVG charts, an interactive HTML dashboard, and a
 * Markdown report comparing StratosDB vs PostgreSQL 17 on Sysbench OLTP (TPS)
 * and TPC-C (tpmC).
 */
public class BenchmarkChartGenerator {

    public record SeriesPoint(
        int threads,
        double stratosValue,
        double pg17Value,
        double stratosP99Ms,
        double pg17P99Ms
    ) {}

    public static void generateAll(
            Path outputDir,
            List<SeriesPoint> sysbenchSeries,
            List<SeriesPoint> tpccSeries,
            String hardwareLabel,
            boolean pg17LiveMeasured) throws IOException {

        Files.createDirectories(outputDir);

        String sysbenchSvg = renderBarComparisonSvg(
            "Sysbench OLTP Throughput: StratosDB vs PostgreSQL 17",
            "Throughput (TPS)",
            "100k TPS Target",
            100_000.0,
            sysbenchSeries
        );
        Files.writeString(outputDir.resolve("sysbench_vs_pg17.svg"), sysbenchSvg, StandardCharsets.UTF_8);

        String tpccSvg = renderBarComparisonSvg(
            "TPC-C Throughput: StratosDB vs PostgreSQL 17",
            "Throughput (tpmC)",
            "50k tpmC Target",
            50_000.0,
            tpccSeries
        );
        Files.writeString(outputDir.resolve("tpcc_vs_pg17.svg"), tpccSvg, StandardCharsets.UTF_8);

        String html = renderHtmlDashboard(sysbenchSvg, tpccSvg, sysbenchSeries, tpccSeries, hardwareLabel, pg17LiveMeasured);
        Files.writeString(outputDir.resolve("stratosdb_vs_pg17_charts.html"), html, StandardCharsets.UTF_8);

        String md = renderMarkdownReport(sysbenchSeries, tpccSeries, hardwareLabel, pg17LiveMeasured);
        Files.writeString(outputDir.resolve("BENCHMARK_100K_REPORT.md"), md, StandardCharsets.UTF_8);
    }

    static String renderBarComparisonSvg(
            String title,
            String yAxisLabel,
            String targetLabel,
            double targetValue,
            List<SeriesPoint> points) {

        int width = 820;
        int height = 420;
        int marginLeft = 90;
        int marginRight = 40;
        int marginTop = 60;
        int marginBottom = 70;
        int plotW = width - marginLeft - marginRight;
        int plotH = height - marginTop - marginBottom;

        double maxVal = targetValue * 1.15;
        for (SeriesPoint p : points) {
            maxVal = Math.max(maxVal, Math.max(p.stratosValue(), p.pg17Value()) * 1.15);
        }
        if (maxVal <= 0) maxVal = 100.0;

        StringBuilder svg = new StringBuilder();
        svg.append(String.format(Locale.ROOT,
            "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"%d\" height=\"%d\" viewBox=\"0 0 %d %d\" style=\"font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #ffffff;\">\n",
            width, height, width, height));

        // Title
        svg.append(String.format(Locale.ROOT,
            "  <text x=\"%d\" y=\"30\" text-anchor=\"middle\" font-size=\"18\" font-weight=\"700\" fill=\"#0f172a\">%s</text>\n",
            width / 2, escapeXml(title)));

        // Legend
        svg.append("  <rect x=\"540\" y=\"42\" width=\"14\" height=\"14\" rx=\"2\" fill=\"#2563eb\"/>\n");
        svg.append("  <text x=\"560\" y=\"54\" font-size=\"12\" fill=\"#334155\">StratosDB</text>\n");
        svg.append("  <rect x=\"645\" y=\"42\" width=\"14\" height=\"14\" rx=\"2\" fill=\"#64748b\"/>\n");
        svg.append("  <text x=\"665\" y=\"54\" font-size=\"12\" fill=\"#334155\">PostgreSQL 17</text>\n");

        // Grid lines (5 steps)
        for (int i = 0; i <= 5; i++) {
            double frac = i / 5.0;
            int y = marginTop + plotH - (int) Math.round(frac * plotH);
            double tickVal = frac * maxVal;
            svg.append(String.format(Locale.ROOT,
                "  <line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" stroke=\"#e2e8f0\" stroke-width=\"1\"/>\n",
                marginLeft, y, marginLeft + plotW, y));
            svg.append(String.format(Locale.ROOT,
                "  <text x=\"%d\" y=\"%d\" text-anchor=\"end\" font-size=\"11\" fill=\"#64748b\">%,.0f</text>\n",
                marginLeft - 10, y + 4, tickVal));
        }

        // Axes
        svg.append(String.format(Locale.ROOT,
            "  <line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" stroke=\"#475569\" stroke-width=\"1.5\"/>\n",
            marginLeft, marginTop, marginLeft, marginTop + plotH));
        svg.append(String.format(Locale.ROOT,
            "  <line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" stroke=\"#475569\" stroke-width=\"1.5\"/>\n",
            marginLeft, marginTop + plotH, marginLeft + plotW, marginTop + plotH));

        // Y-axis label
        svg.append(String.format(Locale.ROOT,
            "  <text x=\"22\" y=\"%d\" text-anchor=\"middle\" font-size=\"12\" font-weight=\"600\" fill=\"#334155\" transform=\"rotate(-90 22 %d)\">%s</text>\n",
            marginTop + plotH / 2, marginTop + plotH / 2, escapeXml(yAxisLabel)));

        // Target threshold line
        int targetY = marginTop + plotH - (int) Math.round((targetValue / maxVal) * plotH);
        svg.append(String.format(Locale.ROOT,
            "  <line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" stroke=\"#16a34a\" stroke-width=\"1.5\" stroke-dasharray=\"6,4\"/>\n",
            marginLeft, targetY, marginLeft + plotW, targetY));
        svg.append(String.format(Locale.ROOT,
            "  <text x=\"%d\" y=\"%d\" text-anchor=\"end\" font-size=\"11\" font-weight=\"600\" fill=\"#15803d\">%s</text>\n",
            marginLeft + plotW - 4, targetY - 5, escapeXml(targetLabel)));

        // Bars
        int groupCount = Math.max(1, points.size());
        double groupWidth = (double) plotW / groupCount;
        int barWidth = Math.max(14, Math.min(36, (int) (groupWidth * 0.28)));
        int gap = 6;

        for (int i = 0; i < points.size(); i++) {
            SeriesPoint p = points.get(i);
            double centerX = marginLeft + groupWidth * (i + 0.5);

            int h1 = (int) Math.round((p.stratosValue() / maxVal) * plotH);
            int x1 = (int) Math.round(centerX - barWidth - gap / 2.0);
            int y1 = marginTop + plotH - h1;

            int h2 = (int) Math.round((p.pg17Value() / maxVal) * plotH);
            int x2 = (int) Math.round(centerX + gap / 2.0);
            int y2 = marginTop + plotH - h2;

            svg.append(String.format(Locale.ROOT,
                "  <rect x=\"%d\" y=\"%d\" width=\"%d\" height=\"%d\" rx=\"3\" fill=\"#2563eb\"/>\n",
                x1, y1, barWidth, h1));
            svg.append(String.format(Locale.ROOT,
                "  <text x=\"%d\" y=\"%d\" text-anchor=\"middle\" font-size=\"10\" font-weight=\"600\" fill=\"#1e40af\">%,.0f</text>\n",
                x1 + barWidth / 2, Math.max(marginTop + 12, y1 - 4), p.stratosValue()));

            svg.append(String.format(Locale.ROOT,
                "  <rect x=\"%d\" y=\"%d\" width=\"%d\" height=\"%d\" rx=\"3\" fill=\"#64748b\"/>\n",
                x2, y2, barWidth, h2));
            svg.append(String.format(Locale.ROOT,
                "  <text x=\"%d\" y=\"%d\" text-anchor=\"middle\" font-size=\"10\" fill=\"#475569\">%,.0f</text>\n",
                x2 + barWidth / 2, Math.max(marginTop + 12, y2 - 4), p.pg17Value()));

            svg.append(String.format(Locale.ROOT,
                "  <text x=\"%d\" y=\"%d\" text-anchor=\"middle\" font-size=\"12\" fill=\"#0f172a\">%d threads</text>\n",
                (int) Math.round(centerX), marginTop + plotH + 22, p.threads()));
        }

        // X-axis label
        svg.append(String.format(Locale.ROOT,
            "  <text x=\"%d\" y=\"%d\" text-anchor=\"middle\" font-size=\"12\" font-weight=\"600\" fill=\"#334155\">Client Concurrency</text>\n",
            marginLeft + plotW / 2, height - 14));

        svg.append("</svg>\n");
        return svg.toString();
    }

    private static String renderHtmlDashboard(
            String sysbenchSvg,
            String tpccSvg,
            List<SeriesPoint> sysbenchSeries,
            List<SeriesPoint> tpccSeries,
            String hardwareLabel,
            boolean pg17LiveMeasured) {

        SeriesPoint peakSys = sysbenchSeries.get(sysbenchSeries.size() - 1);
        SeriesPoint peakTpcc = tpccSeries.get(tpccSeries.size() - 1);

        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\"/>\n");
        html.append("<title>StratosDB vs PostgreSQL 17 Benchmark Dashboard</title>\n");
        html.append("<style>body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:960px;margin:32px auto;padding:0 20px;color:#0f172a;background:#f8fafc;}");
        html.append(".cards{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin:20px 0;}.card{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:16px;}");
        html.append(".kpi{font-size:26px;font-weight:700;color:#2563eb;}.chart{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:16px;margin:20px 0;text-align:center;}");
        html.append("table{width:100%;border-collapse:collapse;background:#fff;border:1px solid #e2e8f0;margin-top:16px;}th,td{padding:10px 12px;border-bottom:1px solid #e2e8f0;text-align:right;}th:first-child,td:first-child{text-align:left;}</style>\n</head>\n<body>\n");
        html.append("<h1>StratosDB vs PostgreSQL 17 — OLTP Benchmark Report</h1>\n");
        html.append("<p><strong>Environment:</strong> ").append(escapeXml(hardwareLabel))
            .append(" | <strong>PG 17 Baseline:</strong> ").append(pg17LiveMeasured ? "Live JDBC" : "Published 32-core PG 17 reference")
            .append("</p>\n");

        html.append("<div class=\"cards\">\n");
        html.append(String.format(Locale.ROOT, "<div class=\"card\"><div>Peak Sysbench OLTP</div><div class=\"kpi\">%,.0f TPS</div><div>vs PG 17: %,.0f TPS</div></div>\n", peakSys.stratosValue(), peakSys.pg17Value()));
        html.append(String.format(Locale.ROOT, "<div class=\"card\"><div>Peak TPC-C</div><div class=\"kpi\">%,.0f tpmC</div><div>vs PG 17: %,.0f tpmC</div></div>\n", peakTpcc.stratosValue(), peakTpcc.pg17Value()));
        html.append(String.format(Locale.ROOT, "<div class=\"card\"><div>Peak P99 Latency</div><div class=\"kpi\">%.2f ms</div><div>Sysbench / TPC-C: %.2f ms</div></div>\n", peakSys.stratosP99Ms(), peakTpcc.stratosP99Ms()));
        html.append("</div>\n");

        html.append("<div class=\"chart\">\n").append(sysbenchSvg).append("</div>\n");
        html.append("<div class=\"chart\">\n").append(tpccSvg).append("</div>\n");

        html.append("<h2>Sysbench OLTP Scaling</h2>\n<table><tr><th>Threads</th><th>StratosDB TPS</th><th>PG 17 TPS</th><th>StratosDB P99 (ms)</th><th>PG 17 P99 (ms)</th></tr>\n");
        for (SeriesPoint p : sysbenchSeries) {
            html.append(String.format(Locale.ROOT, "<tr><td>%d</td><td>%,.1f</td><td>%,.1f</td><td>%.2f</td><td>%.2f</td></tr>\n",
                p.threads(), p.stratosValue(), p.pg17Value(), p.stratosP99Ms(), p.pg17P99Ms()));
        }
        html.append("</table>\n");

        html.append("<h2>TPC-C Scaling</h2>\n<table><tr><th>Threads</th><th>StratosDB tpmC</th><th>PG 17 tpmC</th><th>StratosDB P99 (ms)</th><th>PG 17 P99 (ms)</th></tr>\n");
        for (SeriesPoint p : tpccSeries) {
            html.append(String.format(Locale.ROOT, "<tr><td>%d</td><td>%,.1f</td><td>%,.1f</td><td>%.2f</td><td>%.2f</td></tr>\n",
                p.threads(), p.stratosValue(), p.pg17Value(), p.stratosP99Ms(), p.pg17P99Ms()));
        }
        html.append("</table>\n</body>\n</html>\n");
        return html.toString();
    }

    private static String renderMarkdownReport(
            List<SeriesPoint> sysbenchSeries,
            List<SeriesPoint> tpccSeries,
            String hardwareLabel,
            boolean pg17LiveMeasured) {

        StringBuilder sb = new StringBuilder();
        sb.append("# StratosDB vs PostgreSQL 17 — 100-Warehouse TPC-C & 10M-Row Sysbench Benchmark\n\n");
        sb.append("- **Hardware / Profile:** ").append(hardwareLabel).append("\n");
        sb.append("- **PG 17 Comparison Source:** ").append(pg17LiveMeasured ? "Live PostgreSQL 17 JDBC" : "PostgreSQL 17 32-core reference baseline").append("\n");
        sb.append("- **Charts:** `sysbench_vs_pg17.svg`, `tpcc_vs_pg17.svg`, `stratosdb_vs_pg17_charts.html`\n\n");

        sb.append("## Sysbench OLTP Results\n\n");
        sb.append("| Threads | StratosDB TPS | PG 17 TPS | StratosDB P99 (ms) |\n");
        sb.append("|---:|---:|---:|---:|\n");
        for (SeriesPoint p : sysbenchSeries) {
            sb.append(String.format(Locale.ROOT, "| %d | %,.1f | %,.1f | %.2f |\n",
                p.threads(), p.stratosValue(), p.pg17Value(), p.stratosP99Ms()));
        }

        sb.append("\n## TPC-C Results\n\n");
        sb.append("| Threads | StratosDB tpmC | PG 17 tpmC | StratosDB P99 (ms) |\n");
        sb.append("|---:|---:|---:|---:|\n");
        for (SeriesPoint p : tpccSeries) {
            sb.append(String.format(Locale.ROOT, "| %d | %,.1f | %,.1f | %.2f |\n",
                p.threads(), p.stratosValue(), p.pg17Value(), p.stratosP99Ms()));
        }
        return sb.toString();
    }

    private static String escapeXml(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
