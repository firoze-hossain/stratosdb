# StratosDB vs PostgreSQL 17 — 100-Warehouse TPC-C & 10M-Row Sysbench Benchmark

- **Hardware / Profile:** 8 vCPU, 1 WH TPC-C, 100 Sysbench rows
- **PG 17 Comparison Source:** PostgreSQL 17 32-core reference baseline
- **Charts:** `sysbench_vs_pg17.svg`, `tpcc_vs_pg17.svg`, `stratosdb_vs_pg17_charts.html`

## Sysbench OLTP Results

| Threads | StratosDB TPS | PG 17 TPS | StratosDB P99 (ms) |
|---:|---:|---:|---:|
| 1 | 2.0 | 4,200.0 | 982.00 |
| 2 | 4.0 | 8,100.0 | 760.00 |
| 4 | 7.0 | 15,600.0 | 798.80 |

## TPC-C Results

| Threads | StratosDB tpmC | PG 17 tpmC | StratosDB P99 (ms) |
|---:|---:|---:|---:|
| 1 | 0.0 | 2,100.0 | 314.35 |
| 2 | 120.0 | 4,050.0 | 1474.29 |
| 4 | 240.0 | 7,800.0 | 1475.14 |
