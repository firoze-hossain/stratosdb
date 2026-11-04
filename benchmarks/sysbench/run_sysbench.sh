#!/usr/bin/env bash
# ==============================================================================
# Reproducible Sysbench OLTP Benchmark Runner for StratosDB
# Sprint 13 Week 25 - Performance Certification
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-6583}"
USER="${USER_NAME:-admin}"
PASSWORD="${PASSWORD:-admin}"
DATABASE="${DATABASE:-stratos}"
TABLES="${TABLES:-10}"
TABLE_SIZE="${TABLE_SIZE:-1000000}"
CLIENTS="${CLIENTS:-16}"
DURATION="${DURATION:-30}"
BENCH_JAR="${ROOT_DIR}/stratosdb-benchmark/target/stratosdb-benchmark-1.0.0-SNAPSHOT-standalone.jar"

if [[ ! -f "${BENCH_JAR}" ]]; then
    echo "Building stratosdb-benchmark standalone jar..."
    (cd "${ROOT_DIR}" && mvn package -pl stratosdb-benchmark -DskipTests)
fi

echo "======================================================================"
echo " Starting StratosDB Sysbench OLTP Benchmark (${TABLES} tables, ${TABLE_SIZE} rows)"
echo " Target: ${HOST}:${PORT}/${DATABASE} as ${USER} with ${CLIENTS} clients"
echo "======================================================================"

java -XX:+UseZGC -XX:+AlwaysPreTouch -Xms1g -Xmx4g \
    -cp "${BENCH_JAR}" com.stratosdb.benchmark.StratosBench \
    --benchmark sysbench \
    -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DATABASE}" --password "${PASSWORD}" \
    --tables "${TABLES}" --table-size "${TABLE_SIZE}" \
    -c "${CLIENTS}" -T "${DURATION}"
