#!/usr/bin/env bash
# ==============================================================================
# Reproducible TPC-C Benchmark Runner for StratosDB, PostgreSQL, and MySQL
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
WAREHOUSES="${WAREHOUSES:-50}"
CLIENTS="${CLIENTS:-16}"
DURATION="${DURATION:-30}"
BENCH_JAR="${ROOT_DIR}/stratosdb-benchmark/target/stratosdb-benchmark-1.0.0-SNAPSHOT-standalone.jar}"

if [[ ! -f "${BENCH_JAR}" ]]; then
    echo "Building stratosdb-benchmark standalone jar..."
    (cd "${ROOT_DIR}" && mvn package -pl stratosdb-benchmark -DskipTests)
fi

echo "======================================================================"
echo " Starting StratosDB TPC-C Benchmark (${WAREHOUSES} Warehouses, ${CLIENTS} Clients)"
echo " Target: ${HOST}:${PORT}/${DATABASE} as ${USER}"
echo "======================================================================"

java -XX:+UseZGC -XX:+AlwaysPreTouch -Xms1g -Xmx4g \
    -cp "${BENCH_JAR}" com.stratosdb.benchmark.StratosBench \
    --benchmark tpcc \
    -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DATABASE}" --password "${PASSWORD}" \
    --warehouses "${WAREHOUSES}" \
    -c "${CLIENTS}" -T "${DURATION}"
