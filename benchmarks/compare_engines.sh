#!/usr/bin/env bash
# ==============================================================================
# Cross-Engine Comparison & Certification Suite
# Benchmarks StratosDB vs PostgreSQL 16 vs MySQL 8.0 on Identical Hardware
# Sprint 13 Week 25 - Definition of Done Verification
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== StratosDB vs PostgreSQL 16 vs MySQL 8.0 Performance Certification ==="

# Execute automated multi-engine comparison test
mvn test -pl stratosdb-testing -Dtest=TpccAndSysbenchCertificationEndToEndTest

echo "======================================================================"
echo " Performance Certification Complete!"
echo " Results published to benchmarks/BENCHMARK_REPORT.md"
echo "======================================================================"
