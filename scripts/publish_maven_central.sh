#!/usr/bin/env bash
# ==============================================================================
# StratosDB Maven Central Release & Staging Automation Script
#
# Module: stratosdb-jdbc (Pure Java Type 4 JDBC Driver)
# Target: OSSRH (Sonatype) / Maven Central Repository
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== StratosDB 1.0 Maven Central Release Publisher ==="
echo "Project Root: $PROJECT_ROOT"
cd "$PROJECT_ROOT"

VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo "StratosDB Version: $VERSION"

# Dry run / skip signing flag
DRY_RUN="${DRY_RUN:-false}"

if [ "$DRY_RUN" = "true" ]; then
    echo "[INFO] Running in DRY_RUN mode - packaging artifacts without deploying or signing..."
    mvn clean package -pl stratosdb-jdbc -am -DskipTests
    echo "[SUCCESS] Packaged binary, sources, and javadoc jars successfully."
    ls -lh stratosdb-jdbc/target/*.jar
    exit 0
fi

# Pre-flight check: GPG keys
echo "[1/4] Verifying GPG signing key configuration..."
if ! command -v gpg >/dev/null 2>&1; then
    echo "[ERROR] gpg command not found. GPG is required to sign artifacts for Maven Central."
    exit 1
fi

KEY_COUNT=$(gpg --list-secret-keys --keyid-format=long 2>/dev/null | grep -c "^sec" || true)
if [ "$KEY_COUNT" -eq 0 ]; then
    echo "[WARNING] No GPG secret keys found. For local testing without a key, run with DRY_RUN=true."
    echo "To configure your key: gpg --gen-key"
fi

# Pre-flight check: Maven settings
echo "[2/4] Verifying Maven settings for OSSRH credentials..."
if [ ! -f "$HOME/.m2/settings.xml" ]; then
    echo "[WARNING] $HOME/.m2/settings.xml does not exist. Ensure server 'ossrh' with username/password is configured."
fi

# Building and packaging
echo "[3/4] Building and packaging stratosdb-jdbc with sources and javadoc..."
mvn clean package -pl stratosdb-jdbc -am -DskipTests

echo "[4/4] Deploying stratosdb-jdbc to OSSRH Staging..."
echo "Command to deploy (with release profile):"
echo "  mvn deploy -pl stratosdb-jdbc -am -P release -DskipTests"

if [ "${CONFIRM_DEPLOY:-false}" = "true" ]; then
    mvn deploy -pl stratosdb-jdbc -am -P release -DskipTests
    echo "[SUCCESS] Deployed stratosdb-jdbc to OSSRH successfully!"
else
    echo "[INFO] Deployment skipped. Set CONFIRM_DEPLOY=true to deploy to Sonatype Nexus."
fi
