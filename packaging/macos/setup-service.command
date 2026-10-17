#!/bin/bash
# ==============================================================================
# StratosDB macOS Interactive Setup & Service Installer
# ==============================================================================

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONF_DIR="/usr/local/etc/stratosdb"
CONF_FILE="$CONF_DIR/stratosdb.conf"
LOG_DIR="/usr/local/var/log/stratosdb"
PLIST_DST="/Library/LaunchDaemons/com.stratosdb.server.plist"

echo "================================================================="
echo "               StratosDB macOS Setup Wizard                      "
echo "================================================================="
echo "This script will configure StratosDB and optionally register it"
echo "as a macOS LaunchDaemon background service."
echo ""

# 1. Port
read -r -p "Database Port [6582]: " INPUT_PORT
INPUT_PORT="${INPUT_PORT:-6582}"

# 2. Data Directory
read -r -p "Data Directory [/usr/local/var/stratosdb/data]: " INPUT_DATADIR
INPUT_DATADIR="${INPUT_DATADIR:-/usr/local/var/stratosdb/data}"

# 3. Cluster Mode
read -r -p "Enable Multi-Database Cluster Mode (y/n) [y]: " INPUT_CLUSTER
INPUT_CLUSTER="${INPUT_CLUSTER:-y}"
case "${INPUT_CLUSTER,,}" in
    y|yes) CLUSTER_VAL="true" ;;
    *)     CLUSTER_VAL="false" ;;
esac

echo ""
echo "Creating directories (may require admin password)..."
sudo mkdir -p "$CONF_DIR" "$INPUT_DATADIR" "$LOG_DIR" /usr/local/bin

echo "Saving configuration to $CONF_FILE..."
sudo tee "$CONF_FILE" > /dev/null <<EOF
# StratosDB Configuration File (macOS)
# Generated on $(date)

port=$INPUT_PORT
data_directory=$INPUT_DATADIR
cluster=$CLUSTER_VAL

buffer_pool_size=1024
sync_wal=true
max_connections=100
autovacuum_interval_ms=0
slow_query_threshold_ms=-1
EOF

# Install CLI wrapper
if [ -f "$DIR/stdsql" ]; then
    sudo cp "$DIR/stdsql" /usr/local/bin/stdsql
    sudo chmod +x /usr/local/bin/stdsql
fi

# Service installation
read -r -p "Install and start StratosDB background service (LaunchDaemon)? (y/n) [y]: " INSTALL_SVC
INSTALL_SVC="${INSTALL_SVC:-y}"

if [[ "${INSTALL_SVC,,}" =~ ^(y|yes)$ ]]; then
    echo "Installing LaunchDaemon to $PLIST_DST..."
    if [ -f "$DIR/com.stratosdb.server.plist" ]; then
        sudo cp "$DIR/com.stratosdb.server.plist" "$PLIST_DST"
    else
        sudo tee "$PLIST_DST" > /dev/null <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.stratosdb.server</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Applications/StratosDB.app/Contents/MacOS/stratosdb</string>
        <string>--config</string>
        <string>$CONF_FILE</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$LOG_DIR/stdout.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/stderr.log</string>
</dict>
</plist>
EOF
    fi

    sudo chown root:wheel "$PLIST_DST"
    sudo chmod 644 "$PLIST_DST"
    sudo launchctl unload "$PLIST_DST" 2>/dev/null || true
    sudo launchctl load -w "$PLIST_DST"
    echo "StratosDB LaunchDaemon service loaded."
fi

echo ""
echo "================================================================="
echo "Setup Complete!"
echo "StratosDB is listening on port $INPUT_PORT."
echo "Connect with:"
echo "  stdsql -h localhost -p $INPUT_PORT -U anyuser -d anydb"
echo "================================================================="
read -r -p "Press [Enter] to exit..."
