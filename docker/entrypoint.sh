#!/usr/bin/env bash
set -e

# ==============================================================================
# StratosDB 1.0 Production Docker Entrypoint
# ==============================================================================

cleanup_and_exit() {
    echo "Received termination signal, shutting down StratosDB cleanly..."
    exit 0
}
trap cleanup_and_exit SIGTERM SIGINT SIGHUP

DATA_DIR="${STRATOS_DATA_DIR:-/var/lib/stratosdb/data}"
CONF_FILE="${STRATOS_CONF:-/etc/stratosdb/stratosdb.conf}"
PORT="${STRATOS_PORT:-5431}"
STDWIRE_PORT="${STRATOS_STDWIRE_PORT:-5432}"
BUFFER_POOL_SIZE="${STRATOS_BUFFER_POOL_SIZE:-16384}"
DIRECT_ALLOC="${STRATOS_DIRECT_ALLOCATION:-true}"
SYNC_WAL="${STRATOS_SYNC_WAL:-true}"
MAX_CONN="${STRATOS_MAX_CONNECTIONS:-200}"
AUTOVACUUM_MS="${STRATOS_AUTOVACUUM_INTERVAL_MS:-60000}"
SLOW_QUERY_MS="${STRATOS_SLOW_QUERY_THRESHOLD_MS:-200}"

# Default JVM Memory & GC configuration: Generational ZGC for sub-1.5ms P99 latency
if [ -z "$JAVA_OPTS" ]; then
    JAVA_OPTS="-XX:+UseZGC -XX:+AlwaysPreTouch -Xms512m -Xmx2g -Djdk.attach.allowAttachSelf=true"
fi

mkdir -p "$DATA_DIR"
mkdir -p "$(dirname "$CONF_FILE")"

# 1. Generate or update stratosdb.conf if missing
if [ ! -f "$CONF_FILE" ]; then
    echo "# StratosDB 1.0 Production Configuration" > "$CONF_FILE"
    echo "data_directory = $DATA_DIR" >> "$CONF_FILE"
    echo "port = $PORT" >> "$CONF_FILE"
    echo "stdwire_port = $STDWIRE_PORT" >> "$CONF_FILE"
    echo "buffer_pool_size = $BUFFER_POOL_SIZE" >> "$CONF_FILE"
    echo "direct_allocation = $DIRECT_ALLOC" >> "$CONF_FILE"
    echo "sync_wal = $SYNC_WAL" >> "$CONF_FILE"
    echo "max_connections = $MAX_CONN" >> "$CONF_FILE"
    echo "autovacuum_interval_ms = $AUTOVACUUM_MS" >> "$CONF_FILE"
    echo "slow_query_threshold_ms = $SLOW_QUERY_MS" >> "$CONF_FILE"
    if [ -n "$STRATOS_REPLICATION_PORT" ]; then
        echo "replication_port = $STRATOS_REPLICATION_PORT" >> "$CONF_FILE"
    fi
    if [ -n "$STRATOS_REPLICA_OF" ]; then
        echo "replica_of = $STRATOS_REPLICA_OF" >> "$CONF_FILE"
    fi
    if [ -n "$STRATOS_CLUSTER_MODE" ]; then
        echo "cluster = $STRATOS_CLUSTER_MODE" >> "$CONF_FILE"
    fi
fi

# 2. Automated Replica Networked Bootstrapping (if data dir is empty and replicaOf is specified)
if [ -n "$STRATOS_REPLICA_OF" ] && [ -z "$(ls -A "$DATA_DIR" 2>/dev/null)" ]; then
    echo "StratosDB replica mode detected with empty data directory: bootstrapping via network from $STRATOS_REPLICA_OF..."
    PRIMARY_HOST="${STRATOS_REPLICA_OF%%:*}"
    PRIMARY_PORT="${STRATOS_REPLICA_OF##*:}"
    java -cp "/opt/stratosdb/lib/stratosdb-cli.jar" com.stratosdb.cli.StdBaseBackup \
        -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -D "$DATA_DIR" || {
        echo "Warning: automatic networked base backup bootstrap returned non-zero, continuing with standard startup..."
    }
fi

# 3. Handle Command Routing
if [ "$1" = "stratosdb" ] || [ "$1" = "stratosdb-server" ] || [ -z "$1" ]; then
    SERVER_ARGS=("--config=$CONF_FILE" "--data-dir=$DATA_DIR")
    if [ -n "$PORT" ]; then
        SERVER_ARGS+=("--port=$PORT")
    fi
    if [ -n "$STDWIRE_PORT" ]; then
        SERVER_ARGS+=("--stdwire-port=$STDWIRE_PORT")
    fi
    if [ -n "$STRATOS_REPLICATION_PORT" ]; then
        SERVER_ARGS+=("--replication-port=$STRATOS_REPLICATION_PORT")
    fi
    if [ -n "$STRATOS_REPLICA_OF" ]; then
        SERVER_ARGS+=("--replica-of=$STRATOS_REPLICA_OF")
    fi
    if [ "$STRATOS_CLUSTER_MODE" = "true" ]; then
        SERVER_ARGS+=("--cluster")
    fi

    echo "================================================================="
    echo " Starting StratosDB 1.0 Production Engine"
    echo " Data Directory:      $DATA_DIR"
    echo " Wire Protocol Port:  $STDWIRE_PORT (PostgreSQL v3 wire compatibility)"
    echo " Native SWP Port:     $PORT (Stratos Wire Protocol v1.0)"
    echo " JVM Options:         $JAVA_OPTS"
    echo "================================================================="

    exec java $JAVA_OPTS -jar "/opt/stratosdb/lib/stratosdb-server.jar" "${SERVER_ARGS[@]}"

elif [ "$1" = "stdsql" ]; then
    shift
    exec java -jar "/opt/stratosdb/lib/stratosdb-cli.jar" "$@"

elif [ "$1" = "stratosdump" ]; then
    shift
    exec java -cp "/opt/stratosdb/lib/stratosdb-cli.jar" com.stratosdb.cli.StratosDump "$@"

elif [ "$1" = "stratos-ha" ]; then
    shift
    exec java -cp "/opt/stratosdb/lib/stratosdb-cli.jar" com.stratosdb.cli.StratosHa "$@"

elif [ "$1" = "stratos-backup" ] || [ "$1" = "std-basebackup" ]; then
    shift
    exec java -cp "/opt/stratosdb/lib/stratosdb-cli.jar" com.stratosdb.cli.StdBaseBackup "$@"

else
    # Execute arbitrary command passed to docker run
    exec "$@"
fi
