# StratosDB 1.0 Docker & Containerization Guide

StratosDB 1.0 provides production-ready, security-hardened Docker images (`stratosdb:1.0`, `stratosdb:latest`) built on OpenJDK 25 and configured with Generational ZGC.

---

## 1. Quickstart: One-Click Run

Run a standalone StratosDB 1.0 container:

```bash
docker run -d \
  --name stratos-standalone \
  -p 5432:5432 \
  -e STRATOS_DATABASE=mydb \
  -e STRATOS_USER=admin \
  -e STRATOS_PASSWORD=mysecretpassword \
  stratosdb:1.0
```

Verify container status and health:
```bash
docker ps -f name=stratos-standalone
```

Connect instantly with `psql` or `stdsql`:
```bash
psql -h 127.0.0.1 -p 5432 -U admin -d mydb
```

---

## 2. Production Container with Persistent Storage

In production environments, always mount a persistent host volume or managed container volume:

```bash
docker run -d \
  --name stratos-prod \
  -p 5432:5432 \
  -p 5431:5431 \
  -p 5434:5434 \
  -v /var/lib/stratosdb/data:/var/lib/stratosdb/data \
  -e STRATOS_DATABASE=production \
  -e STRATOS_USER=admin \
  -e STRATOS_PASSWORD=EnterprisePassword999 \
  -e STRATOS_JAVA_OPTS="-XX:+UseZGC -Xms4g -Xmx4g" \
  --restart unless-stopped \
  stratosdb:1.0
```

---

## 3. Environment Variable Reference

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `STRATOS_DATA_DIR` | `/var/lib/stratosdb/data` | Database storage directory inside container |
| `STRATOS_DATABASE` | `stratos` | Default database created on initialization |
| `STRATOS_USER` | `admin` | Superuser username |
| `STRATOS_PASSWORD` | `stratos_secret_password`| Superuser password |
| `STRATOS_PORT` | `5431` | Native binary SWP protocol port |
| `STRATOS_STDWIRE_PORT`| `5432` | PostgreSQL wire protocol compatibility port |
| `STRATOS_REPLICATION_PORT` | `5434` | Physical streaming replication port |
| `STRATOS_CLUSTER_MODE` | `standalone` | Node role: `standalone`, `primary`, or `replica` |
| `STRATOS_REPLICA_OF` | `""` | Target primary host:port (`hostname:5434`) for replica nodes |
| `STRATOS_JAVA_OPTS` | `-XX:+UseZGC -Xms2g -Xmx2g` | Custom JVM arguments |

---

## 4. Multi-Node High Availability Cluster with Docker Compose

StratosDB provides an automated 3-node HA cluster configuration with streaming replication and failover watchdog in `docker-compose.yml`:

```bash
# Launch Primary, Streaming Replica, and Raft Watchdog
docker compose up -d

# Check cluster status
docker compose ps
```

### Services Deployed:
- `stratos-primary`:
  - Port `5432` (read-write applications)
  - Port `5434` (internal replication endpoint)
- `stratos-replica`:
  - Port `5433` (read-only queries)
  - Automatically bootstraps via `std-basebackup` and streams WAL from primary.
- `stratos-watchdog`:
  - Runs `stratos-ha` consensus watchdog monitoring primary health every 1000ms.

---

## 5. Executing Utility Tools inside Container

The container includes all official StratosDB command-line tools:

```bash
# 1. Interactive SQL terminal
docker exec -it stratos-standalone stdsql -p 5432 -U admin -d mydb

# 2. Logical database dump
docker exec stratos-standalone stratosdump -p 5432 -U admin -d mydb > backup.sql

# 3. Base backup client
docker exec stratos-standalone std-basebackup -p 5434 -U admin -D /var/lib/stratosdb/backup
```
