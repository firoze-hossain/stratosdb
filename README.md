# StratosDB 1.0

[![Release](https://img.shields.io/badge/Release-1.0.0--GA-success.svg)](https://github.com/firoze-hossain/stratosdb)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE)
[![Docker Image](https://img.shields.io/badge/Docker-stratosdb%3A1.0-blue.svg)](./docs/DOCKER_GUIDE.md)
[![Java Version](https://img.shields.io/badge/JDK-25-orange.svg)](https://openjdk.org/)
[![PostgreSQL Wire](https://img.shields.io/badge/pgwire-v3.0%20Compatible-brightgreen.svg)](./docs/CLIENT_CONNECTIVITY.md)

StratosDB is a distributed, high-performance hybrid relational database engine written in Java with native disk persistence, write-ahead logging (WAL) crash recovery, MVCC snapshot isolation, cost-based query optimization, Raft consensus high availability, and 100% transparent PostgreSQL wire protocol compatibility.

---

## Key Highlights & Production Capabilities

- **Storage Engine**: Slotted-page heap storage with direct I/O, LRU-K buffer pool eviction, and zero-allocation memory mapped pages.
- **ACID & MVCC**: Multi-version concurrency control (MVCC) with snapshot isolation, multi-statement transactions (`BEGIN`, `COMMIT`, `ROLLBACK`), savepoints, row-level locking, and persistent commit status logs.
- **Crash Recovery & ARIES WAL**: Full Write-Ahead Logging with write ring buffers; verified against real `SIGKILL` crash injection with 100% deterministic recovery.
- **Indexing**: High-concurrency B+Tree (borrowing, merging, root-collapse) and static Hash indexing.
- **Query Engine & Optimizer**: Cost-based optimizer with catalog statistics (`ANALYZE`), Hash Joins, Merge Joins, Nested Loops, Window Functions, and Common Table Expressions (CTEs).
- **Security & Authentication**: RFC 5802 / RFC 7677 `SCRAM-SHA-256` and `SCRAM-SHA-256-PLUS` channel binding (`tls-server-end-point`, `tls-unique`), TLS 1.3 encryption, hierarchical RBAC (`GRANT role TO user`), and Row-Level Security (RLS).
- **High Availability & Replication**: Physical streaming replication (TCP port 5434), automated networked base backup (`std-basebackup` / `BASE_BACKUP`), and distributed Raft consensus watchdog (`stratos-ha`) for automatic failover in < 3 seconds with zero split-brain risk.
- **Ecosystem Compatibility**: Certified with Spring Data JPA, Hibernate ORM 6.x, Prisma ORM, DBeaver, DataGrip, and official `psql`.
- **Driver Ecosystem**: Standalone pure-Java Type 4 JDBC driver (`com.stratosdb:stratosdb-jdbc:1.0.0`) with sources and javadoc published to Maven Central.
- **JVM Zero-Pause Architecture**: Powered by Generational ZGC (`-XX:+UseZGC`), achieving sub-1.5ms P99 latency under 10,000 write TPS.

---

## One-Click Docker Quickstart

Run a production-ready StratosDB instance with a single command:

```bash
docker run -d \
  --name stratosdb \
  -p 5432:5432 \
  -e STRATOS_DATABASE=stratos \
  -e STRATOS_USER=admin \
  -e STRATOS_PASSWORD=secret \
  stratosdb:1.0
```

Connect immediately with standard `psql`:
```bash
psql -h 127.0.0.1 -p 5432 -U admin -d stratos
```

```sql
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, created_at TIMESTAMP);
INSERT INTO users VALUES (1, 'Alice', CURRENT_TIMESTAMP);
SELECT * FROM users;
```

---

## High Availability Cluster (Docker Compose)

Deploy an automated 3-node HA cluster with primary, streaming replica, and Raft watchdog:

```bash
docker compose up -d
```

- Primary (Port `5432`): Read-write transactions.
- Replica (Port `5433`): Read-only scaling; bootstraps automatically via base backup.
- Watchdog: Raft consensus failover watchdog.

---

## Kubernetes Deployment (Helm Chart)

Deploy StratosDB on Kubernetes with automated replication and persistent volume claims:

```bash
helm install stratosdb deploy/helm/stratosdb \
  --set auth.database=mydb \
  --set auth.username=admin \
  --set auth.password=MySecretPassword \
  --set replica.replicaCount=2
```

See the [Helm Chart Documentation](./deploy/helm/stratosdb/README.md) for full configuration values.

---

## Client Connectivity & Drivers

### Pure-Java Type 4 JDBC Driver (`stratosdb-jdbc`)
```xml
<dependency>
    <groupId>com.stratosdb</groupId>
    <artifactId>stratosdb-jdbc</artifactId>
    <version>1.0.0</version>
</dependency>
```

```java
String url = "jdbc:stratos://localhost:5432/stratos";
try (Connection conn = DriverManager.getConnection(url, "admin", "secret")) {
    // Standard JDBC operations
}
```

### PostgreSQL Wire Compatible Drivers (Spring Boot, Prisma, DBeaver)
```bash
# Standard PostgreSQL JDBC
jdbc:postgresql://localhost:5432/stratos

# Prisma ORM (.env)
DATABASE_URL="postgresql://admin:secret@localhost:5432/stratos?schema=public"
```

For complete client configuration guides, see [Client Connectivity Guide](./docs/CLIENT_CONNECTIVITY.md).

---

## Documentation

- [Production Deployment Guide](./docs/PRODUCTION_DEPLOYMENT.md) - Kernel tuning (`sysctl`), Systemd unit, directory layout, memory limits.
- [High Availability & Replication Guide](./docs/HIGH_AVAILABILITY_GUIDE.md) - Streaming replication, base backup, Raft consensus watchdog.
- [Security & Authentication Guide](./docs/SECURITY_GUIDE.md) - SCRAM-SHA-256-PLUS channel binding, TLS 1.3, RBAC role hierarchies.
- [Client Connectivity Guide](./docs/CLIENT_CONNECTIVITY.md) - JDBC, Spring Data JPA, Hibernate ORM 6.x, Prisma, DBeaver, `psql`.
- [Docker & Containerization Guide](./docs/DOCKER_GUIDE.md) - Docker image, environment variables, multi-container compose.
- [Kubernetes Helm Chart Guide](./deploy/helm/stratosdb/README.md) - StatefulSets, persistent volumes, scaling, and rolling updates.
- [Local Development & Building](./RUNNING_LOCALLY.md) - Building from source, testing, and profiling.

---

## Architecture Modules

| Module | Responsibility |
| :--- | :--- |
| `stratosdb-common` | Core interfaces, primitives, and database exception types |
| `stratosdb-storage` | Slotted-page heap files, buffer pool, direct I/O, ARIES WAL, and checkpointing |
| `stratosdb-transaction` | MVCC snapshot isolation, commit log, 2PL row locks, and savepoints |
| `stratosdb-index` | B+Tree indexing with page split/merge, and static Hash indexes |
| `stratosdb-sql` | ANTLR4 SQL grammar, AST parser, physical query execution, and cost optimizer |
| `stratosdb-core` | Unified database lifecycle, schema catalogs, and database management |
| `stratosdb-network` | Native SWP protocol, PostgreSQL wire protocol v3 (`StdWireServer`), TLS, SCRAM auth, and streaming replication |
| `stratosdb-jdbc` | Pure-Java Type 4 JDBC driver implementation |
| `stratosdb-cli` | Native CLI shells (`stdsql`, `stratosdump`, `std-basebackup`, `stratos-ha`) |
| `stratosdb-benchmark` | TPC-C, Sysbench OLTP, and microbenchmarks |
| `stratosdb-testing` | Full end-to-end integration test suite |

---

## License

Licensed under the [Apache License, Version 2.0](./LICENSE).
