# StratosDB 2.0 Enterprise Feature & Architecture Reference

StratosDB 2.0 is a hybrid relational database engine built on Java 25 with off-heap native memory (`Arena`), Generational ZGC, and dual PostgreSQL/MySQL wire compatibility.

## 1. Analytical & Query Engine
- **Volcano Iterator & Vectorized SIMD Execution**: Vectorized batch filtering and aggregation over 64-byte aligned native memory.
- **Cost-Based Optimizer (CBO)**: Histogram/hyperloglog cardinality estimation, DP join reordering, subquery unnesting & decorrelation.
- **Window Functions & Recursive CTEs**: Full `WITH RECURSIVE` evaluation with worktable queue, fixed-point termination, and cycle detection (`CYCLE ... SET`).

## 2. Enterprise Data Types & Indexing
- **Binary JSONB & JSONPath Engine**: Decomposed binary JSONB with header/offset tables, operators (`->`, `->>`, `@>`, `?`), and GIN indexing.
- **Enterprise Types**: Native 16-byte `UUID` (`gen_random_uuid()`), `INET`/`CIDR` subnet containment (`<<`, `>>`), and 1D typed arrays (`int[]`, `text[]`).
- **Index Suite**: Latch-crabbing B-Tree, Hash, BRIN, GIN, GiST, and Visibility-Map-accelerated Index-Only Scans.

## 3. High Availability & Disaster Recovery
- **Synchronous Streaming Replication**: Push-based TCP WAL streaming with `synchronous_commit = remote_apply` and quorum ACK wait (`RPO = 0`).
- **Embedded Raft Consensus**: 3-node and 5-node Raft cluster coordinator (`StdRaftCluster`) with time-bounded leader leases and sub-3-second automated failover.
- **Continuous Archiving & PITR**: Automated `WalArchiver` streaming `.walseg` segments to local directories or S3, plus microsecond-precision `PitrRestore`.

## 4. Enterprise Security & Auditing
- **Transparent Data Encryption (TDE)**: AES-256-GCM encryption for table pages, index pages, and WAL segments with PBKDF2 / external key file support.
- **Tamper-Evident Audit Logging & LDAP**: SHA-256 hash-chained `audit.log` for DDL, DCL, and failed logins; external LDAPv3 / Active Directory authentication.

## 5. Gateways, Multiplexer & JDBC Driver
- **PostgreSQL v3 Wire Protocol**: Port `5432` with SCRAM-SHA-256, extended query protocol, and TLS 1.3.
- **MySQL Protocol v10 Gateway**: Port `3306` supporting `caching_sha2_password`, `mysql_native_password`, and `COM_QUERY`.
- **NIO Connection Multiplexer**: Multi-reactor `Selector` pool decoupling client sockets from execution threads for 10,000+ concurrent connections.
- **Pure-Java Type 4 JDBC Driver**: `stratosdb-jdbc` (`jdbc:stratos://host:port/db`) with Java 25 module descriptor (`module-info.java`) and TLS support.

## 6. Verification & Release Engineering
- **Jepsen Chaos Harness**: Automated 3-node partition, packet loss, clock skew, and `SIGKILL` (`kill -9`) verification during TPC-C workloads.
- **100k TPS Benchmark Suite**: Lock-free L1 buffer pool hot-cache, 64-byte cache-line padding, and automated SVG/HTML comparison charts vs PostgreSQL 17.
- **Multi-Arch Docker & Kubernetes Helm**: `linux/amd64` and `linux/arm64` containers and Helm chart (`deploy/helm/stratosdb`) bringing up a 3-node TLS-enabled Raft cluster in < 60s.
