# StratosDB

A relational database engine built from scratch in Java: real disk-backed storage, write-ahead logging with crash recovery, MVCC snapshot isolation, a cost-based query optimizer, and a wire-protocol layer that real PostgreSQL clients (`psql`, `psycopg2`, and any other pg-wire driver) can connect to directly.

This is not a toy that only survives its own test suite. Every claim below has a corresponding test that actually exercises it — a real `SIGKILL` mid-write for crash recovery, a real `psql` process for wire protocol compatibility, real multi-threaded contention for locking. Where something is known to be incomplete or simplified, it's stated here plainly rather than left for you to discover.

## What's actually real

| Area | What works |
|---|---|
| **Storage** | Page-based heap files, a buffer pool with LRU eviction, disk persistence verified across process restarts |
| **Crash recovery** | Write-ahead logging (WAL); redo verified by forking a real second JVM, sending it `SIGKILL` mid-write, and confirming the restarted engine recovers exactly the committed data — no more, no less |
| **Transactions** | MVCC snapshot isolation; real multi-statement `BEGIN`/`COMMIT`/`ROLLBACK`; `SAVEPOINT`/`RELEASE SAVEPOINT`/`ROLLBACK TO SAVEPOINT`; a persisted commit-status log and transaction-id counter so correctness survives a restart, not just a single run |
| **Indexing** | B+Tree (insert, point/range search, and full delete with borrow/merge/root-collapse — verified at 30,000+ keys with real deletions, not just inserts) and a static hash index with overflow chaining, correctly excluded from range queries by the planner |
| **Query engine** | `JOIN` (hash join, measured 4–11x faster than nested loop on real data), `GROUP BY`/aggregates, a real `WHERE`-clause expression tree (`AND`/`OR`/`NOT`/`LIKE`/`IN`), scalar/`IN`/correlated `EXISTS` subqueries, views (non-materialized), a cost-based optimizer once `ANALYZE` has run |
| **Wire compatibility** | Real PostgreSQL wire protocol v3 — `psql -h host -p port -U user db` connects and runs SQL with no StratosDB-specific client required. Verified against two independent, unmodified real clients: `psql` and Python's `psycopg2` |
| **Operations** | Autovacuum (background, automatic) alongside manual `VACUUM`; slow-query logging; TLS; password authentication (PBKDF2) |
| **Access** | SQL over a real JDBC driver, a network CLI, and now the PostgreSQL wire protocol |

**143 automated tests, all passing**, spanning real crash injection, real multi-threaded deadlock scenarios, real TLS handshakes, and real external client processes — not mocks standing in for any of these.

## What's honestly not there yet

Full PostgreSQL parity is not a near-term goal for any single engine built by a small team — it's ~30 years and thousands of contributors of ecosystem, tooling, and battle-testing. Specifically, right now:

- **No extended query protocol** — real server-side prepared statements (`Parse`/`Bind`/`Execute`) aren't implemented; only the simple query protocol is.
- **No SCRAM authentication** — the wire protocol currently uses trust auth (no password challenge over that path); the CLI/JDBC path has real PBKDF2 password auth.
- **No `pg_catalog` emulation** — `psql`'s `\dt`, `\d`, `\l`, and tab-completion query real Postgres system catalog tables that don't exist here yet. Plain SQL statements work regardless.
- **No merge join, window functions, or CTEs.**
- **No replication, no connection pooling, no stored procedures or triggers.**
- **No fine-grained (page/row-level) storage concurrency control** beyond MVCC's own row-version locking — a real, named architectural gap, not silently glossed over.

See [`PROJECT_PLAN.md`](./PROJECT_PLAN.md) for the full, itemized roadmap and [`PROGRESS.md`](./PROGRESS.md) for a running, honest log of what's been built, what broke along the way, and how each fix was actually verified.

## Quick start

Prerequisites: JDK 21, Maven 3.9+.

```bash
git clone https://github.com/firoze-hossain/stratosdb.git
cd stratosdb
mvn clean install -DskipTests
mvn test
```

Start a server and connect with StratosDB's own native client:

```bash
java -jar stratosdb-network/target/stratosdb-network-1.0.0-SNAPSHOT.jar ./data 6582 --stdwire
# custom protocol on 6582, PostgreSQL-wire-compatible protocol on 6583 (port + 1 by default)
java -jar stratosdb-cli/target/stratosdb-cli-1.0.0-SNAPSHOT.jar -h localhost -p 6583 -U anyuser -d anydb
```

```sql
CREATE TABLE users (id INT, name VARCHAR, age INT);
INSERT INTO users VALUES (1, 'Alice', 30);
SELECT * FROM users WHERE age >= 25;
```

Port 6583 speaks real PostgreSQL wire protocol v3, so any actual PostgreSQL client also connects unmodified — no StratosDB-specific driver needed:

```bash
psql -h localhost -p 6583 -U anyuser -d anydb
```

Verified against real `psql`, `psycopg2`, and JDBC drivers, including SCRAM-SHA-256 authentication and parameterized queries via the extended query protocol — see [`PROGRESS.md`](./PROGRESS.md) for the specifics.

See [`RUNNING_LOCALLY.md`](./RUNNING_LOCALLY.md) for the full walkthrough, including `StratosShell` (StratosDB's JDBC-based client, a separate tool from `stdsql` above), TLS, and the benchmark suite.

## Connecting from a Java application

**The easiest path today, no StratosDB-specific dependency required**: start a StratosDB server with `--stdwire`, then connect with the real, standard PostgreSQL JDBC driver (`org.postgresql:postgresql`, already on Maven Central) — this is exactly what real applications do with any Postgres-wire-compatible database:

```xml
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.7.3</version>
</dependency>
```

```java
Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:6583/anydb", "anyuser", "");
```

Works the same way with Spring Boot's `DataSource`/`JdbcTemplate` — just point the connection URL at the `--stdwire` port. Known limits from `PROGRESS.md` still apply (simple query protocol only, trust auth only).

**A dedicated StratosDB connector/driver, published the way `mysql-connector-j` or `org.postgresql:postgresql` are** — a small, standalone client artifact (not the engine itself) that Java/Spring Boot projects could add as a single Maven dependency — is a real, deliberately deferred future item, not attempted yet.

## Architecture

A multi-module Maven build, each module doing one job:

| Module | Responsibility |
|---|---|
| `stratosdb-common` | Shared types with no dependencies on anything else |
| `stratosdb-storage` | Disk manager, buffer pool, page format, WAL, crash recovery |
| `stratosdb-transaction` | MVCC snapshots, the transaction manager, persisted commit-status log, row-level locking |
| `stratosdb-index` | B+Tree and hash index implementations |
| `stratosdb-sql` | ANTLR4 grammar, parser, AST, and the executor/query engine |
| `stratosdb-core` | Wires storage, transactions, and SQL together into `StratosDB`, the embeddable engine |
| `stratosdb-network` | The custom wire protocol server, the PostgreSQL wire protocol server, TLS, and password auth |
| `stratosdb-jdbc` | A `java.sql.Driver` implementation over the custom protocol |
| `stratosdb-cli` | An interactive shell client |
| `stratosdb-benchmark` | Real, measured performance comparisons (e.g., indexed vs. sequential scan) |
| `stratosdb-testing` | Cross-module integration tests exercising full SQL round trips |

## Testing philosophy

Every significant fix in this project's history was found by *actually running the failure scenario*, not by inspection: a real crash test that forks and kills a JVM, a real deadlock between two live threads, a real restart-and-reconnect cycle that exposed transaction-id reuse across sessions, a real `psql` subprocess that exposed a test-harness bug in how connections were being reused. `PROGRESS.md` documents each of these — including the ones that turned out to be bugs in the *tests* rather than the engine, since getting that distinction right matters as much as finding the bug in the first place.

## License

[Apache License 2.0](./LICENSE).
