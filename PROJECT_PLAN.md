# StratosDB — Project Plan

This merges two planning documents that existed for this project and reconciles them against what's actually true of the codebase today:

1. **The 4-week foundation roadmap** — written after actually auditing the repo, scoped to what a single developer (plus AI-assisted pair programming) can realistically build and verify in a month.
2. **The "DeepSeek" project plan** — a much broader, longer-horizon feature catalog (18 months, a 6-person team, an $842K budget) aimed at eventual feature parity with PostgreSQL.

Both are useful for different things. Document 2's feature enumeration (MVCC, JOINs, indexing, wire protocol, replication, extensibility...) is a genuinely reasonable map of *what a serious relational database needs*, and it's kept below, reorganized as the long-term vision. Its team size, budget, and month-by-month dates are not adopted — they describe a fully-staffed commercial effort, not what's actually happening here, and repeating them would just be decoration. Its "current status" claims (10,000+ lines, MVCC done, various things "✅ COMPLETED") were also not adopted where they didn't match the real repo — see `PROGRESS.md` for what's actually verified.

**For live, checkmarked status, see `PROGRESS.md`. This document is the map; that one is the odometer.**

---

## Part 1: The Foundation (weeks, not months — in progress now)

Goal, stated once so every later phase can be measured against it:

> A single-node relational engine that is **correct under crash and concurrency**, supports real SQL (joins, indexes, transactions with isolation), and is reachable over the network from a JDBC client — with tests and benchmarks proving the claims, not just asserting them.

### Week 1 — Storage durability ✅
Real WAL redo, real crash-recovery testing (kill a forked JVM mid-batch, restart, verify), fixed buffer-pool/heap-table bugs that caused silent data loss. Done — see PROGRESS.md.

### Week 2 — Transactions that actually isolate ✅
MVCC with snapshot isolation (xmin/xmax, Postgres-style visibility rules), a lock manager with deadlock detection, `UPDATE`/`DELETE` actually implemented (they were no-op stubs). Done — see PROGRESS.md.

### Week 3 — SQL engine and indexing 🟡 in progress
- B+Tree index: **done** — disk-backed, real node splitting, tested at 250k-key scale.
- `CREATE INDEX` + index-scan-vs-seq-scan planner choice in the executor: **done** — see PROGRESS.md.
- `EXPLAIN`-style output: **done**.
- JOIN support (nested loop minimum): **not done**.
- Benchmark (indexed point lookup vs. full scan, 100k+ rows): **not done**.

### Week 4 — Reachable, secure, provable 🔲 not started
- Wire protocol + socket server (`stratosdb-network` is currently empty).
- Minimal JDBC driver (`stratosdb-jdbc` is currently empty).
- CLI shell talking over that protocol (a basic in-process shell exists; it doesn't use the network layer because that layer doesn't exist yet).
- Auth (salted+hashed credentials) and TLS.
- Real throughput/latency benchmark numbers, honestly reported.

**Explicitly out of scope for the Foundation phase**, and why that's fine: cost-based query optimization, replication, full vacuum/autovacuum, extensions, stored procedures, triggers, partitioning. Each of these is itself a multi-month-to-multi-year undertaking in Postgres's own history. A month-one engine is the correct base to eventually build them on, not a substitute for them.

---

## Part 2: Long-term vision (beyond the Foundation — not scheduled yet)

This is the reorganized, re-scoped version of the DeepSeek plan's feature catalog. No dates are attached because solo/AI-assisted development timelines for work this size are genuinely uncertain — better to have an honest "not yet dated" than a confident month number that gets quietly ignored later. Ordered roughly by dependency (each phase leans on the one before it).

### Phase A — Harden what exists
- **Free space maps / visibility maps** for the heap, so inserts and scans stop being O(all pages) in the degenerate case.
- **TOAST-style large-object storage** for values that don't fit in a page.
- **Query plan caching** and **prepared statements** — meaningful once there's a planner worth caching the output of.
- **Savepoints** (nested rollback points) — a natural extension of the transaction work in Part 1.

### Phase B — SQL language depth
- **JOINs**: nested loop → hash join → merge join, roughly in that order of implementation effort vs. payoff.
- **Subqueries**: scalar, `EXISTS`, `IN`/`NOT IN`.
- **Aggregations**: `GROUP BY`, `HAVING`, `SUM`/`AVG`/`COUNT`/etc.
- **Window functions**, **CTEs**, eventually `WITH RECURSIVE`.
- **Cost-based optimization** — needs statistics collection (`ANALYZE`-equivalent) first; a rule-based optimizer (Part 1, Week 3) is the honest predecessor step, not this.

### Phase C — More indexing
- Hash index (for equality-only workloads where a B+Tree's ordering is wasted).
- Bitmap index (low-cardinality columns).
- Index-only scans (covering indexes) once the planner can reason about them.

### Phase D — Networking & drivers
- A real wire protocol. Cloning the shape of Postgres's simple-query protocol (rather than implementing full protocol compatibility) is the pragmatic version of this; full Postgres wire-protocol compatibility is a much larger, separate goal worth naming honestly if it's ever actually wanted.
- A more complete JDBC driver (Part 1, Week 4 builds the minimal version).
- SCRAM-style auth, TLS (Part 1, Week 4 builds the minimal version: salted+hashed credentials, basic TLS).
- Basic replication (single leader → follower) once the WAL is solid enough to stream.

### Phase E — Extensibility & operations
- Views, triggers, a minimal stored-procedure-like language.
- JMX/Prometheus-style metrics, slow-query logging.
- Autovacuum — needs the visibility map from Phase A first.

### Phase F — Scaling (genuinely long-horizon; sequence, not schedule)
- Parallel query execution.
- Table partitioning.
- Beyond that — sharding, columnar storage, vectorized execution — are multi-year undertakings even for funded teams with the original PostgreSQL/similar projects as precedent. Naming them here is honest long-term context, not a commitment.

---

## What was deliberately not carried over from the DeepSeek plan, and why

- **Team structure (Technical Lead, Storage Lead, SQL Lead, ...) and the $842K annual budget.** These describe a staffed effort. Repeating them here would misrepresent how this project is actually being built (one developer plus AI-assisted implementation) and wouldn't help track anything real.
- **Month-numbered milestones (M1 through M9, Month 2 through Month 20).** Without a team of the assumed size, these dates aren't load-bearing — keeping them would just create a roadmap that's wrong on day one.
- **Claims of current completion that didn't match the repo** (e.g., "10,000+ lines," "MVCC" listed as done pre-Week-2, WAL described as working before its recovery path was actually fixed). `PROGRESS.md` is the source of truth for what's actually built and tested; this plan doesn't restate unverified claims.
- **GitHub-stars/fork-count/contributor targets and a release-channel/deployment strategy** (Snapshot/Alpha/Beta/Stable channels, Docker/Kubernetes/cloud deployment options). These are real concerns for a project actively courting outside users and contributors; they're not useful to plan against before Part 1 is even finished, and can be revisited once there's something for a wider audience to actually run.

## Testing strategy (kept, because it's good practice regardless of team size)

- Storage engine, buffer pool, WAL: tested against real crash/restart scenarios, not just happy-path unit tests (established in Week 1 — see `CrashRecoveryTest`).
- Transactions/MVCC: tested with real concurrent transactions and a real induced deadlock, not mocked concurrency (established in Week 2).
- Indexing: tested at a scale that actually forces the interesting code paths (multi-level splits), not just a handful of keys (established in Week 3's B+Tree work).
- The standard to hold every future addition to: **if a claim of correctness isn't backed by a test that was actually run, it isn't verified yet.**

## References

- PostgreSQL internals documentation, "Database System Concepts" (Silberschatz et al.) — background for storage/transaction design decisions.
- ANTLR4 documentation — for grammar work in `stratosdb-sql`.
- `PROGRESS.md` in this repo — live, verified status tracker.
