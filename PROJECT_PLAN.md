# StratosDB — Project Plan

This merges the planning documents that existed for this project and reconciles them against what's actually true of the codebase today:

1. **The 4-week foundation roadmap** — written after actually auditing the repo, scoped to what a single developer (plus AI-assisted pair programming) can realistically build and verify in a month. Now complete — see Part 1.
2. **The "DeepSeek" project plan** — a much broader, longer-horizon feature catalog (18 months, a 6-person team, an $842K budget) aimed at eventual feature parity with PostgreSQL.

Both are useful for different things. Document 2's feature enumeration (MVCC, JOINs, indexing, wire protocol, replication, extensibility...) is a genuinely reasonable map of *what a serious relational database needs*, and it's kept below, reorganized as the long-term vision. Its team size, budget, and month-by-month dates are not adopted — they describe a fully-staffed commercial effort, not what's actually happening here, and repeating them would just be decoration. Its "current status" claims (10,000+ lines, MVCC done, various things "✅ COMPLETED") were also not adopted where they didn't match the real repo — see `PROGRESS.md` for what's actually verified.

Part 2 was extended to directly address the actual goal behind both source documents: building something as professional as PostgreSQL, or better than it. That phrase is treated with the same honesty standard as everything else here — see "Read this before the task list," below, for what it does and doesn't mean.

**For live, checkmarked status, see `PROGRESS.md`. This document is the map; that one is the odometer.**

---

## Part 1: The Foundation (weeks, not months — complete)

Goal, stated once so every later phase can be measured against it:

> A single-node relational engine that is **correct under crash and concurrency**, supports real SQL (joins, indexes, transactions with isolation), and is reachable over the network from a JDBC client — with tests and benchmarks proving the claims, not just asserting them.

### Week 1 — Storage durability ✅
Real WAL redo, real crash-recovery testing (kill a forked JVM mid-batch, restart, verify), fixed buffer-pool/heap-table bugs that caused silent data loss. Done — see PROGRESS.md.

### Week 2 — Transactions that actually isolate ✅
MVCC with snapshot isolation (xmin/xmax, Postgres-style visibility rules), a lock manager with deadlock detection, `UPDATE`/`DELETE` actually implemented (they were no-op stubs). Done — see PROGRESS.md.

### Week 3 — SQL engine and indexing ✅ complete
- B+Tree index: **done** — disk-backed, real node splitting, tested at 250k-key scale.
- `CREATE INDEX` + index-scan-vs-seq-scan planner choice in the executor: **done** — see PROGRESS.md.
- `EXPLAIN`-style output: **done** — including a description of the join shape for joined queries.
- JOIN support (nested loop minimum): **done** — see PROGRESS.md for the test coverage and known limitations (no index-accelerated joins yet).
- Benchmark (indexed point lookup vs. full scan, 100k+ rows): **done** — 97.5x speedup measured for real, see PROGRESS.md.

### Week 4 — Reachable, secure, provable ✅ complete
- Wire protocol + socket server: **done** — `stratosdb-network`, virtual-thread-per-connection, tested over real sockets.
- Minimal JDBC driver: **done** — `stratosdb-jdbc`, verified through `java.sql.DriverManager`. See PROGRESS.md for what's real vs. stubbed (dynamic-proxy fallback throws `SQLFeatureNotSupportedException` for the large parts of `Connection`/`Statement`/`ResultSet` not implemented).
- CLI shell talking over that protocol: **done** — verified end-to-end against a real, separate server process.
- Auth: **done** — real PBKDF2 salted password hashing, a mandatory handshake on every connection.
- TLS: **done** on the server side (a real certificate-backed `SSLContext`); client-side certificate verification is honestly not implemented yet (trust-all only) - see PROGRESS.md for exactly what that does and doesn't protect against.
- Real throughput/latency benchmark numbers: **done** in Week 3 (97.5x indexed speedup, measured).

**All four weeks of the Foundation are complete.** Explicitly out of scope for this phase, and why that's fine: cost-based query optimization, replication, full vacuum/autovacuum, extensions, stored procedures, triggers, partitioning, client-side TLS certificate verification. Each of these is itself a multi-month-to-multi-year undertaking in Postgres's own history (or, for client TLS verification, a genuinely important but separable next increment). A month-one engine is the correct base to eventually build them on, not a substitute for them.

---

## Part 2: Toward professional-grade — what matching, and in specific places exceeding, PostgreSQL actually requires

### Read this before the task list

PostgreSQL is the product of roughly 30 years and thousands of contributors, with a massive surrounding ecosystem (extensions, hosting providers, tooling, and decades of production battle-testing across nearly every workload imaginable). Matching it feature-for-feature is not something a solo-developer-plus-AI effort should claim a timeline for — that's a difference in *kind* of effort, not just amount, and no roadmap changes that fact. Saying so plainly here is the same standard this plan has held to since Part 1: real claims, backed by what's actually true, not aspirational framing.

"Better than PostgreSQL" is not one claim — it's several, and they don't all move together. Here's where a from-scratch, modern, pure-Java engine could plausibly have a *real, durable* edge, assessed honestly:

- **Embeddability.** PostgreSQL requires a separate server process, always. StratosDB already runs in-process as a library (Part 1's whole embeddable-core design). For embedded, edge, or test-fixture use cases, this is a structural advantage that doesn't erode over time — Postgres would have to be redesigned to remove it.
- **Zero native dependencies.** Pure JVM bytecode, no `libpq`, no platform-specific builds, runs anywhere a JVM runs. Also durable.
- **A smaller, more legible codebase.** Real LOC counts are in `PROGRESS.md` — StratosDB's entire engine is a few thousand lines; Postgres's is well over a million. Easier to read, audit, and modify for a specific purpose. Genuinely valuable for teaching, research, or forking into a specialized variant. **Not** an advantage for raw performance, feature breadth, or robustness under load — those favor decades of tuning, not a smaller codebase.

And where StratosDB is honestly not going to be "better" any time soon, stated as plainly as the rest of this plan: query optimizer sophistication (decades of tuning against real workloads), the extension ecosystem (PostGIS, `pg_vector`, hundreds of others), replication and HA maturity, raw throughput at scale, and — perhaps most importantly — the sheer volume of production battle-testing that surfaces edge cases no test suite invents on its own.

Given that, the real goal for Part 2 is: close the feature gaps that matter most for actual usage, in priority order, each backed by a real test — the exact standard Part 1 was held to throughout. "Better than PostgreSQL" is realistically an aspiration for a narrow, specific set of properties (embeddability, portability, code legibility), not a blanket claim, and everything below is honest about that split.

### Feature-parity scorecard (checked against the actual repo, not aspirational)

| Capability | StratosDB today | PostgreSQL | Gap |
|---|---|---|---|
| Storage engine, crash recovery | Real WAL redo (tested via an actual `SIGKILL`), real vacuum with a proven cross-transaction safety property | Real, decades-hardened | Moderate — newer and far less battle-tested, not fundamentally different in design |
| Transactions/isolation | Snapshot isolation (MVCC), real multi-statement transactions (`BEGIN`/`COMMIT`/`ROLLBACK`, transaction poisoning on error, proven atomic under an actual crash), no savepoints, no 2PC | Full isolation levels, savepoints, 2PC, prepared transactions | Moderate |
| Indexing | B+Tree with real delete (borrow/merge/root-collapse); real hash index (static, overflow chaining) for equality workloads; no index-accelerated joins | B-Tree, Hash, GiST, GIN, BRIN, SP-GiST | Moderate |
| Query optimizer | Cost-based for scan choice (seq vs. index) when ANALYZE has run, uniform-distribution assumption; join strategy still unconditional (always hash join) | Cost-based, statistics-driven, histograms/MCV lists | Moderate |
| JOINs | Hash join (equality only); no merge join, no join reordering | Nested loop, hash join, merge join, join reordering | Moderate |
| Aggregation | `GROUP BY`/`HAVING`/`COUNT`/`SUM`/`AVG`/`MIN`/`MAX` done; no window functions, no CTEs | Full | Moderate |
| Subqueries | Scalar, `IN`/`NOT IN`, `EXISTS`/`NOT EXISTS` (correlated) done; no CTEs, no recursive queries | Full (scalar, correlated, `EXISTS`, CTEs, recursive) | Moderate |
| Wire protocol | Custom, not pg-wire compatible | The format every pg client/tool already speaks | Large (ecosystem, not just code) |
| Replication | None | Streaming, logical, synchronous options | Large |
| Extensibility | Views (non-materialized); no triggers, no stored procs | Views, triggers, PL/pgSQL, hundreds of extensions | Large |
| Vacuum | **None** — dead tuples from UPDATE/DELETE accumulate forever | Full, with autovacuum | Large, and worth prioritizing before it's actually painful |
| Auth | Password (real PBKDF2), no SCRAM, no roles/grants | SCRAM-SHA-256, certs, LDAP, roles, fine-grained grants | Large |
| Monitoring | Internal-only (`\status` in the CLI) | `pg_stat_*`, extensive | Large |

Reading this honestly: the gap is large in most rows. That's not a discouraging thing to write down — it's what makes the rest of this plan real instead of a vague aspiration.

### Priority framework

There's no team to divide across streams, so priority replaces scheduling — what to build next, based on (a) how much real usage it unlocks per unit of effort, and (b) what it's a prerequisite for. No calendar dates, for the same reason Part 1 never used them: a solo/AI-paced date for work this size would be fiction the moment it was written.

- 🔴 **Critical** — blocks real usage, or blocks several later items.
- 🟡 **High** — meaningfully expands what the engine can actually do today.
- 🟢 **Medium** — valuable, not urgent.
- ⚪ **Long-horizon** — genuinely multi-year even for a funded team; named for honesty, not committed to.

### Phase A — Harden and complete the storage layer 🔴

| Task | Priority | Depends on | Notes |
|---|---|---|---|
| B+Tree delete | ✅ done | none | Full standard algorithm (borrow from sibling, merge, root collapse) - see PROGRESS.md for the real scale verification (30k insert / 40% delete, point search + full range scan both checked) and the SQL-layer wiring (DELETE/UPDATE now genuinely remove the old index entry). Known limitation: a merge orphans a page rather than reclaiming it - no free-space reuse yet. |
| Vacuum | ✅ done | none | Real reclamation (mark-then-compact via SlottedPage.defragment, previously a stub), gated on TransactionManager.getOldestActiveXid so an active older transaction's snapshot is never broken - proven directly with real, separately-controlled transactions, not simulated. See PROGRESS.md for two real bugs found and fixed while building this (a slot-wasting updateTuple, and a page-count check that silently scanned zero pages). Known limitation: manual only, no autovacuum background process yet (Phase E). |
| Visibility map | 🟡 | none | A distinct thing from vacuum, above, though related: a per-page summary of "every tuple here is known-visible to everyone" that Phase C's index-only scans need to confirm visibility without touching the heap at all. Vacuum reclaiming dead tuples doesn't produce this by itself - it's a separate structure to build. |
| Free-space map | 🟡 | none | `HeapTable.insert()` currently scans every existing page looking for room — O(pages) per insert. A free-space map makes this O(1) amortized. |
| Real prepared statements (wire-level) | 🟡 | none | `StratosConnection.prepareStatement()` currently throws `SQLFeatureNotSupportedException`. Needs an actual `PARSE`/`BIND`/`EXECUTE` split in the wire protocol — not client-side string substitution, which would just move the SQL-injection-shaped risk around rather than removing it. |
| Savepoints | 🟢 | none (multi-statement transactions, Phase D, are done) | Nested rollback points only make sense once transactions aren't purely auto-commit - that's no longer the blocker it was. |
| TOAST-style large-value storage | 🟢 | none | Not urgent until something bigger than ~8KB actually needs to be stored. |

### Phase B — Query engine depth 🔴

The largest single gap versus PostgreSQL, and the highest-leverage phase for "feels like a real database" rather than "a working prototype."

| Task | Priority | Depends on | Notes |
|---|---|---|---|
| Statistics collection (`ANALYZE`-equivalent) | ✅ done | none | Row counts and per-column distinct-value/min/max estimates - see PROGRESS.md. In-memory only, no auto-refresh on writes (that's autovacuum's job, Phase E) - can go stale exactly like Postgres's own statistics without a periodic ANALYZE. |
| Cost-based optimizer | ✅ done | Statistics | Replaced the rule-based planner with real cost comparison (seq scan vs. index scan), falling back to the old rule-based heuristic when no statistics exist. Demonstrated with real data switching plans correctly in both directions - see PROGRESS.md. Known limitation: assumes uniform value distribution (no histogram/MCV tracking like real Postgres), and doesn't yet inform join-strategy choice (hash join is still unconditional, not compared against alternatives). |
| `GROUP BY` / `HAVING` / aggregates (`SUM`/`COUNT`/`AVG`/`MIN`/`MAX`) | ✅ done | none | Was completely absent, now real - see PROGRESS.md. Known gap: doesn't combine with JOIN yet (a query using both silently skips grouping rather than erroring - a named follow-up). |
| Hash join | ✅ done | none | Replaced nested-loop as the sole join algorithm - see PROGRESS.md for real measured numbers (4.4x-10.9x, widening with scale, exactly as O(n·m) vs O(n+m) predicts). Not yet a genuine cost-based *choice* between strategies (there's still no statistics to base one on) - it's "always hash join," which is correct since every JOIN this grammar supports is equality-only. |
| Subqueries (scalar, `EXISTS`, `IN`) | ✅ done | none | Scalar comparison, `IN`/`NOT IN`, `EXISTS`/`NOT EXISTS` including correlated `EXISTS` - see PROGRESS.md. Built on top of a real WHERE-clause expression tree that replaced the previous raw-text design (which had silently broken `AND`/`OR`/`NOT`/`LIKE`/`IN` - a real bug found and fixed along the way, not a hypothetical one). Known limit: correlation isn't threaded through a subquery that itself contains a JOIN or GROUP BY. |
| Merge join | 🟡 | none | Valuable for pre-sorted inputs specifically. The cost-based optimizer (done, above) only compares scan strategies so far, not join strategies - hash join is still chosen unconditionally for every equality join. Adding merge join as a real alternative needs that comparison extended to joins too, not just scans. |
| Window functions | 🟢 | Aggregates | `ROW_NUMBER`, `RANK`, etc. — real, but less broadly used than basic aggregation. |
| CTEs, `WITH RECURSIVE` | 🟢 | none (subqueries, above, are done) | Recursive CTEs are a genuinely different execution model (iterate to a fixpoint), not just more grammar. |

### Phase C — More indexing 🟡

| Task | Priority | Depends on | Notes |
|---|---|---|---|
| Hash index | ✅ done | none | Real disk-backed static hashing with overflow chaining, built on the same Page/BufferPool infrastructure as BTreeIndex (reuses BTreePage's leaf layout as the bucket format). A shared `KeyValueIndex` interface lets the planner treat it and BTreeIndex uniformly for equality lookups, preferring hash when both exist on a column. Verified at 100k-key scale forcing real overflow chains - see PROGRESS.md. Known limitation: static bucket count, no dynamic splitting as the index grows. |
| Index-only scans | 🟡 | Visibility map (Phase A) | Needs to confirm a tuple is visible without touching the heap at all — exactly what a visibility map provides. |
| Bitmap index | 🟢 | none | Narrower win, specifically for low-cardinality columns. |
| GiST/GIN-equivalents | ⚪ | Extensibility (Phase E) | In Postgres these exist largely to support extensions (full-text search, geometric types, `pg_trgm`) — not meaningful here without something to index that B+Tree/hash don't already cover. |

### Phase D — Networking, protocol compatibility, and real transactions 🟡

| Task | Priority | Depends on | Notes |
|---|---|---|---|
| Multi-statement transactions over the wire (`BEGIN`/`COMMIT`/`ROLLBACK` as real protocol operations) | ✅ done | none | `Connection.setAutoCommit(false)` no longer throws - real `BEGIN`/`COMMIT`/`ROLLBACK`, transaction poisoning on a failed statement (matching Postgres), and a real crash-recovery test proving atomicity via an actual `SIGKILL` mid-transaction. See PROGRESS.md - including an honest note that the SQL-engine core of this predated this round and was found already built but undocumented; this round verified it, fixed the JDBC driver never using it, and fixed a real gap where its crash test existed but was silently excluded from every test run. |
| PostgreSQL wire protocol compatibility | 🟡 | none (multi-statement transactions, above, are done) | The single highest-leverage *ecosystem* item on this whole list: enough of libpq's wire format for `psql`, pgAdmin, existing PostgreSQL JDBC/ODBC/psycopg drivers, and BI tools to connect without any client-side changes. Doesn't require replicating Postgres's SQL dialect exactly — just its wire framing and connection/auth negotiation. Large, but far smaller than reimplementing Postgres itself, and it's the difference between "a database with its own driver" (Part 1's current state) and "a database the entire Postgres tooling ecosystem already knows how to talk to." |
| SCRAM-SHA-256 auth | 🟢 | PostgreSQL wire compatibility (SCRAM is specifically what pg-wire clients negotiate) | Part 1's PBKDF2 password auth is real and salted, but SCRAM is the actual mechanism needed for genuine protocol compatibility, not just "more secure." |
| Client-side TLS certificate verification | 🟡 | none | Named as an explicit, current gap in `PROGRESS.md` — the JDBC driver trusts any server certificate today. A real truststore (or certificate pinning) closes this. |
| Streaming replication (leader → follower) | 🟡 | none — the WAL redo mechanism from Week 1 is most of the mechanical prerequisite already | Largely "ship the WAL bytes elsewhere and replay them"; the hard part is failure handling (follower catch-up, leader failover), not the core replay logic, which already exists and is tested. |
| Connection pooling | 🟢 | none | Standard, well-understood pattern; low technical risk, real value once more than one client is hitting the server. |
| Logical replication | ⚪ | Streaming replication | Meaningfully harder — needs to decode the WAL into logical row changes rather than replay physical pages, a different representation than what the WAL currently stores. |

### Phase E — Extensibility & operations 🟢

| Task | Priority | Depends on | Notes |
|---|---|---|---|
| Views | ✅ done | none | Non-materialized - a stored query, re-run fresh on every SELECT, with the outer query's WHERE/projection/LIMIT applied to its rows. See PROGRESS.md for two real bugs found and fixed while building this (a view combined with JOIN/aggregate silently ignoring the join/aggregate instead of erroring, and a subquery failure silently treated as "no match" instead of surfacing). Known limit: no joining a view against a table, no aggregating over a view, no view-inside-a-subquery yet - all fail cleanly rather than doing something wrong. |
| Metrics (JMX or Prometheus-style) | 🟡 | none | Low technical risk, real operational value. StratosDB already tracks some internals (buffer pool hit ratio, WAL LSN — see the CLI's `\status`) that just aren't exposed in a monitoring-tool-friendly format yet. |
| Autovacuum (automatic, background) | 🟡 | Manual vacuum (Phase A) | Don't build the scheduler before the thing it schedules is correct. |
| Triggers | 🟢 | Views (similar plan-time hook infrastructure) | Needs a real event model (`BEFORE`/`AFTER` `INSERT`/`UPDATE`/`DELETE`) wired into the executor's write paths. |
| Slow-query logging | 🟢 | none | Cheap — the CLI already measures and shows per-statement duration; formalizing this as a threshold-based log is a small addition. |
| A minimal stored-procedure language | 🟢 | Aggregates/subqueries (Phase B) landing first | PL/pgSQL-equivalent is its own interpreted language — genuinely large, and not worth starting before basic SQL depth exists to call from within it. |

### Phase F — Scaling ⚪ (genuinely long-horizon, named for honesty)

- **Parallel query execution, table partitioning**: real and valuable, each independently a multi-month undertaking even with the cost-based optimizer (Phase B) already in place, since both need the optimizer to reason about them correctly to be worth having.
- **Sharding, columnar storage, vectorized execution**: multi-year even for funded teams with existing engines as precedent — Postgres's own columnar/vectorized story is still largely extension-based (Citus, `pg_columnar`) for exactly this reason. Named here as honest long-term context, not a plan.

---

### If forced to pick just one thing next

Five fronts have real progress now, all with measured/demonstrated proof rather than just claims (see PROGRESS.md for all of it): **Phase A's storage hardening** (B+Tree delete, then vacuum - a table under real UPDATE/DELETE traffic no longer accumulates stale index entries or dead heap space forever), **Phase B's query engine depth** (`GROUP BY`/aggregates, hash join, a genuinely cost-based scan planner, and a real WHERE-clause expression tree with subqueries - which along the way surfaced and fixed a real bug: `AND`/`OR`/`NOT`/`LIKE`/`IN` were accepted by the grammar but silently misevaluated), **Phase C's hash index** (real static hashing with overflow chaining, the planner correctly preferring it over B+Tree for equality and never attempting it for a range), **Phase D's multi-statement transactions** (`Connection.setAutoCommit(false)` no longer throws - real `BEGIN`/`COMMIT`/`ROLLBACK`, verified atomic under an actual crash), and **Phase E's views** (non-materialized, correctly rejecting the join/aggregate combinations they don't support instead of silently ignoring them - a real bug found and fixed while building it, alongside a second one where a failing subquery was silently treated as "no match" instead of surfacing an error). A database with correct MVCC but no way to group two statements into one transaction, an index that could insert but never delete, dead tuples that could never be reclaimed, no `GROUP BY`, every join a nested loop, a planner that only ever asked "does an index exist," only one index type available regardless of workload shape, a WHERE clause that quietly ignored AND, and no way to name a reusable query didn't feel like PostgreSQL; one where all of that is real does — even with plenty of gaps still open elsewhere.

What's left in Phase A: a free-space map (inserts still scan every existing page for room - an efficiency gap, not a correctness one), autovacuum (automating what now works manually), and savepoints (now unblocked - nested rollback points only make sense once transactions aren't purely auto-commit). What's left in Phase B: merge join, window functions, and CTEs. What's left in Phase C: index-only scans (blocked on a visibility map, which doesn't exist yet), bitmap index, GiST/GIN-equivalents. What's left in Phase D: PostgreSQL wire protocol compatibility (the single highest-leverage ecosystem item on the whole list, now unblocked), SCRAM auth, client-side TLS certificate verification, streaming replication, connection pooling. What's left in Phase E: metrics, autovacuum, triggers, slow-query logging, stored procedures.

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
- Networking/auth/TLS: tested over real sockets and, for TLS, a real certificate generated with the JDK's own `keytool` — not a mocked `SSLContext` (established in Week 4).
- The standard to hold every future addition to: **if a claim of correctness isn't backed by a test that was actually run, it isn't verified yet.**

What the same standard will actually require for Part 2's higher-risk items, named now rather than discovered later:
- **Vacuum**: needs a test that proves dead space is genuinely reclaimed (table size stops growing under a sustained update workload) *and* that visibility is preserved for any transaction with an older snapshot still open — getting the second part wrong is a correctness bug, not a performance one.
- **Cost-based optimizer**: needs tests that don't just check the final answer is right (a rule-based planner already gets that right) but that the *chosen plan* is the cheaper one under a known data distribution — otherwise a regression that silently reverts to always-seq-scan would pass every existing correctness test.
- **Replication**: needs real fault injection — kill the follower mid-stream, kill the leader mid-write, verify the follower catches up correctly rather than just testing the happy path where nothing ever fails.
- **PostgreSQL wire protocol compatibility**: the only real test is a genuine, unmodified PostgreSQL client (`psql` itself, or an existing `org.postgresql` JDBC driver) successfully connecting and running SQL — a custom test harness that only exercises what StratosDB's own code expects to receive would miss exactly the compatibility gaps that matter.

## References

- PostgreSQL internals documentation, "Database System Concepts" (Silberschatz et al.) — background for storage/transaction design decisions.
- "Query Optimization" chapters of the above, and the PostgreSQL source's own `optimizer/README` — the honest starting point for Phase B, since a cost-based optimizer isn't something to invent from scratch.
- ANTLR4 documentation — for grammar work in `stratosdb-sql`.
- PostgreSQL's wire protocol documentation (`protocol.sgml` in the Postgres source, or the rendered docs) — the actual spec Phase D's wire-compatibility item would need to implement against, not a secondhand description of it.
- `PROGRESS.md` in this repo — live, verified status tracker.
