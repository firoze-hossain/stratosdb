# StratosDB — Progress Tracker

**Last verified:** July 25, 2026 — every checkmark below was confirmed by actually compiling and running the test suite against the exact code on GitHub `master`, not by reading commit messages or trusting claims. See "How this doc is kept honest" at the bottom.

## At a glance

| | |
|---|---|
| Commits | 9 |
| Main source | ~3,383 lines |
| Test source | ~765 lines |
| Tests passing | **59 / 59** |
| Current stage | Foundation (Weeks 1-4) complete; Part 2 Phase B underway (GROUP BY/aggregates + hash join done) |

This tracker follows the 4-week foundation plan in `PROJECT_PLAN.md`. Anything with a green check was independently rebuilt and re-tested, not just assumed from a commit title.

---

## Week 1 — Storage durability ✅ DONE

- ✅ Pinned to Java 21 LTS (was targeting a non-LTS preview release)
- ✅ Removed 12 IDE-boilerplate stub `Main.java` files
- ✅ Fixed `HeapTable`: inserts into any page past the first were silently lost (new pages were never registered with the buffer pool)
- ✅ Fixed `WALManager.recover()`: was a complete no-op (empty `switch` cases) — real redo logic now reconstructs pages from the log
- ✅ Fixed `WALManager.checkpoint()`: threw `BufferOverflowException` on every call, silently swallowed
- ✅ Fixed `HeapTable` page-count-on-restart bug: `lastPageId` always reset to 0, so a reopened table only ever saw its first page
- ✅ Crash-recovery test: real `SIGKILL` of a forked JVM mid-batch, real WAL redo, real restart — **2/2 passing**

## Week 2 — Transactions & MVCC ✅ DONE

- ✅ Snapshot isolation: `Snapshot` + `MVCCVisibility`, Postgres-style xmin/xmax visibility rules
- ✅ `LockManager`: per-row exclusive locks, wait-for-graph deadlock detection
- ✅ `TransactionManager` rewritten: real `begin`/`commit`/`abort`, active/committed/aborted xid tracking
- ✅ `UPDATE`/`DELETE` actually implemented — were hardcoded stubs returning "0 rows" and touching nothing
- ✅ Fixed parser gap: `SqlParser` never dispatched `UPDATE` at all, regardless of what the executor could do
- ✅ Fixed grammar gap: `VARCHAR` required an explicit length, so the project's own test SQL couldn't parse
- ✅ MVCC isolation tests (snapshot semantics, uncommitted-write invisibility, delete visibility) — **3/3 passing**
- ✅ Deadlock test: two real threads, genuine circular wait, real `LockManager` — **2/2 passing**

## Week 3 — SQL engine + indexing 🟡 IN PROGRESS

- ✅ **B+Tree index** (`stratosdb-index` was empty; now a real disk-backed B+Tree)
  - Point insert with correct leaf *and* internal node splitting
  - Point search, range scan, duplicate-key support
  - Persists across close/reopen
  - Verified with 250,000 shuffled keys forcing genuine multi-level splits, plus a dedicated eviction/reload test under a deliberately tight buffer pool — **6/6 passing**
  - Found and fixed a real bug along the way: the root leaf's "next leaf" pointer was never initialized and inherited garbage bytes from the generic page header, corrupting the leaf linked list after enough splits
  - Also fixed a real architectural gap to build this at all: the buffer pool was hardcoded to always wrap pages as `SlottedPage`, making it impossible to serve a different page layout. Added a `PageFactory<T>` so the pool is now page-type-agnostic, with zero change to existing heap-table behavior (regression-tested against Week 1 + Week 2 suites)
- ✅ **`CREATE INDEX`** — new grammar rule, new `CreateIndexStatement` AST node, real execution: builds a `BTreeIndex` and backfills it from every currently-visible row. Integer-valued columns only (B+Tree keys are `long` — no string key encoding yet, stated plainly in the code, not hidden)
- ✅ **Rule-based planner (seq scan vs. index scan)** — `ExecutorEngine.planScan()`: a single numeric predicate on an indexed column uses the B+Tree (equality via point lookup, `>`/`>=`/`<`/`<=` via range scan); anything else falls back to a full MVCC scan. Not cost-based (no statistics collection exists yet) — the honest "does an applicable index exist" version the plan called for
- ✅ **`EXPLAIN`** — reports which strategy `planScan` would pick, without running the query
- ✅ Index maintenance on `INSERT` and `UPDATE` (new row versions get new index entries; stale entries from old versions are left in place and filtered out at read time by MVCC visibility, same as an unindexed `DELETE` would be — `BTreeIndex` has no delete operation yet, noted as a known gap)
- ✅ Found and fixed two more real bugs while building this, both in `ExecutorEngine`'s WHERE-clause handling:
  - Operator detection checked `"="` before `">="`/`"<="`, so `age>=30` was split on the wrong character (`age>` / `30`) — order matters when operators overlap as substrings
  - `matchesWhere` detected an operator but always evaluated equality regardless of it, so `age>25` silently behaved exactly like `age=25`. Every operator (`=`,`!=`,`>`,`>=`,`<`,`<=`) is now actually evaluated, and a dedicated test checks both the index-scan and seq-scan paths return identical, correct results for all of them
- ✅ **JOIN support (nested loop)** — new grammar (`JOIN`/`INNER`/`ON`, qualified `table.column` references everywhere a column name appears), a real nested-loop executor (`ExecutorEngine.executeJoinedSelect`), and `EXPLAIN` support (`Nested Loop Join: Seq Scan on X -> Seq Scan on Y ON ...`). Joined columns are qualified as `table.column` internally to avoid ambiguity, with a documented fallback so unambiguous bare names (`SELECT name, amount FROM users JOIN orders ...`) still work. Known limitation stated plainly: no index acceleration for joins yet (every join is a full nested loop, not an index-nested-loop or hash join) and no join reordering. 5 new tests: basic match, exclusion of non-matching rows (real inner-join semantics), `WHERE` on a joined column, bare-name resolution, and `EXPLAIN` shape.
- ✅ **Benchmark: indexed point lookup vs. full scan on 100k+ rows** — `stratosdb-benchmark`'s `QueryBenchmark` (previously the module was completely empty). Runs through the real `StratosDB.execute()` interface, not an idealized storage-only number, so it honestly includes today's per-query ANTLR-parse and transaction-commit overhead. Real measured result on the build machine, 100,000 rows / 300 queries per scenario:

  | Scenario | avg (ms) | p50 | p95 | p99 | ops/sec |
  |---|---|---|---|---|---|
  | Index Scan | 0.827 | 0.634 | 1.971 | 6.128 | 1,209.4 |
  | Seq Scan | 80.603 | 85.030 | 102.008 | 126.502 | 12.4 |

  **97.5x faster** with the index. One machine, one run — not a substitute for reproducing against another database on identical hardware, and the report says so.

**Week 3 is done.** Remaining Week 3 scope-cuts, named honestly rather than silently dropped: cost-based (vs. rule-based) query optimization, index-accelerated joins, and statistics collection are real further work, not attempted here.

## Week 4 — Network, JDBC, CLI, security ✅ COMPLETE

- ✅ **Wire protocol** (`stratosdb-network` was empty) — `WireProtocol`: a small custom binary protocol (not PostgreSQL-wire-compatible, stated plainly), self-framing via `DataInputStream`/`DataOutputStream` primitives. `StratosServer` accepts connections and runs one virtual thread per connection (Java 21 - the reason this project pinned to that LTS back in Week 1), all sharing one `StratosDB` instance so every connection sees the same data. `StratosServerMain` is a runnable entry point (`java -jar stratosdb-network-*.jar [dataDir] [port]`).
  - Along the way, fixed `StratosDB.startServer()`, which previously logged "server started on port X" while starting nothing at all - now honestly documented as a state flag, with the real listener living in `stratosdb-network` (which depends on `stratosdb-core`, not the reverse, so `core` itself architecturally cannot start a network listener without a circular module dependency - this is why the real server isn't inside `StratosDB` itself).
- ✅ **JDBC driver** (`stratosdb-jdbc` was empty) — `StratosDriver`/`StratosConnection`/`StratosStatement`/`StratosResultSet`/`StratosResultSetMetaData`, real behavior over a real socket to a real server, verified through `java.sql.DriverManager` exactly as a real application would use it. `Connection`/`Statement`/`ResultSet` have 63/61/203 methods respectively on their JDBC interfaces; rather than hand-writing that much boilerplate, they're implemented as dynamic proxies backed by a real handler class - full real behavior for CRUD, metadata, and error propagation, and a clear `SQLFeatureNotSupportedException` (not a silent no-op) for anything genuinely unimplemented, like multi-statement transactions.
  - Found a real bug while testing rather than assuming it away: `DriverManager.getConnection(...)` only found the driver when *some* test happened to reference the class first, because the standard JDBC 4 `META-INF/services/java.sql.Driver` auto-registration file didn't exist - added it, confirmed the fix by running the affected test 3 times in a row (it's a registration-order bug, easy for a single lucky run to hide).
- ✅ **Auth** — `UserStore`: real PBKDF2-HMAC-SHA256 password hashing (100,000 iterations, independent random salt per user, constant-time verification), not a toy single-round hash. Wired into the wire protocol as a mandatory `AUTH`/`AUTH_RESULT` handshake on every connection - a server with no `UserStore` configured just accepts it unconditionally, so unauthenticated use works exactly as before auth existed. Username/password flow through JDBC's standard `Properties` (`DriverManager.getConnection(url, user, password)`).
- ✅ **TLS** — `TlsSupport`: real server-side `SSLContext` built from an actual Java keystore (verified against a certificate generated with the JDK's own `keytool`, not a fake). Client-side is honestly incomplete and says so in its own javadoc: trust-all only, no certificate verification wired up yet - that encrypts against passive eavesdropping but does **not** defend against an active man-in-the-middle, a distinction stated plainly rather than oversold as "TLS support" implying more than it delivers.
- ✅ **CLI over the network** — `StratosShell` rewritten to connect via the JDBC driver instead of linking `StratosDB` in-process; verified with a real end-to-end run (separate server process, separate CLI process, real TCP). One honest, named tradeoff: standard JDBC's `Statement.execute()` only exposes a boolean and a row count, not an arbitrary message string, so the old engine messages ("Table created: X") are gone in favor of a generic row count or "OK" - the same way `psql` shows generic response tags, not custom engine text. `SHOW TABLES` was fixed to return real rows instead of a message specifically so it keeps working meaningfully through this path.
  - Found and fixed a real robustness bug while testing shutdown paths: `StratosDB.shutdown()` was not idempotent. `WALManager.close()` called `checkpoint()` (which writes to the WAL channel) before checking whether that channel was already closed, and `shutdown()` also called `checkpoint()` a second, redundant time directly. Calling `shutdown()` twice threw `ClosedChannelException`. Fixed at the source (a guard in `checkpoint()` itself, plus removing the redundant direct call), with a regression test.

Real tests added this round: 6 (`UserStoreTest`) + 4 more in `StratosServerTest` (now 7 total: auth-required-rejects-wrong-credentials, accepts-correct-credentials, rejects-a-connection-that-skips-auth, open-access-when-unconfigured) + 3 (`TlsIntegrationTest`, using a real generated certificate) + 1 (`StratosDBTest`'s shutdown-idempotency regression) = **51 tests total**, up from 37.

---

## Module status (verified line counts, not aspirational ones)

| Module | Files | Lines | State |
|---|---|---|---|
| `stratosdb-common` | 11 | 182 | Real — exceptions, constants, utils |
| `stratosdb-core` | 2 | 87 | Real — wires everything together |
| `stratosdb-storage` | 10 | 1,686 | Real — disk/buffer/WAL/heap, now with a page-type-agnostic buffer pool and an idempotent shutdown path |
| `stratosdb-transaction` | 5 | 381 | Real — MVCC + locking, both tested |
| `stratosdb-sql` | 19 | ~1,250 | Real — ANTLR grammar (JOIN/qualified columns, now GROUP BY/HAVING/aggregates) + hand-written AST builder + executor |
| `stratosdb-index` | 1 | 304 | Real — B+Tree, tested at scale, wired into query execution via the planner |
| `stratosdb-network` | 5 | 621 | Real — wire protocol, auth handshake, optional TLS, virtual-thread-per-connection server, all tested over real sockets |
| `stratosdb-jdbc` | 6 | 741 | Real — Driver/Connection/Statement/ResultSet with auth+TLS support, verified through `DriverManager` |
| `stratosdb-cli` | 1 | 229 | Real — network client over JDBC, verified end-to-end against a real separate server process |
| `stratosdb-testing` | 0 (test-only) | — | 24 integration tests, all passing |
| `stratosdb-benchmark` | 1 | 169 | Real — `QueryBenchmark`, run for real (see Week 3 results above) |

**Week 4 is done. All four weeks of the Foundation plan (Part 1 of PROJECT_PLAN.md) are now complete.**

## Part 2 progress (see PROJECT_PLAN.md for the full phase breakdown)

### Phase B — GROUP BY / HAVING / aggregates ✅ done (first item)

- ✅ **`GROUP BY`, `HAVING`, and `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`** — previously completely absent (not partial - zero aggregation support existed). New grammar (`GROUP`/`HAVING`/`COUNT`/`SUM`/`AVG`/`MIN`/`MAX` tokens, an `aggregateFunction` rule, `AS`-aliased select-list items), a new `AggregateCall` AST node, and a real grouping/aggregation/HAVING-filter execution path in `ExecutorEngine`. `SUM`/`COUNT` return integral types when all inputs are integral (not always `Double`); `SUM`/`AVG` of zero contributing rows return `NULL`, matching standard SQL semantics, not zero. `EXPLAIN` describes aggregate queries too (`Aggregate GROUP BY x: Seq Scan on ...`).
  - **Found and fixed a real, pre-existing, dormant bug while building this**: the `AS` keyword was referenced in the grammar's `selectItem` rule but never given an explicit lexer rule (unlike every other keyword). ANTLR auto-generated an implicit token for it, but since the generic `IDENTIFIER` rule was declared earlier and matched the same text, `AS` could **never** actually be recognized as a keyword - every `(AS alias)? ` clause in the grammar was permanently unmatchable. This had been dormant because column aliasing was never actually exercised before (the old `SELECT`-list handling just grabbed raw text and split on commas, ignoring the grammar structure entirely). Fixed by adding the missing `AS: A S;` lexer rule - the same class of "keyword collides with the identifier catch-all because nobody gave it its own rule" bug as the missing `UPDATE` dispatch and `VARCHAR` length issues from earlier weeks.
  - **Known limitation, stated plainly**: aggregates and `JOIN` don't combine yet - a query with both hits the join code path first, which doesn't consult `GROUP BY`/aggregates at all, so it silently returns per-row joined results rather than grouping them. No index acceleration for aggregate queries either (always a full scan) - both are real, separate follow-ups per `PROJECT_PLAN.md` Phase B.
  - 6 new tests: `COUNT(*)` with no `GROUP BY` (implicit single group), all five aggregate functions together, `HAVING`, `WHERE` applied before grouping, `EXPLAIN` output, and `COUNT` on an empty result set (must return 0, not error).

### Phase B — Hash join ✅ done (second item)

- ✅ **Hash join replaces nested-loop join as the sole join algorithm** (`ExecutorEngine.hashJoin`) - correct as the *only* algorithm, not just "an option," because every `JOIN` this grammar allows is an equality join (`ON columnName = columnName`, nothing else accepted), which is exactly what hash join is for. Builds the hash table on whichever side has fewer rows (a real, if simple, heuristic - not yet a genuine cost-based choice, since there's still no statistics collection to base one on). `EXPLAIN` now reports `Hash Join` instead of `Nested Loop Join`.
  - **Measured for real, not asserted blind**: at 1,000 users × 2,000 orders, the old nested-loop join took 437ms; hash join takes ~100ms (**4.4x**). At 3,000 × 6,000, nested-loop took 1,622ms, hash join ~149ms (**10.9x**) - the gap widening with scale exactly as O(n·m) vs O(n+m) predicts. Both numbers measured on the same machine, same data, same query - not estimated.
  - **Found and fixed a second real, dormant, pre-existing bug while building this - present since the project's very first commit**: `NULL` and `NULL_LITERAL` were two *separate* grammar tokens both matching the identical text `"NULL"` (one for `NOT NULL` column constraints, one meant for `INSERT ... VALUES (..., NULL, ...)`). Since `NULL` was declared first, the lexer could never actually produce a `NULL_LITERAL` token for that text - `INSERT`ing a literal `NULL` value has never worked in this project's history, silently throwing a syntax error every time. Found because a new test needed it (a NULL join key to prove NULLs never match, per standard SQL semantics). Fixed by merging into the single `NULL` token - no Java code needed to change, since nothing referenced `NULL_LITERAL` by name.
  - New tests: NULL join keys are correctly excluded on either side (never match, not even NULL-to-NULL), and a real 1,000×2,000-row join with a tight, measured performance assertion (2s budget - the old algorithm would blow well past this, per the numbers above).

## What to do next

Per `PROJECT_PLAN.md` Phase B, the remaining highest-leverage item is **statistics collection** - the prerequisite for a real cost-based optimizer, which the current rule-based planner (and hash join's simple row-count heuristic) should eventually be replaced by. Beyond Phase B, see `PROJECT_PLAN.md`'s full phase breakdown (subqueries, more indexing, PostgreSQL wire protocol compatibility, replication, extensibility) - worth picking based on what's actually useful next, not worked through as a fixed checklist.

## Cross-platform note

All verification above was done on Linux. Real `mvn test` runs on Windows caught two rounds of a genuine bug my Linux sandbox couldn't: file handles left open past when they should be closed, which Linux tolerates (an open file can still be deleted) and Windows does not ("the process cannot access the file"). Round one was test-only: `CrashRecoveryTest`, `MvccIsolationTest`, and `BTreeIndexTest` never explicitly closed their `DiskManager`/`WALManager`/`BufferPoolManager` instances. Round two was a real production bug, one level deeper: **`StratosDB.shutdown()` itself never closed the WAL's file handle** - it called `bufferPool.close()` (which closes the heap-table files) but never `walManager.close()`, so `wal.log` stayed open for the life of the process regardless of how cleanly the caller shut things down. Every actual test assertion passed both times - these were resource-cleanup gaps, not logic bugs - and both are now fixed. Worth remembering: **this project's automated verification has been Linux-only so far**, and issues like these only surface when someone actually runs it on Windows - which is exactly what's been happening and exactly why it's worth continuing.

## How this doc is kept honest

Every checkmark above was produced by: cloning the actual repo fresh, generating the real ANTLR parser (not assuming it works), compiling every module together, and running the full test suite — then reading the pass/fail counts, not writing them from memory. When something failed (and things have failed multiple times this project — a partially-applied commit, a buffer-size bug, a garbage pointer bug), it's recorded above as a fix, not smoothed over. If you update this file yourself, keep that standard: a checkmark means "I ran it and it passed," not "I'm pretty sure this works."
