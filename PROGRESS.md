# StratosDB — Progress Tracker

**Last verified:** July 25, 2026 — every checkmark below was confirmed by actually compiling and running the test suite against the exact code on GitHub `master`, not by reading commit messages or trusting claims. See "How this doc is kept honest" at the bottom.

## At a glance

| | |
|---|---|
| Commits | 9 |
| Main source | ~3,383 lines |
| Test source | ~765 lines |
| Tests passing | **37 / 37** |
| Current stage | Week 4 (Network, JDBC, CLI, security) — wire protocol + JDBC driver done, CLI/auth/TLS remain |

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

## Week 4 — Network, JDBC, CLI, security 🟡 IN PROGRESS

- ✅ **Wire protocol** (`stratosdb-network` was empty) — `WireProtocol`: a small custom binary protocol (not PostgreSQL-wire-compatible, stated plainly), self-framing via `DataInputStream`/`DataOutputStream` primitives. `StratosServer` accepts connections and runs one virtual thread per connection (Java 21 - the reason this project pinned to that LTS back in Week 1), all sharing one `StratosDB` instance so every connection sees the same data. `StratosServerMain` is a runnable entry point (`java -jar stratosdb-network-*.jar [dataDir] [port]`). 3 tests: real socket round-trip, server-side errors surviving the round-trip as a failed result rather than a hang, and two separate connections sharing committed data.
  - Along the way, fixed `StratosDB.startServer()`, which previously logged "server started on port X" while starting nothing at all - now honestly documented as a state flag, with the real listener living in `stratosdb-network` (which depends on `stratosdb-core`, not the reverse, so `core` itself architecturally cannot start a network listener without a circular module dependency - this is why the real server isn't inside `StratosDB` itself).
- ✅ **JDBC driver** (`stratosdb-jdbc` was empty) — `StratosDriver`/`StratosConnection`/`StratosStatement`/`StratosResultSet`/`StratosResultSetMetaData`, real behavior over a real socket to a real server, verified through `java.sql.DriverManager` exactly as a real application would use it (not by referencing the driver class directly). `Connection`/`Statement`/`ResultSet` have 63/61/203 methods respectively on their JDBC interfaces; rather than hand-writing that much boilerplate, they're implemented as dynamic proxies backed by a real handler class - full real behavior for CRUD, metadata, and error propagation, and a clear `SQLFeatureNotSupportedException` (not a silent no-op) for anything genuinely unimplemented, like multi-statement transactions. 4 tests: full CRUD round-trip via `DriverManager`, server errors becoming real `SQLException`s, unsupported features throwing clearly rather than silently doing nothing, and URL-acceptance rules.
  - Found a real bug while testing rather than assuming it away: `DriverManager.getConnection(...)` only found the driver when *some* test happened to reference the class first, because the standard JDBC 4 `META-INF/services/java.sql.Driver` auto-registration file didn't exist - added it, confirmed the fix by running the affected test 3 times in a row (it's a registration-order bug, easy for a single lucky run to hide).
- 🟡 CLI shell exists (`StratosShell`, 133 lines) but talks to `StratosDB` **in-process** — it doesn't go over the network protocol yet, even though that protocol now exists
- 🔲 Auth (salted + hashed credentials)
- 🔲 TLS

---

## Module status (verified line counts, not aspirational ones)

| Module | Files | Lines | State |
|---|---|---|---|
| `stratosdb-common` | 11 | 182 | Real — exceptions, constants, utils |
| `stratosdb-core` | 2 | 87 | Real — wires everything together |
| `stratosdb-storage` | 10 | 1,686 | Real — disk/buffer/WAL/heap, now with a page-type-agnostic buffer pool |
| `stratosdb-transaction` | 5 | 381 | Real — MVCC + locking, both tested |
| `stratosdb-sql` | 17 | 993 | Real — ANTLR grammar (now with JOIN/qualified columns) + hand-written AST builder + executor |
| `stratosdb-index` | 1 | 304 | Real — B+Tree, tested at scale, wired into query execution via the planner |
| `stratosdb-network` | 3 | 351 | Real — wire protocol + virtual-thread-per-connection server, tested over real sockets |
| `stratosdb-jdbc` | 6 | 686 | Real — Driver/Connection/Statement/ResultSet, verified through `DriverManager` |
| `stratosdb-cli` | 1 | 133 | Partial — in-process shell only, doesn't use the network protocol yet |
| `stratosdb-testing` | 0 (test-only) | — | 17 integration tests, all passing |
| `stratosdb-benchmark` | 1 | 169 | Real — `QueryBenchmark`, run for real (see Week 3 results above) |

## What to do next (in order)

1. **CLI over the network** — point `StratosShell` at `StratosConnection`/the wire protocol instead of linking `StratosDB` in-process. Mostly plumbing at this point; the hard parts (protocol, server) are done.
2. **Auth** — salted + hashed credentials, checked at connection time.
3. **TLS** — wrap the server socket with `SSLContext`.

## Cross-platform note

All verification above was done on Linux. Real `mvn test` runs on Windows caught two rounds of a genuine bug my Linux sandbox couldn't: file handles left open past when they should be closed, which Linux tolerates (an open file can still be deleted) and Windows does not ("the process cannot access the file"). Round one was test-only: `CrashRecoveryTest`, `MvccIsolationTest`, and `BTreeIndexTest` never explicitly closed their `DiskManager`/`WALManager`/`BufferPoolManager` instances. Round two was a real production bug, one level deeper: **`StratosDB.shutdown()` itself never closed the WAL's file handle** - it called `bufferPool.close()` (which closes the heap-table files) but never `walManager.close()`, so `wal.log` stayed open for the life of the process regardless of how cleanly the caller shut things down. Every actual test assertion passed both times - these were resource-cleanup gaps, not logic bugs - and both are now fixed. Worth remembering: **this project's automated verification has been Linux-only so far**, and issues like these only surface when someone actually runs it on Windows - which is exactly what's been happening and exactly why it's worth continuing.

## How this doc is kept honest

Every checkmark above was produced by: cloning the actual repo fresh, generating the real ANTLR parser (not assuming it works), compiling every module together, and running the full test suite — then reading the pass/fail counts, not writing them from memory. When something failed (and things have failed multiple times this project — a partially-applied commit, a buffer-size bug, a garbage pointer bug), it's recorded above as a fix, not smoothed over. If you update this file yourself, keep that standard: a checkmark means "I ran it and it passed," not "I'm pretty sure this works."
