# StratosDB — Progress Tracker

**Last verified:** July 25, 2026 — every checkmark below was confirmed by actually compiling and running the test suite against the exact code on GitHub `master`, not by reading commit messages or trusting claims. See "How this doc is kept honest" at the bottom.

## At a glance

| | |
|---|---|
| Commits | 9 |
| Main source | ~3,383 lines |
| Test source | ~765 lines |
| Tests passing | **30 / 30** |
| Current stage | Week 3 (SQL engine + indexing) — **complete** |

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

## Week 4 — Network, JDBC, CLI, security 🔲 NOT STARTED

- 🔲 Wire protocol (`stratosdb-network` is empty)
- 🔲 JDBC driver (`stratosdb-jdbc` is empty)
- 🟡 CLI shell exists (`StratosShell`, 133 lines) but talks to `StratosDB` **in-process** — it doesn't go over a network protocol yet, because that protocol doesn't exist yet
- 🔲 Auth (salted + hashed credentials)
- 🔲 TLS
- 🔲 Real throughput/latency benchmark numbers

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
| `stratosdb-network` | 0 | 0 | Empty |
| `stratosdb-jdbc` | 0 | 0 | Empty |
| `stratosdb-cli` | 1 | 133 | Partial — in-process shell only, no network protocol to connect to yet |
| `stratosdb-testing` | 0 (test-only) | — | 17 integration tests, all passing |
| `stratosdb-benchmark` | 1 | 169 | Real — `QueryBenchmark`, run for real (see Week 3 results above) |

## What to do next (in order)

Week 3 is complete. Week 4 is next:
1. **Wire protocol** (`stratosdb-network` is empty) — a real socket server, length-prefixed frames at minimum.
2. **JDBC driver** (`stratosdb-jdbc` is empty) — even a minimal `Driver`/`Connection`/`Statement`/`ResultSet` makes the engine usable from any Java tool.
3. **CLI over the network** — point the existing shell at the new protocol instead of linking in-process.
4. **Auth + TLS** — salted/hashed credentials, socket-level TLS.

## Cross-platform note

All verification above was done on Linux. Real `mvn test` runs on Windows caught two rounds of a genuine bug my Linux sandbox couldn't: file handles left open past when they should be closed, which Linux tolerates (an open file can still be deleted) and Windows does not ("the process cannot access the file"). Round one was test-only: `CrashRecoveryTest`, `MvccIsolationTest`, and `BTreeIndexTest` never explicitly closed their `DiskManager`/`WALManager`/`BufferPoolManager` instances. Round two was a real production bug, one level deeper: **`StratosDB.shutdown()` itself never closed the WAL's file handle** - it called `bufferPool.close()` (which closes the heap-table files) but never `walManager.close()`, so `wal.log` stayed open for the life of the process regardless of how cleanly the caller shut things down. Every actual test assertion passed both times - these were resource-cleanup gaps, not logic bugs - and both are now fixed. Worth remembering: **this project's automated verification has been Linux-only so far**, and issues like these only surface when someone actually runs it on Windows - which is exactly what's been happening and exactly why it's worth continuing.

## How this doc is kept honest

Every checkmark above was produced by: cloning the actual repo fresh, generating the real ANTLR parser (not assuming it works), compiling every module together, and running the full test suite — then reading the pass/fail counts, not writing them from memory. When something failed (and things have failed multiple times this project — a partially-applied commit, a buffer-size bug, a garbage pointer bug), it's recorded above as a fix, not smoothed over. If you update this file yourself, keep that standard: a checkmark means "I ran it and it passed," not "I'm pretty sure this works."
