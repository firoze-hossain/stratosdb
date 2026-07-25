# StratosDB — Progress Tracker

**Last verified:** July 25, 2026 — every checkmark below was confirmed by actually compiling and running the test suite against the exact code on GitHub `master`, not by reading commit messages or trusting claims. See "How this doc is kept honest" at the bottom.

## At a glance

| | |
|---|---|
| Commits | 9 |
| Main source | ~3,383 lines |
| Test source | ~765 lines |
| Tests passing | **19 / 19** |
| Current stage | Week 3 (SQL engine + indexing) — in progress |

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
- 🔲 Rule-based planner (seq scan vs. index scan choice) — **not started**
- 🔲 Wire the B+Tree into `ExecutorEngine`/`CREATE INDEX` so SQL queries can actually use it — **not started**
- 🔲 JOIN support (nested loop at minimum) — **not started**
- 🔲 `EXPLAIN`-style output — **not started**
- 🔲 Benchmark: point lookup with index vs. full scan on 100k+ rows — **not started**

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
| `stratosdb-sql` | 14 | 610 | Real — ANTLR grammar + hand-written AST builder + executor |
| `stratosdb-index` | 1 | 304 | Real — B+Tree, tested at scale. Not yet wired into query execution |
| `stratosdb-network` | 0 | 0 | Empty |
| `stratosdb-jdbc` | 0 | 0 | Empty |
| `stratosdb-cli` | 1 | 133 | Partial — in-process shell only, no network protocol to connect to yet |
| `stratosdb-testing` | 0 (test-only) | — | 6 integration tests, all passing |
| `stratosdb-benchmark` | 0 | 0 | Empty |

## What to do next (in order)

1. **Wire the B+Tree into query execution.** Right now it's a correct, tested, standalone component that nothing calls. Add `CREATE INDEX`, have `HeapTable`/`ExecutorEngine` maintain an index on insert, and add a rule-based check in the executor: use the index for `WHERE indexed_col = ?`, fall back to a full scan otherwise.
2. **`EXPLAIN`-style output** — cheap to add once there's an actual choice between scan strategies to report on.
3. **JOIN support** (nested loop) — the next SQL-engine gap.
4. **Then Week 4**: wire protocol, JDBC driver, auth, TLS.

## How this doc is kept honest

Every checkmark above was produced by: cloning the actual repo fresh, generating the real ANTLR parser (not assuming it works), compiling every module together, and running the full test suite — then reading the pass/fail counts, not writing them from memory. When something failed (and things have failed multiple times this project — a partially-applied commit, a buffer-size bug, a garbage pointer bug), it's recorded above as a fix, not smoothed over. If you update this file yourself, keep that standard: a checkmark means "I ran it and it passed," not "I'm pretty sure this works."
