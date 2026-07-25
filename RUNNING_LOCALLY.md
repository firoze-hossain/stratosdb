# Running StratosDB Locally

Everything below was verified against the actual code, but not through a real `mvn` run — Maven Central wasn't reachable from the sandbox this was built in, so the code was compiled and tested with a manually assembled classpath (javac + the ANTLR4 tool + JUnit, installed via apt) instead. This is likely **the first time this exact code will go through a real Maven build.** Follow the steps below, and if anything doesn't match what's described, that's exactly the kind of thing worth reporting back.

## Prerequisites

- **JDK 21** (the project is pinned to Java 21 LTS — check with `java -version`)
- **Maven 3.9+** (check with `mvn -version`)
- Internet access to Maven Central, to download dependencies (ANTLR4, JUnit 5, SLF4J, etc.)

## 1. Build everything

```bash
cd stratosdb
mvn clean install -DskipTests
```

`-DskipTests` first, deliberately: this separates "does everything compile, including the ANTLR-generated SQL parser" from "do the tests pass" as two distinct checkpoints. If this step fails, the problem is a compile error or a missing dependency — nothing test-related yet.

## 2. Run the test suite

```bash
mvn test
```

**Expect 37 passing tests** across 7 test classes:

| Test class | Tests | What it actually checks |
|---|---|---|
| `CrashRecoveryTest` | 2 | Forks a real second JVM, sends it a real `SIGKILL` mid-write, restarts, verifies WAL redo recovers exactly the committed rows |
| `MvccIsolationTest` | 3 | Snapshot isolation: uncommitted writes are invisible to others, old snapshots don't see later commits |
| `LockManagerDeadlockTest` | 2 | Two real threads in a genuine circular wait; verifies exactly one is aborted with a deadlock error |
| `BTreeIndexTest` | 6 | Point search, range scan, duplicates, persistence across reopen, and 250,000 shuffled keys forcing real multi-level node splits |
| `StratosServerTest` | 3 | Real socket round-trips: query/result, server-side errors surviving the round-trip as a failed result, two connections sharing committed data |
| `StratosDriverTest` | 4 | The JDBC driver through `java.sql.DriverManager` exactly as a real application would use it: full CRUD, server errors becoming real `SQLException`s, unsupported features throwing clearly, URL-acceptance rules |
| `StratosDBTest` | 17 | Full SQL round-trips: CRUD, `CREATE INDEX`, index-vs-seq-scan planning via `EXPLAIN`, comparison-operator correctness on both scan paths, and JOIN (basic match, inner-join exclusion, WHERE on a joined column, bare-name resolution, EXPLAIN shape) |

Two things not to be alarmed by:
- `BTreeIndexTest`'s large test inserts 250,000 keys — it may take several seconds.
- `CrashRecoveryTest` genuinely kills a JVM process. Log output mentioning a killed/terminated process is the test working, not a crash in your build.

**If anything fails, that's the signal to send back** — paste the failing test name and the assertion message.

## 3. Launch the interactive CLI

```bash
java -jar stratosdb-cli/target/stratosdb-cli-1.0.0-SNAPSHOT.jar [optional-data-directory]
```

Defaults to `./stratosdb_data` if you don't pass a directory. This is an **in-process** shell (it links directly against the engine — there's no network layer yet, see `PROGRESS.md` Week 4), but it exercises the real SQL engine end to end.

The shell reads one line at a time, so **type each statement on a single line** (no multi-line SQL yet). Try this session to prove out CRUD, MVCC, and the new planner all at once:

```sql
CREATE TABLE users (id INT, name VARCHAR, age INT);
INSERT INTO users VALUES (1, 'Alice', 30);
INSERT INTO users VALUES (2, 'Bob', 25);
INSERT INTO users VALUES (3, 'Carol', 40);

SELECT * FROM users;
SELECT * FROM users WHERE age >= 30;

CREATE INDEX idx_age ON users (age);

EXPLAIN SELECT * FROM users WHERE age = 30;
-- expect: Index Scan using idx_age on users (column=age, range=[30, 30])

EXPLAIN SELECT * FROM users WHERE id = 1;
-- expect: Seq Scan on users        (id has no index)

UPDATE users SET age = 31 WHERE id = 1;
SELECT * FROM users WHERE age = 30;   -- expect 0 rows now
SELECT * FROM users WHERE age = 31;   -- expect Alice

DELETE FROM users WHERE id = 2;
SELECT * FROM users;                  -- expect Alice(31) and Carol(40)

CREATE TABLE orders (id INT, user_id INT, amount INT);
INSERT INTO orders VALUES (100, 1, 50);
INSERT INTO orders VALUES (101, 1, 75);
SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id;
EXPLAIN SELECT * FROM users JOIN orders ON users.id = orders.user_id;
-- expect: Nested Loop Join: Seq Scan on users -> Seq Scan on orders ON users.id=orders.user_id

\status
\dt
\exit
```

Then **relaunch the shell pointed at the same data directory** and run `SELECT * FROM users;` again — the table, its rows, and the index should all still be there. That's Week 1 and Week 2's durability and transaction work, visible from the outside rather than just in a test file.

## 4. Sanity-check the planner is actually choosing differently

The clearest proof "the SQL machine works" for this specific round of work: run the same query shape against an indexed and a non-indexed column and confirm `EXPLAIN` reports different strategies (shown above — `idx_age` on `age` vs. no index on `id`). If both report the same strategy, or `EXPLAIN` errors out, something regressed.

## 5. Run the network server and connect with a real JDBC client

```bash
java -jar stratosdb-network/target/stratosdb-network-1.0.0-SNAPSHOT.jar [dataDirectory] [port]
```

Defaults to `./stratosdb_data` and port 5432. Leave it running in one terminal, then from any Java code with `stratosdb-jdbc-1.0.0-SNAPSHOT.jar` on the classpath:

```java
Connection conn = DriverManager.getConnection("jdbc:stratos://localhost:5432/");
Statement stmt = conn.createStatement();
stmt.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)");
stmt.executeUpdate("INSERT INTO users VALUES (1, 'Alice', 30)");
ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE age >= 25");
while (rs.next()) {
    System.out.println(rs.getString("name") + " is " + rs.getInt("age"));
}
```

No `Class.forName(...)` needed — the driver self-registers via the standard JDBC 4 service-loading mechanism the moment it's on the classpath. This is a genuinely minimal driver, stated plainly rather than oversold: `Connection`/`Statement`/`ResultSet` are JDBC's three largest interfaces (63/61/203 methods respectively), and only the commonly-used subset is implemented for real - CRUD, metadata, error propagation. Anything else throws `SQLFeatureNotSupportedException` with a clear message rather than silently doing nothing; if a tool you're pointing at StratosDB hits one, that exception message will say exactly which method it needs.

## 6. Run the benchmark for yourself

```bash
java -jar stratosdb-benchmark/target/stratosdb-benchmark-1.0.0-SNAPSHOT.jar [rowCount] [queryCount]
```

Defaults to 100,000 rows and 300 queries per scenario if you don't pass arguments. On the machine this was built on, that took about 2-3 minutes total (inserting 100k rows one `INSERT` statement at a time is the slow part — each one pays a full SQL parse and transaction commit, which the benchmark's own report calls out honestly rather than hiding). It prints the planner's actual `EXPLAIN` choice for both the indexed and unindexed column before running anything, so the numbers that follow are provably measuring what they claim to. Expect something in the neighborhood of a 50-100x speedup for the indexed path at 100k rows; the exact ratio will vary by machine.

## 7. If you want to see the crash test with your own eyes

`CrashRecoveryTest` does this automatically, but you can reproduce the shape of it manually: start the CLI, insert some rows, and `kill -9 <pid>` the Java process from another terminal instead of using `\exit`. Relaunch pointed at the same directory — everything you committed (each statement auto-commits, per Week 2) should still be there; anything from an interrupted multi-row operation should not partially appear.

## Troubleshooting

- **`mvn test` fails with `IOException: Failed to delete temp directory` / "The process cannot access the file because it is being used by another process" (Windows)**: this was two rounds of a real bug, both found and fixed via actual Windows `mvn test` runs - thank you for those, since my own build environment is Linux and never surfaced either. Round one: several tests never explicitly closed their `DiskManager`/`WALManager`/`BufferPoolManager` instances - fixed by tracking and closing every such resource in `@AfterEach`. Round two, one level deeper: `StratosDB.shutdown()` itself never closed the WAL's own file handle (it closed the heap-table files via `bufferPool.close()`, but never called `walManager.close()`), so `wal.log` stayed open for the life of the process no matter how cleanly a caller shut things down - this affected the real shutdown path, not just tests. Both are fixed now. If you pulled before either fix landed, `git pull` and retry.
- **`mvn test` fails on `BTreeIndexTest`'s big test with a timeout**: likely means the buffer pool eviction path is slower on your machine than expected. The test already sizes its pool to avoid pathological thrashing (a bug I hit and fixed while building this — see `PROGRESS.md`), but if it's still too slow, that's worth reporting with your JDK version and OS.
- **ANTLR-related compile errors**: means the grammar (`StratosSQL.g4`) and the hand-written `SqlParser.java` have drifted out of sync again — this has happened twice already in this project's history (a missing `UPDATE` dispatch, a `VARCHAR` length requirement that broke the project's own tests). Check `SqlParser.buildStatement()` against the grammar's `sqlStatement` alternatives first.
- **`stratosdb-cli` jar not found**: confirm `mvn clean install` (not just `package` on a single module) ran from the repo root, since `stratosdb-cli` depends on every other module having been built and installed to your local `.m2` repository first.
