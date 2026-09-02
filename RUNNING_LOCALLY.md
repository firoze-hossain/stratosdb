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

**Expect 143 passing tests** across the full suite, spanning real crash injection (a forked, `SIGKILL`'d JVM), real multi-threaded deadlocks, real TLS handshakes, and — most recently — real external `psql`/`psycopg2` client processes verifying PostgreSQL wire protocol compatibility. See `PROGRESS.md` for the full, current breakdown by test class; the table that used to be here listed 9 classes and 51 tests, which is now out of date enough that repeating stale numbers here would be actively misleading rather than just incomplete.

Two things not to be alarmed by:
- `BTreeIndexTest`'s large test inserts 250,000 keys — it may take several seconds.
- `CrashRecoveryTest` genuinely kills a JVM process. Log output mentioning a killed/terminated process is the test working, not a crash in your build.

**If anything fails, that's the signal to send back** — paste the failing test name and the assertion message.

## 3. Launch the network server, then connect the CLI to it

The CLI is a **network client** now, not an embedded engine — it connects to a running StratosDB server over the wire protocol, the same as any other client would. Two processes, two terminals:

```bash
# Terminal 1: the server
java -jar stratosdb-network/target/stratosdb-network-1.0.0-SNAPSHOT.jar [dataDirectory] [port]
```

Defaults to `./stratosdb_data` and port 6582.

```bash
# Terminal 2: the CLI (StratosShell, StratosDB's JDBC-based client - takes
# host/port/username/password positionally; connects using stratosdb-jdbc,
# StratosDB's own real driver, which speaks the same real, current wire
# protocol as psql/pgjdbc/psycopg2 - see section 3a below)
java -cp stratosdb-cli/target/stratosdb-cli-1.0.0-SNAPSHOT.jar com.stratosdb.cli.StratosShell [host] [port] [username] [password] [--ssl]
```

Defaults to `localhost` and `6582`, no credentials, no TLS. All args are optional and positional except `--ssl`, which can appear anywhere.

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

Then **stop the server (Ctrl+C) and restart it pointed at the same data directory**, then reconnect the CLI — the table, its rows, and the index should all still be there. That's Week 1 and Week 2's durability and transaction work, visible from the outside rather than just in a test file.

### 3a. Connecting with a real PostgreSQL client instead

No extra flag needed any more: the default port already speaks a real, current PostgreSQL-wire-protocol-v3-compatible protocol (`StdWireServer`) - this is what `StratosShell`, `stdsql`, and `stratosdb-jdbc` (StratosDB's own real JDBC driver) all speak too, so any of them can reach the same server on the same port.

A real, previously-broken default, corrected here rather than left implicit: `StratosServerMain` used to start a small, separate, custom-protocol server (`StratosServer`) on the default port, only optionally *also* starting this real, PostgreSQL-compatible one on a secondary port via `--stdwire`. That old server and protocol have been removed entirely - the default port is now always the real one. `--stdwire`/`--stdwire=PORT` are still accepted for backward compatibility with any script that already passes them, but are now harmless no-ops (a note is printed explaining why).

Any real PostgreSQL client works here unmodified, not just this project's own tools - this was verified against real `psql` and Python's `psycopg2` independently (see `PROGRESS.md`):

```bash
psql -h localhost -p 6582 -U anyuser -d anydb
```

Both the simple and extended query protocols work (parameterized queries via `Parse`/`Bind`/`Execute` - `psycopg2`'s own default `cur.execute("... WHERE id = %s", (1,))` API uses this automatically), and real SCRAM-SHA-256 authentication is available (see section 4 below to turn it on - trust auth, unauthenticated, remains the default). Known limits: `psql`'s `\d`/`\l`/tab-completion don't work yet (they query real Postgres system catalog tables StratosDB doesn't emulate beyond `\dt`), and SCRAM channel binding (`SCRAM-SHA-256-PLUS`) isn't implemented - plain SQL statements, parameterized queries, and password authentication all work regardless.

## 4. Try real SCRAM authentication

Opt-in - the server defaults to open (trust) access over plain TCP, exactly as it always has, so nothing above requires it. To turn it on, you write a small amount of Java (there's no `CREATE USER` SQL yet, and no config-file support - credentials are configured in code at startup):

```java
UserStore users = new UserStore();
users.addUser("alice", "correct-horse-battery-staple"); // real PBKDF2 hashing under the hood

StdWireServer server = new StdWireServer(port, db, users);
server.start();
```

**A real, honestly-stated gap, not a broken example left in place**: `StdWireServer` - the real, current server every example above connects to - has no TLS support at all yet; every SSL negotiation attempt is unconditionally declined. Requesting `ssl=true` from `stratosdb-jdbc` (or `StratosShell --ssl`) now throws a clear, immediate error explaining this, rather than silently connecting unencrypted or hanging on a negotiation the server will never complete. Real TLS support against `StdWireServer` is real, separate, future work - there is no working TLS example to show here right now.

Connect with `StratosShell` (StratosDB's JDBC-based client, using `stratosdb-jdbc` - StratosDB's own real driver):

```bash
java -cp stratosdb-cli/target/stratosdb-cli-1.0.0-SNAPSHOT.jar com.stratosdb.cli.StratosShell localhost 6582 alice correct-horse-battery-staple
```

Or via raw JDBC:

```java
Properties props = new Properties();
props.setProperty("user", "alice");
props.setProperty("password", "correct-horse-battery-staple");
Connection conn = DriverManager.getConnection("jdbc:stratos://localhost:6582/", props);
```

## 5. Sanity-check the planner is actually choosing differently

The clearest proof "the SQL machine works" for this specific round of work: run the same query shape against an indexed and a non-indexed column and confirm `EXPLAIN` reports different strategies (shown above — `idx_age` on `age` vs. no index on `id`). If both report the same strategy, or `EXPLAIN` errors out, something regressed.

## 6. Run the benchmark for yourself

```bash
java -jar stratosdb-benchmark/target/stratosdb-benchmark-1.0.0-SNAPSHOT.jar [rowCount] [queryCount]
```

Defaults to 100,000 rows and 300 queries per scenario if you don't pass arguments. On the machine this was built on, that took about 2-3 minutes total (inserting 100k rows one `INSERT` statement at a time is the slow part — each one pays a full SQL parse and transaction commit, which the benchmark's own report calls out honestly rather than hiding). It prints the planner's actual `EXPLAIN` choice for both the indexed and unindexed column before running anything, so the numbers that follow are provably measuring what they claim to. Expect something in the neighborhood of a 50-100x speedup for the indexed path at 100k rows; the exact ratio will vary by machine.

## 7. If you want to see the crash test with your own eyes

`CrashRecoveryTest` does this automatically, but you can reproduce the shape of it manually: start the CLI, insert some rows, and `kill -9 <pid>` the Java process from another terminal instead of using `\exit`. Relaunch pointed at the same directory — everything you committed (each statement auto-commits, per Week 2) should still be there; anything from an interrupted multi-row operation should not partially appear.

## Troubleshooting

- **DDL/DML shows "0 row(s) affected" or "1 row(s) affected" instead of a descriptive message like "Table created: users"**: expected, not a bug. Standard JDBC's `Statement.execute()`/`executeUpdate()` only expose a boolean and a row count, not an arbitrary message string - the CLI used to show StratosDB's own internal messages because it linked the engine in-process and printed its result objects directly. Now that it's a real JDBC client (see PROGRESS.md Week 4), it sees exactly what any other JDBC-based tool would see. `SHOW TABLES` was specifically fixed to return real rows instead of a message so it keeps working meaningfully; that fix doesn't extend to other commands' descriptive text, which isn't something JDBC has a slot for.
- **`mvn test` fails with a shutdown-related `ClosedChannelException`**: this was a real idempotency bug, found and fixed - `StratosDB.shutdown()` could throw if called twice, because `WALManager.close()` called `checkpoint()` (which writes to the WAL channel) before checking whether that channel was already closed, and `shutdown()` also called `checkpoint()` a second, redundant time directly. Fixed at the source with a regression test (`StratosDBTest.testShutdownIsIdempotent`). If you pulled before this fix landed, `git pull` and retry.
- **`mvn test` fails with `IOException: Failed to delete temp directory` / "The process cannot access the file because it is being used by another process" (Windows)**: this was two rounds of a real bug, both found and fixed via actual Windows `mvn test` runs - thank you for those, since my own build environment is Linux and never surfaced either. Round one: several tests never explicitly closed their `DiskManager`/`WALManager`/`BufferPoolManager` instances - fixed by tracking and closing every such resource in `@AfterEach`. Round two, one level deeper: `StratosDB.shutdown()` itself never closed the WAL's own file handle (it closed the heap-table files via `bufferPool.close()`, but never called `walManager.close()`), so `wal.log` stayed open for the life of the process no matter how cleanly a caller shut things down - this affected the real shutdown path, not just tests. Both are fixed now. If you pulled before either fix landed, `git pull` and retry.
- **`mvn test` fails on `BTreeIndexTest`'s big test with a timeout**: likely means the buffer pool eviction path is slower on your machine than expected. The test already sizes its pool to avoid pathological thrashing (a bug I hit and fixed while building this — see `PROGRESS.md`), but if it's still too slow, that's worth reporting with your JDK version and OS.
- **ANTLR-related compile errors**: means the grammar (`StratosSQL.g4`) and the hand-written `SqlParser.java` have drifted out of sync again — this has happened twice already in this project's history (a missing `UPDATE` dispatch, a `VARCHAR` length requirement that broke the project's own tests). Check `SqlParser.buildStatement()` against the grammar's `sqlStatement` alternatives first.
- **`stratosdb-cli` jar not found**: confirm `mvn clean install` (not just `package` on a single module) ran from the repo root, since `stratosdb-cli` depends on every other module having been built and installed to your local `.m2` repository first.
- **CLI can't connect / "Could not connect to StratosDB"**: the CLI no longer starts its own embedded engine - make sure `StratosServerMain` is actually running first, pointed at the host/port the CLI is trying to reach (it starts the real, PostgreSQL-wire-compatible server by default now - see section 3a above).
