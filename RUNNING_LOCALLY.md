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

Defaults to `./stratosdb_data` and port 5432.

```bash
# Terminal 2: the CLI
java -jar stratosdb-cli/target/stratosdb-cli-1.0.0-SNAPSHOT.jar [host] [port] [username] [password] [--ssl]
```

Defaults to `localhost` and `5432`, no credentials, no TLS. All args are optional and positional except `--ssl`, which can appear anywhere.

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

### Connecting with a real PostgreSQL client instead

Add `--pgwire` to also start a real PostgreSQL wire protocol server against the same database, on `port + 1` by default (or a specific port via `--pgwire=PORT`):

```bash
java -jar stratosdb-network/target/stratosdb-network-1.0.0-SNAPSHOT.jar ./stratosdb_data 5432 --pgwire
# custom protocol on 5432, PostgreSQL wire protocol on 5433
```

```bash
psql -h localhost -p 5433 -U anyuser -d anydb
```

Any real PostgreSQL client works here, not just `psql` — this was verified against `psql` and Python's `psycopg2` independently (see `PROGRESS.md`). Known limits: only the simple query protocol (no server-side prepared statements yet), trust auth only (no password over this path yet), and `psql`'s `\dt`/`\d`/`\l`/tab-completion don't work (they query real Postgres system catalog tables StratosDB doesn't emulate yet) — plain SQL statements work regardless.

## 4. Try authentication and TLS

Both are opt-in — the server defaults to open access over plain TCP, exactly as it always has, so nothing above requires either. To turn them on, you write a small amount of Java (there's no `CREATE USER` SQL yet, and no config-file support — credentials are configured in code at startup):

```java
UserStore users = new UserStore();
users.addUser("alice", "correct-horse-battery-staple"); // real PBKDF2 hashing under the hood

SSLContext serverContext = TlsSupport.loadServerContext("/path/to/keystore.p12", "keystore-password".toCharArray());

StratosServer server = new StratosServer(port, db, users, serverContext);
server.start();
```

Generate a test keystore with the JDK's own `keytool` if you don't have a real certificate handy:

```bash
keytool -genkeypair -alias stratosdb -keyalg RSA -keysize 2048 -validity 365 \
  -keystore keystore.p12 -storetype PKCS12 -storepass changeit -keypass changeit \
  -dname "CN=localhost, OU=StratosDB, O=StratosDB, L=Test, ST=Test, C=US"
```

Then connect the CLI with credentials and TLS:

```bash
java -jar stratosdb-cli/target/stratosdb-cli-1.0.0-SNAPSHOT.jar localhost 5432 alice correct-horse-battery-staple --ssl
```

Or via raw JDBC:

```java
Properties props = new Properties();
props.setProperty("user", "alice");
props.setProperty("password", "correct-horse-battery-staple");
props.setProperty("ssl", "true");
Connection conn = DriverManager.getConnection("jdbc:stratos://localhost:5432/", props);
```

**Read this before relying on TLS for anything real**: the client currently trusts *any* certificate the server presents - there is no certificate verification wired up yet. That's still real encryption (a passive eavesdropper reading the raw bytes off the wire gets nothing useful), but it does **not** protect against an active attacker who intercepts the connection and presents their own certificate - the client has no way to tell a genuine StratosDB server from an impostor. `TlsSupport`'s javadoc says this explicitly. Treat this as "encryption," not "authentication of the server," until real certificate/truststore verification is added.

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
- **CLI can't connect / "Could not connect to StratosDB"**: the CLI no longer starts its own embedded engine - make sure `StratosServerMain` is actually running first, pointed at the host/port the CLI is trying to reach.
