# Publishing StratosDB to Maven Central

This covers everything needed to make `io.github.firoze-hossain:stratosdb-jdbc` (and every other published module) installable via a plain `<dependency>` in any Maven or Gradle project, including Spring Boot.

**What's already done, in the codebase itself**, so you don't have to:
- `groupId` changed from `com.stratosdb` to `io.github.firoze-hossain` — the former needs you to prove ownership of the `stratosdb.com` domain (DNS TXT record), which is a real, ongoing cost; the latter is verified once, for free, via your existing GitHub account.
- A `LICENSE` file (Apache 2.0) at the repo root — Maven Central refuses to publish anything without an OSI-approved license.
- `<licenses>`, `<developers>`, `<scm>` metadata in the parent POM — all required fields.
- A real, fixed bug: three modules (`stratosdb-common`, `stratosdb-core`, `stratosdb-storage`) had `maven.compiler.source`/`target` set to `25`, which doesn't exist as a valid Java release yet — this would have failed a real `mvn compile` outright, though it never surfaced before now since this project's own testing has run through direct `javac` invocations, never `mvn` itself.
- A real dependency-scoping bug: `slf4j-simple` was a normal (compile-scoped) dependency in `stratosdb-core`/`stratosdb-network`/`stratosdb-cli`/`stratosdb-benchmark`. Left as-is, any Spring Boot project depending on `stratosdb-jdbc` would transitively pull in `slf4j-simple`, which conflicts with Spring's own Logback binding (SLF4J only tolerates one binding on the classpath at a time). Fixed with the two correct, different fixes for two different situations: `stratosdb-core` (a pure library, no main class) now has it `test`-scoped only; `stratosdb-network`/`stratosdb-cli`/`stratosdb-benchmark` (which have runnable main classes and their own shaded standalone jars) now mark it `<optional>true</optional>` — kept for their own build, never propagated to a consumer.
- `stratosdb-testing` (internal integration tests, no reusable API) is marked `<maven.deploy.skip>true</maven.deploy.skip>` — it'll still build and run as part of the reactor, just never gets uploaded anywhere.
- A `release` Maven profile in the parent POM with source-jar, javadoc-jar, GPG-signing, and Sonatype's current Central Portal publishing plugin — all gated behind `-Prelease` so ordinary `mvn clean install` never needs signing keys or publishing credentials.

**Important limitation, stated plainly**: this sandbox's network policy blocks `repo.maven.apache.org` (confirmed directly - `mvn compile` here fails with a 403), so none of this could be verified end-to-end with a real Maven build in this environment. Everything above was checked as carefully as possible without that - the POM structure, XML validity, and the actual Java code compiling correctly were all confirmed - but **you should run `mvn clean install` yourself on a machine with normal internet access before doing anything else**, to catch anything this sandbox genuinely couldn't test.

## What only you can do

### 1. Create a Central Portal account and verify your namespace

1. Go to [central.sonatype.com](https://central.sonatype.com) and sign up (this replaced the older OSSRH/`oss.sonatype.org` flow - if you already have an old OSSRH account, it's a separate migration path, not covered here).
2. Under **Namespaces**, add `io.github.firoze-hossain`. Sonatype verifies GitHub-based namespaces automatically once you're logged in with the matching GitHub account - no DNS records needed.

### 2. Generate a GPG key and publish it

Every file gets signed before it can be published.

```bash
gpg --full-generate-key
# RSA and RSA, 4096 bits, no expiration (or your preference)
# Use the same name/email as your Central Portal account

gpg --list-secret-keys --keyid-format LONG
# Note the key ID (the part after rsa4096/)

gpg --keyserver keyserver.ubuntu.com --send-keys YOUR_KEY_ID
```

Central checks the public keyserver for your key during validation, so the upload step matters - not just having the key locally.

### 3. Generate a Central Portal user token

In the Central Portal UI: **Account → Generate User Token**. This gives you a username/password pair for publishing - **not your real account password**, and revocable independently if it ever leaks.

### 4. Configure `~/.m2/settings.xml`

```xml
<settings>
  <servers>
    <server>
      <id>central</id>
      <username>YOUR_TOKEN_USERNAME</username>
      <password>YOUR_TOKEN_PASSWORD</password>
    </server>
  </servers>
  <profiles>
    <profile>
      <id>gpg-passphrase</id>
      <properties>
        <gpg.passphrase>YOUR_GPG_KEY_PASSPHRASE</gpg.passphrase>
      </properties>
    </profile>
  </profiles>
  <activeProfiles>
    <activeProfile>gpg-passphrase</activeProfile>
  </activeProfiles>
</settings>
```

(Or omit the passphrase here and let `maven-gpg-plugin` prompt interactively - safer if this file might ever be shared or backed up somewhere.)

### 5. Fill in your real email address

`pom.xml`'s `<developers>` section currently has a placeholder:

```xml
<email>your-email@example.com</email>
```

Replace it with a real, monitored address before publishing - Sonatype may need to reach you.

### 6. Set a real release version

The project is currently `1.0.0-SNAPSHOT`. Snapshots aren't what gets published to Central as a "real" release. When you're ready:

```bash
mvn versions:set -DnewVersion=1.0.0
mvn versions:commit
```

(Or edit the `<version>` in every POM by hand - same effect, `versions:set` just does it consistently across all 11 modules in one step.)

### 7. Build and verify locally first

```bash
mvn clean install
```

This alone would catch anything this sandbox couldn't - dependency resolution, plugin versions, the actual multi-module build order. **Do this before attempting to publish anything.**

### 8. Publish

```bash
mvn clean deploy -Prelease
```

This builds every module, signs every jar/sources-jar/javadoc-jar/pom with GPG, and uploads a bundle to the Central Portal as a reviewable, *not yet public* deployment (`autoPublish=false` in the POM is deliberate - nothing goes live automatically).

### 9. Review and publish for real

In the Central Portal UI, find the uploaded deployment under **Deployments**. Check that all the expected artifacts and signatures are there, then click **Publish**. Propagation to Maven Central's actual CDN typically takes 15-30 minutes after that.

### 10. Verify it actually works

In a **separate**, throwaway Maven or Spring Boot project:

```xml
<dependency>
    <groupId>io.github.firoze-hossain</groupId>
    <artifactId>stratosdb-jdbc</artifactId>
    <version>1.0.0</version>
</dependency>
```

```bash
mvn dependency:tree
```

Confirm `stratosdb-jdbc` resolves correctly and does **not** pull in `slf4j-simple` transitively (this is exactly what the `optional`/`test`-scope fixes above were for - if it does show up, something regressed).

## Using StratosDB from Spring Boot once published

Two real options, worth knowing about explicitly:

- **`stratosdb-jdbc`** - StratosDB's own JDBC driver, speaking its custom protocol. Standard `DataSource`/`JdbcTemplate` usage once the dependency and driver class (`com.stratosdb.jdbc.StratosDriver`) are configured.
- **The real PostgreSQL JDBC driver** (`org.postgresql:postgresql`, already on Maven Central) - works directly against a StratosDB server started with `--stdwire`, since that server speaks real PostgreSQL wire protocol v3. No StratosDB-specific dependency needed at all for this path - just point a normal `org.postgresql.Driver` connection string at the `--stdwire` port. Remember the known gaps from `PROGRESS.md` (simple query protocol only, trust auth only) before relying on this for anything beyond straightforward SQL.
