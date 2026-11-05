# StratosDB 1.0 Client Connectivity & Ecosystem Integration

StratosDB 1.0 provides dual client connectivity:
1. **Official Pure-Java Type 4 Driver (`stratosdb-jdbc`)** for low-latency Java/JVM applications.
2. **PostgreSQL Wire Protocol v3 (`StdWireServer`)** for 100% transparent compatibility with standard PostgreSQL drivers, ORMs, and GUI query tools.

---

## 1. Official Pure-Java Type 4 Driver (`stratosdb-jdbc`)

### Maven Coordinates
```xml
<dependency>
    <groupId>com.stratosdb</groupId>
    <artifactId>stratosdb-jdbc</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle Coordinates
```groovy
implementation 'com.stratosdb:stratosdb-jdbc:1.0.0'
```

### Standard JDBC Usage
```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class StratosQuickstart {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:stratos://localhost:5432/stratos";
        try (Connection conn = DriverManager.getConnection(url, "admin", "secret")) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM users WHERE active = ?")) {
                stmt.setBoolean(1, true);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        System.out.println("User: " + rs.getString("name"));
                    }
                }
            }
        }
    }
}
```

### HikariCP Connection Pool Configuration
```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:stratos://localhost:5432/stratos");
config.setUsername("admin");
config.setPassword("secret");
config.setMaximumPoolSize(20);
config.setMinimumIdle(5);
config.setIdleTimeout(30000);
config.setConnectionTimeout(5000);
config.setAutoCommit(true);

HikariDataSource dataSource = new HikariDataSource(config);
```

---

## 2. Spring Boot & Spring Data JPA

StratosDB connects out-of-the-box with standard Spring Boot applications using Hibernate ORM 6.x:

### `application.yml`
```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/stratos
    username: admin
    password: secret
    driver-class-name: org.postgresql.Driver
    hikari:
      maximum-pool-size: 25
      minimum-idle: 5
      pool-name: StratosHikariPool
  jpa:
    database-platform: org.hibernate.dialect.PostgreSQLDialect
    hibernate:
      ddl-auto: validate
    properties:
      hibernate:
        format_sql: false
        jdbc:
          batch_size: 50
```

---

## 3. Prisma ORM (Node.js & TypeScript)

Configure StratosDB as your PostgreSQL datasource in `prisma/schema.prisma`:

```prisma
datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

generator client {
  provider = "prisma-client-js"
}

model User {
  id        Int      @id @default(autoincrement())
  email     String   @unique
  name      String?
  createdAt DateTime @default(now())
}
```

In `.env`:
```env
DATABASE_URL="postgresql://admin:secret@localhost:5432/stratos?schema=public"
```

Run migrations and query:
```bash
npx prisma db push
```

---

## 4. DBeaver & JetBrains DataGrip

Connect GUI database clients without installing special plugins:
1. Select the standard **PostgreSQL** connection type.
2. Host: `localhost` (or server IP)
3. Port: `5432`
4. Database: `stratos`
5. Username: `admin`
6. Password: `secret`
7. Driver: Standard `org.postgresql.Driver` (auto-downloaded by DBeaver / DataGrip).

---

## 5. Native Stratos Interactive CLI (`stdsql`)

StratosDB ships with a native, high-performance command-line interface:

```bash
# Connect interactively
stdsql -h 127.0.0.1 -p 5432 -U admin -d stratos

# Execute command non-interactively
echo "SELECT 1;" | stdsql -h 127.0.0.1 -p 5432 -U admin -d stratos
```

---

## 6. Official PostgreSQL Command-line Client (`psql`)

```bash
psql -h 127.0.0.1 -p 5432 -U admin -d stratos
```
All standard meta-commands (`\dt`, `\d table`, `\l`, `\dn`, `\du`) are fully supported.
