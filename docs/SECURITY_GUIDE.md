# StratosDB 1.0 Security & Authentication Guide

StratosDB 1.0 provides multi-layered enterprise defense, featuring cryptographic authentication, MITM-resistant TLS channel binding, hierarchical role-based access control (RBAC), and Row-Level Security (RLS).

---

## 1. Cryptographic Authentication

StratosDB supports industry-standard authentication methods:

| Method | Security Level | Standards | Description |
| :--- | :--- | :--- | :--- |
| `scram-sha-256-plus` | Maximum (Default for TLS) | RFC 5802 / RFC 7677 / RFC 5929 | Cryptographic channel binding preventing MITM interception |
| `scram-sha-256` | High | RFC 5802 / RFC 7677 | Salted challenge-response authentication with 4096 iterations |
| `md5` | Legacy (Optional) | RFC 1321 | Supported for backwards compatibility with legacy client tools |

### SCRAM-SHA-256-PLUS Channel Binding
When connecting over TLS, `SCRAM-SHA-256-PLUS` binds the authentication exchange to the TLS channel parameters:
- **`tls-server-end-point`**: Incorporates the SHA-256 hash of the server's TLS certificate into the client proof.
- **`tls-unique`**: Incorporates the TLS session finished message bytes.
Even if a malicious TLS proxy decrypts and re-encrypts the transport layer, channel binding fails because the channel signature does not match the server's cryptographic identity.

---

## 2. TLS 1.3 Transport Encryption

StratosDB supports TLS 1.2 and TLS 1.3 with strict modern cipher suites:
- `TLS_AES_256_GCM_SHA384`
- `TLS_CHACHA20_POLY1305_SHA256`
- `TLS_AES_128_GCM_SHA256`

### Server TLS Configuration
Provide the server PKCS12 or JKS keystore via CLI or environment:
```bash
stratosdb --data-dir=/var/lib/stratosdb/data \
          --ssl=on \
          --ssl-cert=/etc/stratosdb/server.crt \
          --ssl-key=/etc/stratosdb/server.key \
          --ssl-ciphers=HIGH:!aNULL:!MD5
```

### Client SSL Modes
JDBC and PostgreSQL wire clients can configure SSL mode:
- `sslmode=disable`: Plaintext TCP.
- `sslmode=require`: Enforce TLS encryption without certificate validation.
- `sslmode=verify-ca`: Enforce TLS and verify CA certificate chain.
- `sslmode=verify-full`: Enforce TLS, verify CA chain, and validate server hostname (SAN/CN).

---

## 3. Role-Based Access Control (RBAC) & Hierarchy

StratosDB implements full multi-tier role inheritance compliant with ANSI SQL:

```
                      +------------------+
                      |    superadmin    |
                      +------------------+
                               |
                               v
                      +------------------+
                      |      dba         |
                      +------------------+
                               |
                               v
                      +------------------+
                      |    data_analyst  |
                      +------------------+
                               |
                               v
                      +------------------+
                      |  alice (User)    |
                      +------------------+
```

### Creating Roles & Granting Permissions:
```sql
-- 1. Create specialized roles
CREATE ROLE reader;
CREATE ROLE writer;
CREATE ROLE data_analyst;

-- 2. Grant privileges to roles
GRANT CONNECT ON DATABASE production TO reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;

GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO writer;

-- 3. Configure hierarchical role inheritance
GRANT reader TO writer;
GRANT writer TO data_analyst;

-- 4. Assign role to human or application users
CREATE USER alice WITH PASSWORD 'SecurePassword123!';
GRANT data_analyst TO alice;

-- Alice automatically inherits permissions of data_analyst, writer, and reader.
```

### Revoking Roles and Privileges:
```sql
REVOKE writer FROM alice;
REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM reader;
```

---

## 4. Row-Level Security (RLS)

StratosDB allows fine-grained security policies directly at the row level:

```sql
-- Enable RLS on sensitive table
ALTER TABLE customer_orders ENABLE ROW LEVEL SECURITY;

-- Create policy restricting users to only view orders from their tenant
CREATE POLICY tenant_isolation_policy ON customer_orders
  FOR ALL
  USING (tenant_id = CURRENT_USER_TENANT());
```

---

## 5. Security Hardening Checklist

- [x] Run StratosDB as unprivileged system user (`stratos` UID 1001).
- [x] Enforce `scram-sha-256-plus` channel binding for all external network connections.
- [x] Restrict file permissions: `chmod 700 /var/lib/stratosdb/data`.
- [x] Never use plaintext passwords in configuration files; use environment variables or Kubernetes Secrets.
- [x] Separate administrative access from application read/write roles.
