# StratosDB Kubernetes Helm Chart

Production-ready Helm chart to deploy a High-Availability StratosDB 1.0 cluster on Kubernetes with automated streaming replication, consensus-driven failover watchdog, and PostgreSQL wire protocol compatibility.

---

## Architecture Overview

```
                      +-----------------------------+
                      |   Kubernetes Client Apps    |
                      +-----------------------------+
                         |                       |
        Read-Write (5432)|                       | Read-Only (5432)
                         v                       v
            +------------------------+  +------------------------+
            |  stratosdb-primary Svc |  |  stratosdb-replica Svc |
            +------------------------+  +------------------------+
                         |                       |
                         v                       v
               +------------------+     +------------------+
               | Primary Pod (0)  |==+  | Replica Pod (0)  |
               | (Read-Write)     |  |  | (Read-Only)      |
               +------------------+  |  +------------------+
                         |           |           |
       Consensus Monitor |           |           | Base Backup /
       & Failover Heartbeat          | WAL Stream| Streaming WAL
                         v           |           v
               +------------------+  |  +------------------+
               | stratos-watchdog |  +=>| Replica Pod (1)  |
               | (Raft Watchdog)  |     | (Read-Only)      |
               +------------------+     +------------------+
```

### Components Deployed
1. **Primary StatefulSet**: Runs 1 active read-write StratosDB instance with persistent volume storage (PVC).
2. **Replica StatefulSet**: Runs $N$ read-only replicas configured for streaming replication. Replicas automatically bootstrap via `std-basebackup` upon first startup.
3. **Headless Service**: Provides stable network identifiers (`pod-name.headless-svc`) for internal node-to-node replication and consensus discovery.
4. **Primary Service (ClusterIP)**: Routes read-write traffic on port `5432` (stdwire) and `5431` (SWP) to the primary node.
5. **Replica Service (ClusterIP)**: Load-balances read queries on port `5432` across all healthy replicas.
6. **Watchdog Deployment**: StratosHa consensus watchdog monitoring primary health and coordinating failover.

---

## Quickstart Installation

### 1. Add and Install Chart
```bash
helm install my-stratos deploy/helm/stratosdb \
  --set auth.database=mydb \
  --set auth.username=admin \
  --set auth.password="YourSecurePassword123" \
  --set replica.replicaCount=2
```

### 2. Verify Deployment
```bash
# Check Pod statuses
kubectl get pods -l "app.kubernetes.io/name=stratosdb"

# Expected output:
# my-stratos-primary-0      1/1     Running   0          60s
# my-stratos-replica-0      1/1     Running   0          45s
# my-stratos-replica-1      1/1     Running   0          30s
# my-stratos-watchdog-...   1/1     Running   0          60s
```

### 3. Connect via Primary Service
```bash
# Port-forward to primary
kubectl port-forward svc/my-stratos-primary 5432:5432

# Connect using standard psql or Stratos CLI
psql -h 127.0.0.1 -p 5432 -U admin -d mydb
```

---

## Configuration Parameters

| Parameter | Description | Default |
| :--- | :--- | :--- |
| `image.repository` | StratosDB container image repository | `stratosdb` |
| `image.tag` | Container image tag | `1.0` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `auth.database` | Default database name | `stratos` |
| `auth.username` | Superuser username | `admin` |
| `auth.password` | Superuser password | `stratos_secret_password` |
| `auth.existingSecret` | Name of existing secret for credentials | `""` |
| `ports.stdwire` | PostgreSQL wire protocol compatibility port | `5432` |
| `ports.native` | Stratos native binary protocol port | `5431` |
| `ports.replication` | Physical streaming replication port | `5434` |
| `primary.replicaCount` | Number of primary pods | `1` |
| `primary.persistence.enabled` | Enable persistent storage for primary | `true` |
| `primary.persistence.size` | PVC disk size | `20Gi` |
| `primary.jvm.heapSize` | Primary JVM heap size (`-Xms` / `-Xmx`) | `3g` |
| `primary.jvm.extraOpts` | Extra JVM options (Generational ZGC) | `-XX:+UseZGC -XX:ZAllocationSpikeTolerance=4` |
| `replica.enabled` | Enable read replicas | `true` |
| `replica.replicaCount` | Number of replica pods | `2` |
| `replica.persistence.size` | PVC disk size for replicas | `20Gi` |
| `replica.jvm.heapSize` | Replica JVM heap size | `3g` |
| `watchdog.enabled` | Enable Raft failover watchdog | `true` |
| `watchdog.heartbeatIntervalMs` | Watchdog heartbeat interval in ms | `1000` |
| `watchdog.electionTimeoutMs` | Watchdog consensus election timeout in ms | `3000` |
| `service.primary.type` | Primary service Kubernetes type | `ClusterIP` |
| `service.replica.type` | Replica service Kubernetes type | `ClusterIP` |

---

## High Availability & Failover Workflow

1. **Initial Replication Setup**: When replica Pods launch, `docker/entrypoint.sh` detects an uninitialized data directory and automatically triggers `std-basebackup` against the primary headless service. Once the snapshot is loaded, the replica starts in read-only mode and attaches to the streaming replication WAL stream on port `5434`.
2. **Consensus Heartbeat**: The Raft watchdog continuously queries the primary node on port `5434`.
3. **Partition Detection & Promotion**: If the primary stops responding within `electionTimeoutMs` (3 seconds), the watchdog promotes the most up-to-date replica to primary, preventing split-brain without data corruption.

---

## Scaling Replicas

Scale the read capacity dynamically at runtime:
```bash
helm upgrade my-stratos deploy/helm/stratosdb --set replica.replicaCount=4
```
New replica pods will automatically clone the primary's state via networked base backup and begin serving queries through `my-stratos-replica`.
