# StratosDB 1.0 High Availability & Replication Guide

StratosDB 1.0 features enterprise-grade High Availability (HA) built on:
1. **Networked Physical Base Backup Protocol (`std-basebackup` / `BASE_BACKUP`)**
2. **Real-time Streaming WAL Replication over TCP (port 5434)**
3. **Distributed Raft Consensus Failover Watchdog (`stratos-ha`)** with zero single-point-of-failure split-brain risk.

---

## Architecture

```
                       +----------------------------+
                       | Application / Load Balancer|
                       +----------------------------+
                           |                     |
           Writes (Port 5432)                    | Reads (Port 5432)
                           v                     v
               +----------------------+   +----------------------+
               |    Primary Node      |   |     Replica Node     |
               | (Read-Write Mode)    |   |  (Read-Only Mode)    |
               +----------------------+   +----------------------+
                     |         ^                  ^
       Streaming WAL |         |                  | WAL Catchup &
          (Port 5434)|         | Heartbeat        | Base Backup
                     v         | (1000ms)         |
            +-------------------------------------------+
            |      Stratos-HA Raft Watchdog Cluster     |
            |      (Consensus Leader Election)          |
            +-------------------------------------------+
```

---

## 1. Networked Base Backup Protocol

When provisioning a new standby node, StratosDB eliminates the need for manual filesystem snapshots, rsync, or tape copies.

The primary exposes the PostgreSQL-compatible replication command `BASE_BACKUP` over the replication port (default `5434`):
1. **Checkpointing**: The primary issues a background checkpoint, flushing dirty buffer pool pages to disk.
2. **Non-blocking Snapshot Streaming**: The primary streams tar-formatted page files and schema catalogs over the network socket while simultaneously recording concurrent WAL changes.
3. **Replica Auto-Bootstrap**: The replica receives the stream, unpacks it into its local `$STRATOS_DATA_DIR`, marks itself as `read-only`, and automatically transitions into streaming replication mode.

### Running a Base Backup Manually:
```bash
std-basebackup \
  -h primary.stratosdb.internal \
  -p 5434 \
  -U admin \
  -D /var/lib/stratosdb/data \
  -Ft -z -P
```

---

## 2. Streaming WAL Replication

Once bootstrapped, the standby replica establishes a persistent streaming replication connection to the primary:

- **Asynchronous & Semi-Synchronous Options**: Replicas stream WAL segments as records are committed to the primary WAL ring buffer.
- **Microsecond Replication Lag**: Under a 10,000 TPS write load, physical replication lag stays under 5ms over local datacenter networks.
- **Fail-Safe Read Consistency**: Queries on replicas read MVCC snapshots consistent with the latest applied WAL sequence number (LSN).

### Configuring a Replica via CLI or Environment:
```bash
# Using CLI flag:
stratosdb --data-dir=/var/lib/stratosdb/data \
          --replica-of=primary.stratosdb.internal:5434 \
          --stdwire-port=5432

# Or via Environment Variable:
export STRATOS_CLUSTER_MODE=replica
export STRATOS_REPLICA_OF=primary.stratosdb.internal:5434
export STRATOS_STDWIRE_PORT=5432
stratosdb
```

---

## 3. Raft Consensus Failover Watchdog (`stratos-ha`)

To prevent dual-master "split-brain" scenarios during datacenter network partitions, StratosDB uses a Raft consensus watchdog:

### How Consensus Failover Works:
1. **Heartbeat**: The watchdog cluster exchanges heartbeats with the primary every `1000ms`.
2. **Partition Detection**: If the primary misses heartbeats exceeding the `electionTimeout` (default `3000ms`), the Raft watchdog initiates an election.
3. **Quorum Lease**: A replica cannot be promoted without obtaining a majority quorum vote from the Raft cluster, guaranteeing that a partitioned primary is isolated before promotion.
4. **Sub-3-Second Promotion**:
   - The promoted replica stops read-only mode and transitions to read-write primary.
   - DNS or Kubernetes Service endpoints update to route writes to the new primary.
   - Total failover time: **< 2.4 seconds**.

### Running the Watchdog:
```bash
stratos-ha \
  --primary=primary.stratosdb.internal:5434 \
  --nodes=replica-1.stratosdb.internal:5434,replica-2.stratosdb.internal:5434 \
  --heartbeat=1000 \
  --election-timeout=3000
```

---

## 4. Disaster Recovery & Failback

When the partitioned or failed primary returns:
1. It detects that its lease has expired and refuses to accept write transactions.
2. The operator (or automated orchestrator) restarts it pointing to the current primary:
   ```bash
   stratosdb --data-dir=/var/lib/stratosdb/data \
             --replica-of=new-primary.stratosdb.internal:5434
   ```
3. The node rewinds uncommitted divergence (if any) and catches up via streaming WAL, resuming its role as a healthy read replica.
