# StratosDB 1.0 Production Deployment Guide

This guide details enterprise-grade production deployment best practices for StratosDB 1.0, covering OS kernel tuning, storage engine configuration, JVM Generational ZGC runtime tuning, Systemd service orchestration, and automated backup operations.

---

## 1. System Requirements & Hardware Sizing

### Minimum vs. Recommended Hardware

| Resource | Minimum | Recommended Production | High-Throughput (TPC-C / OLTP) |
| :--- | :--- | :--- | :--- |
| **CPU** | 2 Cores | 8–16 Cores | 32–64 Cores (AMD EPYC / Intel Xeon) |
| **RAM** | 4 GB | 16–32 GB | 64–128 GB (ECC DDR5) |
| **Disk Storage** | 20 GB SSD | 500 GB NVMe SSD (PCIe 4.0) | Multi-TB NVMe RAID-10 (Direct I/O) |
| **Network** | 1 GbE | 10 GbE | 25/100 GbE (Bonded) |
| **OS** | Linux (Kernel 5.15+) | Ubuntu 24.04 LTS / RHEL 9 (Kernel 6.8+) | Linux with XFS / ext4 mount options |

### Storage Subsystem Configuration
- **Filesystem**: XFS or ext4. Format XFS with 4KB block size:
  ```bash
  mkfs.xfs -b size=4096 /dev/nvme0n1
  ```
- **Mount Options**: Mount with `noatime` and `nodiratime` to eliminate disk write overhead on page reads:
  ```fstab
  UUID=... /var/lib/stratosdb xfs defaults,noatime,nodiratime,allocsize=64m 0 2
  ```
- **Disk I/O Scheduler**: Use `mq-deadline` or `none` for NVMe drives. Avoid `bfq` for server workloads:
  ```bash
  echo "none" | sudo tee /sys/block/nvme0n1/queue/scheduler
  ```

---

## 2. Linux Kernel & OS Tuning (`sysctl`)

Create `/etc/sysctl.d/99-stratosdb.conf` with the following parameters:

```ini
# Maximum number of memory map areas for direct memory & buffer pool allocations
vm.max_map_count = 1048576

# Maximum open file descriptors system-wide
fs.file-max = 2097152

# TCP socket backlog queue size for high concurrent client connections
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 16384

# Enable rapid TCP port reuse for high connection churn
net.ipv4.tcp_tw_reuse = 1
net.ipv4.ip_local_port_range = 1024 65535

# Minimize swapping while keeping emergency swap headroom
vm.swappiness = 1

# Disable memory overcommit aggressive heuristics
vm.overcommit_memory = 0

# Network buffer memory sizing
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
```

Apply immediately without reboot:
```bash
sudo sysctl --system
```

### User Limits (`/etc/security/limits.d/99-stratosdb.conf`)
Ensure the `stratos` system user has sufficient open file handles and process limits:
```ini
stratos  soft  nofile  65536
stratos  hard  nofile  1048576
stratos  soft  nproc   32768
stratos  hard  nproc   65536
stratos  soft  memlock unlimited
stratos  hard  memlock unlimited
```

---

## 3. Dedicated User & Directory Layout

Create the dedicated system user and runtime directories:
```bash
# Create service group and user
sudo groupadd -g 1001 stratos
sudo useradd -u 1001 -g stratos -s /sbin/nologin -d /var/lib/stratosdb stratos

# Directory layout
sudo mkdir -p /var/lib/stratosdb/data \
              /var/lib/stratosdb/wal \
              /var/log/stratosdb \
              /etc/stratosdb

sudo chown -R stratos:stratos /var/lib/stratosdb /var/log/stratosdb /etc/stratosdb
sudo chmod 700 /var/lib/stratosdb /var/lib/stratosdb/data
```

---

## 4. Production JVM Tuning (Generational ZGC)

StratosDB 1.0 is engineered for **sub-millisecond P99 latency** with zero stop-the-world pauses over 2ms.
Java 21+ Generational ZGC is mandatory for production workloads.

### Recommended JVM Flags:
```bash
STRATOS_JAVA_OPTS="\
  -XX:+UseZGC \
  -XX:+AlwaysPreTouch \
  -XX:ZAllocationSpikeTolerance=4 \
  -Xms16g \
  -Xmx16g \
  -XX:+UnlockDiagnosticVMOptions \
  -XX:GuaranteedSafepointInterval=0 \
  -Djava.awt.headless=true \
  -Djdk.attach.allowAttachSelf=true \
  -Xlog:gc*,gc+phases=info:/var/log/stratosdb/gc.log:time,uptime,pid:filecount=5,filesize=64M"
```

### Rationale:
- `-XX:+UseZGC`: Enables concurrent generational ZGC (Java 24/25 default).
- `-XX:+AlwaysPreTouch`: Zeroes physical pages during server bootstrap to eliminate runtime page fault latency during peak load.
- `-XX:ZAllocationSpikeTolerance=4`: Provides headroom for rapid batch inserts and burst write transactions.
- `-Xms = -Xmx`: Locks heap allocation to prevent runtime heap resizing pauses.

---

## 5. Systemd Service Deployment

Create `/etc/systemd/system/stratosdb.service`:

```ini
[Unit]
Description=StratosDB High-Performance Relational Database
Documentation=https://github.com/firoze-hossain/stratosdb
After=network.target network-online.target local-fs.target
Wants=network-online.target

[Service]
Type=simple
User=stratos
Group=stratos
WorkingDirectory=/var/lib/stratosdb

Environment=STRATOS_DATA_DIR=/var/lib/stratosdb/data
Environment=STRATOS_PORT=5431
Environment=STRATOS_STDWIRE_PORT=5432
Environment=STRATOS_REPLICATION_PORT=5434
Environment=STRATOS_CLUSTER_MODE=primary
Environment=STRATOS_JAVA_OPTS="-XX:+UseZGC -XX:+AlwaysPreTouch -Xms16g -Xmx16g"

ExecStart=/usr/bin/java $STRATOS_JAVA_OPTS \
  -jar /usr/share/stratosdb/stratosdb-network.jar \
  --data-dir ${STRATOS_DATA_DIR} \
  --port ${STRATOS_PORT} \
  --stdwire-port ${STRATOS_STDWIRE_PORT} \
  --replication-port ${STRATOS_REPLICATION_PORT}

ExecReload=/bin/kill -HUP $MAINPID
KillMode=mixed
KillSignal=SIGTERM
TimeoutStopSec=45
Restart=on-failure
RestartSec=5s

# Security hardening
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
LimitNOFILE=1048576
LimitNPROC=65536

[Install]
WantedBy=multi-user.target
```

Enable and start the service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now stratosdb
sudo systemctl status stratosdb
```

---

## 6. Backup & Disaster Recovery

### Online Physical Base Backup (`std-basebackup`)
To take an online non-blocking base backup of a live primary:
```bash
std-basebackup \
  -h 127.0.0.1 \
  -p 5434 \
  -U admin \
  -D /backup/stratosdb-$(date +%Y%m%d_%H%M%S) \
  -Ft -z
```

### Logical Backup & Restore (`stratosdump` / `stdsql`)
```bash
# Logical full database backup
stratosdump -h 127.0.0.1 -p 5432 -U admin -d stratos > /backup/backup_stratos.sql

# Restore into target cluster
stdsql -h 127.0.0.1 -p 5432 -U admin -d stratos < /backup/backup_stratos.sql
```
