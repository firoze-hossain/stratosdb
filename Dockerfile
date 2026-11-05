# ==============================================================================
# StratosDB 1.0 Production Dockerfile
# OpenJDK 25 LTS Runtime with Generational ZGC & Off-Heap Direct Buffer Pool
# ==============================================================================

FROM openjdk:25-ea-slim

LABEL org.opencontainers.image.title="StratosDB" \
      org.opencontainers.image.description="PostgreSQL-compatible Relational Database Engine in Java 25 with Zero-Pause Generational ZGC" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.vendor="StratosDB Project" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.authors="Firoze Hossain <firoze.hossain@outlook.com>"

ENV STRATOS_HOME=/opt/stratosdb \
    STRATOS_DATA_DIR=/var/lib/stratosdb/data \
    STRATOS_CONF=/etc/stratosdb/stratosdb.conf \
    STRATOS_PORT=5431 \
    STRATOS_STDWIRE_PORT=5432 \
    PATH=/opt/stratosdb/bin:$PATH \
    JAVA_OPTS="-XX:+UseZGC -XX:+AlwaysPreTouch -Xms512m -Xmx2g -Djdk.attach.allowAttachSelf=true"

# Install network diagnostic and monitoring utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        procps \
        netcat-openbsd \
        curl \
        bash && \
    rm -rf /var/lib/apt/lists/*

# Setup application directories and dedicated unprivileged user
RUN groupadd -g 1001 stratos && \
    useradd -u 1001 -g stratos -m -s /bin/bash stratos && \
    mkdir -p /opt/stratosdb/bin \
             /opt/stratosdb/lib \
             /etc/stratosdb \
             /var/lib/stratosdb/data \
             /var/log/stratosdb

# Copy compiled standalone jars
COPY stratosdb-network/target/stratosdb-network-1.0.0-SNAPSHOT-standalone.jar /opt/stratosdb/lib/stratosdb-server.jar
COPY stratosdb-cli/target/stratosdb-cli-1.0.0-SNAPSHOT-standalone.jar /opt/stratosdb/lib/stratosdb-cli.jar

# Copy default production configuration
COPY packaging/linux/stratosdb.conf /etc/stratosdb/stratosdb.conf

# Create CLI executable wrapper scripts
RUN echo '#!/usr/bin/env bash\nexec java $JAVA_OPTS -jar /opt/stratosdb/lib/stratosdb-server.jar "$@"' > /opt/stratosdb/bin/stratosdb-server && \
    echo '#!/usr/bin/env bash\nexec java -jar /opt/stratosdb/lib/stratosdb-cli.jar "$@"' > /opt/stratosdb/bin/stdsql && \
    echo '#!/usr/bin/env bash\nexec java -cp /opt/stratosdb/lib/stratosdb-cli.jar com.stratosdb.cli.StratosDump "$@"' > /opt/stratosdb/bin/stratosdump && \
    echo '#!/usr/bin/env bash\nexec java -cp /opt/stratosdb/lib/stratosdb-cli.jar com.stratosdb.cli.StratosHa "$@"' > /opt/stratosdb/bin/stratos-ha && \
    echo '#!/usr/bin/env bash\nexec java -cp /opt/stratosdb/lib/stratosdb-cli.jar com.stratosdb.cli.StdBaseBackup "$@"' > /opt/stratosdb/bin/stratos-backup && \
    ln -sf /opt/stratosdb/bin/stratosdb-server /opt/stratosdb/bin/stratosdb && \
    chmod +x /opt/stratosdb/bin/*

# Copy entrypoint script
COPY docker/entrypoint.sh /usr/local/bin/stratos-entrypoint.sh
RUN chmod +x /usr/local/bin/stratos-entrypoint.sh

# Set correct filesystem ownership
RUN chown -R stratos:stratos /opt/stratosdb \
                            /etc/stratosdb \
                            /var/lib/stratosdb \
                            /var/log/stratosdb

# Volumes for persistent database storage
VOLUME ["/var/lib/stratosdb/data"]

# Expose ports:
# 5432 - Standard PostgreSQL v3 wire protocol (psql, pgjdbc, Hibernate, Prisma)
# 5431 - Stratos Wire Protocol SWP v1.0 (native driver, DBNavigator)
# 5434 - Streaming Physical Replication (WAL shipping & networked base backup)
EXPOSE 5432 5431 5434

# Container Healthcheck (readiness probe on wire port 5432)
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=5 \
    CMD nc -z 127.0.0.1 5432 || exit 1

# Run as unprivileged stratos user for defense in depth
USER stratos
WORKDIR /var/lib/stratosdb

ENTRYPOINT ["/usr/local/bin/stratos-entrypoint.sh"]
CMD ["stratosdb"]
