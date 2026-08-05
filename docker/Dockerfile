# Workshop toolbox image.
#
# Bundles the three tools the labs need — rpk, redpanda-connect, and psql —
# into one container so students never install anything but Docker itself.
#
# Built automatically by `docker compose build workbench`.
FROM debian:bookworm-slim

# Pinned so every student gets the same Connect build. postgres_cdc (Lab 2)
# needs >= 4.102, so do not pin lower.
ARG REDPANDA_CONNECT_VERSION=4.102.0

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      unzip \
      postgresql-client \
 && rm -rf /var/lib/apt/lists/*

# rpk — the Redpanda CLI. Arch-aware so the image builds on both Apple
# Silicon (arm64) and Intel/AMD (amd64) machines.
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    curl -fsSL "https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-${arch}.zip" -o /tmp/rpk.zip; \
    unzip -o /tmp/rpk.zip -d /usr/local/bin; \
    rm /tmp/rpk.zip; \
    chmod +x /usr/local/bin/rpk; \
    rpk version

# redpanda-connect — the standalone stream processor used by both labs.
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    curl -fsSL "https://github.com/redpanda-data/connect/releases/download/v${REDPANDA_CONNECT_VERSION}/redpanda-connect_${REDPANDA_CONNECT_VERSION}_linux_${arch}.tar.gz" -o /tmp/rc.tgz; \
    mkdir -p /tmp/rc; \
    tar -xzf /tmp/rc.tgz -C /tmp/rc; \
    install -m 0755 "$(find /tmp/rc -type f -name redpanda-connect | head -1)" /usr/local/bin/redpanda-connect; \
    rm -rf /tmp/rc /tmp/rc.tgz; \
    redpanda-connect --version

WORKDIR /workshop
