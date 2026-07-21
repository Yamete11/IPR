# Single PostgreSQL instance hosting BOTH extensions the architecture calls for:
#   - Apache AGE      (graph / topology)      -> already compiled into this base image
#   - TimescaleDB     (time-series metrics)   -> added below from Timescale's apt repo
#
# Base is the official Apache AGE image for PostgreSQL 16 (which is itself built
# FROM postgres:16-bookworm), so AGE is present and we only layer TimescaleDB on top.
FROM apache/age:release_PG16_1.6.0

USER root

# TimescaleDB for PG16 from the official Timescale apt repository.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends gnupg wget ca-certificates; \
    . /etc/os-release; \
    echo "deb https://packagecloud.io/timescale/timescaledb/debian/ ${VERSION_CODENAME} main" \
        > /etc/apt/sources.list.d/timescaledb.list; \
    wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey \
        | gpg --dearmor -o /etc/apt/trusted.gpg.d/timescaledb.gpg; \
    apt-get update; \
    apt-get install -y --no-install-recommends timescaledb-2-postgresql-16; \
    rm -rf /var/lib/apt/lists/*

# First-boot bootstrap (runs only when the data volume is empty).
COPY init/ /docker-entrypoint-initdb.d/

USER postgres
