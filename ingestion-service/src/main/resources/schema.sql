-- Metrics storage owned by ingestion-service (the sole writer of the metrics hypertable).
-- Run on every startup via `spring.sql.init.mode: always`; every statement is idempotent, so
-- re-running the app never fails or duplicates structure. Mirrors the pattern where each service
-- owns its schema (topology-service creates its AGE graph in its seed loader, not in compose init).

-- Serialise concurrent replicas. `IF NOT EXISTS` is idempotent but NOT concurrency-safe: two pods
-- starting together (the normal case under the HPA this service scales with) can collide inside
-- CREATE TABLE / CREATE INDEX / create_hypertable and fail with `relation already exists` or a
-- pg_type unique violation, which aborts context refresh and kills the pod. Spring runs this whole
-- script on a single connection, so a session-level advisory lock spans every statement below and
-- is released at the end -- or by the connection closing, if startup fails partway.
SELECT pg_advisory_lock(91024001);

-- Raw time-series metrics from the `city-telemetry` Kafka topic.
CREATE TABLE IF NOT EXISTS metrics (
    timestamp   TIMESTAMPTZ      NOT NULL,
    sensor_id   TEXT             NOT NULL,
    metric_type TEXT             NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    status      TEXT             NOT NULL
);

-- Time-partition the table. `if_not_exists => TRUE` makes this a no-op after the first boot.
SELECT create_hypertable('metrics', 'timestamp', if_not_exists => TRUE);

-- Natural key for the Idempotent Consumer pattern: a redelivered Kafka message collapses to one row
-- via `INSERT ... ON CONFLICT DO NOTHING`. TimescaleDB requires a unique index to include the
-- partitioning column (`timestamp`), which this does.
CREATE UNIQUE INDEX IF NOT EXISTS metrics_uq ON metrics (timestamp, sensor_id, metric_type);

SELECT pg_advisory_unlock(91024001);
