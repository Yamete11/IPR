-- Metrics storage owned by ingestion-service (the sole writer of the metrics hypertable).
-- Run on every startup via `spring.sql.init.mode: always`; every statement is idempotent, so
-- re-running the app never fails or duplicates structure. Mirrors the pattern where each service
-- owns its schema (topology-service creates its AGE graph in its seed loader, not in compose init).

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
