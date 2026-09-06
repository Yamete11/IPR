-- Metrics storage owned by ingestion-service (the sole writer of the metrics hypertable).
-- Run on every startup via `spring.sql.init.mode: always`; every statement is idempotent, so
-- re-running the app never fails or duplicates structure. Mirrors the pattern where each service
-- owns its schema (topology-service creates its AGE graph in its seed loader, not in compose init).
--
-- No `DO $$ ... $$` blocks: Spring's script splitter does not understand dollar quoting.

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

-- Time-partition the table into 1-day chunks. ~1000 sensors at a few readings per second is
-- tens of millions of rows a day, which keeps each chunk (and its indexes) inside memory-friendly
-- bounds; TimescaleDB's 7-day default would be an order of magnitude too coarse for that rate.
-- `if_not_exists => TRUE` makes this a no-op after the first boot, so the interval is re-asserted
-- separately below to converge hypertables created by older builds.
SELECT create_hypertable('metrics', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT set_chunk_time_interval('metrics', INTERVAL '1 day');

-- Natural key for the Idempotent Consumer pattern: a redelivered Kafka message collapses to one row
-- via `INSERT ... ON CONFLICT DO NOTHING`. TimescaleDB requires a unique index to include the
-- partitioning column (`timestamp`), which this does.
CREATE UNIQUE INDEX IF NOT EXISTS metrics_uq ON metrics (timestamp, sensor_id, metric_type);

-- Read path used by every consumer of this table: the anomaly detector's aggregate, the incident
-- responder's last-hour summary (`sensor_id = ? AND metric_type = ? AND timestamp >= ?`) and
-- incident-api's recent readings (`sensor_id = ? ORDER BY timestamp DESC`). One composite index
-- with the series columns leading serves all three; `metrics_uq` leads with timestamp and cannot.
CREATE INDEX IF NOT EXISTS metrics_sensor_metric_ts ON metrics (sensor_id, metric_type, timestamp DESC);

-- Columnar compression for history. Segmenting by series keeps each compressed batch one sensor's
-- one metric, which is exactly how the table is read; ordering by time inside the segment gives the
-- best delta encoding. Re-applying identical settings is a no-op; changed settings apply to chunks
-- compressed from then on.
ALTER TABLE metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_id, metric_type',
    timescaledb.compress_orderby   = 'timestamp DESC'
);

-- Background policies (TimescaleDB job scheduler). Chunks older than 7 days are compressed; chunks
-- older than 30 days are dropped -- the demo keeps a month of raw history, and the 5-minute
-- continuous aggregate (owned by anomaly-detector) is what long-range reads should hit anyway.
SELECT add_compression_policy('metrics', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('metrics', INTERVAL '30 days', if_not_exists => TRUE);

SELECT pg_advisory_unlock(91024001);
