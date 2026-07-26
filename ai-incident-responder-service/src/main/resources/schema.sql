-- Incidents storage owned by ai-incident-responder-service (the sole writer of this table).
-- Lives in TimescaleDB alongside the metrics it references. Run on every startup via
-- `spring.sql.init.mode: always` against the primary (TimescaleDB) datasource; every statement is
-- idempotent, so re-running the app never fails or duplicates structure.

CREATE TABLE IF NOT EXISTS incidents (
    incident_id       TEXT PRIMARY KEY,
    -- The originating alert. UNIQUE makes the consumer idempotent: a redelivered
    -- `infrastructure-alerts` message collapses to one row via INSERT ... ON CONFLICT DO NOTHING,
    -- and the responder skips the (expensive) Claude call when a row already exists.
    alert_id          TEXT             NOT NULL UNIQUE,
    sensor_id         TEXT             NOT NULL,
    metric_type       TEXT,
    -- Structured remediation plan returned by Claude.
    criticality       TEXT             NOT NULL,
    impacted_objects  JSONB            NOT NULL,
    remediation_steps JSONB            NOT NULL,
    summary           TEXT,
    -- The Claude model that produced the plan (provenance).
    model             TEXT,
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT now()
);
