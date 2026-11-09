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

-- Root-cause correlation over the AGE graph (IncidentCorrelator). Added after the first release, so
-- they are ALTERs: a table created by an older build gains them on the next start.
--   asset_id         the graph asset the alert's sensor monitors (NULL: sensor not in the graph)
--   root_incident_id the root incident this one is a secondary effect of (NULL: it is a root)
--   detected_at      the alert's detection time; the correlation window is measured on it
--   crews            repair crews for the impacted assets, resolved from the graph (JSON array)
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS asset_id TEXT;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS root_incident_id TEXT;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS detected_at TIMESTAMPTZ;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS crews JSONB NOT NULL DEFAULT '[]'::jsonb;

-- findActiveRoot: root incidents on a set of assets within a detected_at window.
CREATE INDEX IF NOT EXISTS incidents_asset_detected_idx ON incidents (asset_id, detected_at DESC);
-- linkedIncidents (incident-api) and adopt: the children of a root.
CREATE INDEX IF NOT EXISTS incidents_root_idx ON incidents (root_incident_id);
