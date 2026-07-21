-- Runs once, on first initialization of an empty data volume.
-- Enables both extensions inside the single application database.
--
-- Deliberately does NOT create the AGE graph or the metrics hypertable:
--   * the `city_topology` graph is owned/created by topology-service (sole writer);
--   * the `metrics` hypertable is created by ingestion-service.
-- This file only turns the extensions on.

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS age;
