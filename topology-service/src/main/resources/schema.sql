-- Relational side of age-db owned by topology-service: repair crews and the assets they maintain.
-- Joined against Cypher results in TopologyRepository.findResponsibleCrews (hybrid SQL + Cypher).
-- Run on every startup via `spring.sql.init.mode: always`; every statement is idempotent.
-- Schema-qualified: the connection's search_path starts with ag_catalog (see application.yaml).

CREATE TABLE IF NOT EXISTS public.repair_crews (
    crew_id TEXT PRIMARY KEY,
    name    TEXT NOT NULL,
    phone   TEXT
);

-- asset_id is a graph vertex id (the `id` property); there is no FK because the graph is not a table.
CREATE TABLE IF NOT EXISTS public.crew_assignments (
    crew_id  TEXT NOT NULL REFERENCES public.repair_crews (crew_id),
    asset_id TEXT NOT NULL,
    PRIMARY KEY (crew_id, asset_id)
);

CREATE INDEX IF NOT EXISTS crew_assignments_asset_idx ON public.crew_assignments (asset_id);
