# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working rule: Claude may write code

Claude **may author and edit source code** in this repository. The user lifted the earlier "user writes all the code" restriction on 2026-07-23, so Claude can now scaffold projects, write classes/config, and fix files as usual — while still advising (explaining approaches/APIs, pointing at docs, reviewing against a checklist) and verifying (running code, tests, and tooling; reporting results).

Follow the design documents and conventions below; when a change is large or ambiguous, confirm the approach before writing a lot of code.

## Project status: all six services implemented; Stage 4 (Kubernetes) packaged, cluster run pending

This repository is design documents plus **six implemented services** (five standalone Gradle projects on Java 25 / Spring Boot 4.1.0, plus the Python simulator). A requirements audit on 2026-09-06 found Stages 1–3 complete and Stage 4 (Docker/Helm/K8s) not started; the remediation work is tracked below and in the root `README.md`.

- **`topology-service/`** — AGE graph write (seed loader) + GraphQL read API (`vertices`/`edges`/`dependents`/`dependencies`); done and verified against `age-db`. Added 2026-10-25 to cover the AGE learning topics: `impactSummary` (Cypher `count`/`sum` aggregation), `paths` (`MATCH p = ...`, `nodes(p)` parsed by `AgtypeParser.parseVertexList` — list comprehensions fail in AGE 1.6), `responsibleCrews` (hybrid: `cypher()` row source JOINed to relational `public.repair_crews`/`crew_assignments`, created by `schema.sql`/`data.sql` via `spring.sql.init` — schema-qualify, the search_path starts with `ag_catalog`). Testcontainers AGE tests (`TopologyRepositoryIntegrationTest`, incl. EXPLAIN proof the GIN index is used) and a `@Tag("benchmark")` test run by `./gradlew graphBenchmark` (excluded from `build`). Finding written up in `docs/graph-vs-sql.md`: AGE variable-length paths are ~100–300× slower than a recursive CTE, while a CTE over AGE's own `_ag_label_edge` tables is about as fast as a plain edge table — the VLE executor is the bottleneck, not storage; AGE creates no `start_id`/`end_id` indexes on edge tables.
- **`ingestion-service/`** — consumes Kafka `city-telemetry` → TimescaleDB `metrics` hypertable, idempotent (`ON CONFLICT`) + DLQ (`city-telemetry.DLQ`); done and verified end-to-end (2026-07-24). **Batch listener** (`spring.kafka.listener.type: batch`, one JDBC batch per poll via `MetricRepository.saveAll` + `reWriteBatchedInserts=true`; a poison record is isolated with `BatchListenerFailedException` so its neighbours persist once). `schema.sql` owns the hypertable tuning: 1-day chunks, `(sensor_id, metric_type, timestamp DESC)` index, compression segmented by series, 7-day compression + 30-day retention policies (asserted by the Testcontainers schema test). See `ingestion-service/TODO.md` for the step log.
- **`sensor-simulator/`** (Python 3.12) — load generator producing telemetry to `city-telemetry` (keyed by `sensor_id`); mean-reverting random walk per metric with injectable faults. Emits for the 3 graph-seeded sensors (`s-00042`/`s-00108`/`s-00311`) plus optional synthetic ones. Verified feeding the full simulator→Kafka→ingestion→TimescaleDB path.
- **`anomaly-detector-service/`** — owns the 5-min `metrics_5m_avg` continuous aggregate (created on startup via a JdbcTemplate `ApplicationRunner`, **`materialized_only = false`** so real-time aggregation folds in recent unmaterialized rows — the modern TimescaleDB default flipped to `true`); scheduled poll of TimescaleDB → threshold rules on min/max (**externalised** in `application.yaml` `anomaly.rules[]`, bound by `AnomalyRulesProperties`, validated by `ThresholdRuleRegistry`; alert `rule` label is derived as `<metric>_<min|max><op><threshold>`, e.g. `cable_temperature_max>120`) → produces critical events to Kafka `infrastructure-alerts` (edge-triggered dedup, producer-only, no DLQ). Done and verified end-to-end (2026-07-25): simulator→ingestion→TimescaleDB→detector→`infrastructure-alerts` emits one alert per fault (3/3), no re-fire on standing faults. See `anomaly-detector-service/TODO.md`.
- **`ai-incident-responder-service/`** — **built in Java** (the design docs say Python; user overrode on 2026-07-25 for consistency/reuse). Consumes `infrastructure-alerts`; gathers context (last-1h metric history from TimescaleDB + the dependency graph from AGE — the only service reading **both** Postgres instances, via two qualified `JdbcTemplate`s); calls the **Claude API** (official `com.anthropic:anthropic-java` SDK, default `claude-opus-5`, adaptive thinking, **structured output** typed to `IncidentPlan`) guarded by a Resilience4j-**core** circuit breaker; writes the `incidents` table (idempotent via `UNIQUE(alert_id)` + `ON CONFLICT`, DLQ for poison alerts). Code complete and `./gradlew build` green. **Verified end-to-end in mock mode** (2026-07-25): `AI_CLAUDE_MOCK=true` swaps the Claude call for a deterministic offline planner (`MockIncidentPlanner`; the `AnthropicClient` bean is `@ConditionalOnProperty` so no key needed) — alert→context(AGE+TimescaleDB)→incident→`incident-api` GraphQL all confirmed, 3 impacted objects resolved from the graph, idempotent. Only the **real Claude call is unexercised** (needs `ANTHROPIC_API_KEY`; billed). See `ai-incident-responder-service/TODO.md`.
- **`incident-api-service/`** (Java) — read-only **GraphQL** for the dashboard over the `incidents` table + TimescaleDB `metrics` (single datasource, owns no tables). Queries: `incidents`/`incident`/`incidentsBySensor`/`recentMetrics`, every list query takes a `limit` clamped to `[0, 1000]`; the two JSONB array columns are exposed as `[String!]` (parsed in the repo, no JSON scalar). Runs on **8081** (topology uses 8080). Done and verified (2026-07-25) via GraphiQL/`POST /graphql`. See `incident-api-service/TODO.md`.
- **`dashboard/`** (Angular **21**, added 2026-10-25) — Material UI over both GraphQL APIs: incidents list/detail, cytoscape topology graph with dependents highlighting, chart.js sensor charts. Browser calls same-origin `/api/topology/graphql` + `/api/incidents/graphql` — `proxy.conf.json` in dev, `nginx.conf.template` (env `TOPOLOGY_UPSTREAM`/`INCIDENT_API_UPSTREAM`) in the nginx-unprivileged image; no CORS in the backends. Detector thresholds are duplicated in `src/app/core/detector-rules.ts` — keep in sync with `anomaly.rules[]`. Angular 21 not 22 because the host has Node 22.20 (22 needs 22.22+); install with **npm 11** (`npx npm@11 ci`) — npm 10 crashes on vitest's peer deps. Verified: build, vitest, UI against a mock GraphQL, Docker image + nginx proxy, `helm lint`/`template`; **not yet against the real backends**.

All six designed services now exist in code. Do not invent build/lint/test commands for services that have no code. **Update this file as real code, tooling, and commands are added.**

Cross-cutting conventions added 2026-09-06:
- Every Java service has the web starter + actuator + `spring-boot-starter-micrometer-metrics` + Prometheus registry; `management.endpoints.web.exposure.include: health,info,metrics,prometheus`, probes enabled, readiness group = `readinessState,db`. Tests that exclude the datasource must set `management.endpoint.health.group.readiness.include=readinessState`.
- JDBC hosts: `${AGE_HOST:${POSTGRES_HOST:localhost}}` / `${TIMESCALE_HOST:${POSTGRES_HOST:localhost}}` so compose (one host, two ports) and Kubernetes (two Services) both work.
- `topology-service` creates a GIN index on each label's `properties` after seeding (`topology.seed.index-enabled`).
- `anomaly-detector-service` waits for the `metrics` hypertable before creating its continuous aggregate (`anomaly.schema.wait-timeout`).
- `ai-incident-responder-service` publishes the Claude circuit breaker as `resilience4j_circuitbreaker_*` meters (`resilience4j-micrometer`).

Build the Java services (first compile provisions JDK 25 via the foojay toolchain resolver):
```bash
cd topology-service && ./gradlew build
cd ingestion-service && ./gradlew build
cd anomaly-detector-service && ./gradlew build
cd ai-incident-responder-service && ./gradlew build   # needs ANTHROPIC_API_KEY only to run, not to build
cd incident-api-service && ./gradlew build
```

Run the sensor-simulator (Python):
```bash
cd sensor-simulator && python -m venv .venv && ./.venv/Scripts/python.exe -m pip install -r requirements.txt
./.venv/Scripts/python.exe sensor_simulator.py            # feed city-telemetry
```

Dashboard (Angular):
```bash
cd dashboard && npx npm@11 ci
npm start                       # :4200, proxies to :8080 / :8081
npm run build && npx ng test --watch=false
```

Local backing services (`docker-compose.yml`, repo root): `age-db` (5432), `timescaledb` (5433), `kafka` (KRaft, 9092). Run `ingestion-service` end-to-end with `docker compose up -d timescaledb kafka` then `cd ingestion-service && ./gradlew bootRun`.

**Stage 4 packaging (`deploy/`, added 2026-09-06):** every service has a multi-stage `Dockerfile` (`eclipse-temurin:25-jdk` build with the committed wrapper → `eclipse-temurin:25-jre` runtime, layered jar, `USER 1000:1000`); `deploy/charts/<service>` is one Helm chart per service (Deployment + Service + probes + `nc` init-container waits; ingestion adds a KEDA `ScaledObject` on `city-telemetry` consumer lag, max 6 = partition count); `deploy/charts/smartcity-infra` holds the two Postgres StatefulSets, the `smartcity-config` ConfigMap and the `postgres-credentials`/`anthropic` Secrets; `deploy/charts/smartcity` is the umbrella (file:// deps; run `helm dependency build` first — `deploy/scripts/up.*` does). Kafka is a Strimzi **1.x** `Kafka` + `KafkaNodePool` (API `kafka.strimzi.io/v1`, KRaft only) in `deploy/strimzi/`, topics as `KafkaTopic` CRs. Runbook: `deploy/README.md`; scripts: `deploy/scripts/{up,build-images,demo-hpa,down}.{ps1,sh}`. `deploy/charts/dashboard` (nginx, Service port 80) was added 2026-10-25. Verified so far: `helm lint` on all 8 charts, a full `helm template` render (20 resources), and Docker builds of `ingestion-service`, `topology-service` and `sensor-simulator` (the other three Dockerfiles are identical apart from the name). **Not yet verified:** a full Kind bring-up — the first attempt on 2026-09-06 was cut short because the 16 GB host was saturated by other workloads; re-run `deploy/scripts/up.ps1` on an idle machine.

> **Boot 4 note:** autoconfiguration is modularised — depend on `spring-boot-starter-<tech>` (e.g. `spring-boot-starter-kafka`), not the raw framework jar, or Boot ships no autoconfig for it (silent no-op).

These design documents are the source of truth; read them before making architectural decisions:

- `project_architecture_and_plan.md` (Russian) — system architecture and a 16-week, 4-stage implementation plan.
- `technology_stack_topics.md` (English) — the theory/learning topics behind each technology in the stack.
- `docs/microservices.md` — the finalized service decomposition, communication contracts, and message schemas.
- `docs/architecture.mmd` / `docs/incident-response-sequence.mmd` — Mermaid diagrams (component view + alert→remediation sequence).

## What this project is

A "Smart City" infrastructure **telemetry + AI incident-response** system. Simulated city sensors stream metrics; the system stores them, models the physical network as a graph, detects critical failures, and uses the Claude API to generate a structured incident-remediation plan for a repair crew.

## Target architecture / data flow

```
Python Sensor Simulator
        │  JSON metrics (high volume)
        ▼
Kafka topic: city-telemetry
        │
        ▼
Ingestion Service ──► TimescaleDB      (time-series metrics, hypertable)
        │
        │   Topology Service ──► PostgreSQL AGE   (network topology graph;
        │       ▲                                  seeded from a seed file)
        │   seed file
        │
   (anomaly detector fires on critical failures)
        ▼
Kafka topic: infrastructure-alerts
        │
        ▼
AI Worker (AI-Incident-Responder)
        │  pulls context: TimescaleDB history + AGE dependency graph
        ▼
   Claude API (Anthropic)  ──►  structured incident/remediation plan
        ▼
   Incidents table
```

Everything is deployed to a **local Kubernetes cluster** (Minikube or Kind). Kafka is run via an operator (**Strimzi**). Storage is split across **two PostgreSQL instances**, one per role, each on an off-the-shelf image: **`timescaledb`** (TimescaleDB — time-series metrics) and **`age-db`** (Apache AGE — the topology graph). See `docker-compose.yml` for the local-dev definition of both.

## Microservices (see `docs/microservices.md`)

Six services. The **pipeline** services communicate **asynchronously over Kafka**; `topology-service` and `incident-api` are **request/response GraphQL** for external clients. Backend services never call each other directly — shared state lives in the two PostgreSQL instances (`age-db` for the graph, `timescaledb` for metrics):

- **sensor-simulator** (Python) — generates telemetry → `city-telemetry` (`--extra-sensors 997` for the ~1000-sensor load profile).
- **ingestion-service** (Java) — consumes `city-telemetry`; writes **metrics only** → TimescaleDB; HPA on Kafka lag.
- **topology-service** (Java) — sole owner/writer of the **AGE** graph (assets + connections); seeds it from a seed file; **GraphQL** API for asset/topology management + the dashboard network view.
- **anomaly-detector** (Java) — polls TimescaleDB continuous aggregates/thresholds; emits critical events → `infrastructure-alerts`.
- **ai-incident-responder** (Java; the design says Python — overridden 2026-07-25) — consumes `infrastructure-alerts`; gathers context (TimescaleDB history + AGE graph, read directly); calls Claude API; writes the `incidents` table.
- **incident-api** (Java) — read-only **GraphQL** over `incidents` + metrics for the demo dashboard.

## Key conventions (decided in the plan — don't re-derive)

- **Language:** Java is the primary language (ingestion-service, topology-service, anomaly-detector, incident-api). Python is used where it fits better — the `sensor-simulator` script and the `ai-incident-responder` (Claude API / RAG).
- **Java toolchain:** Java services target **JDK 25 (LTS)**, built with **Gradle (Kotlin DSL)** and **Spring Boot 4.1.0** (4.0.7 also supports JDK 25; the 3.5.x line is out of OSS support). Use a Gradle Java toolchain (`languageVersion = 25`) + the `foojay-resolver-convention` plugin (`1.0.0`) so the JDK is auto-provisioned regardless of the machine's default `java` (which is JDK 21 here). Spring Boot 4.x requires Gradle 8.14+/9.x. Reference build files: `topology-service/{build.gradle.kts,settings.gradle.kts}`.
- **Assets/topology ownership:** the AGE graph is written **only** by `topology-service` (seeded from a seed file); `ingestion-service` no longer touches AGE. Other services read the graph directly from AGE.
- **Read API:** `incident-api` and `topology-service` expose **GraphQL** (single typed endpoint each) for the dashboard, not REST.
- **Telemetry message schema (JSON):** `timestamp`, `sensor_id`, `metric_type`, `value`, `status`.
- **Kafka topics:** `city-telemetry` (raw metrics from the simulator), `infrastructure-alerts` (critical events from the anomaly detector).
- **TimescaleDB:** metrics live in a **hypertable** (time-partitioned). Rolling 5-minute averages are computed with **Continuous Aggregates**, not ad-hoc queries. Use compression/retention policies for historical data.
- **PostgreSQL AGE graph model** — Vertices: `Substation`, `Distribution Node`, `Residential Building`, `Sensor`. Edges: power line, pipeline, sensor-to-node link. Query dependencies with **Cypher embedded in SQL** via `agtype` (e.g. "find all buildings depending on Distribution Node #5").
- **AI Worker output:** must be **structured JSON** — criticality level, list of impacted objects, step-by-step remediation instructions. Context enrichment is a **lightweight RAG** combining TimescaleDB metric history with the AGE dependency graph, kept within Claude's token limits.
- **Packaging & scaling:** each service ships as a **Helm chart**. The Ingestion Service scales via **HPA** driven by Kafka consumer lag. Secrets (Claude API key, Postgres credentials) go in **K8s Secrets**; broker/DB addresses in **ConfigMaps**.

## Event-driven patterns this design commits to

- **Idempotent Consumer** — handle duplicate Kafka events safely.
- **Circuit Breaker** — guard against Claude API downtime in the AI Worker.
- **Dead Letter Queue (DLQ)** — isolate poison messages.

## Implementation roadmap

1. **Infra + data collection** — local K8s + Kafka (Strimzi); Python sensor simulator → `city-telemetry`; basic Ingestion consumer.
2. **Storage + analytics** — PostgreSQL with TimescaleDB + AGE; hypertable + Continuous Aggregates; topology graph + Cypher dependency queries.
3. **Intelligence layer** — anomaly detector → `infrastructure-alerts`; AI Worker gathers context and calls the Claude API; persist structured incident reports.
4. **Production-ready K8s** — Helm charts for all services; HPA on the Ingestion Service; secrets/config via K8s Secrets and ConfigMaps.

See `project_architecture_and_plan.md` for the full per-stage detail.
