# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working rule: Claude may write code

Claude **may author and edit source code** in this repository. The user lifted the earlier "user writes all the code" restriction on 2026-07-23, so Claude can now scaffold projects, write classes/config, and fix files as usual — while still advising (explaining approaches/APIs, pointing at docs, reviewing against a checklist) and verifying (running code, tests, and tooling; reporting results).

Follow the design documents and conventions below; when a change is large or ambiguous, confirm the approach before writing a lot of code.

## Project status: all six services + dashboard implemented; Stage 4 brought up on Kind, autoscaling demo pending

This repository is design documents plus **six implemented services** (five standalone Gradle projects on Java 25 / Spring Boot 4.1.0, plus the Python simulator). A requirements audit on 2026-09-06 found Stages 1–3 complete and Stage 4 (Docker/Helm/K8s) not started; the remediation work is tracked below and in the root `README.md`.

- **`topology-service/`** — AGE graph write (hand seed loader + a generated city: `SyntheticCity` / `SyntheticCityLoader`, gated by `topology.synthetic.enabled`, size `CITY_EXTRA_SENSORS` (997) → 21 substations `sub-dDD`, 84 nodes `node-dDD-k`, 333 buildings `bld-dDD-k-b`, 997 sensors, one crew `crew-dDD` per district, 1444 vertices / 1422 edges with the hand seed; one transaction of `UNWIND ... CREATE` batches, fingerprint in `public.topology_seed_state` so restarts are a no-op; layout rule mirrored by `sensor_simulator.py:layout_of`, both test suites pin the same fixture ids) + GraphQL read API (`vertices`/`edges`/`dependents`/`dependencies`); done and verified against `age-db` (the new queries re-verified against the compose `age-db` on 2026-10-25: `impactSummary(node-5)` = 2 objects / 204 apartments, `paths(sub-1, bld-12)` length 2, `responsibleCrews(node-5)` = crew-grid + crew-north). Added 2026-10-25 to cover the AGE learning topics: `impactSummary` (Cypher `count`/`sum` aggregation), `paths` (`MATCH p = ...`, `nodes(p)` parsed by `AgtypeParser.parseVertexList` — list comprehensions fail in AGE 1.6), `responsibleCrews` (hybrid: `cypher()` row source JOINed to relational `public.repair_crews`/`crew_assignments`, created by `schema.sql`/`data.sql` via `spring.sql.init` — schema-qualify, the search_path starts with `ag_catalog`). Testcontainers AGE tests (`TopologyRepositoryIntegrationTest`, incl. EXPLAIN proof the GIN index is used) and a `@Tag("benchmark")` test run by `./gradlew graphBenchmark` (excluded from `build`). Finding written up in `docs/graph-vs-sql.md`: AGE variable-length paths are ~100–300× slower than a recursive CTE, while a CTE over AGE's own `_ag_label_edge` tables is about as fast as a plain edge table — the VLE executor is the bottleneck, not storage; AGE creates no `start_id`/`end_id` indexes on edge tables, so `SeedLoader` now creates them (`AgeClient.ensureEdgeIndexes`).
- **`ingestion-service/`** — consumes Kafka `city-telemetry` → TimescaleDB `metrics` hypertable, idempotent (`ON CONFLICT`) + DLQ (`city-telemetry.DLQ`); done and verified end-to-end (2026-07-24). **Batch listener** (`spring.kafka.listener.type: batch`, one JDBC batch per poll via `MetricRepository.saveAll` + `reWriteBatchedInserts=true`; a poison record is isolated with `BatchListenerFailedException` so its neighbours persist once). `schema.sql` owns the hypertable tuning: 1-day chunks, `(sensor_id, metric_type, timestamp DESC)` index, compression segmented by series, 7-day compression + 30-day retention policies (asserted by the Testcontainers schema test). `@Tag("benchmark")` `TimeseriesStorageBenchmarkTest`, run by `./gradlew timeseriesBenchmark` (excluded from `build`, ~3 min), covers the TimescaleDB learning topics against a plain B-tree table: chunk exclusion, CA vs MV (incremental refresh, real-time, late-data invalidation), `by_hash` space partitioning (demo table only), compression (26.9×), self-hosted tiering via `move_chunk` to a second tablespace (Cloud's S3 tiering is not in self-hosted), `drop_chunks` vs `DELETE`+`VACUUM`. Write-up with the 2026-10-25 numbers: `docs/timeseries-storage.md`. See `ingestion-service/TODO.md` for the step log.
- **`sensor-simulator/`** (Python 3.12) — load generator producing telemetry to `city-telemetry` (keyed by `sensor_id`); mean-reverting random walk per metric with injectable faults. Emits for the 3 graph-seeded sensors (`s-00042`/`s-00108`/`s-00311`) plus synthetic ones (`--extra-sensors`, default env `CITY_EXTRA_SENSORS`), all linked in the generated city. `--cascade-every N` (+ `--cascade-delay`, default 5 s) plays substation outages district by district: all voltage sensors of a substation fault, then the pressure sensors of its buildings; the Helm chart runs `--interval 0.1 --anomaly-prob 0 --cascade-every 300`. Verified feeding the full simulator→Kafka→ingestion→TimescaleDB path.
- **`anomaly-detector-service/`** — owns the 5-min `metrics_5m_avg` continuous aggregate (created on startup via a JdbcTemplate `ApplicationRunner`, **`materialized_only = false`** so real-time aggregation folds in recent unmaterialized rows — the modern TimescaleDB default flipped to `true`); scheduled poll of TimescaleDB → threshold rules on min/max (**externalised** in `application.yaml` `anomaly.rules[]`, bound by `AnomalyRulesProperties`, validated by `ThresholdRuleRegistry`; alert `rule` label is derived as `<metric>_<min|max><op><threshold>`, e.g. `cable_temperature_max>120`) → produces critical events to Kafka `infrastructure-alerts` (edge-triggered dedup, producer-only, no DLQ). Done and verified end-to-end (2026-07-25): simulator→ingestion→TimescaleDB→detector→`infrastructure-alerts` emits one alert per fault (3/3), no re-fire on standing faults. See `anomaly-detector-service/TODO.md`.
- **`ai-incident-responder-service/`** — **built in Java** (the design docs say Python; user overrode on 2026-07-25 for consistency/reuse). Consumes `infrastructure-alerts`; gathers context (last-1h metric history from TimescaleDB + the dependency graph from AGE — the only service reading **both** Postgres instances, via two qualified `JdbcTemplate`s); calls the **Claude API** (official `com.anthropic:anthropic-java` SDK, default `claude-opus-5-5` (switched from `claude-opus-5` on 2026-10-25), adaptive thinking at an **explicit effort** (`ai.claude.effort`, default `high`; Opus 5.5 defaults to `medium`). `outputConfig(IncidentPlan.class)` replaces the whole `output_config`, so `ClaudeIncidentPlanner.buildParams` merges effort into the generated config afterwards (pinned by `ClaudeIncidentPlannerTest`); **structured output** typed to `IncidentPlan`) guarded by a Resilience4j-**core** circuit breaker; writes the `incidents` table (idempotent via `UNIQUE(alert_id)` + `ON CONFLICT`, DLQ for poison alerts). **Graph use since 2026-11-08:** `GraphRepository.loadContext` = monitored asset + one hybrid statement for impacted objects **and** crews (`cypher([*0..3]) LEFT JOIN public.crew_assignments/repair_crews`) + upstream walk (`<-[*1..3]-`); the prompt has `## Responsible crews`; `incidents` gains `asset_id`, `root_incident_id`, `detected_at`, `crews` (ALTERs in `schema.sql`). **Root-cause correlation** (`IncidentCorrelator`, `ai.correlation.enabled`/`window` = 30m): an alert whose asset or anything upstream has a root incident detected within ±window becomes a child (`model=correlated`, no planner call); a new root adopts open roots downstream; re-check + insert + adopt in one transaction under `pg_advisory_xact_lock`. Testcontainers AGE + TimescaleDB tests (`GraphRepositoryIntegrationTest`, `IncidentRepositoryIntegrationTest`, `CorrelationIntegrationTest`). Code complete and `./gradlew build` green. **Verified end-to-end in mock mode** (2026-07-25): `AI_CLAUDE_MOCK=true` swaps the Claude call for a deterministic offline planner (`MockIncidentPlanner`; the `AnthropicClient` bean is `@ConditionalOnProperty` so no key needed) — alert→context(AGE+TimescaleDB)→incident→`incident-api` GraphQL all confirmed, 3 impacted objects resolved from the graph, idempotent; re-verified 2026-10-25 (3 faults -> 3 alerts -> 3 incidents). Only the **real Claude call is unexercised** (needs `ANTHROPIC_API_KEY`; billed). See `ai-incident-responder-service/TODO.md`. Added 2026-10-25 for the LLM learning topics: the role/rules are a top-level `system` prompt (`IncidentPromptBuilder.SYSTEM_PROMPT`), the user message carries only the per-alert RAG context, impacted objects are capped at 50 in the prompt (the rest counted, not dropped), every real call logs `response.usage()`, and `@Tag("llm")` `PromptTokenBudgetTest` (run by `./gradlew promptTokens`, excluded from `build`, needs a key; count_tokens only, no generation) measures the prompt's input tokens — **not run yet**. Write-up: `docs/llm-integration.md` (its token figures are estimates until that test runs).
- **`incident-api-service/`** (Java) — read-only **GraphQL** for the dashboard over the `incidents` table + TimescaleDB `metrics` (single datasource, owns no tables). Queries: `incidents`/`incident`/`incidentsBySensor`/`recentMetrics`, every list query takes a `limit` clamped to `[0, 1000]`; the two JSONB string-array columns are exposed as `[String!]` and `crews` as `[Crew!]!` (parsed in the repo, no JSON scalar); `Incident` also has `assetId`, `rootIncidentId`, `detectedAt` and the per-incident `@SchemaMapping` fields `rootIncident` / `linkedIncidents` (asked only by the detail page). Runs on **8081** (topology uses 8080). Done and verified (2026-07-25) via GraphiQL/`POST /graphql`. See `incident-api-service/TODO.md`.
- **`dashboard/`** (Angular **21**, added 2026-10-25) — Material UI over both GraphQL APIs: incidents list/detail (detail shows crews, "secondary effect of" link, linked incidents; list tags linked ones), cytoscape topology graph with dependents highlighting and a district filter (`topology/district.ts`; sensors hidden by default, ~444 nodes instead of 1444), chart.js sensor charts with an autocomplete sensor search. Browser calls same-origin `/api/topology/graphql` + `/api/incidents/graphql` — `proxy.conf.json` in dev, `nginx.conf.template` (env `TOPOLOGY_UPSTREAM`/`INCIDENT_API_UPSTREAM`) in the nginx-unprivileged image; no CORS in the backends. Detector thresholds are duplicated in `src/app/core/detector-rules.ts` — keep in sync with `anomaly.rules[]`. Angular 21 not 22 because the host has Node 22.20 (22 needs 22.22+); install with **npm 11** (`npx npm@11 ci`) — npm 10 crashes on vitest's peer deps. Verified (2026-10-25): build, vitest, Docker image + nginx proxy, `helm lint`/`template`, and all four screens against the real backends on docker compose (incidents, incident detail with chart, topology with the impact/crews panel, sensor page). In Kind the dashboard pod ran Ready but was not opened through a port-forward.

All six designed services now exist in code. Do not invent build/lint/test commands for services that have no code. **Update this file as real code, tooling, and commands are added.**

Cross-cutting conventions added 2026-09-06:
- Every Java service has the web starter + actuator + `spring-boot-starter-micrometer-metrics` + Prometheus registry; `management.endpoints.web.exposure.include: health,info,metrics,prometheus`, probes enabled, readiness group = `readinessState,db`. Tests that exclude the datasource must set `management.endpoint.health.group.readiness.include=readinessState`.
- JDBC hosts: `${AGE_HOST:${POSTGRES_HOST:localhost}}` / `${TIMESCALE_HOST:${POSTGRES_HOST:localhost}}` so compose (one host, two ports) and Kubernetes (two Services) both work.
- `topology-service` creates a GIN index on each vertex label's `properties` and B-tree indexes on each edge type's `start_id`/`end_id` after seeding (`topology.seed.index-enabled`).
- `anomaly-detector-service` waits for the `metrics` hypertable before creating its continuous aggregate (`anomaly.schema.wait-timeout`).
- `ai-incident-responder-service` publishes the Claude circuit breaker as `resilience4j_circuitbreaker_*` meters (`resilience4j-micrometer`).
- Bug sweep 2026-10-25 (verified on compose: 3 faults → 3 alerts → 3 incidents, future-dated reading → `city-telemetry.DLQ`): responder dead-letters permanent failures (`ResilienceConfig.PERMANENT_FAILURES`: `UnplannableAlertException` for refusal/`max_tokens`/no content, Anthropic 400/401/403/404/422) instead of retrying forever, and the breaker ignores them; metric history is the hour ending at the alert's `detected_at`; DLQ recoverers publish to partition `-1` and the Strimzi `city-telemetry.DLQ` has 6 partitions; the detector evaluates the **two newest** buckets per series (`bucket <= now()`), disarms series that stop reporting (`AlertPublisher.retainOnly`) and uses full-UUID alert ids; ingestion rejects timestamps >5 min in the future; GraphQL `limit` args are `Int! = N`; the ingestion chart omits `replicas` when KEDA autoscaling is on; `up.ps1` checks every native command (`Native { ... }`).
- **Comment style (user request, 2026-10-25):** Java classes (main + test) carry **no comments inside the type body** — no `//`, no member/field/nested-type Javadoc. All explanation lives in the Javadoc above the top-level type, one `<p>` per member, anchored with `{@link #method(...)}` (public) or `{@code name}` (private). Keep it that way when adding code.

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

**Stage 4 packaging (`deploy/`, added 2026-09-06):** every service has a multi-stage `Dockerfile` (`eclipse-temurin:25-jdk` build with the committed wrapper → `eclipse-temurin:25-jre` runtime, layered jar, `USER 1000:1000`); `deploy/charts/<service>` is one Helm chart per service (Deployment + Service + probes + `nc` init-container waits; ingestion adds a KEDA `ScaledObject` on `city-telemetry` consumer lag, max 6 = partition count); `deploy/charts/smartcity-infra` holds the two Postgres StatefulSets, the `smartcity-config` ConfigMap and the `postgres-credentials`/`anthropic` Secrets; `deploy/charts/smartcity` is the umbrella (file:// deps; run `helm dependency build` first — `deploy/scripts/up.*` does). Kafka is a Strimzi **1.x** `Kafka` + `KafkaNodePool` (API `kafka.strimzi.io/v1`, KRaft only) in `deploy/strimzi/`, topics as `KafkaTopic` CRs. Runbook: `deploy/README.md`; scripts: `deploy/scripts/{up,build-images,demo-hpa,down}.{ps1,sh}`. `deploy/charts/dashboard` (nginx, Service port 80) was added 2026-10-25. **First real Kind run (2026-10-25).** Reached: cluster (kind v0.32, Kubernetes 1.36), Strimzi 1.3 operator + Kafka 4.3.1 Ready, all four `KafkaTopic`s Ready (`city-telemetry` with 6 partitions), KEDA installed, all 7 images built and `kind load`ed, Helm release installed, every service pod Running/Ready at some point after the fixes below. Two bugs found and fixed: (1) Windows PowerShell 5.1 turns a native command's stderr into a terminating error under `ErrorActionPreference=Stop` once output is redirected, so `up.ps1` died on kind's "No kind clusters found."; the scripts now run natives under `Continue` and check `$LASTEXITCODE`. (2) `timescaledb-tune` set `max_connections=25` for the 2Gi limit and the detector's init container got "too many clients"; the StatefulSet now passes `-c max_connections` (`timescaledb.maxConnections`, 150). The first run was cut short by memory pressure from the user's unrelated `Desktop/DBT` compose stack on the same 8 GB Docker VM (25 containers, `restart: unless-stopped`; it holds ports 5432/5433/8080/8081/4200 and comes back after a Docker restart). **Later on 2026-10-25, with DBT stopped, the following were verified:** all 11 pods Ready together; `demo-hpa` scaled ingestion 1→6 in under a minute and back to 1 about 5 min after the load stopped (HPA's default 300 s scale-down stabilization, not KEDA's cooldown). Release revision 2 adds the `smartcity-retain` StorageClass and a static hostPath PV demo (`smartcity-infra.storage.*`, off by default; `storage.className` is left out of `volumeClaimTemplates` when empty because they're immutable). The interrupted install had left the release `pending-install`; it was fixed by setting the release Secret's status to `deployed`. Write-up of the K8s storage/QoS/HPA/EDA learning topics with the measurements: `docs/k8s-storage-and-scaling.md`. Kafka learning topics (commit log/zero-copy, partitioning, consumer groups/rebalance, acks/EOS, Strimzi/KRaft) with real broker output: `docs/kafka.md`. Following it (2026-10-25), both consumers (ingestion, responder) set `partition.assignment.strategy: CooperativeStickyAssignor` (the default picked eager `range`, so every KEDA scale step stopped all replicas), and the simulator sets `enable.idempotence`. Verified on compose, then rebuilt + `kind load`ed + `rollout restart`ed in Kind: both groups report `cooperative-sticky` there (the in-cluster simulator image is loaded but its Deployment sits at 0 replicas, so it was not exercised). **Re-verified 2026-11-08 on current code:** the Kind images had been built before the bug sweep (the detector still emitted 8-char alert ids) and `city-telemetry-dlq` had 3 partitions, so all 7 images were rebuilt + `kind load`ed, `topics.yaml` re-applied (DLQ now 6), the release upgraded to revision 3 (only change: ingestion `replicas` omitted under KEDA) and every Deployment `rollout restart`ed — the tag stays `:dev`, so Helm alone never rolls the pods. Then the simulator (1000 sensors) produced in-cluster incidents with full-UUID alert ids and graph-resolved impacted objects, KEDA scaled ingestion up, and `kubectl port-forward svc/dashboard 8088:80` served the page plus both GraphQL APIs through nginx (`impactSummary(node-5)` = 2 / 204, two crews). KEDA warns that `cooldownPeriod`/`pollingInterval` are irrelevant with `minReplicaCount: 1`. **Verified 2026-11-08 in Kind (release revision 4):** the generated city loads in 1.4 s (1444/1422 via GraphQL, `dependents(sub-d00)` = 20, `responsibleCrews(bld-d00-0-0)` = crew-d00, hand-seed answers unchanged); responder AGE queries 0.4 / 10 (cold 64) / 3–5 ms; one cascade of district d00 (16 voltage + 16 pressure alerts) gave 32 incidents with **2 planner calls**: one root on `sub-d00` with crew-d00, the first pressure alert planned then adopted (it arrived before its substation's), 30 correlated; `linkedIncidents` = 31 via the dashboard proxy. Dashboard screens were not opened in a browser (extension not connected). **Not verified yet:** the real Claude call (cluster runs `AI_CLAUDE_MOCK=true`, no `anthropic` Secret); `promptTokens` after the crews section.

> **Boot 4 note:** autoconfiguration is modularised — depend on `spring-boot-starter-<tech>` (e.g. `spring-boot-starter-kafka`), not the raw framework jar, or Boot ships no autoconfig for it (silent no-op).

These design documents are the source of truth; read them before making architectural decisions:

- `project_architecture_and_plan.md` (Russian) — system architecture and a 16-week, 4-stage implementation plan.
- `technology_stack_topics.md` (English) — the theory/learning topics behind each technology in the stack.
- `docs/microservices.md` — the finalized service decomposition, communication contracts, and message schemas.
- `docs/system-overview.md` (Russian) — how the running system works end to end: flow, Kafka topics/producers/consumers, error handling, delivery guarantees. Keep it in sync when Kafka settings or retry/DLQ policy change.
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
