# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working rule: Claude may write code

Claude **may author and edit source code** in this repository. The user lifted the earlier "user writes all the code" restriction on 2026-07-23, so Claude can now scaffold projects, write classes/config, and fix files as usual — while still advising (explaining approaches/APIs, pointing at docs, reviewing against a checklist) and verifying (running code, tests, and tooling; reporting results).

Follow the design documents and conventions below; when a change is large or ambiguous, confirm the approach before writing a lot of code.

## Project status: early implementation

This repository is design documents plus **two implemented services**, each its own standalone Gradle project (Java 25 / Spring Boot 4.1.0):

- **`topology-service/`** — AGE graph write (seed loader) + GraphQL read API (`vertices`/`edges`/`dependents`/`dependencies`); done and verified against `age-db`.
- **`ingestion-service/`** — consumes Kafka `city-telemetry` → TimescaleDB `metrics` hypertable, idempotent (`ON CONFLICT`) + DLQ (`city-telemetry.DLQ`); done and verified end-to-end (2026-07-24). See `ingestion-service/TODO.md` for the step log.
- **`sensor-simulator/`** (Python 3.12) — load generator producing telemetry to `city-telemetry` (keyed by `sensor_id`); mean-reverting random walk per metric with injectable faults. Emits for the 3 graph-seeded sensors (`s-00042`/`s-00108`/`s-00311`) plus optional synthetic ones. Verified feeding the full simulator→Kafka→ingestion→TimescaleDB path.

The remaining services (`anomaly-detector`, `ai-incident-responder`, `incident-api`) are still design-only. Do not invent build/lint/test commands for services that have no code. **Update this file as real code, tooling, and commands are added.**

Build the Java services (first compile provisions JDK 25 via the foojay toolchain resolver):
```bash
cd topology-service && ./gradlew build
cd ingestion-service && ./gradlew build
```

Run the sensor-simulator (Python):
```bash
cd sensor-simulator && python -m venv .venv && ./.venv/Scripts/python.exe -m pip install -r requirements.txt
./.venv/Scripts/python.exe sensor_simulator.py            # feed city-telemetry
```

Local backing services (`docker-compose.yml`, repo root): `age-db` (5432), `timescaledb` (5433), `kafka` (KRaft, 9092). Run `ingestion-service` end-to-end with `docker compose up -d timescaledb kafka` then `cd ingestion-service && ./gradlew bootRun`.

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

- **sensor-simulator** (Python) — generates telemetry → `city-telemetry` (design; script not currently in the repo).
- **ingestion-service** (Java) — consumes `city-telemetry`; writes **metrics only** → TimescaleDB; HPA on Kafka lag.
- **topology-service** (Java) — sole owner/writer of the **AGE** graph (assets + connections); seeds it from a seed file; **GraphQL** API for asset/topology management + the dashboard network view.
- **anomaly-detector** (Java) — polls TimescaleDB continuous aggregates/thresholds; emits critical events → `infrastructure-alerts`.
- **ai-incident-responder** (Python) — consumes `infrastructure-alerts`; gathers context (TimescaleDB history + AGE graph, read directly); calls Claude API; writes the `incidents` table.
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
