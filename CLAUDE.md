# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working rule: the user writes all the code

**Claude must NOT author or edit source code in this repository.** The user creates the projects and writes all implementation code themselves. Claude's role is limited to:
- **Advising** — suggesting an approach, explaining APIs/patterns, pointing at docs, reviewing the user's code against a checklist.
- **Verifying** — running the code, tests, and tooling; checking that things work; reporting results.

Do not scaffold projects, write classes/config, or "fix" files for the user unless they explicitly ask for that specific code. If a request is ambiguous about whether it wants written code, ask first. (Design docs, this `CLAUDE.md`, diagrams, and memory are not "source code" — those Claude may edit as usual.)

## Project status: planning stage

This repository is mostly design documents plus one service under active implementation: **`topology-service/`** (Java 25 / Spring Boot 4.1.0, at the repo root) — Gradle build is set up and dependencies resolve; application code (AGE client, seed loader) is being written. The other services are still design-only. The earlier **`sensor-simulator`** Python script was removed during a restructuring and is not currently in the repo. Do not invent build/lint/test commands for services that have no code. **Update this file as real code, tooling, and commands are added.**

Build topology-service (first compile provisions JDK 25 via the foojay toolchain resolver):
```bash
cd topology-service && ./gradlew build
```

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

Everything is deployed to a **local Kubernetes cluster** (Minikube or Kind). Kafka is run via an operator (**Strimzi**). A **single PostgreSQL instance** hosts both the **TimescaleDB** and **Apache AGE** extensions (one datastore, two logical roles — not two separate databases).

## Microservices (see `docs/microservices.md`)

Six services. The **pipeline** services communicate **asynchronously over Kafka**; `topology-service` and `incident-api` are **request/response GraphQL** for external clients. Backend services never call each other directly — shared state lives in the single PostgreSQL instance:

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
