# Smart City telemetry + AI incident response

Simulated city sensors stream metrics into Kafka. The system stores them as time series, models
the physical network as a graph, detects critical failures, and asks the Claude API for a
structured remediation plan that a repair crew can follow.

Design documents (source of truth): [`project_architecture_and_plan.md`](project_architecture_and_plan.md)
(4-stage plan, Russian), [`docs/microservices.md`](docs/microservices.md) (service contracts and message
schemas), [`technology_stack_topics.md`](technology_stack_topics.md) (theory behind the stack), and the
Mermaid diagrams in [`docs/`](docs/). End-to-end walkthrough of the running system — flow, Kafka
topics/producers/consumers, error handling and delivery guarantees (Russian):
[`docs/system-overview.md`](docs/system-overview.md).

## Data flow

```
Python sensor-simulator
        │  JSON {timestamp, sensor_id, metric_type, value, status}
        ▼
Kafka: city-telemetry ──► ingestion-service ──► TimescaleDB `metrics` hypertable
                                                        │  5-min continuous aggregate
                             topology-service ──► AGE   │  (metrics_5m_avg)
                             (seed file → graph)  graph │
                                                        ▼
                                              anomaly-detector-service
                                                        │
                                                        ▼
                                          Kafka: infrastructure-alerts
                                                        │
                                                        ▼
                                         ai-incident-responder-service
                                    (1 h history from TimescaleDB + dependency
                                     graph from AGE → Claude API → `incidents`)
                                                        │
                                                        ▼
                                          incident-api-service (GraphQL)
```

## Services

| Service | Lang | Port | Consumes | Produces / writes |
|---|---|---|---|---|
| `sensor-simulator` | Python 3.12 | — | — | Kafka `city-telemetry`, keyed by `sensor_id` |
| `ingestion-service` | Java 25 / Boot 4.1 | 8080 | `city-telemetry` | TimescaleDB `metrics`; poison → `city-telemetry.DLQ` |
| `topology-service` | Java 25 / Boot 4.1 | 8080 | seed file | AGE graph `city_topology`; GraphQL read API |
| `anomaly-detector-service` | Java 25 / Boot 4.1 | 8080 | TimescaleDB `metrics_5m_avg` | Kafka `infrastructure-alerts` |
| `ai-incident-responder-service` | Java 25 / Boot 4.1 | 8080 | `infrastructure-alerts` | `incidents` table; poison → `infrastructure-alerts.DLQ` |
| `incident-api-service` | Java 25 / Boot 4.1 | 8081 | — | GraphQL read API over `incidents` + `metrics` |
| `dashboard` | Angular 21 | 4200 | both GraphQL APIs | — (UI: incidents, topology graph, sensor charts) |

Each Java service is a standalone Gradle (Kotlin DSL) project. Backend services never call each
other: shared state lives in two PostgreSQL instances, `timescaledb` (metrics, incidents) and
`age-db` (topology graph).

## Prerequisites

- Docker Desktop (Compose v2).
- Nothing else for Java: the first `./gradlew` run provisions JDK 25 through the foojay toolchain
  resolver, whatever your default `java` is.
- Python 3.12 for the simulator.
- For the Kubernetes path: `kind`, `kubectl`, `helm` (see [`deploy/README.md`](deploy/README.md)).

## Quick start (Docker Compose for the backing services, everything else on the host)

```bash
cp .env.example .env            # defaults work as-is; set ANTHROPIC_API_KEY only for real Claude calls
docker compose up -d            # age-db :5432, timescaledb :5433, kafka :9092
```

Start the services in this order, each in its own terminal (each `bootRun` provisions the JDK
on first use):

```bash
cd topology-service            && ./gradlew bootRun    # seeds the graph: 9 vertices / 8 edges
cd ingestion-service           && ./gradlew bootRun    # creates the metrics hypertable + policies
cd anomaly-detector-service    && ./gradlew bootRun    # creates metrics_5m_avg, starts polling
cd ai-incident-responder-service && AI_CLAUDE_MOCK=true ./gradlew bootRun   # offline planner, no key
cd incident-api-service        && ./gradlew bootRun --args='--spring.profiles.active=dev'
```

Then generate telemetry:

```bash
cd sensor-simulator
python -m venv .venv && ./.venv/Scripts/python.exe -m pip install -r requirements.txt
./.venv/Scripts/python.exe sensor_simulator.py --max-messages 40 --interval 0.3 --anomaly-prob 0.4 --seed 7
```

Within a minute the detector logs three alerts (one per injected fault), the responder writes
three incidents, and you can read them back:

- Incident API GraphiQL: <http://localhost:8081/graphiql> (needs the `dev` profile) — try
  `{ incidents { incidentId criticality impactedObjects remediationSteps } }`.
- Topology GraphiQL: <http://localhost:8080/graphiql> with `--spring.profiles.active=dev` on
  `topology-service` — try `{ dependents(id: "node-5") { id label } }`,
  `{ impactSummary(id: "node-5") { totalObjects apartments byLabel { label count } } }`,
  `{ paths(from: "sub-1", to: "bld-12") { length nodes { id } } }` or
  `{ responsibleCrews(id: "node-5") { name assets } }` (graph model, Cypher features and the
  AGE-vs-SQL benchmark: [`docs/graph-vs-sql.md`](docs/graph-vs-sql.md)).

Or open the dashboard (Node 22.12+; `ng serve` proxies `/api/*/graphql` to :8080 / :8081):

```bash
cd dashboard && npx npm@11 ci && npm start      # http://localhost:4200 (npm 10 can't install it)
```

Load profile: `sensor_simulator.py --extra-sensors 997 --interval 0.5` emits ~2000 messages per
second from ~1000 sensors.

## Real Claude calls

Set `ANTHROPIC_API_KEY` and run the responder with `AI_CLAUDE_MOCK=false`. The model defaults to
`claude-opus-5-5` (`AI_CLAUDE_MODEL`). Calls are billed and guarded by a circuit breaker; while the
breaker is open the consumer pauses and retries rather than dead-lettering the alert.

## Building and testing

```bash
cd <service> && ./gradlew build          # compiles, runs unit + integration tests
cd topology-service && ./gradlew graphBenchmark   # Cypher vs recursive CTE on ~10k vertices (~3 min, Docker)
cd sensor-simulator && ./.venv/Scripts/python.exe -m pytest -q
cd dashboard && npm run build && npx ng test --watch=false
```

Integration tests that need a database use Testcontainers and skip themselves when Docker is not
running; Kafka integration tests use an embedded broker and always run.

## Kubernetes

Stage 4 of the plan packages every service as a Docker image and a Helm chart on a local Kind
cluster, with Kafka via the Strimzi operator, KEDA scaling the ingestion service on Kafka consumer
lag, and Secrets / ConfigMaps for credentials and addresses. One command brings it all up:

```
deploy/scripts/up.ps1        # Windows (PowerShell); deploy/scripts/up.sh elsewhere
```

The runbook, expected end state and the autoscaling demo are in [`deploy/README.md`](deploy/README.md).

## Repository layout

```
docker-compose.yml           local backing services (age-db, timescaledb, kafka)
docker/age/init/01-age.sql   first-boot: CREATE EXTENSION age
deploy/                      Kind config, Strimzi CRs, Helm charts, scripts (Stage 4)
sensor-simulator/            Python load generator + pytest suite
ingestion-service/           Kafka → TimescaleDB
topology-service/            seed → AGE graph, GraphQL
anomaly-detector-service/    continuous aggregate → threshold rules → alerts
ai-incident-responder-service/ alerts → context (TimescaleDB + AGE) → Claude → incidents
incident-api-service/        GraphQL over incidents + metrics
dashboard/                   Angular UI over both GraphQL APIs (nginx image for Kubernetes)
docs/                        microservices contract + Mermaid diagrams
```

Each service directory carries a `TODO.md` with its implementation log and deferred items.
