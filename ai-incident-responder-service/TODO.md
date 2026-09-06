# TODO — ai-incident-responder-service: `infrastructure-alerts` → Claude → `incidents`

## Context

Four services are done (`topology-service`, `ingestion-service`, `sensor-simulator`,
`anomaly-detector-service`), and `anomaly-detector` emits critical events to Kafka
`infrastructure-alerts`. This service closes the intelligence loop (roadmap **Stage 3**): it
consumes `infrastructure-alerts`, gathers context (last-1h metric history from TimescaleDB + the
dependency graph from AGE), calls the **Claude API** for a structured remediation plan, and writes
the `incidents` table.

The design docs assign this service to Python; **it is being built in Java** (decided 2026-07-25)
for consistency with the three existing Java services (Java 25 / Spring Boot 4.1.0) and to reuse
their Kafka/JDBC/AGE scaffolding. Turns the stock Boot skeleton into a working worker: **plain JDBC
(not JPA)**, constructor injection, javadoc voice, **verify by running** (no tests).

**Design contract (from `docs/microservices.md`) — do not re-derive:**
- `infrastructure-alerts` message (JSON): `{alert_id, sensor_id, metric_type, value, detected_at, rule}`.
- Reads TimescaleDB (last **1h** history) **and** AGE (dependency graph) directly.
- Calls Claude → **structured JSON**: `{criticality, impacted_objects[], remediation_steps[]}`.
- Writes `incidents` (in TimescaleDB, per the data-infra doc).
- Resilience: **idempotent consumer**, **circuit breaker** (Claude), **DLQ**.

## Decisions (baked in)

- **Two datasources** — first service to touch both Postgres instances: TimescaleDB
  (`localhost:${TIMESCALE_PORT:5433}`, 1h history + `incidents`, **@Primary**) and age-db
  (`localhost:${AGE_PORT:5432}`, graph, Hikari `connection-init-sql:
  SET search_path = ag_catalog, "$user", public`). `config/DataSourceConfig` defines both
  `DataSource`s + two qualified `JdbcTemplate`s (`timescaleJdbc` / `ageJdbc`).
- **`incidents` owned here** — created on startup via `spring.sql.init` + `schema.sql` (runs against
  the primary = TimescaleDB), idempotent like ingestion. `UNIQUE(alert_id)` = idempotency key.
- **Idempotent consumer** — before any Claude call, `SELECT 1 FROM incidents WHERE alert_id=?`; skip
  if present. Insert `ON CONFLICT (alert_id) DO NOTHING`. Survives restarts (no in-memory set).
- **Claude via the official Anthropic Java SDK** (`com.anthropic:anthropic-java`). Default model
  `claude-opus-5` (property `ai.claude.model`), adaptive thinking, and the SDK's **structured
  output** (`.outputConfig(IncidentPlan.class)`) so the plan is typed + schema-validated.
  `AnthropicClient` from `AnthropicOkHttpClient.fromEnv()` (reads `ANTHROPIC_API_KEY`).
- **Circuit breaker = Resilience4j core** (`io.github.resilience4j:resilience4j-circuitbreaker`),
  wired programmatically — the resilience4j **Spring-Boot-3 starter is not Boot-4 compatible**, so no
  starter. Wrap the Claude call; open circuit fails fast.
- **DLQ + error routing** — mirror ingestion's `KafkaConfig`: `NewTopic infrastructure-alerts.DLQ`,
  `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` + explicit `KafkaTemplate<String,String>`,
  consumer group `ai-incident-responder`. Parse/validation failures (`InvalidAlertException`) are
  **non-retryable** → straight to DLQ; transient Claude failures are retried (circuit breaker guards).
- **Impacted-objects query reuses topology's idiom** — alert `sensor_id` →
  `MATCH (s:Sensor {id:$id})-[:SensorLink]->(a)` gives the monitored asset; then the dependents walk
  (`MATCH (n {id:$id})-[*1..N]->(d) WHERE label(d)<>'Sensor' RETURN DISTINCT d.id,label(d),properties(d)`,
  from `TopologyRepository.findDependents`). Impacted = asset + dependents. Bind `id` as `?::agtype`
  (`Types.OTHER`), never string-interpolate.

## Steps

Base package `inno.ipr.ai_incident_responder_service`.

### A. Build & toolchain
- [x] **1. foojay plugin.** `settings.gradle.kts` — add `org.gradle.toolchains.foojay-resolver-convention` `1.0.0`.
- [x] **2. Dependencies.** `build.gradle.kts` — `spring-boot-starter`, `-jdbc`, `-json`, `-actuator`,
  **`spring-boot-starter-kafka`**, `runtimeOnly("org.postgresql:postgresql")`,
  `com.anthropic:anthropic-java:<current>`, `io.github.resilience4j:resilience4j-circuitbreaker:<2.x>`.

### B. Datasources & schema
- [x] **3. `config/DataSourceConfig.java`** — two `DataSource`s (@Primary TimescaleDB; age-db with
  Hikari init-sql) + two qualified `JdbcTemplate`s (`timescaleJdbc`, `ageJdbc`).
- [x] **4. `resources/schema.sql`** — `incidents` (`incident_id` PK, `alert_id` UNIQUE NOT NULL,
  `sensor_id`, `metric_type`, `criticality`, `impacted_objects` JSONB, `remediation_steps` JSONB,
  `summary`, `model`, `created_at` TIMESTAMPTZ DEFAULT now()). `spring.sql.init.mode: always`.

### C. Read sides
- [x] **5. AGE.** Copy `age/AgtypeParser.java` from topology; new `repository/GraphRepository.java`
  (`@Qualifier("ageJdbc")`): `findMonitoredAsset(sensorId)` + `findImpacted(assetId)` →
  `List<ImpactedObject>` (`domain/ImpactedObject`: id, label, properties).
- [x] **6. Metric history.** `repository/MetricHistoryRepository.java` (`@Qualifier("timescaleJdbc")`):
  `findLastHour(sensorId, metricType)` → `domain/MetricSummary` (count, min, max, avg, latest, latestAt).

### D. Persistence & Claude output
- [x] **7. `repository/IncidentRepository.java`** — `existsByAlertId`, `save(Incident)` with
  `ON CONFLICT (alert_id) DO NOTHING` (JSONB via `?::jsonb`); `domain/Incident` record.
- [x] **8. `ai/IncidentPlan.java`** — record (`criticality`, `impactedObjects`, `remediationSteps`)
  with Jackson descriptions for the structured-output schema.

### E. AI + resilience + config
- [x] **9. `config/AnthropicConfig.java`** (`AnthropicClient` bean) + `config/ResilienceConfig.java`
  (`CircuitBreaker` bean).
- [x] **10. `ai/IncidentPlanner.java`** — build the RAG prompt (alert + metric summary + impacted
  objects); call Claude (Opus 4.8, adaptive thinking, structured output) inside the circuit breaker.
- [x] **11. `alert/Alert.java`** (record, snake_case `@JsonProperty`) + `exception/InvalidAlertException.java`.
- [x] **12. `config/KafkaConfig.java`** — DLQ topic + error handler + `KafkaTemplate` (copy ingestion);
  `InvalidAlertException` non-retryable.
- [x] **13. `consumer/AlertConsumer.java`** — `@KafkaListener(topics="infrastructure-alerts",
  groupId="ai-incident-responder")`: parse → idempotency check → gather context → plan → persist.
- [x] **14. `application.yaml`** — `spring.datasource.timescale.*` + `spring.datasource.age.*`,
  `spring.kafka.bootstrap-servers`, `spring.sql.init.mode: always`, `ai.claude.model`, actuator.
  Delete the default `AiIncidentResponderServiceApplicationTests`.

## Critical files
- New under `src/main/java/inno/ipr/ai_incident_responder_service/`:
  `config/{DataSourceConfig,KafkaConfig,AnthropicConfig,ResilienceConfig}.java`,
  `age/AgtypeParser.java`, `repository/{GraphRepository,MetricHistoryRepository,IncidentRepository}.java`,
  `domain/{ImpactedObject,MetricSummary,Incident}.java`, `ai/{IncidentPlan,IncidentPlanner}.java`,
  `alert/Alert.java`, `exception/InvalidAlertException.java`, `consumer/AlertConsumer.java`
- New resources: `src/main/resources/schema.sql`
- Edit: `settings.gradle.kts`, `build.gradle.kts`, `application.yaml`
- Delete: `src/test/.../AiIncidentResponderServiceApplicationTests.java`

**Reuse / mirror:** ingestion `config/KafkaConfig` (DLQ + `KafkaTemplate`), `consumer/TelemetryConsumer`,
`telemetry/TelemetryParser`; topology `age/AgeClient` + `AgtypeParser` +
`repository/TopologyRepository.findDependents`; anomaly-detector `alert/Alert`; Boot-4
`spring-boot-starter-<tech>` rule; instance-`JdbcTemplate` `@Repository` shape.

## Mock mode (offline verification, no API key)

`ai.claude.mock=true` (env `AI_CLAUDE_MOCK=true`) swaps `ClaudeIncidentPlanner` for
`MockIncidentPlanner` — a deterministic planner that builds the plan from the alert + the **real**
graph-derived impacted objects, with **no Claude call and no `ANTHROPIC_API_KEY`** (the
`AnthropicClient` bean is `@ConditionalOnProperty` and isn't created). Everything else runs for real.

**Verified end-to-end in mock mode 2026-07-25:** `docker compose up -d age-db timescaledb kafka` →
topology-service seeded the graph (9v/8e) → responder (`AI_CLAUDE_MOCK=true bootRun`) → produced an
alert for `s-00042` to `infrastructure-alerts`. Result: consumer resolved **3 impacted objects from
AGE** (`node-5`, `bld-12`, `bld-19` via the sensor→SensorLink→dependents walk), persisted incident
`i-17155ca7` (criticality HIGH, JSONB arrays, `model=mock`); re-producing the alert left the count at
1 (idempotent); and `incident-api` returned the row over GraphQL — the full alert→Claude(mock)→
incidents→read-API chain. Only the **real Claude call** remains unexercised (needs the key).

## Verification (real Claude — needs `ANTHROPIC_API_KEY`; makes a real billed Claude call)

1. `docker compose up -d age-db timescaledb kafka`.
2. Run `topology-service` (`bootRun`) to seed the graph, then `ingestion-service` (`bootRun`).
3. Feed a fault: `cd sensor-simulator && ./.venv/Scripts/python.exe sensor_simulator.py
   --max-messages 40 --interval 0.3 --anomaly-prob 0.4 --seed 7`; run `anomaly-detector-service` to
   emit an alert on `infrastructure-alerts` (e.g. s-00042 pipe_pressure).
4. `export ANTHROPIC_API_KEY=...`; run `ai-incident-responder-service` (`bootRun`).
5. Expect a log line for the consumed alert + a Claude call; then
   `docker exec smartcity-timescaledb psql -U smartcity -d smartcity -c "SELECT incident_id, alert_id,
   criticality, impacted_objects, remediation_steps FROM incidents;"` shows one structured row
   (impacted objects for s-00042 = node-5, bld-12, bld-19).
6. **Idempotency:** re-emit/reconsume the alert → no second row, no second Claude call.
   **DLQ:** produce a malformed message to `infrastructure-alerts` → lands on
   `infrastructure-alerts.DLQ`, no crash.

## Deferred
- Recovery/clear handling; per-metric prompt tuning.
- ~~Retry/backoff for prolonged Claude outage~~ — done: `KafkaConfig` uses an unlimited
  `ExponentialBackOff` (1 s → 30 s cap) through a `ContainerPausingBackOffHandler`, so a valid alert
  is never dead-lettered; only `InvalidAlertException` (poison) goes to the DLQ.
- Compression/retention on `incidents`. ~~Helm/HPA/K8s Secret for the API key (Stage 4)~~ → packaged
  2026-09-06: `Dockerfile`, `deploy/charts/ai-incident-responder-service` (optional `anthropic` Secret via
  `envFrom`, created by `deploy/charts/smartcity-infra` when `anthropic.apiKey` is set). Cluster run
  still to be exercised.
- Next service after this: `incident-api` (read-only GraphQL over `incidents` + metrics).
