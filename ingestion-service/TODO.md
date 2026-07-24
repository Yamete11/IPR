# TODO — ingestion-service: Kafka `city-telemetry` → TimescaleDB (metrics plane)

> **STATUS: DONE & verified end-to-end (2026-07-24).** All 12 steps complete. Ran against real
> `timescaledb` + `kafka` (KRaft, added to compose): happy-path insert (1 row), duplicate → still
> 1 row (idempotent `ON CONFLICT`), malformed JSON → `city-telemetry.DLQ`, valid-JSON-missing-field
> → DLQ. Two deviations from the original plan, marked inline below:
> **(2)** use `org.springframework.boot:spring-boot-starter-kafka`, **not** raw `spring-kafka` — Boot 4
> modularised autoconfig and the raw jar ships **no** Boot Kafka autoconfiguration (no
> `KafkaTemplate`/`KafkaAdmin`/listener beans → silent no-op). **(9)** the DLQ uses an **explicit**
> `KafkaTemplate<String,String>` bean (StringSerializer), since Boot doesn't autoconfigure one for a
> consumer-only app.

## Context

`topology-service` is feature-complete (AGE graph write + GraphQL read). Next service, built
bottom-up, is **`ingestion-service`** — roadmap **Stage 1**, the metrics-plane foundation everything
downstream (`anomaly-detector`, `ai-incident-responder`, `incident-api`) needs. Per
`docs/microservices.md` it is **metrics-only**: consume Kafka `city-telemetry`, write rows into a
TimescaleDB `metrics` hypertable, **idempotently**, routing poison messages to a **DLQ**. It never
touches the AGE topology graph.

A stock Spring Boot 4.1.0 skeleton already exists (`IngestionServiceApplication`, empty
`application.yaml`, generated context test). This turns it into a working consumer that mirrors
`topology-service` conventions: **plain JDBC (not JPA)**, constructor injection, an `exception/` type,
javadoc in the established voice, **verify by running** (no tests).

**Design contract (from `docs/microservices.md`) — do not re-derive:**
- `city-telemetry` message (JSON, keyed by `sensor_id`):
  `{ "timestamp":"...Z", "sensor_id":"s-00042", "metric_type":"pipe_pressure", "value":3.2, "status":"OK" }`
- Patterns committed: **Idempotent Consumer** + **Dead Letter Queue**.
- Sole writer of the `metrics` hypertable; HPA-on-lag is Stage 4 (out of scope here).

**Decisions (baked in):**
- **`StringDeserializer` + parse JSON ourselves** (Boot-4 Jackson 3 `tools.jackson.databind.ObjectMapper`)
  — dodges spring-kafka⇄Jackson version coupling; every failure becomes one uniform exception → DLQ.
- **Idempotency = natural key + `ON CONFLICT DO NOTHING`**, unique index `(timestamp, sensor_id,
  metric_type)` (includes the hypertable time column, as TimescaleDB requires).
- **Record listener first** (one message → one insert); batch throughput is a deferred optimization.
- **Schema via `schema.sql` + `spring.sql.init.mode: always`** (idempotent), the low-code analog of
  topology's startup seed.
- **`metrics_5m_avg` continuous aggregate deferred** to the analytics/`anomaly-detector` step so this
  service stays a pure metrics writer.

## Steps

### A. Build & toolchain
- [x] **1. foojay plugin.** `settings.gradle.kts` — add the `org.gradle.toolchains.foojay-resolver-convention`
  `1.0.0` plugin block (copy from `topology-service/settings.gradle.kts`) so JDK 25 auto-provisions
  (machine default is JDK 21). Keep `rootProject.name = "ingestion-service"`.
- [x] **2. Dependencies.** `build.gradle.kts` — add `spring-boot-starter-jdbc`,
  `spring-boot-starter-actuator`, `spring-boot-starter-json` (Jackson 3 `ObjectMapper` bean — the plain
  starter lacks it), `org.springframework.kafka:spring-kafka` (Boot BOM manages the version),
  `runtimeOnly("org.postgresql:postgresql")`. Keep existing test deps.

### B. Schema (TimescaleDB owns the metrics table)
- [x] **3. `src/main/resources/schema.sql`** (new) —
  `CREATE TABLE IF NOT EXISTS metrics (timestamp TIMESTAMPTZ NOT NULL, sensor_id TEXT NOT NULL,
  metric_type TEXT NOT NULL, value DOUBLE PRECISION NOT NULL, status TEXT NOT NULL)`;
  `SELECT create_hypertable('metrics','timestamp', if_not_exists => TRUE)`;
  `CREATE UNIQUE INDEX IF NOT EXISTS metrics_uq ON metrics (timestamp, sensor_id, metric_type)`.

### C. Consume → validate → persist
- [x] **4. `telemetry/TelemetryMessage.java`** (new record) — `Instant timestamp, String sensorId,
  String metricType, double value, String status`; Jackson `@JsonProperty` for snake_case
  (`sensor_id`, `metric_type`).
- [x] **5. `exception/InvalidTelemetryException.java`** (new) — mirrors `AgeQueryException`; thrown on
  parse/validation failure, carries the raw payload for the DLQ.
- [x] **6. `telemetry/TelemetryParser.java`** (new) — `parse(String) → TelemetryMessage` via the
  injected Jackson 3 `ObjectMapper`, then validate (non-blank `sensor_id`/`metric_type`/`status`,
  non-null `timestamp`). Any problem → `InvalidTelemetryException`.
- [x] **7. `repository/MetricRepository.java`** (new) — `@Repository`, **instance** `JdbcTemplate`
  (constructor-injected, like `TopologyRepository`); `save(...)` =
  `INSERT INTO metrics (...) VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING`.
- [x] **8. `consumer/TelemetryConsumer.java`** (new) — `@KafkaListener(topics="city-telemetry",
  groupId="ingestion-service")` on the `String` payload → `TelemetryParser` → `MetricRepository`;
  slf4j logging in the topology voice.

### D. Kafka wiring & resilience (DLQ)
- [x] **9. `config/KafkaConfig.java`** (new) — `NewTopic` beans for `city-telemetry` (≈3 partitions,
  per-`sensor_id` ordering) and `city-telemetry.DLQ` (`KafkaAdmin` auto-creates them);
  `DefaultErrorHandler` with a small `FixedBackOff` (≈2 retries) + `DeadLetterPublishingRecoverer`
  (uses the autoconfigured `KafkaTemplate`) publishing failures to `city-telemetry.DLQ`; register on
  the listener container factory.
- [x] **10. `src/main/resources/application.yaml`** — `spring.kafka`: `bootstrap-servers`
  (`${KAFKA_BOOTSTRAP:localhost:9092}`), consumer `group-id: ingestion-service`,
  `key/value-deserializer: StringDeserializer`, `auto-offset-reset: earliest`,
  `enable-auto-commit: false`. `spring.datasource` → TimescaleDB
  (`localhost:${TIMESCALE_PORT:5433}`, `smartcity`), Hikari (no AGE `connection-init-sql`).
  `spring.sql.init.mode: always`. Actuator `health,info`.

### E. Local-dev infra
- [x] **11. `docker-compose.yml`** (repo root) — add a single-node **KRaft** `kafka` service (official
  `apache/kafka` image, `localhost:9092`, healthcheck); update the header comment that currently says
  the file is "just the shared databases." Prod target stays Strimzi/K8s.

### F. Cleanup
- [x] **12. Delete `src/test/.../IngestionServiceApplicationTests.java`** — per the no-tests
  preference, and because `contextLoads` would now require a live broker + DB (listeners start on boot).

## Critical files

- New: `src/main/java/inno/ipr/ingestion_service/{telemetry/TelemetryMessage.java,
  telemetry/TelemetryParser.java, consumer/TelemetryConsumer.java, repository/MetricRepository.java,
  config/KafkaConfig.java, exception/InvalidTelemetryException.java}`
- New: `src/main/resources/schema.sql`
- Edit: `settings.gradle.kts`, `build.gradle.kts`, `src/main/resources/application.yaml`,
  repo-root `docker-compose.yml`
- Delete: `src/test/.../IngestionServiceApplicationTests.java`

**Reuse / mirror (don't reinvent):** constructor-injected instance `JdbcTemplate` + `@Repository` shape
from `topology-service/.../repository/TopologyRepository.java`; the `exception/AgeQueryException`
pattern; javadoc voice + slf4j from `SeedLoader`; the Jackson 3 `ObjectMapper` (`tools.jackson.databind`)
used across topology-service.

## Verification (by running, not tests)

1. `docker compose up -d timescaledb kafka` (age-db not needed for this service).
2. `cd ingestion-service && ./gradlew bootRun` — `schema.sql` creates the `metrics` hypertable;
   `NewTopic` beans create `city-telemetry` + `city-telemetry.DLQ`; the consumer starts.
3. **Happy path:** produce one message with `kafka-console-producer` (inside the `kafka` container):
   `{"timestamp":"2026-07-24T12:00:00Z","sensor_id":"s-00042","metric_type":"pipe_pressure","value":3.2,"status":"OK"}`;
   then `psql postgres://smartcity:smartcity@localhost:5433/smartcity -c "SELECT * FROM metrics;"` → 1 row.
4. **Idempotency:** produce the same message again → still 1 row (`ON CONFLICT DO NOTHING`).
5. **DLQ:** produce malformed JSON (or missing `sensor_id`) → no new `metrics` row; the record appears
   on `city-telemetry.DLQ` (confirm with `kafka-console-consumer`).

## Deferred (out of scope here)

- `metrics_5m_avg` continuous aggregate + compression/retention → analytics/`anomaly-detector` step.
- Batch listener + `jdbcTemplate.batchUpdate` throughput optimization.
- HPA on Kafka lag, Helm chart, K8s/Strimzi deployment → Stage 4.
