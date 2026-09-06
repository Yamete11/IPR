# TODO — anomaly-detector-service: TimescaleDB → Kafka `infrastructure-alerts`

## Context

The pipeline runs end to end up to storage: `sensor-simulator` → Kafka `city-telemetry` →
`ingestion-service` → TimescaleDB `metrics`. Next is **`anomaly-detector-service`** — roadmap
**Stage 3 (intelligence layer)**. Per `docs/microservices.md` it is a **decoupled, scheduled** service
(its own deployable, so detection scales independently of ingestion write throughput): it **polls
TimescaleDB** (does *not* consume Kafka), evaluates threshold rules over rolling 5-minute aggregates,
and **produces critical events → Kafka `infrastructure-alerts`**. That topic feeds the future
`ai-incident-responder`, which reads the AGE graph — closing the loop back toward the topology graph.

Turns the stock Boot 4.1.0 skeleton into a working detector, mirroring `ingestion-service`/
`topology-service`: **plain JDBC (not JPA)**, constructor injection, javadoc voice, **verify by
running** (no tests).

**Design contract (from `docs/microservices.md`) — do not re-derive:**
- `infrastructure-alerts` message (JSON), keyed by `sensor_id`:
  `{ "alert_id":"a-1001", "sensor_id":"s-00042", "metric_type":"pipe_pressure", "value":0.0, "detected_at":"...Z", "rule":"pipe_pressure_min<0.5" }`
- Reads 5-min **continuous aggregates** / thresholds; **scheduled** (no Kafka consumer).
- Reads TimescaleDB directly (metrics owned/written by ingestion).

## Decisions (baked in)

- **Owns the deferred `metrics_5m_avg` continuous aggregate** (left out of ingestion): 5-min
  `time_bucket` over `metrics` → **avg, min, max, count** per `(sensor_id, metric_type)` + refresh
  policy. Depends on the `metrics` hypertable existing (ingestion-service creates it).
- **Detect on min/max, not avg** — a single fault reading averaged over hundreds of OK readings would
  never cross an avg threshold; the boundary aggregate fires on any excursion. Real-time aggregation
  includes recent unmaterialized rows, so detection is near-immediate — but modern TimescaleDB
  (2.13+, incl. 2.28 here) defaults `materialized_only` to **true**, so the aggregate must be created
  with `timescaledb.materialized_only = false` explicitly or a fresh `WITH NO DATA` view returns
  nothing until its first refresh (found and fixed during verification).
- **Continuous-aggregate DDL via a JdbcTemplate `ApplicationRunner`, not `schema.sql`** —
  `CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous)` cannot run inside a transaction block;
  `spring.sql.init` may wrap the script in one. Autocommit JdbcTemplate avoids that. Idempotent, gated
  by a property, off in tests. Mirrors topology's `SeedLoader`.
- **Scheduled poll + edge-triggered dedup** — alert fires only on the OK→breach transition (in-memory
  active-set keyed `sensor_id|metric_type`), cleared on recovery; a standing fault doesn't re-fire.
- **Producer only** (`spring-boot-starter-kafka` — Boot-4 module gotcha). Explicit
  `KafkaTemplate<String,String>` + `NewTopic`. No `@KafkaListener`, no DLQ (nothing consumed).

## Steps

### A. Build & toolchain
- [x] **1. foojay plugin.** `settings.gradle.kts` — add `org.gradle.toolchains.foojay-resolver-convention`
  `1.0.0` (copy from a sibling) so JDK 25 auto-provisions.
- [x] **2. Dependencies.** `build.gradle.kts` — add `spring-boot-starter-jdbc`, `spring-boot-starter-json`,
  `spring-boot-starter-actuator`, **`spring-boot-starter-kafka`** (NOT raw `spring-kafka`),
  `runtimeOnly("org.postgresql:postgresql")`.

### B. Continuous aggregate (owns the 5-min rollup)
- [x] **3. `db/ContinuousAggregateInitializer.java`** — `@Component implements ApplicationRunner`,
  `@ConditionalOnProperty("anomaly.schema.init-enabled", matchIfMissing=true)`, ctor `JdbcTemplate`.
  Idempotently: `CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_5m_avg WITH (timescaledb.continuous) AS
  SELECT time_bucket('5 minutes', "timestamp") AS bucket, sensor_id, metric_type, avg(value) avg_value,
  min(value) min_value, max(value) max_value, count(*) sample_count FROM metrics GROUP BY bucket,
  sensor_id, metric_type WITH NO DATA`; then `SELECT add_continuous_aggregate_policy('metrics_5m_avg',
  start_offset => INTERVAL '1 hour', end_offset => INTERVAL '10 minutes',
  schedule_interval => INTERVAL '1 minute', if_not_exists => TRUE)`. Each via `jdbc.execute(...)`
  (autocommit). Summary log like `SeedLoader`.

### C. Read side
- [x] **4. `domain/MetricWindow.java`** (record) — `String sensorId, String metricType, Instant bucket,
  double avg, double min, double max, long count`.
- [x] **5. `repository/MetricAggregateRepository.java`** — `@Repository`, instance `JdbcTemplate`.
  `findLatestWindows(Duration lookback)`: `SELECT DISTINCT ON (sensor_id, metric_type) sensor_id,
  metric_type, bucket, avg_value, min_value, max_value, sample_count FROM metrics_5m_avg
  WHERE bucket >= now() - ?::interval ORDER BY sensor_id, metric_type, bucket DESC` → `List<MetricWindow>`.

### D. Detection rules
- [x] **6. `detection/ThresholdRule.java`** — per-`metric_type` rule (which aggregate + comparison +
  label). Rules (on min/max, tuned to the simulator faults): `pipe_pressure` min<0.5
  (`"pipe_pressure_min<0.5"`); `cable_temperature` max>100 (`"cable_temperature_max>100"`);
  `substation_voltage` min<50 (`"substation_voltage_min<50"`). `evaluate(MetricWindow) ->
  Optional<Breach{value, rule}>`. Rules are configured in `application.yaml` under `anomaly.rules[]`
  (bound by `AnomalyRulesProperties`, validated by `ThresholdRuleRegistry`; label derived by
  `ThresholdRule.of`). Defaults: pipe_pressure min<0.5, cable_temperature max>120, substation_voltage min<50.

### E. Alert publish + scheduler
- [x] **7. `alert/Alert.java`** (record) — `alertId, sensorId, metricType, double value, Instant
  detectedAt, rule`; Jackson `@JsonProperty` snake_case (`alert_id`, `detected_at`).
- [x] **8. `config/KafkaConfig.java`** — `NewTopic` `infrastructure-alerts` (3 partitions); explicit
  `KafkaTemplate<String,String>` + `ProducerFactory` (StringSerializer, bootstrap from
  `spring.kafka.bootstrap-servers`) — same shape as ingestion's DLQ template.
- [x] **9. `alert/AlertPublisher.java`** — ctor `KafkaTemplate` + Jackson `ObjectMapper`. `alert_id` =
  `"a-" + short UUID`. Holds the **edge-trigger active-set** (`Set<String>` key `sensorId|metricType`):
  `publishIfNew(...)` sends only on transition into breach; `clear(...)` on recovery.
  `send("infrastructure-alerts", sensorId, json)`.
- [x] **10. `detection/AnomalyScheduler.java`** — `@Scheduled(fixedDelayString=
  "${anomaly.poll-interval-ms:15000}")`: `findLatestWindows(lookback)` → evaluate rule → breach ⇒
  `publishIfNew`; else `clear`. slf4j logging.
- [x] **11. Enable scheduling.** `@EnableScheduling` on `AnomalyDetectorServiceApplication` or a config.

### F. Config + cleanup
- [x] **12. `application.yaml`** — `spring.datasource` → TimescaleDB (`localhost:${TIMESCALE_PORT:5433}`,
  `smartcity`), Hikari (no AGE init-sql); `spring.kafka.bootstrap-servers: ${KAFKA_BOOTSTRAP:localhost:9092}`;
  `anomaly.poll-interval-ms` + `anomaly.lookback` + `anomaly.schema.init-enabled`; actuator `health,info`.
- [x] **13. Delete `src/test/.../AnomalyDetectorServiceApplicationTests.java`** — no-tests preference;
  `contextLoads` would need a live DB.

## Critical files

- New: `src/main/java/inno/ipr/anomaly_detector_service/{db/ContinuousAggregateInitializer.java,
  domain/MetricWindow.java, repository/MetricAggregateRepository.java, detection/ThresholdRule.java,
  detection/AnomalyScheduler.java, alert/Alert.java, alert/AlertPublisher.java, config/KafkaConfig.java}`
- Edit: `settings.gradle.kts`, `build.gradle.kts`, `application.yaml`,
  `AnomalyDetectorServiceApplication.java` (`@EnableScheduling`)
- Delete: `src/test/.../AnomalyDetectorServiceApplicationTests.java`

**Reuse / mirror:** ingestion `config/KafkaConfig.java` (explicit `KafkaTemplate<String,String>` +
`NewTopic` producer); `repository/MetricRepository.java` instance-`JdbcTemplate` `@Repository` shape;
topology `SeedLoader` (ApplicationRunner + property gate + summary log); Boot-4
`spring-boot-starter-<tech>` rule; Jackson 3 `tools.jackson.databind.ObjectMapper`.

## Verification (by running, not tests) — ✅ DONE 2026-07-25

Ran the full path (`docker compose up -d timescaledb kafka` → ingestion `bootRun` → simulator
`--max-messages 40 --anomaly-prob 0.4 --seed 7` → detector `bootRun`). Result: `metrics_5m_avg`
created (real-time on), and exactly **3** alerts on `infrastructure-alerts`, one per fault —
`pipe_pressure` 0.0 / s-00042, `cable_temperature` 130.0 / s-00108, `substation_voltage` 0.0 /
s-00311 — matching the snake_case contract. Edge-trigger held: 3 alert lines / 3 topic messages
across multiple later polls (no re-fire). Two fixes made during the run: `materialized_only=false`
(see Decisions) and an `initialDelay` on the scheduler so the first poll waits for the
`ApplicationRunner` to create the view (avoids a transient first-poll "relation does not exist").


1. `docker compose up -d timescaledb kafka`.
2. Start `ingestion-service` (`./gradlew bootRun`) so `metrics` exists and is fed.
3. Force faults: `cd sensor-simulator && ./.venv/Scripts/python.exe sensor_simulator.py --max-messages 40
   --interval 0.3 --anomaly-prob 0.4 --seed 7`.
4. Start `anomaly-detector-service` (`./gradlew bootRun`) — initializer creates `metrics_5m_avg`; scheduler polls.
5. `docker exec smartcity-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092
   --topic infrastructure-alerts --from-beginning --timeout-ms 8000` → expect alert JSON for faulted sensors.
6. Edge-trigger: standing fault does not re-fire on later polls. Sanity: `... psql ... "SELECT * FROM
   metrics_5m_avg ORDER BY bucket DESC LIMIT 10;"`.

## Deferred

- Per-sensor rules; recovery/clear events on the alert topic. (Thresholds are externalised — `anomaly.rules[]`.)
- Startup now waits for the `metrics` hypertable (`anomaly.schema.wait-timeout`, default 5 m) instead of
  failing when ingestion-service has not created it yet.
- Compression/retention policies. HPA/Helm/K8s (Stage 4).
- Next service: `ai-incident-responder` (consumes `infrastructure-alerts`, reads TimescaleDB + AGE, calls Claude).
