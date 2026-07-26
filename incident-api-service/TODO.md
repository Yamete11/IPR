# TODO — incident-api-service: read-only GraphQL over `incidents` + `metrics`

## Context

All five pipeline services exist in code; `ai-incident-responder-service` writes the `incidents`
table in TimescaleDB. **`incident-api-service`** is the last service in the design — the read side
the demo dashboard queries. Per `docs/microservices.md` it is a **stateless, read-only GraphQL**
endpoint over the `incidents` table + TimescaleDB `metrics` (communication path 11:
`dashboard → incident-api → incidents + TimescaleDB`). It owns no tables and consumes no Kafka.

Java 25 / Spring Boot 4.1.0, mirroring `topology-service`'s GraphQL setup: **plain JDBC (not JPA)**,
constructor injection, javadoc voice, **verify by running** (GraphiQL, no tests). The scaffold is the
stock Boot skeleton (bare `build.gradle.kts`, name-only `application.yaml`, default app + test,
`HELP.md`).

**Design contract (from `docs/microservices.md`) — do not re-derive:**
- Read-only **GraphQL** (single typed endpoint) for the dashboard; no writes, no Kafka.
- Reads the `incidents` table **and** TimescaleDB `metrics` (both in the same TimescaleDB instance).
- Stateless, horizontally scalable.

## Decisions (baked in)

- **Single datasource — TimescaleDB only.** Both `incidents` (written by ai-incident-responder) and
  `metrics` (written by ingestion) live in the TimescaleDB instance, so this service needs just the
  default `spring.datasource` at `localhost:${TIMESCALE_PORT:5433}`. No age-db, no second datasource.
- **Read-only — does NOT create tables.** Unlike ingestion/ai-responder, no `schema.sql` and no
  `spring.sql.init`; it reads tables the writer services own. If a table is missing at query time the
  query errors (acceptable for a read API); it does not block startup.
- **GraphQL modelled directly, no JSON scalar.** The two JSONB array columns (`impacted_objects`,
  `remediation_steps`) are parsed in the repository into `List<String>` and exposed as `[String!]!`,
  so no `graphql-java-extended-scalars` dependency is needed (simpler than topology, which needed the
  JSON scalar for arbitrary agtype maps). Timestamps are exposed as ISO `String`.
- **Distinct dev port.** `topology-service` binds 8080; set `server.port: ${SERVER_PORT:8081}` so both
  GraphQL services can run locally at once.
- **Controller → repositories directly.** Queries are simple `SELECT`s, so no separate service layer
  (topology needed one only for agtype parsing, which here lives in the repository).

## GraphQL schema (draft — `resources/graphql/schema.graphqls`)
```graphql
type Incident {
  incidentId: ID!
  alertId: String!
  sensorId: String!
  metricType: String
  criticality: String!
  impactedObjects: [String!]!
  remediationSteps: [String!]!
  summary: String
  model: String
  createdAt: String!
}

type Metric {
  timestamp: String!
  sensorId: String!
  metricType: String!
  value: Float!
  status: String!
}

type Query {
  incidents(limit: Int = 50): [Incident!]!          # most recent first
  incident(id: ID!): Incident                        # by incident_id
  incidentsBySensor(sensorId: ID!): [Incident!]!
  recentMetrics(sensorId: ID!, limit: Int = 100): [Metric!]!
}
```

## Steps

Base package `inno.ipr.incident_api_service`.

### A. Build & toolchain
- [x] **1. foojay plugin.** `settings.gradle.kts` — add `org.gradle.toolchains.foojay-resolver-convention` `1.0.0`.
- [x] **2. Dependencies.** `build.gradle.kts` — `spring-boot-starter-web`, `spring-boot-starter-graphql`,
  `spring-boot-starter-jdbc`, `spring-boot-starter-json`, `spring-boot-starter-actuator`,
  `runtimeOnly("org.postgresql:postgresql")`. (Copy topology's GraphQL deps; add `-json` for the
  Jackson 3 `ObjectMapper` used to parse JSONB arrays.)

### B. Schema & DTOs
- [x] **3. `resources/graphql/schema.graphqls`** — the schema above.
- [x] **4. DTOs** — `graphql/dto/IncidentDto.java` (record: `incidentId, alertId, sensorId, metricType,
  criticality, impactedObjects: List<String>, remediationSteps: List<String>, summary, model,
  createdAt: String`) and `graphql/dto/MetricDto.java` (record: `timestamp, sensorId, metricType,
  value, status` — timestamp/created as ISO String). Record component names must equal schema fields.

### C. Read repositories
- [x] **5. `repository/IncidentReadRepository.java`** — instance `JdbcTemplate`, Jackson 3 `ObjectMapper`.
  - `findRecent(int limit)`: `SELECT ... FROM incidents ORDER BY created_at DESC LIMIT ?`.
  - `findById(String incidentId)`: single row (return `Optional`/nullable for the `incident` field).
  - `findBySensor(String sensorId)`: `... WHERE sensor_id = ? ORDER BY created_at DESC`.
  - RowMapper parses `impacted_objects`/`remediation_steps` JSONB text → `List<String>` via
    `objectMapper`; `created_at` timestamptz → ISO string (`Instant.toString()`).
- [x] **6. `repository/MetricReadRepository.java`** — instance `JdbcTemplate`.
  - `findRecentBySensor(String sensorId, int limit)`:
    `SELECT "timestamp", sensor_id, metric_type, value, status FROM metrics WHERE sensor_id = ?
     ORDER BY "timestamp" DESC LIMIT ?` → `List<MetricDto>`.

### D. Controller & config
- [x] **7. `graphql/IncidentGraphQLController.java`** — `@Controller` with four `@QueryMapping`
  methods (`incidents`, `incident`, `incidentsBySensor`, `recentMetrics`) using `@Argument`;
  method names = schema `Query` fields; delegate to the two repositories.
- [x] **8. `application.yaml`** — `spring.datasource` → TimescaleDB (`localhost:${TIMESCALE_PORT:5433}`,
  `smartcity`), Hikari pool; `server.port: ${SERVER_PORT:8081}`; actuator `health,info`; a `dev`
  profile enabling GraphiQL + schema printer (copy topology's `---`/`on-profile: dev` block). No
  `spring.sql.init`.

### E. Cleanup
- [x] **9. Delete** `src/test/.../IncidentApiServiceApplicationTests.java` (no-tests preference;
  `contextLoads` would need a live DB) and `HELP.md`.

## Critical files
- New under `src/main/java/inno/ipr/incident_api_service/`:
  `graphql/IncidentGraphQLController.java`, `graphql/dto/{IncidentDto,MetricDto}.java`,
  `repository/{IncidentReadRepository,MetricReadRepository}.java`
- New resources: `src/main/resources/graphql/schema.graphqls`
- Edit: `settings.gradle.kts`, `build.gradle.kts`, `application.yaml`
- Delete: `src/test/.../IncidentApiServiceApplicationTests.java`, `HELP.md`

**Reuse / mirror:** topology-service `graphql/TopologyGraphQLController` (`@Controller` +
`@QueryMapping`/`@Argument`, method-name = schema-field), its `application.yaml` dev-profile GraphiQL
block, and the `build.gradle.kts` GraphQL dependency set; ingestion/anomaly `repository/*` instance-
`JdbcTemplate` `@Repository` shape; the `incidents` DDL in `ai-incident-responder-service/schema.sql`
and the `metrics` DDL in `ingestion-service/schema.sql` for exact column names/types.

## Verification (by running — GraphiQL, not tests) — ✅ DONE 2026-07-25

Ran self-contained (no `ANTHROPIC_API_KEY` needed): started `timescaledb`, created the `incidents`
table + seeded one row via psql (`metrics` already held s-00042 data from the anomaly-detector run),
`bootRun --args='--spring.profiles.active=dev'` → started on port 8081. All four GraphQL queries
returned correctly over `POST /graphql`: `incidents` (JSONB arrays surfaced as `[String!]`),
`incident(id:"i-demo")`, `incidentsBySensor(sensorId:"s-00042")`, and
`recentMetrics(sensorId:"s-00042")` (real readings). Torn down afterward.


1. `docker compose up -d timescaledb`.
2. Ensure the tables have data. Either run the full pipeline once (ingestion → simulator →
   anomaly-detector → ai-incident-responder, which needs `ANTHROPIC_API_KEY`), **or** for a
   self-contained check insert sample rows with psql:
   ```sql
   INSERT INTO metrics VALUES (now(),'s-00042','pipe_pressure',0.0,'FAULT');
   INSERT INTO incidents (incident_id, alert_id, sensor_id, metric_type, criticality,
       impacted_objects, remediation_steps, summary, model)
   VALUES ('i-demo','a-demo','s-00042','pipe_pressure','HIGH',
       '["node-5","bld-12","bld-19"]','["Dispatch crew to node-5","Isolate the pipeline"]',
       'Zero pressure at node-5','claude-opus-4-8');
   ```
   (The `metrics`/`incidents` tables must exist first — created by ingestion / ai-incident-responder.)
3. `cd incident-api-service && ./gradlew bootRun` (add `--args='--spring.profiles.active=dev'` for GraphiQL).
4. Open GraphiQL at `http://localhost:8081/graphiql` and run:
   - `{ incidents(limit:10){ incidentId sensorId criticality summary impactedObjects remediationSteps createdAt } }`
   - `{ incident(id:"i-demo"){ criticality remediationSteps } }`
   - `{ incidentsBySensor(sensorId:"s-00042"){ incidentId criticality } }`
   - `{ recentMetrics(sensorId:"s-00042", limit:20){ timestamp value status } }`
5. Expect the seeded/real rows back with `impactedObjects`/`remediationSteps` as JSON string arrays.
   Tick each `[ ]`→`[x]` after the run.

## Deferred
- Pagination/cursors, criticality/time-range filters, subscriptions for live dashboard updates.
- Auth on the endpoint; Helm/HPA/K8s (Stage 4). This is the final service in the design.
