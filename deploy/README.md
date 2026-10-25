# Kubernetes deployment (Stage 4)

Everything runs on a local **Kind** cluster: Kafka through the **Strimzi** operator, the two
PostgreSQL instances as StatefulSets, one **Helm chart per service** under an umbrella chart, and
**KEDA** scaling the ingestion service on Kafka consumer lag. Credentials are Kubernetes
**Secrets**, addresses a **ConfigMap**.

```
deploy/
  kind/kind-config.yaml          1 control-plane + 2 workers
  strimzi/kafka.yaml             KafkaNodePool + Kafka (KRaft, single node, persistent)
  strimzi/topics.yaml            city-telemetry (6 partitions), infrastructure-alerts, both .DLQ topics
  charts/smartcity-infra/        age-db + timescaledb StatefulSets, ConfigMap smartcity-config,
                                 Secrets postgres-credentials + anthropic
  charts/<service>/              one chart per service (Deployment, Service, probes, init waits);
                                 ingestion-service also has the KEDA ScaledObject
  charts/sensor-simulator/       load generator, 0 replicas until you scale it
  charts/dashboard/              Angular UI on nginx, proxying both GraphQL services
  charts/smartcity/              umbrella chart depending on all of the above
  scripts/up.{ps1,sh}            the whole bring-up, idempotent
  scripts/build-images.{ps1,sh}  docker build + kind load for the six images + dashboard
  scripts/demo-hpa.{ps1,sh}      start load, watch KEDA scale ingestion-service 1 -> 6
  scripts/down.{ps1,sh}          delete the cluster
```

Each service directory in the repo root carries its `Dockerfile` (multi-stage: JDK 25 build with
the committed Gradle wrapper, JRE 25 runtime with the Boot jar exploded into layers, non-root).

## Prerequisites

Docker Desktop, `kind`, `kubectl`, `helm` (v3.8+ for OCI charts; Helm 4 works). About 6 GB of
RAM for Docker: Kafka, two databases and six JVMs. Stop other container stacks first: with an
8 GB Docker VM shared with another compose project, pods restarted under memory pressure and the
Kind API server dropped connections (observed on 2026-10-25).

```
cp .env.example .env     # credentials for the Secrets; ANTHROPIC_API_KEY only for real Claude calls
```

## Bring-up

```powershell
.\deploy\scripts\up.ps1            # Windows
```
```bash
deploy/scripts/up.sh               # macOS / Linux / Git Bash
```

What it does, in order (each step is safe to re-run):

1. `kind create cluster --config deploy/kind/kind-config.yaml` (skipped if it exists).
2. Installs the Strimzi operator from its OCI chart into the `smartcity` namespace.
3. Applies `strimzi/kafka.yaml`, waits for the `Kafka` resource to be Ready, applies the four
   `KafkaTopic` resources and waits for them.
4. Installs KEDA into the `keda` namespace.
5. Builds the six images and loads them into the Kind nodes (`-SkipImages` / `SKIP_IMAGES=1`
   to skip on a redeploy).
6. `helm dependency build` + `helm upgrade --install smartcity deploy/charts/smartcity`, passing
   the Postgres credentials, database name, Claude mock flag, model and API key from `.env` as
   `--set` values. The infra chart turns them into the Secrets and the ConfigMap.

First run takes a while: image pulls for Strimzi, Kafka, KEDA and the two databases, plus a
Gradle build inside each service image. Rebuilds are fast thanks to BuildKit cache mounts.

Expected end state:

```
$ kubectl -n smartcity get pods
NAME                                             READY   STATUS
age-db-0                                         1/1     Running
timescaledb-0                                    1/1     Running
smartcity-dual-role-0                            1/1     Running
smartcity-entity-operator-...                    1/1     Running
strimzi-cluster-operator-...                     1/1     Running
topology-service-...                             1/1     Running
ingestion-service-...                            1/1     Running
anomaly-detector-service-...                     1/1     Running
ai-incident-responder-service-...                1/1     Running
incident-api-service-...                         1/1     Running
dashboard-...                                    1/1     Running
```

`sensor-simulator` exists as a Deployment with 0 replicas.

Watching the cluster: `kubectl -n smartcity get pods -w` in a terminal, or a UI such as k9s
(`winget install Derailed.k9s`, then `k9s -n smartcity`) or Headlamp. Docker Desktop's Kubernetes
tab shows only its own built-in cluster, not Kind.

## Start-up ordering

Nothing is installed in sequence; the charts encode the dependencies instead:

- Every service pod has init containers that wait for its databases and/or Kafka to accept
  connections.
- `anomaly-detector-service` additionally waits for the `metrics` hypertable (created by
  `ingestion-service`), both in an init container (`psql` against
  `timescaledb_information.hypertables`) and inside the app (`anomaly.schema.wait-timeout`).
- Topics are created by the Strimzi Topic Operator before any service starts; the services'
  own `NewTopic` beans find them and do nothing.
- Readiness probes include the datasource, so a pod is not routed to until its database is
  reachable.

## Using it

The dashboard (nginx proxies `/api/topology/graphql` and `/api/incidents/graphql` to the two
services in-cluster, so only one port-forward is needed):

```
kubectl -n smartcity port-forward svc/dashboard 8088:80     # http://localhost:8088
```

Or port-forward the two GraphQL services directly:

```
kubectl -n smartcity port-forward svc/topology-service 8080:8080
kubectl -n smartcity port-forward svc/incident-api-service 8081:8081
```

Then `POST http://localhost:8080/graphql` with `{ "query": "{ dependents(id: \"node-5\") { id label } }" }`
returns `bld-12` and `bld-19`, and `{ vertices { id } }` returns the 9 seeded vertices. On
`http://localhost:8081/graphql`, `{ incidents { incidentId criticality impactedObjects } }`
is empty until the load runs.

Generate telemetry and watch the pipeline:

```powershell
.\deploy\scripts\demo-hpa.ps1        # scales sensor-simulator to 1: ~1000 sensors at 100 ms
```

Within a minute or two:

- `kubectl -n smartcity get hpa -w` shows `keda-hpa-ingestion-service` growing from 1 towards 6
  replicas as consumer-group lag on `city-telemetry` crosses the 1000-message threshold
  (`kubectl -n smartcity get scaledobject` shows it Active).
- `kubectl -n smartcity logs deploy/anomaly-detector-service` shows `ALERT` lines for the
  injected faults.
- `{ incidentsBySensor(sensorId: "s-00042") { criticality impactedObjects remediationSteps } }`
  on the incident API returns incidents whose impacted objects include `node-5`, `bld-12` and
  `bld-19` (resolved from the AGE graph). In mock mode the plan is the deterministic offline
  one; `model` is `mock`.

Stop the load with `.\deploy\scripts\demo-hpa.ps1 -Stop`; after the 120 s cooldown the ingestion
deployment returns to 1 replica.

Prometheus-format metrics are on every service at `/actuator/prometheus` (pods carry
`prometheus.io/scrape` annotations); the responder exposes the Claude circuit breaker as
`resilience4j_circuitbreaker_state`.

## Real Claude calls

Put `ANTHROPIC_API_KEY=...` and `AI_CLAUDE_MOCK=false` in `.env` and re-run `up` with
`-SkipImages`. The infra chart then creates the `anthropic` Secret and sets the mock flag in the
ConfigMap; the responder pod restarts and calls `claude-opus-5-5` (`AI_CLAUDE_MODEL`). Calls are
billed. While the circuit breaker is open the consumer pauses and retries rather than
dead-lettering the alert.

## Redeploying after a code change

```powershell
.\deploy\scripts\build-images.ps1 -Services ingestion-service
kubectl -n smartcity rollout restart deployment/ingestion-service
```

Chart-only changes: `.\deploy\scripts\up.ps1 -SkipCluster -SkipImages`.

## Tear-down

```powershell
.\deploy\scripts\down.ps1
```

## Verification status

First real run on 2026-10-25 (kind v0.32 / Kubernetes 1.36, Strimzi 1.3 / Kafka 4.3.1, KEDA,
Helm 4.2): cluster, Kafka and all four topics Ready, KEDA installed, all 7 images built and loaded,
Helm release installed, every service pod Running/Ready after the two fixes listed under Notes.
Later the same day, with the other compose stack stopped, all 11 pods were Running/Ready together.
The `demo-hpa` run scaled ingestion-service from 1 to 6 replicas in under a minute, and back to 1 about
5 minutes after the load stopped (HPA scale-down stabilization of 300 s). The optional StorageClass
and static PV demo (`smartcity-infra.storage.*`) bound as expected. Measurements and the theory
behind them: `docs/k8s-storage-and-scaling.md`. Not yet verified: incidents produced in the cluster
and the dashboard via port-forward.

If an interrupted install leaves the release in `pending-install`, `helm upgrade` refuses to run
("another operation is in progress"). Once the resources exist, either `down` + `up`, or set the
`status` in the `sh.helm.release.v1.smartcity.v1` Secret to `deployed`.

## Notes and limits

- Single-broker Kafka: replication factors are 1. Raise `replicas` in `strimzi/kafka.yaml` and
  the topic `replicas` for anything beyond a demo.
- KEDA cannot usefully exceed 6 ingestion replicas: that is the partition count of
  `city-telemetry`. Change both together.
- The databases keep their data in PersistentVolumeClaims on Kind's default storage class; they
  disappear with the cluster. `smartcity-infra.storage.createClass=true` adds a `smartcity-retain`
  class (Retain, WaitForFirstConsumer), and `storage.className` points the StatefulSets at it. That only
  works on a fresh install: `volumeClaimTemplates` are immutable. `storage.staticPvDemo.enabled=true`
  adds a hand-made hostPath PV and a PVC bound to it by name.
- TimescaleDB's image runs `timescaledb-tune` on first boot, which sizes `max_connections` from the
  memory limit (25 at 2Gi), too few for every service's Hikari pool plus a scaled-out ingestion.
  The StatefulSet overrides it on the command line (`smartcity-infra` value
  `timescaledb.maxConnections`, default 150).
- The PowerShell scripts run native commands (kind, helm, docker, kubectl) with
  `ErrorActionPreference = Continue` and check `$LASTEXITCODE`: under `Stop`, Windows PowerShell 5.1
  aborts on the first progress line those tools print to stderr whenever output is redirected.
- `bootBuildImage` (Paketo buildpacks) is a Dockerfile-free alternative for the Java images:
  `./gradlew bootBuildImage --imageName smartcity/<service>:dev`, then `kind load` as usual.
