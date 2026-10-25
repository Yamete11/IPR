# Kubernetes storage, resources and autoscaling; event-driven patterns

How the Stage 4 deployment (`deploy/`) uses Kubernetes primitives for the stateful parts of the system,
how the ingestion service scales on Kafka lag, and where the three event-driven patterns live in code.
The numbers in sections 3 and 4 come from a run on the Kind cluster (Kubernetes 1.36, KEDA 2.21,
Strimzi 1.3) on 2026-10-25.

## 1. Deployment vs StatefulSet

| | Deployment | StatefulSet |
|---|---|---|
| Pod identity | Interchangeable, random suffix (`ingestion-service-6889f74766-fw4mq`) | Stable ordinal name (`timescaledb-0`) that survives rescheduling |
| Storage | Pods share whatever volume the template names; nothing per pod | `volumeClaimTemplates` give **each** ordinal its own PVC (`data-timescaledb-0`), re-attached to the same pod after a restart |
| Network | Reached through a Service that load-balances | A governing Service (`serviceName`) gives each pod a stable DNS name (`timescaledb-0.timescaledb`) |
| Rollout / scale | Parallel, surge new pods before old ones go | Ordered: pod N+1 starts only after N is Ready; scale-in removes the highest ordinal first |
| Deleting it | Removes the pods | Removes the pods but **keeps the PVCs**: data outlives the workload |

**In this repo:** every service (the five Java services, the simulator and the dashboard) is a
`Deployment` (`deploy/charts/<service>/templates/deployment.yaml`). They hold no local state, since
everything lives in Postgres or Kafka, so any replica can take any partition or request. KEDA can
scale ingestion-service from 1 to 6 replicas for this reason.

The two databases are `StatefulSet`s (`deploy/charts/smartcity-infra/templates/{timescaledb,age-db}.yaml`)
with `replicas: 1` and a `volumeClaimTemplates` entry `data`. Kafka is a StatefulSet-like pod too, but
Strimzi manages it (`smartcity-dual-role-0`, storage `persistent-claim` in `deploy/strimzi/kafka.yaml`)
through its own StrimziPodSet rather than a plain StatefulSet.

## 2. PersistentVolume, PersistentVolumeClaim, StorageClass

- **PersistentVolume (PV):** a piece of storage in the cluster (a disk, an NFS export, a node
  directory), cluster-scoped, with a capacity, access modes and a `persistentVolumeReclaimPolicy`.
- **PersistentVolumeClaim (PVC):** a namespaced *request* for storage ("5Gi, ReadWriteOnce, class X").
  Pods mount claims, never PVs directly. The control plane binds each PVC to exactly one PV.
- **StorageClass (SC):** a recipe for making PVs on demand: `provisioner`, `reclaimPolicy`,
  `volumeBindingMode`, `allowVolumeExpansion`. One class can be marked default; a PVC that omits
  `storageClassName` gets it.

There are two ways a PVC finds its PV.

**Dynamic provisioning** (used by both databases and Kafka). The PVC names a class (or gets the
default), and the class's provisioner creates a PV to match. On Kind the default class is `standard`,
provisioner `rancher.io/local-path`, which creates a directory on the node.

```
$ kubectl get pvc -n smartcity
data-age-db-0        Bound  pvc-4fec022d-…  5Gi   RWO  standard
data-timescaledb-0   Bound  pvc-f02e0e58-…  10Gi  RWO  standard
```

**Static provisioning** (`storage.staticPvDemo.enabled`). An admin creates the PV by hand, here a
`hostPath` PV, and the PVC claims it by `volumeName`. Both set `storageClassName: ""` so the default
class does not provision a fresh volume instead. No provisioner is involved, and the PVC binds at once:

```
smartcity-static-pv   1Gi  RWO  Retain  Bound  smartcity/smartcity-static-pvc
```

### The chart's StorageClass

`storage.createClass=true` creates `smartcity-retain` (`deploy/charts/smartcity-infra/templates/storage.yaml`).
It uses the same provisioner as `standard` but changes two policies:

- **`reclaimPolicy: Retain`** (the default class uses `Delete`). When the PVC is deleted, the PV moves
  to `Released` and the data stays. With `Delete` the PV and its data go with the claim. Measured on Kind:
  a probe PVC on `smartcity-retain`, used by one pod and then deleted, left its PV `Released`, and the
  PV had to be removed by hand.
- **`volumeBindingMode: WaitForFirstConsumer`**. The PVC stays `Pending` until a pod using it is
  scheduled, so the volume is created on that pod's node. This matters for node-local storage. Measured:
  `Pending` before the pod, `Bound` right after. `Immediate` would bind straight away, possibly on a
  node the pod can't use.

To put the databases on it, set `storage.className=smartcity-retain`. When `className` is empty, the
field is left out of `volumeClaimTemplates` entirely. That matters because a StatefulSet's
`volumeClaimTemplates` are **immutable**: switching class on a running release needs
`kubectl delete statefulset <name> --cascade=orphan` (pods and PVCs stay), then `helm upgrade`, and
existing PVCs keep their old class anyway. Only new ordinals get the new one.

Access modes: everything here is `ReadWriteOnce` (one node mounts it read-write), which is right for a
database. `ReadWriteMany` needs a shared filesystem (NFS, CephFS) and is not available from local-path.

## 3. Requests, limits and QoS

`requests` are what the **scheduler** reserves: a pod only lands on a node with that much
unreserved CPU and memory. `limits` are what the **kubelet/kernel** enforces at runtime:

- **CPU** is compressible. Over the limit, the container is *throttled* (CFS quota), not killed.
- **Memory** is not. Over the limit, the container is **OOM-killed** and restarted.

Every chart sets a CPU request, a memory request and a memory limit, but **no CPU limit**
(e.g. ingestion-service: `cpu: 250m`, `memory: 512Mi` → limit `1Gi`). That is deliberate:

- A CPU limit would throttle the JVM during startup (JIT, class loading) and during GC, long after
  the node had idle cores to spare. Without a limit, a pod can burst into idle CPU and the request
  still guarantees its share under contention.
- The memory limit is kept, because a runaway pod must not take memory from its neighbours. Since
  JDK 10 the JVM is container-aware and sizes its heap from the cgroup limit. The default is 25% of
  it; the Dockerfiles raise that to `-XX:MaxRAMPercentage=75.0` and add `-XX:+ExitOnOutOfMemoryError`,
  so a heap OOM restarts the pod instead of leaving a broken JVM running.

Requests ≠ limits makes every pod **Burstable** QoS class. `Guaranteed` needs requests = limits for
both resources. `BestEffort` has none and is evicted first under node memory pressure. Two effects
seen in this project:

- `timescaledb-tune` reads the **memory limit** and derives Postgres settings from it, including
  `max_connections=25` at 2Gi. That was too few once ingestion scaled out, so the StatefulSet overrides
  it (`timescaledb.maxConnections: 150`).
- On the 8 GB Docker VM the sum of *limits* exceeds the node's memory (overcommit is allowed, only
  requests have to fit). When another compose stack shared the VM, pods were OOM-killed and restarted.

Requests also feed autoscaling. A CPU-based HPA computes utilisation as usage ÷ request, so a pod
without a CPU request can't be scaled on CPU at all.

## 4. HPA on a custom metric: Kafka consumer lag

The standard HPA loop runs every 15 s:

```
desiredReplicas = ceil( currentReplicas × currentMetric / targetMetric )
```

For an `AverageValue` external metric, that is `ceil(totalLag / lagThreshold)`, clamped to
`[minReplicas, maxReplicas]`. CPU is the wrong signal for a Kafka consumer: a consumer that has
fallen behind may well sit at low CPU, blocked on database writes. **Lag** is the backlog the service
actually has to clear.

HPA can't read Kafka itself; it only reads the metrics APIs (`metrics.k8s.io`, `custom.metrics.k8s.io`,
`external.metrics.k8s.io`). The usual route is Prometheus + kafka-exporter + prometheus-adapter.
**KEDA** shortens it: its operator serves `external.metrics.k8s.io`, queries the broker for the
consumer-group lag itself, and creates and owns the HPA (`keda-hpa-ingestion-service`). The chart only
declares a `ScaledObject` (`deploy/charts/ingestion-service/templates/scaledobject.yaml`):

| Setting | Value | Why |
|---|---|---|
| `lagThreshold` | 1000 | target lag **per replica** |
| `minReplicaCount` / `maxReplicaCount` | 1 / 6 | 6 = partitions of `city-telemetry`; a 7th consumer in the group would get no partition |
| `allowIdleConsumers` | false | KEDA itself caps replicas at the partition count |
| `pollingInterval` | 10 s | how often KEDA checks lag |
| `cooldownPeriod` | 120 s | only governs scale to **zero**; between 1 and N the HPA's own policy applies |

Unlike plain HPA, KEDA can also scale to zero and back (`activationLagThreshold`). It is not used
here (min 1), so the consumer group stays joined and the lag stays measurable.

**Measured run** (`deploy/scripts/demo-hpa.sh`: sensor-simulator at ~1000 sensors, 100 ms interval):

```
18:45:55  lag 0       target 0/1k    replicas 1
18:46:16  lag 5036    target 6k/1k   replicas 1 → 4   (HPA scale-up policy: max 4 pods or 100% per 15 s)
18:46:48  lag 3513    target 830/1k  replicas 6
…         lag 2–5k for the remaining 4 minutes, replicas 6, avg lag per replica < 1000
18:51:18  load stopped
18:51:38  replicas 5
18:55:23  replicas 4
18:56:24  replicas 1
```

Scale-up is fast: 1 → 6 in under a minute, with the default policy allowing a doubling or +4 pods
every 15 s. Scale-down is slow by design. The HPA's default
`behavior.scaleDown.stabilizationWindowSeconds: 300` makes it act on the **highest** recommendation
of the last 5 minutes, so a brief dip in lag doesn't trigger a scale-in followed by another scale-out.
Each scale event moves partitions between consumers through a group rebalance, which pauses
consumption. That is a reason to change the replica count rarely. The run reached 1 replica about
5 minutes after the load stopped, which is that window and not KEDA's 120 s cooldown.

Ingestion stays correct while partitions move during a rebalance thanks to the idempotent consumer
(next section). A partition handed to a new replica can be re-read from its last committed offset.

## 5. Event-driven patterns

### Idempotent Consumer

Kafka gives at-least-once delivery: a crash or rebalance between processing and committing the offset
redelivers records. Each consumer therefore turns a duplicate into a no-op at the store:

- **ingestion-service**: `INSERT … ON CONFLICT DO NOTHING` against the unique
  `(timestamp, sensor_id, metric_type)` key (`MetricRepository`). A redelivered batch writes nothing new.
- **ai-incident-responder**: `UNIQUE(alert_id)` + `ON CONFLICT (alert_id) DO NOTHING`
  (`IncidentRepository`), plus an `existsByAlertId` check before calling Claude, so a redelivered
  alert costs neither a second incident nor a second billed API call.
- **anomaly-detector** prevents duplicates at the source instead: alerts are edge-triggered, so a fault
  that stays active doesn't emit a new alert on every poll.

### Circuit Breaker

The Claude call in `ClaudeIncidentPlanner` is wrapped in a Resilience4j breaker (`ResilienceConfig`):
count-based window of 5 calls, at least 3 calls, opens at ≥ 50% failures, stays **open** 30 s
(calls fail at once with `CallNotPermittedException`, no request leaves the pod), then **half-open**
lets 2 trial calls through, which close it again or re-open it. State and counts are exported as
`resilience4j_circuitbreaker_*` on `/actuator/prometheus`.

The breaker is combined with the Kafka error handler (`ai-incident-responder-service/.../config/KafkaConfig.java`).
Failures that are not bad data are retried with **unlimited** exponential back-off, which pauses the
container rather than blocking the thread. A short retry budget would make every retry hit the open
breaker within 30 s and dead-letter a *valid* alert. The alert stays in Kafka until Claude is back.

### Dead Letter Queue

A poison message (one that can never be processed) must not block its partition forever.

- **ingestion-service**: `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` → `city-telemetry.DLQ`.
  In batch mode, `BatchListenerFailedException` names the bad record's index, so only that record is
  dead-lettered and its neighbours in the batch are committed once (`KafkaErrorHandlingIntegrationTest`).
- **ai-incident-responder**: `InvalidAlertException` (malformed JSON, missing `alert_id`/`sensor_id`)
  is non-retryable and goes straight to `infrastructure-alerts.DLQ`. Everything else is retried,
  as described above.

The DLQ keeps the original payload and headers (exception class, message, original
topic/partition/offset), so a fixed record can be replayed onto the main topic.

## Reproduce

```bash
SKIP_IMAGES=1 deploy/scripts/up.sh          # or up.ps1 -SkipImages
deploy/scripts/demo-hpa.sh                  # watch 1 -> 6
deploy/scripts/demo-hpa.sh stop             # watch 6 -> 1 (~5 min)

helm upgrade smartcity deploy/charts/smartcity -n smartcity --reuse-values \
  --set smartcity-infra.storage.createClass=true \
  --set smartcity-infra.storage.staticPvDemo.enabled=true
kubectl get sc,pv; kubectl -n smartcity get pvc
```
