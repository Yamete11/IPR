# Kafka: architecture, producers/consumers, delivery semantics, Strimzi

The Kafka learning topics from `technology_stack_topics.md`, tied to how this repo uses Kafka. All
command output below comes from real runs on 2026-10-25:

- **compose broker**: `apache/kafka:3.9.1`, one KRaft node (`docker-compose.yml`), after
  `sensor_simulator.py --extra-sensors 97 --interval 0.2 --max-messages 30000 --seed 7` (100 sensors).
  `city-telemetry` has **3** partitions here (created by `ingestion-service`'s `NewTopic` bean).
- **Kind cluster**: Strimzi 1.3, Kafka 4.3.1, one node (`deploy/strimzi/`). `city-telemetry` has
  **6** partitions here (a `KafkaTopic` CR).

Commands run inside the broker (`docker exec smartcity-kafka …` or
`kubectl -n smartcity exec smartcity-dual-role-0 -- …`); `K=/opt/kafka/bin`, `B=--bootstrap-server localhost:9092`.

## 1. Architecture: brokers, controller, topics, partitions

| Term | What it is | Here |
|---|---|---|
| **Broker** | A server that stores partition replicas and serves produce/fetch requests | 1 broker (node id 1 in compose, 0 in Kind) |
| **Controller** | Owns cluster metadata: which brokers are alive, partition leaders, ISR, topic configs. In KRaft a quorum of controllers replicates the metadata as a Raft log (`__cluster_metadata`) | Same process as the broker (`process.roles=broker,controller`, "combined mode") |
| **Topic** | A named stream, split into partitions | `city-telemetry`, `infrastructure-alerts`, both `*.DLQ` |
| **Partition** | An ordered, append-only log. The unit of ordering, parallelism and replication | 3 (compose) / 6 (Kind) for `city-telemetry` |
| **Replica / leader / ISR** | Each partition has RF copies; one is the leader that takes reads and writes; the in-sync replicas (ISR) are followers that have caught up | RF = 1, so the leader is the only replica and ISR = {leader} |

```
$ $K/kafka-metadata-quorum.sh $B describe --status          # compose
LeaderId:               1
HighWatermark:          1123
CurrentVoters:          [{"id": 1, "endpoints": ["CONTROLLER://localhost:9093"]}]

$ $K/kafka-topics.sh $B --describe --topic city-telemetry
Topic: city-telemetry  PartitionCount: 3  ReplicationFactor: 1
  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

The controller quorum has one voter, so it has no fault tolerance: Raft needs a majority, and
production runs 3 or 5 controllers (usually on dedicated nodes). The broker and the controller
listen on separate listeners (`PLAINTEXT://:9092` for clients, `CONTROLLER://:9093` for the quorum).

### The commit log

A partition is a directory of **segments** on the broker's disk. Each segment is three files named
after the segment's base offset:

```
$ ls -la /tmp/kafka-logs/city-telemetry-0
10485760  00000000000000000000.index       # offset    -> byte position (sparse)
  504343  00000000000000000000.log         # the record batches themselves
10485756  00000000000000000000.timeindex   # timestamp -> offset       (sparse)
       8  leader-epoch-checkpoint
      43  partition.metadata
```

- **Append-only.** A producer's batch goes to the end of the active segment and gets the next
  offsets. Records are never updated in place. A consumer's position is an offset in this file.
- **Record batches.** The log stores what the producer sent: whole compressed batches. The
  simulator's `linger.ms=50` + `compression.type=lz4` produce one batch per emission round per
  partition (42 sensors hash to partition 0, so each batch holds 42 records):
  ```
  $ $K/kafka-dump-log.sh --files .../city-telemetry-0/00000000000000000000.log --print-data-log
  baseOffset: 0 lastOffset: 41 count: 42 producerId: -1 ... position: 0 size: 1618 compresscodec: lz4
  | offset: 0 key: s-01000 payload: {"timestamp": "2026-10-25T18:06:03.414903Z", "sensor_id": "s-01000", ...}
  | offset: 1 key: s-01001 payload: {...}
  baseOffset: 42 lastOffset: 83 count: 42 ... position: 1618 size: 1603 compresscodec: lz4
  ```
  12 600 records take 504 343 bytes, about 40 bytes per record including batch headers. Each JSON
  payload alone is about 135 bytes.
- **Sparse indexes.** `.index` gets an entry every `index.interval.bytes` (4096) of log, here about
  every 126 records. A fetch at offset N binary-searches the index for the nearest entry below N, then
  scans forward from that byte position. `.timeindex` does the same for timestamps, which is how
  `--reset-offsets --to-datetime` (below) finds an offset. Both files are preallocated to
  `segment.index.bytes` (10 MiB) and trimmed when the segment rolls.
  ```
  $ $K/kafka-dump-log.sh --files .../00000000000000000000.index
  offset: 167 position: 4867
  offset: 293 position: 9769
  offset: 419 position: 14761
  $ $K/kafka-dump-log.sh --files .../00000000000000000000.timeindex
  timestamp: 1792951564040 offset: 167
  timestamp: 1792951564648 offset: 293
  ```
- **Segment roll and retention.** The active segment rolls when it reaches `segment.bytes` (1 GiB) or
  is `segment.ms` old (7 days). Retention (`cleanup.policy=delete`) removes **whole closed segments**
  once their newest record is older than `retention.ms`, or the partition exceeds `retention.bytes`.
  Kafka never deletes individual records. It is the same idea as TimescaleDB's `drop_chunks`
  (`docs/timeseries-storage.md`): unlinking a file is cheap and leaves no dead tuples. The compose
  topic keeps the 7-day default. The `KafkaTopic` in Kind sets `retention.ms: 86400000` (1 day).
  `cleanup.policy=compact` is the alternative: it keeps only the latest record per key (that is how
  `__consumer_offsets` works). None of our data topics use it.
- **The metadata log is a commit log too.** `__cluster_metadata-0` has the same `.log`/`.index`
  layout plus `*.snapshot` files (`00000000000000000848.snapshot`) that let a restarting controller
  skip replaying the whole log.

### Zero-copy

A consumer fetch for data already on disk is served with `FileChannel.transferTo` (the
`sendfile(2)` syscall on Linux): the kernel copies bytes from the page cache straight to the socket,
and they never enter the JVM heap. A naive server would do disk → kernel buffer → user buffer →
socket buffer, with two extra copies and two context switches per chunk.

Two design choices make this possible:
1. **On-disk format = wire format.** The broker stores the producer's batches unchanged (see
   `compresscodec: lz4` above), so it can send the file bytes as they are. It doesn't decompress or
   re-encode them. This is also why `compression.type=producer` (the topic default shown by
   `kafka-configs --describe --all`) is the efficient setting.
2. **The page cache is the cache.** Kafka relies on the OS page cache rather than an in-heap cache.
   A consumer close to the head of the log reads memory, and `sendfile` sends it from there.

Zero-copy is lost when the bytes must be transformed in user space. **TLS** is the main case: the
broker has to encrypt every byte, so it reads it into memory. Both of our client listeners are
`PLAINTEXT` (compose `KAFKA_LISTENERS`, Strimzi `tls: false`), so consumer fetches use zero-copy.
Strimzi's internal replication listener (9091) is always TLS, but with one broker nothing replicates
over it. Converting messages for old clients (message format down-conversion) also loses
zero-copy. Not measured here: the image has no `strace` to show the syscall.

## 2. Producers and consumers

### Partitioning and keys

The producer picks the partition, not the broker:

| Record | Partition choice (Java default partitioner, librdkafka `consistent_random`) |
|---|---|
| Key present | `murmur2(keyBytes) % numPartitions`. The same key always goes to the same partition while the partition count stays the same |
| Key null | Sticky: fill one batch for a random partition, then switch (KIP-480). Better batching than round-robin |
| Explicit partition | Used as-is |

Every telemetry record is keyed by `sensor_id` (`sensor_simulator.py:202`). Every alert is keyed
by sensor too (`anomaly-detector-service/.../config/KafkaConfig.java`). This gives
**per-sensor ordering**: all readings of `s-00042` live in one partition, in produce order, so one
consumer sees them in order. The three graph-seeded sensors all landed in partition 1:

```
partition 1:  314 key: s-00042   313 key: s-00108   313 key: s-00311
distinct keys per partition:  p0: 42   p1: 31   p2: 27        # of 100 sensors
latest offsets:               p0: 12600   p1: 9340   p2: 8100
```

Hashing 100 keys onto 3 partitions is uneven (42/31/27), and the per-partition load follows it.
There are two consequences:
- **Hot partitions.** One heavy key, or an unlucky hash spread, overloads one partition and its
  consumer. Adding partitions does not help a single heavy key.
- **Changing the partition count remaps keys.** `% numPartitions` changes, so new records for
  `s-00042` may go to a different partition than older ones. Order across that boundary is no longer
  guaranteed. Partition counts are therefore chosen up front (6 in Kind = the KEDA replica ceiling)
  and never shrunk.

### Consumer groups

Consumers that share a `group.id` split a topic's partitions between them. Each partition goes to
exactly one member, and one member can hold several partitions. Different groups each get the full
stream independently (`ingestion-service` and the console consumers below read the same topic
without affecting each other). A member beyond the partition count gets nothing. That is why KEDA's
`maxReplicaCount` for ingestion equals the partition count (6).

The **group coordinator** is the broker that leads the `__consumer_offsets` partition for
`hash(group.id) % 50`. It tracks members through heartbeats and stores their committed offsets.

Live run with console consumers in group `demo-rebalance` (`--members --verbose`, compose, 3 partitions):

```
1 member   c1 (0,1,2)
2 members  c1 (0,1)    c2 (2)
3 members  c1 (0)      c2 (2)   c3 (1)
4 members  c1 (0)      c2 (2)   c3 (1)   c4 -          # idle: no partition left
c1 leaves  c4 (0)      c2 (2)   c3 (1)                 # only c1's partition moved
STATE Stable  ASSIGNMENT-STRATEGY cooperative-sticky  #MEMBERS 3
```

### Offset management

A committed offset is the position of the **next** record the group will read, per partition. It is
stored as a record in the compacted `__consumer_offsets` topic. On (re)assignment a consumer resumes
from the committed offset. If none exists, it uses `auto.offset.reset` (`earliest` in our services,
`latest` in the console consumer, which logged `Found no committed offset for partition city-telemetry-2`).

When the commit happens relative to the processing decides the delivery guarantee (section 3). Our
services commit **after** processing. Spring Kafka disables `enable.auto.commit` and commits per poll
batch (`AckMode.BATCH`) once the listener returns. Ingestion's batch listener writes one JDBC batch
per poll, and the offsets are committed only after that batch is in TimescaleDB.

Offsets are just data, so an operator can move them while the group is stopped:

```
$ $K/kafka-consumer-groups.sh $B --group demo-rebalance --topic city-telemetry \
    --reset-offsets --to-datetime 2026-10-25T18:06:30.000 --dry-run
city-telemetry  2  3483
city-telemetry  1  4039
city-telemetry  0  5418          # found through .timeindex
$ ... --reset-offsets --shift-by -100 --dry-run
city-telemetry  2  8000   1  9240   0  12500
```

`--execute` instead of `--dry-run` applies it. This is how you replay after a bug fix. It is safe for
ingestion because writes are idempotent (`ON CONFLICT`).

Lag = log-end offset − committed offset. It is what KEDA scales on:

```
$ $K/kafka-consumer-groups.sh $B --describe --group ingestion-service      # compose, service stopped
TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
city-telemetry  1          40              9340            9300
```

### Rebalance

A rebalance happens whenever group membership or the subscribed partitions change: a member
joins, leaves, misses heartbeats for `session.timeout.ms`, or exceeds `max.poll.interval.ms` between
polls. Partitions are added too. Each KEDA scale step is therefore one or more rebalances.

The classic protocol has two styles. Each was run with two console consumers; this is member `a`'s
coordinator log as `b` joins:

**Eager** (`RangeAssignor`): one round, but every member revokes **all** its partitions before
rejoining. The whole group stops consuming until the new assignment arrives.
```
18:10:40,849 Request joining group due to: group is already rebalancing
18:10:40,864 Successfully joined group with generation 2, protocol='range'
18:10:40,869 Notifying assignor about the new Assignment(partitions=[city-telemetry-0, city-telemetry-1])
```

**Cooperative** (`CooperativeStickyAssignor`, KIP-429): two rounds. Members keep consuming their
partitions; only the ones that have to move are revoked (here only `city-telemetry-2`), then a second
round gives them to the new member:
```
18:11:07,670 Successfully joined group with generation 2, protocol='cooperative-sticky'
18:11:07,675 Updating assignment with  Revoked partitions (owned - assigned): [city-telemetry-2]
18:11:07,675 Request joining group due to: need to revoke partitions [city-telemetry-2] ... and re-join
18:11:10,677 Successfully joined group with generation 3, protocol='cooperative-sticky'
18:11:10,680 Updating assignment with  Revoked partitions (owned - assigned): []
```

**Our services used eager rebalancing until 2026-10-25.** The Java client's default
`partition.assignment.strategy` is `[RangeAssignor, CooperativeStickyAssignor]`, and the group picks
the first one all members support, so the result was `range`. In Kind, before the change:
```
$ $K/kafka-consumer-groups.sh $B --describe --group ingestion-service --state
GROUP              ASSIGNMENT-STRATEGY  STATE   #MEMBERS
ingestion-service  range                Stable  1
```
Every ingestion scale-out step under KEDA therefore briefly stopped all replicas. Both consumers
(`ingestion-service`, `ai-incident-responder`) now set
`spring.kafka.consumer.properties.partition.assignment.strategy` to `CooperativeStickyAssignor` in
`application.yaml`. On the compose broker after the change:
```
ingestion-service  cooperative-sticky  Stable  1
```
The switch works as one rolling update because the old default list already contains the
cooperative assignor, so old and new pods agree on it during the roll. On a Kafka 4.x broker, the
new consumer protocol `group.protocol=consumer` (KIP-848) goes further: the broker computes
assignments incrementally and there is no group-wide barrier at all. The compose broker (3.9) would
need it enabled explicitly.

Correctness doesn't depend on the rebalance style. A partition that moves is re-read from its last
committed offset, so the records processed but not yet committed are delivered again. The idempotent
consumer absorbs them (section 3).

## 3. Delivery semantics

### `acks`

| `acks` | The leader responds… | A record can be lost when… |
|---|---|---|
| `0` | never; the producer does not wait | always possible: the broker may never have received it |
| `1` | after writing to its own log | the leader dies before any follower copies the record |
| `all` (`-1`) | after **every ISR replica** has it | fewer than `min.insync.replicas` replicas survive. `acks=all` + RF=3 + `min.insync.replicas=2` tolerates one broker loss without losing an acked record |

`min.insync.replicas` only matters with `acks=all`. If the ISR shrinks below it, the leader rejects
writes with `NOT_ENOUGH_REPLICAS` instead of accepting a record that is less durable than promised.
That means it chooses availability loss over durability loss.

Throughput on the compose broker (`kafka-producer-perf-test`, 150-byte records, `linger.ms=5`,
three runs, noisy laptop numbers):

| acks | records/s | avg latency |
|---|---|---|
| 0 | 111 k, 165 k, 187 k | 235–277 ms |
| 1 | 118 k, 124 k, 122 k | 217–857 ms |
| all | 70 k, 72 k, 120 k | 553–1780 ms |

With one broker, `acks=1` and `acks=all` wait for the same single replica, so the durability is
identical and the throughput gap is mostly noise. On a real cluster `acks=all` adds a follower round
trip. Also on one broker, setting `min.insync.replicas=2` on an RF=1 topic did **not** reject
`acks=all` writes (3/3 accepted, ISR = 1): Kafka 3.9 caps the effective min-ISR at the replication
factor. Seeing `NOT_ENOUGH_REPLICAS` needs RF > 1 and a lost follower, so a multi-broker cluster.

### At-most-once, at-least-once, exactly-once

| Guarantee | Producer | Consumer | Failure outcome |
|---|---|---|---|
| **At-most-once** | `acks=0`, or no retries | commit offset **before** processing | crash after commit → record skipped |
| **At-least-once** | `acks=all` + retries | commit offset **after** processing | crash before commit → record processed again |
| **Exactly-once** | idempotent + transactional | `isolation.level=read_committed`; offsets committed inside the producer's transaction (`sendOffsetsToTransaction`) | each record's effect lands once, within Kafka |

**Idempotent producer** (`enable.idempotence=true`). The broker gives the producer a
`producerId`, and every batch carries a per-partition sequence number. When a retry resends a batch
the broker already has, the broker sees the duplicate sequence and drops it, so retries cannot
duplicate or reorder records. It needs `acks=all`. Since Kafka 3.0 it is the Java default.

**Transactional producer** (`transactional.id`). Writes to several partitions (including
`__consumer_offsets`) commit or abort atomically. The broker writes a **control record** (a COMMIT or
ABORT marker) into each partition. A `read_committed` consumer reads only up to the last stable
offset and skips aborted batches. A `read_uncommitted` consumer (the default) sees everything.

Demo (scratch Python script, `confluent_kafka`, 1-partition topic `eos-demo`): one plain record, three
idempotent records, a committed transaction of three, and an aborted transaction of three:

```
$ $K/kafka-dump-log.sh --files .../eos-demo-0/00000000000000000000.log --print-data-log
baseOffset: 0 lastOffset: 0  baseSequence: -1 producerId: -1   isTransactional: false            # plain
baseOffset: 1 lastOffset: 3  baseSequence: 0 lastSequence: 2 producerId: 1003 isTransactional: false   # idempotent
baseOffset: 4 lastOffset: 6  baseSequence: 0 lastSequence: 2 producerId: 1004 isTransactional: true
baseOffset: 7                producerId: 1004 isControl: true  | endTxnMarker: COMMIT
baseOffset: 8 lastOffset: 10 baseSequence: 3 lastSequence: 5 producerId: 1004 isTransactional: true
baseOffset: 11               producerId: 1004 isControl: true  | endTxnMarker: ABORT

read_uncommitted: 0:plain-1 1..3:idempotent-* 4..6:tx-committed-* 8..10:tx-aborted-*
read_committed:   0:plain-1 1..3:idempotent-* 4..6:tx-committed-*
```

The markers take up offsets 7 and 11, so offsets in a transactional topic have gaps a consumer
never sees.

### Where this repo stands

| Component | Setting | Guarantee |
|---|---|---|
| `sensor-simulator` (librdkafka) | `enable.idempotence: True` (implies `acks=all`), added 2026-10-25. Before that the dump showed `producerId: -1`; now `producerId: 2000 baseSequence: 297 lastSequence: 299` | at-least-once, no producer-side duplicates |
| `anomaly-detector` → `infrastructure-alerts` (Java) | client defaults: `acks=all`, idempotent (dump of `infrastructure-alerts` shows `producerId: 0, baseSequence: 0`) | at-least-once, no producer-side duplicates |
| DLQ producers (ingestion, responder) | client defaults, as above | at-least-once |
| `ingestion-service`, `ai-incident-responder` consumers | commit after processing, `read_uncommitted` | at-least-once |

The pipeline is **at-least-once + idempotent consumer**, which gives exactly-once *effects*:
- ingestion: `INSERT … ON CONFLICT DO NOTHING` against the unique index on
  `(timestamp, sensor_id, metric_type)`, so a redelivered or duplicated reading is a no-op;
- responder: `existsByAlertId` before the Claude call, plus `UNIQUE(alert_id)` +
  `ON CONFLICT (alert_id) DO NOTHING` on insert, so a redelivered alert doesn't create a second
  incident. It normally doesn't trigger a second Claude call either; a race between two redeliveries
  is the exception.

Kafka transactions would not help here. They cover Kafka-to-Kafka (consume → produce → commit
offsets atomically), but our side effects go to Postgres and the Claude API, outside any Kafka
transaction. Idempotent writes at the sink are the standard answer for that case
(see `docs/k8s-storage-and-scaling.md` §5).

## 4. Kafka on Kubernetes: Strimzi, KRaft vs ZooKeeper

### Stateful workloads and the operator pattern

A broker is a stateful process: a stable identity (`node.id`, which ties it to the partitions it
hosts), its own disk (the log), and a stable network name that clients get back in metadata.
Kubernetes primitives give the building blocks (stable pod names, per-pod PVCs, a headless Service;
see `docs/k8s-storage-and-scaling.md` §1). Running Kafka well also needs knowledge Kubernetes doesn't
have: roll brokers one at a time and only when partitions are in sync, keep `node.id` and the voter
list consistent, generate listener and TLS config, and turn "a topic should exist with these configs"
into admin API calls.

An **operator** is a controller that holds that knowledge. It watches custom resources and reconciles
the cluster towards them in a loop. Strimzi has three parts:

| Component | Watches | Creates / does |
|---|---|---|
| Cluster Operator (`strimzi-cluster-operator`) | `Kafka`, `KafkaNodePool` | `StrimziPodSet`s, pods, Services, ConfigMaps, certs, PVCs; rolling restarts |
| Topic Operator (in `smartcity-entity-operator`) | `KafkaTopic` | creates/alters topics through the Admin API |
| User Operator (not enabled here) | `KafkaUser` | users, ACLs, credentials |

What it built from `deploy/strimzi/kafka.yaml`, observed in Kind:

```
kafka.kafka.strimzi.io/smartcity                 READY True  KAFKA VERSION 4.3.1  METADATA VERSION 4.3-IV0
kafkanodepool.kafka.strimzi.io/dual-role         REPLICAS 1  ROLES ["controller","broker"]  NODEIDS [0]
strimzipodset.core.strimzi.io/smartcity-dual-role  PODS 1  READY 1
pod smartcity-dual-role-0    owner: StrimziPodSet/smartcity-dual-role (owner: KafkaNodePool/dual-role)
svc smartcity-kafka-bootstrap  ClusterIP  9091,9092         # what clients use
svc smartcity-kafka-brokers    None       9090,9091,8443,9092   # headless: per-pod DNS
```

The generated broker config (`/tmp/strimzi.properties`):
```
node.id=0
process.roles=broker,controller
controller.quorum.voters=0@smartcity-dual-role-0.smartcity-kafka-brokers.smartcity.svc:9090
metadata.log.dir=/var/lib/kafka/data-0/kafka-log0       # kraftMetadata: shared
log.dirs=/var/lib/kafka/data-0/kafka-log0
```

Strimzi uses its own **StrimziPodSet** instead of a StatefulSet. A StatefulSet treats all pods as
identical and rolls them in ordinal order. Strimzi needs pods with different configurations (node
pools with different roles or storage, in one cluster) and needs to choose the roll order itself
(e.g. controllers before brokers, a follower before the partition leader). It still keeps the
StatefulSet properties that matter: stable names, a PVC per node, and a headless Service.

**Reconcile loop, observed.** A topic config was changed behind the operator's back, directly through
the Admin API. The Topic Operator's periodic reconciliation (every ~150 s in the log) put the
`KafkaTopic` spec back:
```
18:14:53 right after alter: retention.ms=172800000
18:16:47 retention.ms=172800000
18:16:50 topic-operator: [Batch #37318] Reconciling batch of 4 topics
18:16:56 retention.ms=86400000                # back to spec.config.retention.ms
```
The CR is the source of truth, so a manual `kafka-configs --alter` doesn't last. The
`auto.create.topics.enable: false` in `kafka.yaml` exists for the same reason, so that services'
clients can't create topics outside the CRs.

### ZooKeeper vs KRaft

| | ZooKeeper mode (legacy) | KRaft mode |
|---|---|---|
| Metadata store | A separate ZooKeeper ensemble (znodes) | `__cluster_metadata`, a Raft-replicated log inside Kafka |
| Controller | One broker elected as controller through ZooKeeper; it caches the metadata and pushes it to brokers by RPC | A quorum of controller nodes; the active one is the Raft leader. Brokers fetch the metadata log like followers |
| Failover | The new controller reloads all metadata from ZooKeeper; slow with many partitions | Standby controllers already have the log in memory; near-instant |
| Scale | Practically limited to ~200 k partitions per cluster | Millions of partitions (metadata is an incremental log + snapshots) |
| Operations | Two distributed systems to deploy, secure, monitor and upgrade | One system, one config model, one security model |
| Status | Deprecated in 3.x; **removed in Kafka 4.0** | The only mode in 4.x and in Strimzi 1.x |

Both of our brokers are KRaft in combined mode. The compose 3.9 image could still run ZooKeeper, but
it is configured with `KAFKA_PROCESS_ROLES: broker,controller`. Strimzi 1.x and Kafka 4.3.1 in Kind
support KRaft only. Combined mode is fine for development. Production separates controller-only and
broker-only node pools (in Strimzi, two `KafkaNodePool`s with `roles: [controller]` and
`roles: [broker]`), so a broker overloaded with client traffic cannot starve the metadata quorum.
