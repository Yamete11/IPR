# Time-series storage: metrics on TimescaleDB

Why the `metrics` table is a TimescaleDB hypertable and not a plain PostgreSQL table, how each storage
mechanism works, and what it buys. Every claim here is backed by code and a test in `ingestion-service`
(production schema: `src/main/resources/schema.sql`; measurements: `TimeseriesStorageBenchmarkTest`)
and `anomaly-detector-service` (`ContinuousAggregateInitializer`).

## 1. Time-series data, and where a single B-tree breaks down

Telemetry has a distinctive shape:

- **Append-mostly and time-ordered.** Rows arrive roughly in timestamp order and are almost never updated.
- **Read by recent time ranges.** "The last hour of sensor X" and "5-minute buckets over the last day"
  are typical; random point lookups across all of history are not.
- **Volume grows without bound and ages out.** ~1000 sensors at a few readings a second is tens of
  millions of rows a day. Old data is first summarised, then deleted.

A plain table keeps **one** B-tree per index over **all** of history. That stops scaling for three reasons:

1. **The insert working set is the whole index.** A B-tree insert walks root → leaf, and the root and
   inner pages have to be in memory for that to be cheap. An index over months of data eventually
   outgrows RAM. From then on, every insert whose leaf is not cached (anything not keyed on time,
   such as `(sensor_id, metric_type, timestamp)`) becomes random I/O, and insert throughput falls off
   a cliff.
2. **Retention means `DELETE`.** Removing a month of data is a delete per row, then `VACUUM` work on the
   heap and on every index, and the bloat stays until it is vacuumed.
3. **Maintenance is all-or-nothing.** `VACUUM`, `REINDEX` and `ANALYZE` always work on the whole
   multi-terabyte relation.

TimescaleDB keeps the SQL interface and partitions the table under it. Each chunk has its own small
indexes, so the hot working set is only the newest chunk's indexes.

**What the benchmark shows at laptop scale** (2.9 M rows, about 600 MB per table, so everything still
fits in memory): the per-day insert cost is **flat for both** tables (≈5–6 s per 288 k-row day). The
cliff only appears once the index no longer fits in RAM, and this data set does not reach that point.
The numbers that point toward it are:

| | Plain table | Hypertable |
|---|---:|---:|
| index an insert of *now* has to touch | 380.5 MB (all of history, growing) | 41.2 MB (newest chunk, constant) |
| query "last hour, all sensors" plans | 1 table + indexes over every row | 1 of 10 chunks |
| drop 2 days of data | 538 ms `DELETE` + 1247 ms `VACUUM` | 23 ms `drop_chunks` |

On the plain table the index an insert touches grows linearly with retained history. On the
hypertable it stays at one day's worth (§2).

## 2. Hypertables and chunks

`create_hypertable('metrics', 'timestamp', chunk_time_interval => INTERVAL '1 day')` turns `metrics`
into a parent with no rows of its own. Each **chunk** is an ordinary child table covering one time range
(enforced with a `CHECK` constraint), and it has its own copy of every index. Inserts are routed to
the right chunk, and a new chunk is created the first time a row lands in a new range.

- **Chunk sizing.** Timescale's rule of thumb is that the newest chunk's indexes should fit comfortably
  in memory (about 25 % of RAM). At this project's load profile that means 1 day, not the default 7
  (comment in `schema.sql`). The benchmark measures 41 MB of index per day at 100 sensors, which scales
  to roughly 0.4 GB per day at ~1000 sensors.
- **Chunk exclusion.** Before planning, the planner discards every chunk whose range cannot match the
  `WHERE timestamp ...` predicate (benchmark §2: 1 of 10 chunks planned). This happens at plan time for
  constants and at executor start-up for `now()`-relative bounds.
- **Partitioning across space.** `add_dimension('t', by_hash('sensor_id', 4))` adds a second, hash
  dimension, so each time slice is split into 4 chunks. The benchmark builds this on a demo table: 2 days
  give 8 chunks, and a single-sensor query plans **2 of 8**, because time exclusion and hash exclusion
  both apply. The production `metrics` table does **not** do this. On a single node with one volume,
  hash partitioning only multiplies the chunk count without adding I/O parallelism. It pays off when
  chunks are spread over several tablespaces or disks (`attach_tablespace`).

## 3. Columnar compression

`ALTER TABLE metrics SET (timescaledb.compress, compress_segmentby = 'sensor_id, metric_type',
compress_orderby = 'timestamp DESC')`, followed by `add_compression_policy('metrics', INTERVAL '7 days')`.

How it works: compressing a chunk rewrites its rows into a hidden "compressed chunk". Rows are grouped
by the **segmentby** values, so one sensor's one metric forms one segment. Each segment is cut into
batches of up to 1000 rows, and each batch is stored as **one row holding one compressed array per
column**:

- the `segmentby` columns are stored once per batch, as plain values;
- timestamps and integers use delta-of-delta + Simple-8b (a 1-minute cadence compresses to almost nothing);
- floats use Gorilla XOR (smooth sensor signals share most of their bits with the previous value);
- low-cardinality text uses dictionary compression.

Each batch also records the min/max of the **orderby** column, so a time-range query skips whole
batches without decompressing them, and a `segmentby` filter selects whole segments. The policy is a
background job that compresses every chunk older than 7 days.

Benchmark (§5): 7 chunks go from **435.8 MB to 16.2 MB, 26.9× smaller**, and the hypertable from 623 MB
to 204 MB. Reads get faster too, because they do less I/O. A one-series day takes 5.5 → 2.3 ms, and an
all-sensors day 56 → 13 ms (the plain table takes 55 ms for the same query). The cost is on writes:
DML on compressed chunks works, but it decompresses the affected batches. That is why compression waits
7 days, by which point late data has stopped arriving.

## 4. Continuous aggregates vs. materialized views

`anomaly-detector-service` owns `metrics_5m_avg`, a 5-minute `time_bucket` with
avg/min/max/count per series. It is created `WITH (timescaledb.continuous, timescaledb.materialized_only
= false)` and has a refresh policy with `start_offset 1 hour`, `end_offset 10 minutes`, every 1 minute.

| | `CREATE MATERIALIZED VIEW` | Continuous aggregate |
|---|---|---|
| storage | one heap table, a snapshot | its own hypertable (the *materialization hypertable*) |
| tracking changes | none | an **invalidation log**: changes to the source hypertable record the modified time range |
| refresh | `REFRESH` recomputes **everything** (and locks readers out unless `CONCURRENTLY` is used with a unique index) | `refresh_continuous_aggregate(window)` recomputes only invalidated buckets inside the window |
| freshness | stale until the next full refresh | **real-time aggregation**: a query is the union of materialized buckets below the **watermark** and an on-the-fly aggregate of raw rows above it |
| automation | none (cron + `REFRESH`) | `add_continuous_aggregate_policy` (background job) |
| retention | tied to the source | independent: raw chunks can be dropped while the aggregate keeps the history |

**Lifecycle** of a CA bucket:

1. Rows newer than the watermark are aggregated at query time.
2. The refresh policy materializes the window `[now − 1 h, now − 10 min)` and moves the watermark
   forward. The 10-minute end offset keeps it from materializing a bucket that is still filling.
3. A late row behind the watermark invalidates its bucket. That bucket keeps serving the old value until
   a refresh covers it, and that refresh recomputes only that bucket.

Benchmark (§3) shows each step:

| Step | Continuous aggregate | Materialized view |
|---|---:|---:|
| build over 10 days | 14.2 s | 4.7 s |
| 5 new minutes visible without a refresh | 1000 rows (real-time) | 0 |
| catch up after them | 13 ms (2-hour window) | 4.1 s (full `REFRESH`) |
| late row in an old bucket: `max` before → after refresh | 232.9 → 999 in 13 ms (one bucket) | full `REFRESH` again |

The initial build is *slower* for the CA, because it materializes into an indexed hypertable in
batches. That is a one-time cost; every later refresh is incremental, whereas an MV pays the full price
every time. Querying the CA for a day of per-series maxima takes 25 ms, against 50 ms over raw data.

`materialized_only = false` matters here. Since TimescaleDB 2.13 a new CA defaults to `true`, so a CA
created `WITH NO DATA` would return nothing until the policy first runs, and a fresh fault reading would
reach the detector only after a refresh.

## 5. Retention and tiering

**Retention.** `add_retention_policy('metrics', INTERVAL '30 days')` is a background job that calls
`drop_chunks(older_than => now() - 30 days)`. A whole chunk is a table, so dropping it unlinks its files.
There is no per-row delete, no dead tuples and no `VACUUM` (benchmark §7: 23 ms, against 538 ms +
1247 ms on the plain table). Long-range history lives on in `metrics_5m_avg`, which `drop_chunks` does
not touch.

**Tiering** means moving data that is still kept onto cheaper, slower storage:

- **Timescale Cloud** has tiered storage that moves chunks to S3 (Parquet) through
  `add_tiering_policy`. Tiered chunks stay queryable through the same hypertable. It is **not
  available in self-hosted TimescaleDB**, which is what this project runs.
- **Self-hosted**: tablespaces. Create a tablespace on a cheaper volume and move old chunks there with
  `move_chunk(chunk, destination_tablespace, index_destination_tablespace)`. To automate this, wrap the
  call in a user-defined action scheduled with `add_job` (the same scheduler the built-in policies use).
  `attach_tablespace` puts *new* chunks on additional tablespaces.

The benchmark (§6) creates a `history` tablespace and moves the 4 oldest (already compressed) chunks in
140 ms without losing a row. The resulting lifecycle is the one this design implies:

| Age | State | Mechanism |
|---|---|---|
| < 3 days | hot: row-oriented, `pg_default` | — |
| 3–6 days | warm: columnar, 27× smaller | compression |
| 6+ days | cold: compressed, `history` tablespace | `move_chunk` |
| past retention | gone (aggregate remains) | `drop_chunks` |

(The production policies are 7-day compression and 30-day retention. The benchmark uses shorter
cut-offs to fit 10 days of data.) Tiering is not configured in production. Compose and the Kind cluster
each give TimescaleDB a single volume, so a second tablespace would sit on the same disk and buy nothing.

## Reproduce

`cd ingestion-service && ./gradlew timeseriesBenchmark` (needs Docker; about 3 min, most of it the two
loads). It writes `build/reports/timeseries-storage.md`. The numbers above come from the run on
2026-10-25 (Docker Desktop, `timescale/timescaledb:2.28.3-pg16`, with a Kind cluster sharing the VM),
using a single wall-clock run for DDL/DML and the median of 7 runs for queries. The test asserts the
mechanisms rather than the timings:

- the hypertable has 10 chunks, and the last-hour query plans one of them;
- the CA sees rows the MV does not;
- a late row is stale until refresh and correct after it;
- the hash dimension yields 8 chunks, of which a single-sensor query plans 2;
- compression shrinks the chunks;
- moved chunks report the `history` tablespace;
- `drop_chunks` and `DELETE` leave equal row counts.
