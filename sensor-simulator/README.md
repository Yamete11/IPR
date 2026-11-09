# sensor-simulator

Python load generator for the Smart City pipeline. Produces synthetic telemetry to the Kafka
`city-telemetry` topic — the upstream consumed by `ingestion-service` into TimescaleDB.

Messages match the design schema (keyed by `sensor_id`):

```json
{ "timestamp": "2026-07-24T12:00:00.123456Z", "sensor_id": "s-00042",
  "metric_type": "pipe_pressure", "value": 3.2, "status": "OK" }
```

Each sensor follows a bounded, mean-reverting random walk around a per-metric baseline; with a small
probability (`--anomaly-prob`) it enters a hard fault (e.g. `pipe_pressure` → `0.0`, status
`CRITICAL`) that holds for `--fault-hold` readings and then decays back toward the baseline, for the
downstream anomaly-detector to catch. The three **known** sensors (`s-00042`, `s-00108`, `s-00311`)
are exactly the ones hand-seeded into the AGE topology graph by `topology-service`.
`--extra-sensors` adds synthetic ones (`s-01000`+); its default is the env var `CITY_EXTRA_SENSORS`,
the same one topology-service generates its city from, so they resolve to real graph assets too.
The layout rule (`layout_of`) mirrors topology-service's `SyntheticCity`, and both test suites pin
the same fixture ids (`s-01000 → bld-d00-0-0`, `s-01002 → sub-d00`, …).

## Cascades

`--cascade-every SECONDS` plays substation outages along that graph: 10 s after start, then every
`SECONDS`, the next district (round-robin, the hand-seeded `sub-1` last) loses voltage — all its
voltage sensors fault — and `--cascade-delay` seconds later (default 5) every building of the
district loses water pressure. An outage lasts ~30 s. Downstream the responder plans the first
voltage alert and files the rest under it without calling Claude (root-cause correlation over the
graph). Run it with `--anomaly-prob 0`: random faults would correlate with each other.

```bash
python sensor_simulator.py --extra-sensors 997 --interval 1 --anomaly-prob 0 --cascade-every 300
```

Timestamps are microsecond-precision and strictly increasing. The wall clock is far coarser than the
emission rate (~15 ms granularity on Windows), and a repeated `(timestamp, sensor_id, metric_type)`
tuple is silently dropped by `ingestion-service`'s `ON CONFLICT DO NOTHING` — so a fast run would
otherwise under-count rows in TimescaleDB with no error anywhere.

## Setup

```bash
cd sensor-simulator
python -m venv .venv
# Windows (PowerShell):  .venv\Scripts\Activate.ps1
# Git Bash / macOS / Linux:  source .venv/Scripts/activate  (Windows) or source .venv/bin/activate
pip install -r requirements.txt
```

## Run

Start the broker first (repo root): `docker compose up -d kafka`.

```bash
python sensor_simulator.py                          # 3 known sensors, one round every 2s
python sensor_simulator.py --extra-sensors 100 --interval 0.5   # load test
python sensor_simulator.py --max-messages 30 --anomaly-prob 0.1 # short burst with faults
```

| Flag | Default | Meaning |
|---|---|---|
| `--bootstrap` | `localhost:9092` (env `KAFKA_BOOTSTRAP`) | Kafka bootstrap servers |
| `--topic` | `city-telemetry` | destination topic |
| `--interval` | `2.0` | seconds between emission rounds (one reading per sensor per round) |
| `--extra-sensors` | `0` | synthetic sensors added on top of the 3 known ones |
| `--anomaly-prob` | `0.02` | per-reading probability of a hard fault |
| `--fault-hold` | `3` | readings a hard fault lasts before recovery starts |
| `--max-messages` | `0` | stop after N messages (`0` = run until Ctrl-C) |
| `--seed` | — | RNG seed for reproducible runs |

Invalid values (non-positive `--interval`, `--anomaly-prob` outside `0..1`, negative counts) are
rejected up front with exit code 2.

Ctrl-C / SIGTERM interrupts the wait immediately, flushes in-flight messages and exits. **Exit code
0 only if every message was delivered** — a delivery error or anything still queued when the 10s
flush times out (e.g. an unreachable broker) exits 1, so the script is safe to use in a script or CI
check. If the broker is slow enough to fill librdkafka's queue, the generator applies backpressure
and waits rather than failing.

## Tests

Unit tests cover the walk, fault episodes/recovery, the message schema and argument validation —
no broker needed:

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

## End-to-end check

With `ingestion-service` running and the simulator producing, rows land in TimescaleDB:

```bash
docker exec smartcity-timescaledb psql -U smartcity -d smartcity \
  -c "SELECT sensor_id, metric_type, count(*) FROM metrics GROUP BY 1,2 ORDER BY 1;"
```
