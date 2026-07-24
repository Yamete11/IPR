# sensor-simulator

Python load generator for the Smart City pipeline. Produces synthetic telemetry to the Kafka
`city-telemetry` topic — the upstream consumed by `ingestion-service` into TimescaleDB.

Messages match the design schema (keyed by `sensor_id`):

```json
{ "timestamp": "2026-07-24T12:00:00.000Z", "sensor_id": "s-00042",
  "metric_type": "pipe_pressure", "value": 3.2, "status": "OK" }
```

Each sensor follows a bounded, mean-reverting random walk around a per-metric baseline; with a small
probability (`--anomaly-prob`) it emits a hard fault (e.g. `pipe_pressure` → `0.0`, status `CRITICAL`)
for the downstream anomaly-detector to catch. The three **known** sensors (`s-00042`, `s-00108`,
`s-00311`) are exactly the ones seeded into the AGE topology graph by `topology-service`, so an alert
resolves to real graph dependents; `--extra-sensors` adds synthetic ones (`s-01000`+) for load only.

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
| `--max-messages` | `0` | stop after N messages (`0` = run until Ctrl-C) |
| `--seed` | — | RNG seed for reproducible runs |

Ctrl-C flushes in-flight messages and exits cleanly.

## End-to-end check

With `ingestion-service` running and the simulator producing, rows land in TimescaleDB:

```bash
docker exec smartcity-timescaledb psql -U smartcity -d smartcity \
  -c "SELECT sensor_id, metric_type, count(*) FROM metrics GROUP BY 1,2 ORDER BY 1;"
```
