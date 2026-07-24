"""Smart City sensor simulator.

Generates synthetic telemetry and produces it to the Kafka ``city-telemetry`` topic — the upstream
that ``ingestion-service`` consumes into the TimescaleDB ``metrics`` hypertable. Messages are keyed by
``sensor_id`` (per-sensor ordering / partitioning) and match the design schema:

    { "timestamp": "...Z", "sensor_id": "s-00042", "metric_type": "pipe_pressure",
      "value": 3.2, "status": "OK" }

Each sensor follows a bounded random walk around a per-metric baseline; with a small probability a
reading is a hard fault (e.g. ``pipe_pressure`` → 0.0), which the downstream anomaly-detector is meant
to catch. The three "known" sensors are the ones seeded into the AGE topology graph by
``topology-service`` (linked via ``SensorLink``), so the whole pipeline lines up end to end; extra
synthetic sensors can be added purely for load.

Run:
    python sensor_simulator.py                 # emit for the 3 known sensors every 2s
    python sensor_simulator.py --extra-sensors 100 --interval 0.5
    KAFKA_BOOTSTRAP=localhost:9092 python sensor_simulator.py
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from confluent_kafka import Producer

log = logging.getLogger("sensor-simulator")

# Per-metric behaviour: a baseline the random walk hovers around, step noise, the OK band, and the
# value a hard fault drives the reading to (what the anomaly-detector should flag).
METRICS: dict[str, dict[str, float]] = {
    "pipe_pressure":      {"baseline": 4.0,   "noise": 0.15, "ok_low": 2.0,   "ok_high": 6.0,   "fault": 0.0},
    "cable_temperature":  {"baseline": 45.0,  "noise": 2.0,  "ok_low": -10.0, "ok_high": 90.0,  "fault": 130.0},
    "substation_voltage": {"baseline": 110.0, "noise": 0.5,  "ok_low": 104.0, "ok_high": 116.0, "fault": 0.0},
}

# Sensors seeded into the AGE topology graph (topology-service/src/main/resources/seed/city-topology.json).
# Keeping these ids/metric-types in sync means an alert on, say, s-00042 resolves to real graph
# dependents (node-5 → buildings) downstream.
KNOWN_SENSORS: list[tuple[str, str]] = [
    ("s-00042", "pipe_pressure"),
    ("s-00108", "cable_temperature"),
    ("s-00311", "substation_voltage"),
]


@dataclass
class Sensor:
    """A single sensor and its current (stateful) reading."""

    sensor_id: str
    metric_type: str
    value: float

    def next_reading(self, anomaly_prob: float) -> tuple[float, str]:
        """Advance the random walk one step and return ``(value, status)``.

        With probability ``anomaly_prob`` the sensor emits a hard fault instead of a normal reading.
        Otherwise the value drifts (mean-reverting toward the baseline) and ``status`` reflects whether
        it sits inside the metric's OK band.
        """
        spec = METRICS[self.metric_type]

        if random.random() < anomaly_prob:
            self.value = spec["fault"]
            return round(self.value, 3), "CRITICAL"

        # Mean-reverting random walk: nudge toward baseline, add Gaussian step, clamp to a sane range.
        drift = (spec["baseline"] - self.value) * 0.1
        self.value += drift + random.gauss(0.0, spec["noise"])
        self.value = _clamp(self.value, spec["ok_low"] - 5.0, spec["ok_high"] + 5.0)

        status = "OK" if spec["ok_low"] <= self.value <= spec["ok_high"] else "WARN"
        return round(self.value, 3), status


def _clamp(x: float, low: float, high: float) -> float:
    return max(low, min(high, x))


def build_sensors(extra: int) -> list[Sensor]:
    """The 3 known (graph-linked) sensors plus ``extra`` synthetic ones for load."""
    sensors = [Sensor(sid, mtype, METRICS[mtype]["baseline"]) for sid, mtype in KNOWN_SENSORS]
    metric_types = list(METRICS)
    for i in range(extra):
        mtype = metric_types[i % len(metric_types)]
        # Number synthetic sensors from 1000 up so they never collide with the known s-000xx ids.
        sensors.append(Sensor(f"s-{1000 + i:05d}", mtype, METRICS[mtype]["baseline"]))
    return sensors


def _now_iso() -> str:
    """ISO-8601 UTC with a trailing ``Z`` and millisecond precision (matches the message schema)."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_message(sensor: Sensor, anomaly_prob: float) -> dict[str, object]:
    value, status = sensor.next_reading(anomaly_prob)
    return {
        "timestamp": _now_iso(),
        "sensor_id": sensor.sensor_id,
        "metric_type": sensor.metric_type,
        "value": value,
        "status": status,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Produce Smart City telemetry to Kafka city-telemetry.")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
                   help="Kafka bootstrap servers (default: env KAFKA_BOOTSTRAP or localhost:9092).")
    p.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "city-telemetry"),
                   help="Destination topic (default: city-telemetry).")
    p.add_argument("--interval", type=float, default=2.0,
                   help="Seconds between emission rounds; each round emits one reading per sensor.")
    p.add_argument("--extra-sensors", type=int, default=0,
                   help="Synthetic sensors added on top of the 3 known ones, for load (default: 0).")
    p.add_argument("--anomaly-prob", type=float, default=0.02,
                   help="Per-reading probability of a hard fault (default: 0.02).")
    p.add_argument("--max-messages", type=int, default=0,
                   help="Stop after N messages; 0 (default) runs until interrupted.")
    p.add_argument("--seed", type=int, default=None, help="RNG seed for reproducible runs.")
    return p.parse_args(argv)


def run(args: argparse.Namespace) -> int:
    if args.seed is not None:
        random.seed(args.seed)

    sensors = build_sensors(args.extra_sensors)
    producer = Producer({"bootstrap.servers": args.bootstrap, "client.id": "sensor-simulator"})
    log.info("Producing to '%s' at %s: %d sensors (%d known + %d synthetic), interval=%.2fs, anomaly_prob=%.3f",
             args.topic, args.bootstrap, len(sensors), len(KNOWN_SENSORS), args.extra_sensors,
             args.interval, args.anomaly_prob)

    stats = {"produced": 0, "failed": 0}

    def on_delivery(err, _msg):
        if err is not None:
            stats["failed"] += 1
            log.warning("Delivery failed: %s", err)

    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False
        log.info("Shutting down (signal received)...")

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    try:
        while running:
            for sensor in sensors:
                message = make_message(sensor, args.anomaly_prob)
                producer.produce(
                    args.topic,
                    key=sensor.sensor_id.encode("utf-8"),
                    value=json.dumps(message).encode("utf-8"),
                    on_delivery=on_delivery,
                )
                stats["produced"] += 1
                # Serve delivery callbacks and keep the internal queue from filling under load.
                producer.poll(0)
                if args.max_messages and stats["produced"] >= args.max_messages:
                    running = False
                    break
            if running:
                time.sleep(args.interval)
    finally:
        remaining = producer.flush(10.0)
        if remaining:
            log.warning("%d message(s) still undelivered after flush timeout", remaining)
        log.info("Done. produced=%d failed=%d", stats["produced"], stats["failed"])

    return 1 if stats["failed"] else 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    return run(parse_args())


if __name__ == "__main__":
    sys.exit(main())
