"""Smart City sensor simulator.

Generates synthetic telemetry and produces it to the Kafka ``city-telemetry`` topic — the upstream
that ``ingestion-service`` consumes into the TimescaleDB ``metrics`` hypertable. Messages are keyed by
``sensor_id`` (per-sensor ordering / partitioning) and match the design schema:

    { "timestamp": "...Z", "sensor_id": "s-00042", "metric_type": "pipe_pressure",
      "value": 3.2, "status": "OK" }

Each sensor follows a bounded random walk around a per-metric baseline; with a small probability a
reading is a hard fault (e.g. ``pipe_pressure`` → 0.0) that holds for ``--fault-hold`` readings and
then decays back toward the baseline, which the downstream anomaly-detector is meant to catch. The
three "known" sensors are the ones seeded into the AGE topology graph by ``topology-service``
(linked via ``SensorLink``), so the whole pipeline lines up end to end; extra synthetic sensors can
be added purely for load.

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
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

log = logging.getLogger("sensor-simulator")

# How hard the walk is pulled back toward the baseline each step, and how far outside the OK band a
# normal (non-fault) reading is allowed to wander.
RECOVERY_RATE = 0.1
CLAMP_MARGIN = 5.0

# Seconds to wait for deliveries when librdkafka's queue is full, and at final flush.
BACKPRESSURE_POLL_SECONDS = 1.0
FLUSH_TIMEOUT_SECONDS = 10.0

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
    fault_ticks: int = 0  # readings still to emit in the current hard-fault episode

    def next_reading(self, anomaly_prob: float, fault_hold: int = 1) -> tuple[float, str]:
        """Advance the random walk one step and return ``(value, status)``.

        With probability ``anomaly_prob`` the sensor enters a hard-fault episode: it emits the
        metric's fault value for ``fault_hold`` consecutive readings, so a fault is visible as a
        sustained breach rather than a single blip. Otherwise the value drifts (mean-reverting
        toward the baseline) and ``status`` reflects whether it sits inside the metric's OK band.
        """
        spec = METRICS[self.metric_type]

        if self.fault_ticks > 0:
            self.fault_ticks -= 1
            self.value = spec["fault"]
            return round(self.value, 3), "CRITICAL"

        if random.random() < anomaly_prob:
            self.fault_ticks = fault_hold - 1
            self.value = spec["fault"]
            return round(self.value, 3), "CRITICAL"

        # Mean-reverting random walk: nudge toward baseline, add Gaussian step, clamp to a sane
        # range. The clamp band always contains the fault value, otherwise the first reading after
        # a fault would teleport to the band edge (0.0 → 99.0 for voltage) instead of decaying back.
        drift = (spec["baseline"] - self.value) * RECOVERY_RATE
        self.value += drift + random.gauss(0.0, spec["noise"])
        self.value = _clamp(self.value,
                            min(spec["ok_low"] - CLAMP_MARGIN, spec["fault"]),
                            max(spec["ok_high"] + CLAMP_MARGIN, spec["fault"]))

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
        # Number synthetic sensors from 1000 up so they never collide with the known s-00xxx ids.
        sensors.append(Sensor(f"s-{1000 + i:05d}", mtype, METRICS[mtype]["baseline"]))
    return sensors


_last_stamp: datetime | None = None


def _now_iso() -> str:
    """ISO-8601 UTC with a trailing ``Z`` and microsecond precision (matches the message schema).

    Strictly increasing, because the wall clock is not: ``datetime.now()`` has ~15ms granularity on
    Windows, so a whole burst of readings can share one value. A repeated
    ``(timestamp, sensor_id, metric_type)`` tuple is silently discarded by ingestion-service's
    ``ON CONFLICT DO NOTHING``, which would make a fast load run under-count rows in TimescaleDB
    with no error anywhere. Nudging by 1µs keeps every reading distinct and correctly ordered; the
    skew is bounded by the emission rate and vanishes as soon as the real clock catches up.

    Not thread-safe — the producer loop is single-threaded.
    """
    global _last_stamp
    now = datetime.now(timezone.utc)
    if _last_stamp is not None and now <= _last_stamp:
        now = _last_stamp + timedelta(microseconds=1)
    _last_stamp = now
    return now.isoformat(timespec="microseconds").replace("+00:00", "Z")


def make_message(sensor: Sensor, anomaly_prob: float, fault_hold: int = 1) -> dict[str, object]:
    value, status = sensor.next_reading(anomaly_prob, fault_hold)
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
    p.add_argument("--fault-hold", type=int, default=3,
                   help="Readings a hard fault lasts before recovery starts (default: 3).")
    p.add_argument("--max-messages", type=int, default=0,
                   help="Stop after N messages; 0 (default) runs until interrupted.")
    p.add_argument("--seed", type=int, default=None, help="RNG seed for reproducible runs.")

    args = p.parse_args(argv)

    # A zero/negative interval would busy-loop and emit duplicate timestamps per sensor; the other
    # bounds just catch typos before a long run.
    if args.interval <= 0:
        p.error("--interval must be greater than 0")
    if not 0.0 <= args.anomaly_prob <= 1.0:
        p.error("--anomaly-prob must be between 0.0 and 1.0")
    if args.fault_hold < 1:
        p.error("--fault-hold must be at least 1")
    if args.extra_sensors < 0:
        p.error("--extra-sensors cannot be negative")
    if args.max_messages < 0:
        p.error("--max-messages cannot be negative")
    return args


def _produce(producer: Producer, topic: str, sensor_id: str, message: dict[str, object],
             on_delivery, stop_event: threading.Event) -> bool:
    """Enqueue one message, applying backpressure when the producer queue is full.

    ``produce()`` raises ``BufferError`` once ``queue.buffering.max.messages`` (100k by default) is
    reached — routine when the broker is slow or down during a long load run. Poll to drain the
    queue and retry instead of dying with a traceback. Returns ``False`` if the message was
    abandoned because shutdown was requested while waiting.
    """
    key = sensor_id.encode("utf-8")
    payload = json.dumps(message).encode("utf-8")
    while True:
        try:
            producer.produce(topic, key=key, value=payload, on_delivery=on_delivery)
            return True
        except BufferError:
            if stop_event.is_set():
                return False
            log.warning("Producer queue full (%d messages), waiting for deliveries...", len(producer))
            producer.poll(BACKPRESSURE_POLL_SECONDS)


def run(args: argparse.Namespace) -> int:
    if args.seed is not None:
        random.seed(args.seed)

    sensors = build_sensors(args.extra_sensors)
    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": "sensor-simulator",
        # Telemetry messages are tiny and near-identical JSON, so batching and compressing them is
        # a large throughput win for a load generator at the cost of ~50ms of latency.
        "linger.ms": 50,
        "compression.type": "lz4",
    })
    log.info("Producing to '%s' at %s: %d sensors (%d known + %d synthetic), interval=%.2fs, anomaly_prob=%.3f",
             args.topic, args.bootstrap, len(sensors), len(KNOWN_SENSORS), args.extra_sensors,
             args.interval, args.anomaly_prob)

    stats = {"enqueued": 0, "delivered": 0, "failed": 0}

    def on_delivery(err, _msg):
        if err is None:
            stats["delivered"] += 1
        else:
            stats["failed"] += 1
            log.warning("Delivery failed: %s", err)

    # An Event (not a bool) so a signal arriving mid-sleep ends the wait immediately: a handler that
    # returns normally lets time.sleep() resume for the remaining time (PEP 475), which would delay
    # SIGTERM shutdown by up to --interval seconds.
    stop_event = threading.Event()

    def stop(_signum, _frame):
        log.info("Shutting down (signal received)...")
        stop_event.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    remaining = 0
    try:
        while not stop_event.is_set():
            for sensor in sensors:
                message = make_message(sensor, args.anomaly_prob, args.fault_hold)
                if _produce(producer, args.topic, sensor.sensor_id, message, on_delivery, stop_event):
                    stats["enqueued"] += 1
                # Serve delivery callbacks and keep the internal queue from filling under load.
                producer.poll(0)
                if stop_event.is_set() or (args.max_messages and stats["enqueued"] >= args.max_messages):
                    stop_event.set()
                    break
            if stop_event.wait(args.interval):
                break
    finally:
        remaining = producer.flush(FLUSH_TIMEOUT_SECONDS)
        if remaining:
            log.warning("%d message(s) still undelivered after flush timeout", remaining)
        log.info("Done. enqueued=%d delivered=%d failed=%d",
                 stats["enqueued"], stats["delivered"], stats["failed"])

    # Undelivered-at-flush counts as failure too: with an unreachable broker nothing is delivered
    # but no error callback has fired yet either (message.timeout.ms outlives the flush timeout).
    return 1 if stats["failed"] or remaining else 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    return run(parse_args())


if __name__ == "__main__":
    sys.exit(main())
