"""Unit tests for the sensor simulator's pure parts — the walk, the sensor set, the message
schema and argument validation. Nothing here touches Kafka.

Run:  ./.venv/Scripts/python.exe -m pytest -q
"""

from __future__ import annotations

import random
from datetime import datetime

import pytest

from sensor_simulator import (
    KNOWN_SENSORS,
    METRICS,
    CascadeScheduler,
    District,
    Sensor,
    build_sensors,
    districts_of,
    layout_of,
    make_message,
    parse_args,
)


# --------------------------------------------------------------------------------------- sensors

def test_build_sensors_starts_with_the_graph_seeded_ones():
    sensors = build_sensors(0)
    assert [(s.sensor_id, s.metric_type) for s in sensors] == KNOWN_SENSORS
    assert all(s.value == METRICS[s.metric_type]["baseline"] for s in sensors)


def test_synthetic_sensor_ids_are_unique_and_never_shadow_known_ones():
    sensors = build_sensors(50)
    ids = [s.sensor_id for s in sensors]

    assert len(sensors) == len(KNOWN_SENSORS) + 50
    assert len(set(ids)) == len(ids)
    assert ids[len(KNOWN_SENSORS)] == "s-01000"
    # Synthetic sensors still cycle through real metric types, so downstream rules apply to them.
    assert {s.metric_type for s in sensors} == set(METRICS)


# ------------------------------------------------------------------------------------- the walk

def test_normal_reading_stays_in_the_ok_band_and_is_reported_ok():
    random.seed(7)
    sensor = Sensor("s-00042", "pipe_pressure", METRICS["pipe_pressure"]["baseline"])
    spec = METRICS["pipe_pressure"]

    for _ in range(200):
        value, status = sensor.next_reading(anomaly_prob=0.0)
        assert status in ("OK", "WARN")
        if spec["ok_low"] <= value <= spec["ok_high"]:
            assert status == "OK"
        else:
            assert status == "WARN"


def test_status_is_warn_outside_the_ok_band():
    # Park the sensor below the band; one mean-reverting step is far too small to re-enter it.
    sensor = Sensor("s-00311", "substation_voltage", 100.0)
    _, status = sensor.next_reading(anomaly_prob=0.0)
    assert status == "WARN"


@pytest.mark.parametrize("metric_type", list(METRICS))
def test_fault_emits_the_metric_fault_value_as_critical(metric_type):
    sensor = Sensor("s-test", metric_type, METRICS[metric_type]["baseline"])
    value, status = sensor.next_reading(anomaly_prob=1.0, fault_hold=1)

    assert status == "CRITICAL"
    assert value == METRICS[metric_type]["fault"]


@pytest.mark.parametrize("metric_type", list(METRICS))
def test_fault_holds_for_the_requested_number_of_readings(metric_type):
    """A fault must be a sustained breach, not a single blip the aggregate could smooth over."""
    sensor = Sensor("s-test", metric_type, METRICS[metric_type]["baseline"])
    fault_value = METRICS[metric_type]["fault"]

    # Trigger the episode, then keep reading with no chance of a *new* fault.
    first = sensor.next_reading(anomaly_prob=1.0, fault_hold=3)
    rest = [sensor.next_reading(anomaly_prob=0.0, fault_hold=3) for _ in range(3)]

    assert [first, rest[0], rest[1]] == [(fault_value, "CRITICAL")] * 3
    assert rest[2][1] != "CRITICAL"  # episode over: recovery starts


@pytest.mark.parametrize("metric_type", list(METRICS))
def test_recovery_after_a_fault_is_gradual_not_a_snap_to_the_band_edge(metric_type):
    """Regression: the clamp used to exclude the fault value, so voltage jumped 0.0 -> 99.0 in one
    step. Recovery must move toward the baseline by roughly the mean-reversion rate instead."""
    random.seed(11)
    spec = METRICS[metric_type]
    sensor = Sensor("s-test", metric_type, spec["baseline"])

    fault_value, _ = sensor.next_reading(anomaly_prob=1.0, fault_hold=1)
    next_value, _ = sensor.next_reading(anomaly_prob=0.0)

    gap = abs(spec["baseline"] - fault_value)
    moved = abs(next_value - fault_value)
    # One step closes ~10% of the gap (plus noise), nowhere near the whole distance.
    assert moved < gap * 0.5
    assert abs(spec["baseline"] - next_value) < gap  # but it is heading the right way


def test_walk_eventually_returns_to_the_ok_band_after_a_fault():
    random.seed(3)
    sensor = Sensor("s-00311", "substation_voltage", METRICS["substation_voltage"]["baseline"])
    sensor.next_reading(anomaly_prob=1.0, fault_hold=1)

    statuses = [sensor.next_reading(anomaly_prob=0.0)[1] for _ in range(100)]
    assert statuses[-1] == "OK"


def test_seeded_runs_are_reproducible():
    def walk():
        random.seed(99)
        sensor = Sensor("s-00042", "pipe_pressure", METRICS["pipe_pressure"]["baseline"])
        return [sensor.next_reading(anomaly_prob=0.05, fault_hold=3) for _ in range(50)]

    assert walk() == walk()


# ----------------------------------------------------------------------------------- the message

def test_message_matches_the_telemetry_schema():
    sensor = Sensor("s-00042", "pipe_pressure", 4.0)
    message = make_message(sensor, anomaly_prob=0.0)

    assert set(message) == {"timestamp", "sensor_id", "metric_type", "value", "status"}
    assert message["sensor_id"] == "s-00042"
    assert message["metric_type"] == "pipe_pressure"
    assert isinstance(message["value"], float)
    assert message["status"] in ("OK", "WARN", "CRITICAL")


def test_timestamp_is_utc_iso8601_with_a_trailing_z():
    stamp = make_message(Sensor("s-00042", "pipe_pressure", 4.0), anomaly_prob=0.0)["timestamp"]

    assert stamp.endswith("Z")
    assert "+00:00" not in stamp
    parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    assert parsed.tzinfo is not None


def test_timestamps_are_distinct_and_increasing_for_back_to_back_readings():
    """Duplicate (timestamp, sensor_id, metric_type) tuples are silently dropped by
    ingestion-service's ON CONFLICT DO NOTHING, and the wall clock is far too coarse on Windows to
    separate a burst on its own."""
    sensor = Sensor("s-00042", "pipe_pressure", 4.0)
    stamps = [make_message(sensor, anomaly_prob=0.0)["timestamp"] for _ in range(500)]

    assert len(set(stamps)) == len(stamps)
    assert stamps == sorted(stamps)


# --------------------------------------------------------------------------------------- the CLI

def test_defaults(monkeypatch):
    monkeypatch.delenv("CITY_EXTRA_SENSORS", raising=False)
    args = parse_args([])
    assert args.topic == "city-telemetry"
    assert args.interval == 2.0
    assert args.extra_sensors == 0
    assert args.fault_hold >= 1
    assert 0.0 <= args.anomaly_prob <= 1.0


@pytest.mark.parametrize("argv", [
    ["--interval", "0"],
    ["--interval", "-1"],
    ["--anomaly-prob", "1.5"],
    ["--anomaly-prob", "-0.1"],
    ["--fault-hold", "0"],
    ["--extra-sensors", "-1"],
    ["--max-messages", "-5"],
    ["--cascade-every", "-1"],
    ["--cascade-delay", "-1"],
])
def test_invalid_arguments_are_rejected(argv):
    with pytest.raises(SystemExit) as exit_info:
        parse_args(argv)
    assert exit_info.value.code == 2


def test_valid_arguments_are_accepted():
    args = parse_args(["--interval", "0.5", "--anomaly-prob", "1.0", "--extra-sensors", "100",
                       "--max-messages", "0", "--fault-hold", "1"])
    assert args.interval == 0.5
    assert args.anomaly_prob == 1.0
    assert args.extra_sensors == 100


def test_extra_sensors_default_comes_from_the_shared_city_variable(monkeypatch):
    monkeypatch.setenv("CITY_EXTRA_SENSORS", "997")
    assert parse_args([]).extra_sensors == 997
    assert parse_args(["--extra-sensors", "5"]).extra_sensors == 5


def test_cascade_is_off_by_default(monkeypatch):
    monkeypatch.delenv("CITY_EXTRA_SENSORS", raising=False)
    args = parse_args([])
    assert args.cascade_every == 0
    assert args.cascade_delay >= 0


# ------------------------------------------------------------------------------ the city layout

# Same fixture as topology-service SyntheticCityTest: the cascade only works if both sides agree.
@pytest.mark.parametrize("sensor_id, asset, district", [
    ("s-01000", "bld-d00-0-0", "d00"),
    ("s-01001", "node-d00-0", "d00"),
    ("s-01002", "sub-d00", "d00"),
    ("s-01012", "bld-d00-1-0", "d00"),
    ("s-01996", "bld-d20-3-0", "d20"),
])
def test_layout_matches_the_topology_service_city(sensor_id, asset, district):
    index = int(sensor_id[2:]) - 1000
    assert layout_of(index) == (asset, district)
    # The simulator gives that index the metric the layout links it by.
    assert build_sensors(index + 1)[-1].sensor_id == sensor_id


def test_districts_group_voltage_and_pressure_sensors_by_substation():
    districts = districts_of(997)

    assert len(districts) == 21 + 1
    first = districts[0]
    assert first.substation == "sub-d00"
    assert first.voltage[0] == "s-01002" and len(first.voltage) == 16
    assert first.pressure[0] == "s-01000" and len(first.pressure) == 16
    assert districts[-1] == District("sub-1", ["s-00311"], ["s-00042"])


def test_without_synthetic_sensors_the_hand_seeded_district_still_cascades():
    assert districts_of(0) == [District("sub-1", ["s-00311"], ["s-00042"])]


# --------------------------------------------------------------------------------- the cascade

def test_cascade_fails_voltage_first_then_pressure_after_the_delay():
    district = District("sub-d00", ["s-01002"], ["s-01000", "s-01003"])
    scheduler = CascadeScheduler([district], every=300, delay=5, start=0, first_after=10)

    assert scheduler.due(9.9) == []
    assert scheduler.due(10) == [("sub-d00", "voltage", ["s-01002"])]
    assert scheduler.due(14) == []
    assert scheduler.due(15) == [("sub-d00", "pressure", ["s-01000", "s-01003"])]
    assert scheduler.due(100) == []


def test_cascade_visits_districts_round_robin():
    districts = [District("sub-a", ["v-a"], ["p-a"]), District("sub-b", ["v-b"], ["p-b"])]
    scheduler = CascadeScheduler(districts, every=60, delay=0, start=0, first_after=0)

    fired = [group[0] for now in (0, 60, 120) for group in scheduler.due(now) if group[1] == "voltage"]

    assert fired == ["sub-a", "sub-b", "sub-a"]


def test_forced_fault_holds_the_fault_value():
    sensor = Sensor("s-01000", "pipe_pressure", METRICS["pipe_pressure"]["baseline"])
    sensor.force_fault(3)

    readings = [sensor.next_reading(anomaly_prob=0.0) for _ in range(4)]

    assert readings[:3] == [(0.0, "CRITICAL")] * 3
    assert readings[3][1] != "CRITICAL"
