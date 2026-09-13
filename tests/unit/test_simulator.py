"""The simulator must be reproducible and physically coherent.

If these properties do not hold, every downstream assertion about trips, fuel and
distance is measuring noise.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from itertools import pairwise

import pytest

from fleetstream.common.config import SimulatorConfig
from fleetstream.simulator.generator import CORRUPTIONS, TelemetryGenerator, corrupt
from fleetstream.simulator.reference import build_reference, haversine_km, interpolate
from fleetstream.simulator.vehicle import build_simulators, deterministic_event_id

SEED = 20260908
START = datetime(2026, 9, 8, 6, 0, tzinfo=timezone.utc)


@pytest.fixture
def reference():
    return build_reference(30, SEED, "V9999")


def simulate(reference, ticks: int = 1500, seed: int = SEED):
    sims = build_simulators(reference.vehicles, reference.drivers, reference.routes, seed)
    events = []
    for step in range(ticks):
        now = START + timedelta(seconds=6 * step)
        for sim in sims.values():
            event = sim.tick(now, 6.0)
            if event is not None:
                events.append(event)
    return events


# -- reference data ---------------------------------------------------------


def test_reference_is_deterministic():
    a = build_reference(50, SEED, "V9999")
    b = build_reference(50, SEED, "V9999")
    assert a.vehicles == b.vehicles
    assert a.drivers == b.drivers


def test_different_seeds_produce_different_fleets():
    a = build_reference(50, 1, None)
    b = build_reference(50, 2, None)
    assert a.vehicles != b.vehicles


def test_hot_vehicle_is_always_present_in_the_dimension():
    """Its telemetry must have somewhere to join, or skew looks like a data bug."""
    reference = build_reference(10, SEED, "V9999")
    assert "V9999" in reference.by_vehicle


def test_hot_vehicle_not_duplicated_when_already_in_range():
    reference = build_reference(10, SEED, "V1005")
    assert [v.vehicle_id for v in reference.vehicles].count("V1005") == 1


def test_haversine_matches_known_distance():
    """Pune to Mumbai is roughly 120 km great-circle."""
    d = haversine_km((18.5204, 73.8567), (19.0760, 72.8777))
    assert 110 < d < 130


def test_interpolate_clamps_outside_unit_interval():
    a, b = (0.0, 0.0), (10.0, 20.0)
    assert interpolate(a, b, -5) == a
    assert interpolate(a, b, 5) == b
    assert interpolate(a, b, 0.5) == (5.0, 10.0)


# -- vehicle physics --------------------------------------------------------


def test_event_ids_are_deterministic_and_distinct():
    """Deterministic IDs are what make the Silver MERGE idempotent under replay."""
    assert deterministic_event_id("V1", "T1", 0) == deterministic_event_id("V1", "T1", 0)
    assert deterministic_event_id("V1", "T1", 0) != deterministic_event_id("V1", "T1", 1)
    assert deterministic_event_id("V1", "T1", 0) != deterministic_event_id("V2", "T1", 0)


def test_simulation_is_reproducible(reference):
    first = simulate(reference, ticks=300)
    second = simulate(reference, ticks=300)
    assert [e.event_id for e in first] == [e.event_id for e in second]
    assert [e.odometer_km for e in first] == [e.odometer_km for e in second]


def test_odometer_never_decreases(reference):
    events = simulate(reference)
    by_vehicle: dict[str, list[float]] = {}
    for e in events:
        by_vehicle.setdefault(e.vehicle_id, []).append(e.odometer_km)
    for vehicle, readings in by_vehicle.items():
        assert all(b >= a for a, b in pairwise(readings)), f"{vehicle} odometer went back"


def test_fuel_never_rises_within_a_trip(reference):
    """Refuelling happens only while parked. Trip-level fuel consumption is computed
    as a start-minus-end difference, which is wrong the moment fuel can rise mid-trip.
    """
    events = simulate(reference)
    by_trip: dict[str, list[float]] = {}
    for e in events:
        by_trip.setdefault(e.trip_id, []).append(e.fuel_level)
    for trip, levels in by_trip.items():
        assert all(b <= a + 1e-9 for a, b in pairwise(levels)), f"{trip} refuelled mid-trip"


def test_readings_stay_within_the_validation_ranges(reference):
    """Clean events must pass the quality rules; otherwise the quarantine counts
    would be dominated by the simulator rather than by injected corruption."""
    for e in simulate(reference):
        assert 0 <= e.fuel_level <= 100
        assert 0 <= e.battery_level <= 100
        assert -90 <= e.latitude <= 90
        assert -180 <= e.longitude <= 180
        assert e.speed >= 0
        assert -50 <= e.engine_temperature <= 200


def test_overheating_is_rare_and_concentrated_on_faulty_vehicles(reference):
    """A threshold every vehicle crosses constantly is not an incident signal."""
    events = simulate(reference)
    hot = [e for e in events if e.engine_temperature > 100]
    assert hot, "no overheating at all - the incident path would be untested"
    assert len(hot) / len(events) < 0.05

    faulty = {v.vehicle_id for v in reference.vehicles if v.faulty_cooling}
    from_faulty = sum(1 for e in hot if e.vehicle_id in faulty)
    assert from_faulty / len(hot) > 0.7


def test_trips_have_multiple_events(reference):
    """One row per trip would make fact_trip and fact_telemetry the same grain."""
    events = simulate(reference)
    counts: dict[str, int] = {}
    for e in events:
        counts[e.trip_id] = counts.get(e.trip_id, 0) + 1
    assert len(counts) > 5
    assert sum(counts.values()) / len(counts) > 10


def test_event_payload_is_json_ready(reference):
    event = simulate(reference, ticks=5)[0]
    payload = event.to_dict()
    assert payload["event_time"].endswith("Z")
    assert isinstance(payload["speed"], float)
    assert set(payload) == {
        "event_id",
        "vehicle_id",
        "driver_id",
        "trip_id",
        "route_id",
        "event_time",
        "latitude",
        "longitude",
        "speed",
        "fuel_level",
        "engine_temperature",
        "battery_level",
        "odometer_km",
        "schema_version",
    }


# -- injections -------------------------------------------------------------


def make_generator(reference, **overrides):
    defaults = dict(
        vehicle_count=len(reference.vehicles),
        events_per_vehicle_per_min=10,
        speedup=60.0,
        seed=SEED,
        duplicate_rate=0.0,
        late_rate=0.0,
        invalid_rate=0.0,
        hot_vehicle="V9999",
        hot_vehicle_multiplier=1,
    )
    defaults.update(overrides)
    return TelemetryGenerator(SimulatorConfig(**defaults), reference, START)


def test_produced_records_reconcile_with_counted_injections(reference):
    """The identity the whole verification suite rests on."""
    gen = make_generator(reference, duplicate_rate=0.05, late_rate=0.05, invalid_rate=0.05)
    records = list(gen.run(200))
    assert gen.stats.total_produced == len(records)
    assert gen.pending_late == 0, "late buffer was not flushed - those events are lost"


def test_duplicates_repeat_an_existing_event_id(reference):
    gen = make_generator(reference, duplicate_rate=0.5)
    records = list(gen.run(100))
    ids = [r[1]["event_id"] for r in records]
    assert len(ids) - len(set(ids)) == gen.stats.duplicates


def test_no_duplicates_when_the_rate_is_zero(reference):
    gen = make_generator(reference)
    ids = [r[1]["event_id"] for r in gen.run(100)]
    assert len(ids) == len(set(ids))


def test_late_events_keep_their_original_event_time(reference):
    """Only arrival moves; event_time is what watermarking and partitioning use."""
    gen = make_generator(reference, late_rate=1.0)
    records = list(gen.run(50))
    assert gen.stats.late == len(records)
    times = [r[1]["event_time"] for r in records]
    assert len(set(times)) > 1


def test_late_events_are_counted_as_valid(reference):
    """They do reach Silver, so excluding them would break reconciliation."""
    gen = make_generator(reference, late_rate=1.0)
    list(gen.run(50))
    assert gen.stats.clean == gen.stats.late


def test_hot_vehicle_dominates_its_partition_without_swamping_the_fleet(reference):
    gen = make_generator(reference, hot_vehicle_multiplier=25)
    records = list(gen.run(100))
    hot = sum(1 for key, _ in records if key == "V9999")
    share = hot / len(records)
    assert share > 0.2, "no visible skew to demonstrate"
    assert share < 0.6, "one vehicle became the whole dataset"


def test_every_corruption_kind_changes_the_payload(reference):
    gen = make_generator(reference)
    clean = next(iter(gen.run(1)))[1]
    for kind in CORRUPTIONS:
        assert corrupt(clean, kind) != clean, f"{kind} left the payload untouched"


def test_unknown_corruption_is_rejected():
    with pytest.raises(ValueError, match="unknown corruption"):
        corrupt({}, "not_a_real_corruption")


def test_invalid_records_replace_rather_than_accompany_the_clean_one(reference):
    """Sending both would make the quarantine count untestable."""
    gen = make_generator(reference, invalid_rate=1.0)
    records = list(gen.run(30))
    assert gen.stats.clean == 0
    assert gen.stats.invalid == len(records)
