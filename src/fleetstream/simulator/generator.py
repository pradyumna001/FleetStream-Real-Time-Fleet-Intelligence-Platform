"""Event stream assembly, including the deliberate injection of bad data.

A simulator that produced only clean, in-order, unique events would let the whole
platform pass its own tests while the deduplication, watermarking and quarantine
logic was silently broken. Nothing downstream would ever exercise those paths.

So the generator injects, at configurable rates, exactly the pathologies the
platform claims to handle:

===================  ==========================================================
duplicates           the same event_id produced more than once
late arrivals        events held back and produced well after their event_time
invalid records      out-of-range, wrong-typed or missing required fields
hot partition        one vehicle emitting orders of magnitude more than the rest
===================  ==========================================================

Every injection is counted, and the counts are written to a run manifest so the
verification suite can assert that the pipeline caught what was thrown at it,
rather than merely that it produced *some* output.
"""

from __future__ import annotations

import heapq
import itertools
import logging
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from random import Random
from typing import Any

from fleetstream.common.config import SimulatorConfig
from fleetstream.simulator.reference import FleetReference
from fleetstream.simulator.vehicle import TelemetryEvent, VehicleSimulator, build_simulators

logger = logging.getLogger(__name__)

#: Seconds of simulated time between readings from one vehicle.
TICK_SECONDS = 6.0


@dataclass
class InjectionStats:
    """Counts of what the generator produced, for end-to-end reconciliation."""

    #: Distinct, valid records produced. Late arrivals are included: they are
    #: perfectly valid events that simply arrived after the fact, and they do reach
    #: Silver, so excluding them would break reconciliation.
    clean: int = 0
    #: Extra copies of an already-counted record. Not part of ``clean``.
    duplicates: int = 0
    #: Subset of ``clean`` that was held back and delivered late.
    late: int = 0
    #: Corrupted records, sent instead of their clean original.
    invalid: int = 0
    #: Records emitted by the deliberately skewed vehicle, valid or not.
    hot_vehicle: int = 0

    @property
    def total_produced(self) -> int:
        """Every record handed to Kafka, duplicates included."""
        return self.clean + self.duplicates + self.invalid

    @property
    def distinct_valid(self) -> int:
        """Records that should survive to Silver: unique and passing validation."""
        return self.clean

    def as_dict(self) -> dict[str, int]:
        d = {k: int(v) for k, v in asdict(self).items()}
        d["total_produced"] = self.total_produced
        d["distinct_valid"] = self.distinct_valid
        return d


@dataclass(order=True)
class _PendingLate:
    """A late event waiting for its release time. Ordered by release time for the heap."""

    release_at: datetime
    sequence: int
    payload: dict[str, Any] = field(compare=False)
    key: str = field(compare=False)


#: The ways a record can be invalid. Each targets a specific rule in quality/rules.yml,
#: so a rule that stops working shows up as a drop in that corruption's quarantine count
#: rather than as a vague overall shortfall.
CORRUPTIONS: tuple[str, ...] = (
    "null_vehicle_id",
    "negative_speed",
    "fuel_out_of_range",
    "latitude_out_of_range",
    "longitude_out_of_range",
    "battery_out_of_range",
    "missing_event_time",
    "absurd_engine_temperature",
    "unparseable_speed",
)


def corrupt(payload: dict[str, Any], kind: str) -> dict[str, Any]:
    """Return a copy of ``payload`` broken in one specific, named way."""
    bad = dict(payload)
    if kind == "null_vehicle_id":
        bad["vehicle_id"] = None
    elif kind == "negative_speed":
        bad["speed"] = -abs(float(bad.get("speed") or 10.0)) - 5.0
    elif kind == "fuel_out_of_range":
        bad["fuel_level"] = 142.7
    elif kind == "latitude_out_of_range":
        bad["latitude"] = 118.4
    elif kind == "longitude_out_of_range":
        bad["longitude"] = -241.9
    elif kind == "battery_out_of_range":
        bad["battery_level"] = -12.5
    elif kind == "missing_event_time":
        bad["event_time"] = None
    elif kind == "absurd_engine_temperature":
        bad["engine_temperature"] = 934.0
    elif kind == "unparseable_speed":
        # A string where a number belongs: exercises the parse path rather than a
        # range rule, which is how a real producer-side bug usually presents.
        bad["speed"] = "seventy-two"
    else:  # pragma: no cover - guarded by CORRUPTIONS
        raise ValueError(f"unknown corruption kind: {kind}")
    return bad


class TelemetryGenerator:
    """Drives every vehicle simulator and layers the injections on top.

    The generator owns simulated time. It starts in the past and advances by
    ``TICK_SECONDS`` per tick, so a short run produces hours of history that ends
    at roughly the present - which is what makes "yesterday" and "today" dashboard
    queries return data immediately after a run.
    """

    def __init__(
        self,
        config: SimulatorConfig,
        reference: FleetReference,
        start_time: datetime,
    ) -> None:
        self.config = config
        self.reference = reference
        self.sim_time = start_time
        self.stats = InjectionStats()
        self.rng = Random(config.seed ^ 0x5EED)
        self.simulators: dict[str, VehicleSimulator] = build_simulators(
            reference.vehicles, reference.drivers, reference.routes, config.seed
        )
        self._late_heap: list[_PendingLate] = []
        self._counter = itertools.count()

    # -- injections --------------------------------------------------------

    def _emit_multiplier(self, vehicle_id: str) -> int:
        """How many readings this vehicle produces per tick.

        The hot vehicle produces many. Because it is also the Kafka partition key,
        all of them land on one partition, reproducing a genuine hot-partition
        problem instead of a simulated-looking one.
        """
        if vehicle_id == self.config.hot_vehicle:
            return max(1, self.config.hot_vehicle_multiplier)
        return 1

    def _schedule_late(self, payload: dict[str, Any], key: str) -> None:
        """Hold an event back so it arrives long after the time it describes.

        The event keeps its original ``event_time``; only its arrival moves. That is
        precisely the shape of a vehicle that lost connectivity and buffered
        readings, and it is what the watermark and the dbt lookback window exist for.
        """
        delay = timedelta(minutes=self.rng.uniform(5.0, 20.0))
        heapq.heappush(
            self._late_heap,
            _PendingLate(self.sim_time + delay, next(self._counter), payload, key),
        )
        # Counted as clean as well as late: run() guarantees the buffer is flushed,
        # so this record will be produced, and it is a valid one when it lands.
        self.stats.clean += 1
        self.stats.late += 1

    def _drain_late(self) -> Iterator[tuple[str, dict[str, Any]]]:
        """Release any late events whose delay has now elapsed."""
        while self._late_heap and self._late_heap[0].release_at <= self.sim_time:
            pending = heapq.heappop(self._late_heap)
            yield pending.key, pending.payload

    def _process(self, event: TelemetryEvent) -> Iterator[tuple[str, dict[str, Any]]]:
        """Turn one simulated reading into zero or more records on the wire."""
        payload = event.to_dict()
        key = event.vehicle_id
        if event.vehicle_id == self.config.hot_vehicle:
            self.stats.hot_vehicle += 1

        # Invalid records are corrupted copies. The clean original is not also sent,
        # because a real broken producer emits the broken record instead of the good
        # one - sending both would make the quarantine count untestable.
        if self.rng.random() < self.config.invalid_rate:
            self.stats.invalid += 1
            yield key, corrupt(payload, self.rng.choice(CORRUPTIONS))
            return

        if self.rng.random() < self.config.late_rate:
            self._schedule_late(payload, key)
            return

        self.stats.clean += 1
        yield key, payload

        # A duplicate is the identical record produced a second time, exactly as a
        # producer retry after an ambiguous ack would do.
        if self.rng.random() < self.config.duplicate_rate:
            self.stats.duplicates += 1
            yield key, dict(payload)

    # -- main loop ---------------------------------------------------------

    def tick(self) -> Iterator[tuple[str, dict[str, Any]]]:
        """Advance simulated time by one tick and yield ``(key, payload)`` records."""
        for vehicle_id, simulator in self.simulators.items():
            multiplier = self._emit_multiplier(vehicle_id)
            # Sub-ticks keep the physics consistent: the hot vehicle covers the same
            # ground in the same time, it just reports far more often.
            sub_dt = TICK_SECONDS / multiplier
            for i in range(multiplier):
                moment = self.sim_time + timedelta(seconds=sub_dt * i)
                event = simulator.tick(moment, sub_dt)
                if event is not None:
                    yield from self._process(event)

        yield from self._drain_late()
        self.sim_time += timedelta(seconds=TICK_SECONDS)

    def flush_late(self) -> Iterator[tuple[str, dict[str, Any]]]:
        """Release every still-buffered late event, regardless of its release time.

        Callers must invoke this at the end of a run. Without it, events still held
        in the late buffer are silently dropped and the produced count can never
        reconcile with the injected count - the buffer would quietly become a data
        loss bug in the thing built to prove there is no data loss.
        """
        while self._late_heap:
            pending = heapq.heappop(self._late_heap)
            yield pending.key, pending.payload

    def run(self, ticks: int) -> Iterator[tuple[str, dict[str, Any]]]:
        """Yield records for ``ticks`` ticks, then flush any still-pending late events."""
        for _ in range(ticks):
            yield from self.tick()
        yield from self.flush_late()

    @property
    def pending_late(self) -> int:
        return len(self._late_heap)
