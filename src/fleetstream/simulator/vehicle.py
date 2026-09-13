"""Per-vehicle state machine producing physically coherent telemetry.

Emitting independent random values per field would be much simpler, and would make
every downstream fact meaningless: fuel would not fall as distance rises, trips
would have no duration, and "average fuel consumption per route" would return
noise that no test could distinguish from a broken pipeline.

So each vehicle is simulated with state that persists between events:

* position advances along a route by ``speed * dt``,
* the odometer accumulates that distance and never decreases,
* fuel falls in proportion to distance through the vehicle's consumption rate,
* engine temperature warms towards a steady state and runs hotter for the vehicles
  flagged with a faulty cooling system.

The consequence is that the Gold layer can be checked against arithmetic — trip
distance really is the odometer delta, and fuel consumed really is distance times
the consumption rate — rather than merely "looking plausible".
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from random import Random

from fleetstream.common.schemas import SCHEMA_VERSION
from fleetstream.simulator.reference import (
    DriverProfile,
    Route,
    VehicleProfile,
    haversine_km,
    interpolate,
)

#: Stable namespace for deterministic event IDs. Fixed forever: changing it would
#: make every previously produced event_id irreproducible, and replay-based
#: idempotency depends on the same logical event hashing to the same ID.
EVENT_NAMESPACE = uuid.UUID("6f2b9a54-2d51-5c3e-9f2a-1c4d7e8b0a31")

AMBIENT_TEMP_C = 32.0
COLD_START_TEMP_C = 40.0
NORMAL_OPERATING_TEMP_C = 88.0
#: Kept just below the 100C incident threshold. A faulty vehicle that sat
#: permanently above the threshold would make every one of its readings an
#: 'incident', which is a constant state rather than an event worth alerting on.
#: Running hot and spiking over is what produces detectable, countable incidents.
FAULTY_OPERATING_TEMP_C = 95.0
#: Fraction of the gap to operating temperature closed per second of driving.
THERMAL_RESPONSE = 0.004
#: Cooling rate per second once the engine is off.
COOLDOWN_RATE = 0.002
#: Physical ceiling - beyond this an engine would seize rather than keep reporting.
MAX_ENGINE_TEMP_C = 125.0
#: Per-tick probability of a transient overheating spike.
SPIKE_RATE_FAULTY = 0.0025
SPIKE_RATE_HEALTHY = 0.00004

MIN_REST_MINUTES = 20
MAX_REST_MINUTES = 90
#: Refuel when the tank drops below this fraction; happens only while parked.
REFUEL_THRESHOLD = 0.15


class VehicleStatus(str, Enum):
    PARKED = "parked"
    DRIVING = "driving"


@dataclass(frozen=True)
class TelemetryEvent:
    """One telemetry reading, matching the contract in ``common.schemas``."""

    event_id: str
    vehicle_id: str
    driver_id: str
    trip_id: str
    route_id: str | None
    event_time: datetime
    latitude: float
    longitude: float
    speed: float
    fuel_level: float
    engine_temperature: float
    battery_level: float
    odometer_km: float
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, object]:
        """JSON-ready mapping. ``event_time`` is ISO-8601 UTC with a trailing Z."""
        return {
            "event_id": self.event_id,
            "vehicle_id": self.vehicle_id,
            "driver_id": self.driver_id,
            "trip_id": self.trip_id,
            "route_id": self.route_id,
            "event_time": self.event_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),
            "speed": round(self.speed, 2),
            "fuel_level": round(self.fuel_level, 2),
            "engine_temperature": round(self.engine_temperature, 2),
            "battery_level": round(self.battery_level, 2),
            "odometer_km": round(self.odometer_km, 3),
            "schema_version": self.schema_version,
        }


def _cumulative_distances(route: Route) -> tuple[float, ...]:
    """Distance from the route start to each waypoint."""
    out = [0.0]
    for i in range(len(route.waypoints) - 1):
        out.append(out[-1] + haversine_km(route.waypoints[i], route.waypoints[i + 1]))
    return tuple(out)


_ROUTE_CUMULATIVE: dict[str, tuple[float, ...]] = {}


def position_at(route: Route, progress_km: float) -> tuple[float, float]:
    """Interpolated ``(lat, lon)`` at ``progress_km`` along the route."""
    cumulative = _ROUTE_CUMULATIVE.get(route.route_id)
    if cumulative is None:
        cumulative = _cumulative_distances(route)
        _ROUTE_CUMULATIVE[route.route_id] = cumulative

    if progress_km <= 0:
        return route.waypoints[0]
    if progress_km >= cumulative[-1]:
        return route.waypoints[-1]

    for i in range(len(cumulative) - 1):
        if cumulative[i] <= progress_km <= cumulative[i + 1]:
            span = cumulative[i + 1] - cumulative[i]
            t = 0.0 if span == 0 else (progress_km - cumulative[i]) / span
            return interpolate(route.waypoints[i], route.waypoints[i + 1], t)
    return route.waypoints[-1]


def deterministic_event_id(vehicle_id: str, trip_id: str, sequence: int) -> str:
    """A UUIDv5 over ``(vehicle, trip, sequence)``.

    Deterministic rather than random on purpose. A random UUID would make a replayed
    event look brand new, defeating the MERGE in Silver: the same logical reading
    must always carry the same key no matter how many times it is produced.
    """
    return str(uuid.uuid5(EVENT_NAMESPACE, f"{vehicle_id}|{trip_id}|{sequence}"))


@dataclass
class VehicleSimulator:
    """Mutable simulation state for one vehicle."""

    profile: VehicleProfile
    rng: Random
    routes: tuple[Route, ...]
    drivers: tuple[DriverProfile, ...]

    status: VehicleStatus = VehicleStatus.PARKED
    trip_seq: int = 0
    event_seq: int = 0
    trip_id: str = ""
    route: Route | None = None
    driver: DriverProfile | None = None
    progress_km: float = 0.0
    odometer_km: float = 0.0
    fuel_l: float = 0.0
    engine_temp_c: float = AMBIENT_TEMP_C
    battery_level: float = 95.0
    speed_kmh: float = 0.0
    #: Simulation time at which a parked vehicle starts its next trip.
    resume_at: datetime | None = None

    def __post_init__(self) -> None:
        self.fuel_l = self.profile.fuel_capacity_l * self.rng.uniform(0.55, 1.0)
        self.odometer_km = round(self.rng.uniform(15_000, 240_000), 1)
        self.engine_temp_c = AMBIENT_TEMP_C + self.rng.uniform(-3, 6)
        self.battery_level = self.rng.uniform(88.0, 100.0)

    # -- lifecycle ---------------------------------------------------------

    def _start_trip(self, now: datetime) -> None:
        self.trip_seq += 1
        self.event_seq = 0
        self.trip_id = f"T{self.profile.vehicle_id[1:]}{self.trip_seq:04d}"
        self.route = self.rng.choice(self.routes)
        self.driver = self.rng.choice(self.drivers)
        self.progress_km = 0.0
        self.status = VehicleStatus.DRIVING
        self.engine_temp_c = max(self.engine_temp_c, COLD_START_TEMP_C)
        self.resume_at = None

    def _end_trip(self, now: datetime) -> None:
        self.status = VehicleStatus.PARKED
        self.speed_kmh = 0.0
        rest = self.rng.uniform(MIN_REST_MINUTES, MAX_REST_MINUTES)
        self.resume_at = now + timedelta(minutes=rest)
        # Refuelling happens only while parked, so fuel never rises mid-trip - a
        # property the trip-level fuel calculations rely on.
        if self.fuel_l < self.profile.fuel_capacity_l * REFUEL_THRESHOLD:
            self.fuel_l = self.profile.fuel_capacity_l * self.rng.uniform(0.9, 1.0)

    # -- per-tick physics --------------------------------------------------

    def _target_speed(self) -> float:
        """Cruising speed, with the driver's aggression pushing it over the limit."""
        assert self.driver is not None
        limit = self.profile.max_speed_kmh
        base = limit * self.rng.uniform(0.55, 0.85)
        # Aggression raises both the chance and the size of a speeding episode.
        if self.rng.random() < 0.06 * self.driver.aggression:
            return limit * self.rng.uniform(1.02, 1.25)
        return base

    def _update_temperature(self, dt_s: float, moving: bool) -> None:
        if moving:
            target = (
                FAULTY_OPERATING_TEMP_C if self.profile.faulty_cooling else NORMAL_OPERATING_TEMP_C
            )
            # Load-dependent: working harder at speed runs hotter.
            target += (self.speed_kmh / max(self.profile.max_speed_kmh, 1.0)) * 3.0
            self.engine_temp_c += (target - self.engine_temp_c) * THERMAL_RESPONSE * dt_s
            self.engine_temp_c += self.rng.gauss(0.0, 0.35)
            self.engine_temp_c = min(self.engine_temp_c, MAX_ENGINE_TEMP_C)
            # Occasional transient spike - the operational incidents the platform
            # is meant to catch in near real time.
            # A spike decays back over several minutes, so each one makes a run of
            # events hot. The healthy-vehicle rate is therefore kept very low: a
            # higher one would put most of the fleet over the threshold at some
            # point and drown the vehicles that genuinely need maintenance.
            if self.rng.random() < (
                SPIKE_RATE_FAULTY if self.profile.faulty_cooling else SPIKE_RATE_HEALTHY
            ):
                self.engine_temp_c += self.rng.uniform(8.0, 22.0)
        else:
            self.engine_temp_c += (AMBIENT_TEMP_C - self.engine_temp_c) * COOLDOWN_RATE * dt_s

    def tick(self, now: datetime, dt_s: float) -> TelemetryEvent | None:
        """Advance the simulation by ``dt_s`` seconds and emit a reading.

        Returns ``None`` while the vehicle is resting between trips: a parked
        vehicle producing no telemetry is realistic, and it gives fleet utilisation
        a denominator that is not simply "all vehicles, always".
        """
        if self.status is VehicleStatus.PARKED:
            if self.resume_at is None or now >= self.resume_at:
                self._start_trip(now)
            else:
                self._update_temperature(dt_s, moving=False)
                return None

        assert self.route is not None and self.driver is not None

        self.speed_kmh = max(0.0, self._target_speed() + self.rng.gauss(0.0, 4.0))
        distance_km = self.speed_kmh * (dt_s / 3600.0)

        self.progress_km += distance_km
        self.odometer_km += distance_km

        burn_l = distance_km * self.profile.consumption_l_per_100km / 100.0
        self.fuel_l = max(0.0, self.fuel_l - burn_l)

        self._update_temperature(dt_s, moving=True)

        # Alternator tops the battery up while the engine runs.
        self.battery_level = min(100.0, self.battery_level + 0.02 * dt_s / 60.0)

        lat, lon = position_at(self.route, self.progress_km)
        # GPS jitter, roughly a few metres.
        lat += self.rng.gauss(0.0, 0.00004)
        lon += self.rng.gauss(0.0, 0.00004)

        event = TelemetryEvent(
            event_id=deterministic_event_id(self.profile.vehicle_id, self.trip_id, self.event_seq),
            vehicle_id=self.profile.vehicle_id,
            driver_id=self.driver.driver_id,
            trip_id=self.trip_id,
            route_id=self.route.route_id,
            event_time=now,
            latitude=lat,
            longitude=lon,
            speed=self.speed_kmh,
            fuel_level=self.fuel_l / self.profile.fuel_capacity_l * 100.0,
            engine_temperature=self.engine_temp_c,
            battery_level=self.battery_level,
            odometer_km=self.odometer_km,
        )
        self.event_seq += 1

        # Out of route, or out of fuel: either way the trip is over.
        if self.progress_km >= self.route.distance_km or self.fuel_l <= 0.0:
            self._end_trip(now)

        return event


def build_simulators(
    vehicles: tuple[VehicleProfile, ...],
    drivers: tuple[DriverProfile, ...],
    routes: tuple[Route, ...],
    seed: int,
) -> dict[str, VehicleSimulator]:
    """One simulator per vehicle, each with its own seeded RNG.

    Per-vehicle RNGs (derived from the run seed and the vehicle ID) rather than one
    shared generator: with a shared RNG, the values a vehicle produces would depend
    on how many other vehicles ticked first, so adding a vehicle would change every
    other vehicle's history and the whole run would stop being reproducible.
    """
    simulators: dict[str, VehicleSimulator] = {}
    for profile in vehicles:
        vehicle_rng = Random(f"{seed}:{profile.vehicle_id}")
        simulators[profile.vehicle_id] = VehicleSimulator(
            profile=profile, rng=vehicle_rng, routes=routes, drivers=drivers
        )
    return simulators


__all__ = [
    "TelemetryEvent",
    "VehicleSimulator",
    "VehicleStatus",
    "build_simulators",
    "deterministic_event_id",
    "position_at",
]
