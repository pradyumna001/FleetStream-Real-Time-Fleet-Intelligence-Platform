"""Deterministic reference data: vehicles, drivers and routes.

These are the dimensions the telemetry facts join to. They are generated from a
seed rather than checked in as fixtures so the fleet can be resized with one
setting, while any given seed always produces exactly the same fleet — which is
what lets enrichment joins and dbt tests assert on specific IDs.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field

VEHICLE_TYPES: tuple[tuple[str, float, float], ...] = (
    # (type, max_speed_kmh, fuel_capacity_litres)
    ("panel_van", 120.0, 70.0),
    ("box_truck", 100.0, 150.0),
    ("semi_trailer", 90.0, 400.0),
    ("refrigerated_van", 110.0, 90.0),
    ("light_pickup", 130.0, 60.0),
)

MANUFACTURERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Tata Motors", ("Ace", "Intra V30", "Prima 4928")),
    ("Ashok Leyland", ("Dost+", "Boss 1115", "Captain 2523")),
    ("Mahindra", ("Bolero Pikup", "Furio 7", "Blazo X28")),
    ("Eicher", ("Pro 2049", "Pro 3015", "Pro 6028")),
)

REGIONS: tuple[str, ...] = ("Pune", "Mumbai", "Nashik", "Nagpur", "Aurangabad")
FLEET_OWNERS: tuple[str, ...] = ("FleetStream Logistics", "Deccan Haulage", "Sahyadri Transport")
DRIVER_CATEGORIES: tuple[str, ...] = ("senior", "standard", "probationary")
LICENSE_TYPES: tuple[str, ...] = ("LMV", "HMV", "HTV")

_FIRST_NAMES = (
    "Amit",
    "Priya",
    "Rahul",
    "Sneha",
    "Vikram",
    "Anjali",
    "Rohan",
    "Kavita",
    "Suresh",
    "Meera",
    "Arjun",
    "Divya",
    "Nikhil",
    "Pooja",
    "Sanjay",
    "Neha",
)
_LAST_NAMES = (
    "Sharma",
    "Patil",
    "Deshmukh",
    "Iyer",
    "Joshi",
    "Kulkarni",
    "Nair",
    "Reddy",
    "Gupta",
    "Bhosale",
    "Chavan",
    "Rao",
)

#: Aggression multiplier by driver category. Senior drivers speed less often, which
#: makes driver-behaviour rankings stable and explainable rather than noise.
_AGGRESSION_BY_CATEGORY = {"senior": 0.6, "standard": 1.0, "probationary": 1.6}


@dataclass(frozen=True)
class Route:
    """A planned route as an ordered list of waypoints."""

    route_id: str
    route_name: str
    waypoints: tuple[tuple[float, float], ...]
    distance_km: float


@dataclass(frozen=True)
class VehicleProfile:
    vehicle_id: str
    vehicle_type: str
    manufacturer: str
    model: str
    region: str
    fleet_owner: str
    max_speed_kmh: float
    fuel_capacity_l: float
    #: Litres per 100 km. Fuel burn is derived from distance travelled through this,
    #: so fuel_consumed stays internally consistent with distance instead of being an
    #: independent random walk that trip analytics could never reconcile.
    consumption_l_per_100km: float
    overheat_threshold_c: float = 100.0
    #: A small share of the fleet runs hot on purpose, so "which vehicles need
    #: maintenance" has a real, findable answer rather than uniform noise.
    faulty_cooling: bool = False


@dataclass(frozen=True)
class DriverProfile:
    driver_id: str
    driver_name: str
    driver_category: str
    license_type: str
    aggression: float


@dataclass(frozen=True)
class FleetReference:
    vehicles: tuple[VehicleProfile, ...]
    drivers: tuple[DriverProfile, ...]
    routes: tuple[Route, ...]
    by_vehicle: dict[str, VehicleProfile] = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "by_vehicle", {v.vehicle_id: v for v in self.vehicles})


# Waypoints trace plausible freight corridors out of Pune. Coordinates only need to
# be geographically sane, not survey-accurate.
_ROUTE_SEEDS: tuple[tuple[str, str, tuple[tuple[float, float], ...]], ...] = (
    (
        "R001",
        "Pune - Mumbai Expressway",
        (
            (18.5204, 73.8567),
            (18.6298, 73.7997),
            (18.7500, 73.4000),
            (18.9200, 73.3300),
            (19.0760, 72.8777),
        ),
    ),
    (
        "R002",
        "Pune - Nashik Highway",
        (
            (18.5204, 73.8567),
            (18.7500, 73.8800),
            (19.1000, 73.9000),
            (19.6000, 73.8500),
            (19.9975, 73.7898),
        ),
    ),
    (
        "R003",
        "Pune - Nagpur Corridor",
        (
            (18.5204, 73.8567),
            (18.9000, 74.5000),
            (19.4000, 75.6000),
            (20.2000, 77.5000),
            (21.1458, 79.0882),
        ),
    ),
    (
        "R004",
        "Pune - Aurangabad Route",
        ((18.5204, 73.8567), (18.9500, 74.1000), (19.4000, 74.6000), (19.8762, 75.3433)),
    ),
    (
        "R005",
        "Pune City Distribution Loop",
        (
            (18.5204, 73.8567),
            (18.5600, 73.9100),
            (18.5900, 73.8200),
            (18.5100, 73.7900),
            (18.5204, 73.8567),
        ),
    ),
    (
        "R006",
        "Pune - Satara Run",
        ((18.5204, 73.8567), (18.3000, 73.9000), (18.0000, 74.0000), (17.6805, 74.0183)),
    ),
)

_EARTH_RADIUS_KM = 6371.0088


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Great-circle distance in km between two ``(lat, lon)`` points."""
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * _EARTH_RADIUS_KM * math.asin(math.sqrt(h))


def interpolate(a: tuple[float, float], b: tuple[float, float], t: float) -> tuple[float, float]:
    """Linear interpolation between two points; ``t`` is clamped to [0, 1].

    Linear rather than great-circle: over the tens of kilometres between adjacent
    waypoints the difference is far below GPS noise, and it keeps positions cheap
    to compute for every event.
    """
    t = min(1.0, max(0.0, t))
    return (a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t)


def _route_distance(waypoints: tuple[tuple[float, float], ...]) -> float:
    return sum(haversine_km(waypoints[i], waypoints[i + 1]) for i in range(len(waypoints) - 1))


def build_routes() -> tuple[Route, ...]:
    return tuple(
        Route(rid, name, wps, round(_route_distance(wps), 2)) for rid, name, wps in _ROUTE_SEEDS
    )


def build_reference(
    vehicle_count: int,
    seed: int,
    hot_vehicle: str | None = None,
) -> FleetReference:
    """Generate the fleet. The same seed and count always yield the same fleet.

    ``hot_vehicle`` is appended when it falls outside the generated ID range, so the
    deliberately skewed vehicle always exists in the dimension. Without this its
    enrichment join would produce nulls, and the partition-skew demonstration would
    look like a data-quality bug instead.
    """
    rng = random.Random(seed)

    vehicles: list[VehicleProfile] = []
    for i in range(vehicle_count):
        vtype, max_speed, capacity = rng.choice(VEHICLE_TYPES)
        manufacturer, models = rng.choice(MANUFACTURERS)
        vehicles.append(
            VehicleProfile(
                vehicle_id=f"V{1000 + i}",
                vehicle_type=vtype,
                manufacturer=manufacturer,
                model=rng.choice(models),
                region=rng.choice(REGIONS),
                fleet_owner=rng.choice(FLEET_OWNERS),
                max_speed_kmh=max_speed,
                fuel_capacity_l=capacity,
                consumption_l_per_100km=round(rng.uniform(7.0, 34.0), 2),
                faulty_cooling=rng.random() < 0.06,
            )
        )

    if hot_vehicle and all(v.vehicle_id != hot_vehicle for v in vehicles):
        vtype, max_speed, capacity = VEHICLE_TYPES[1]
        manufacturer, models = MANUFACTURERS[0]
        vehicles.append(
            VehicleProfile(
                vehicle_id=hot_vehicle,
                vehicle_type=vtype,
                manufacturer=manufacturer,
                model=models[0],
                region=REGIONS[0],
                fleet_owner=FLEET_OWNERS[0],
                max_speed_kmh=max_speed,
                fuel_capacity_l=capacity,
                consumption_l_per_100km=18.0,
                faulty_cooling=True,
            )
        )

    # More drivers than vehicles: drivers rotate across vehicles, which is what makes
    # driver and vehicle analytics genuinely different questions.
    driver_count = max(1, int(len(vehicles) * 1.3))
    drivers: list[DriverProfile] = []
    for i in range(driver_count):
        category = rng.choice(DRIVER_CATEGORIES)
        drivers.append(
            DriverProfile(
                driver_id=f"D{500 + i}",
                driver_name=f"{rng.choice(_FIRST_NAMES)} {rng.choice(_LAST_NAMES)}",
                driver_category=category,
                license_type=rng.choice(LICENSE_TYPES),
                aggression=round(_AGGRESSION_BY_CATEGORY[category] * rng.uniform(0.7, 1.4), 3),
            )
        )

    return FleetReference(tuple(vehicles), tuple(drivers), build_routes())
