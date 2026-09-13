"""Load the vehicle, driver and route dimensions into Iceberg.

In a real deployment these arrive from fleet-management and HR systems via CDC.
Here they come from the same seeded generator the simulator uses, which guarantees
that every ``vehicle_id`` appearing in telemetry also exists in the dimension - so
a null after an enrichment join means a genuine bug, not a gap in the test data.

The write is a full overwrite rather than an append. These tables are small and
authoritative, the source is deterministic, and overwriting makes the job safe to
re-run any number of times without accumulating duplicate dimension rows.
"""

from __future__ import annotations

import logging
import sys

from fleetstream.common.config import Settings, get_settings
from fleetstream.common.iceberg import bootstrap
from fleetstream.common.logging import configure_logging
from fleetstream.common.spark import build_spark
from fleetstream.simulator.reference import build_reference

logger = logging.getLogger(__name__)

DIM_VEHICLE_SCHEMA = (
    "vehicle_id STRING, vehicle_type STRING, manufacturer STRING, model STRING, "
    "region STRING, fleet_owner STRING, max_speed_kmh DOUBLE, fuel_capacity_l DOUBLE, "
    "consumption_l_per_100km DOUBLE, overheat_threshold_c DOUBLE, has_faulty_cooling BOOLEAN"
)

DIM_DRIVER_SCHEMA = (
    "driver_id STRING, driver_name STRING, driver_category STRING, "
    "license_type STRING, aggression_index DOUBLE"
)

DIM_ROUTE_SCHEMA = (
    "route_id STRING, route_name STRING, distance_km DOUBLE, waypoint_count INT, "
    "origin_latitude DOUBLE, origin_longitude DOUBLE, "
    "destination_latitude DOUBLE, destination_longitude DOUBLE"
)


def seed(spark, settings: Settings) -> dict[str, int]:
    cat, ns = settings.catalog.name, settings.catalog.silver
    cfg = settings.simulator
    reference = build_reference(cfg.vehicle_count, cfg.seed, cfg.hot_vehicle)

    vehicles = spark.createDataFrame(
        [
            (
                v.vehicle_id,
                v.vehicle_type,
                v.manufacturer,
                v.model,
                v.region,
                v.fleet_owner,
                float(v.max_speed_kmh),
                float(v.fuel_capacity_l),
                float(v.consumption_l_per_100km),
                float(v.overheat_threshold_c),
                bool(v.faulty_cooling),
            )
            for v in reference.vehicles
        ],
        schema=DIM_VEHICLE_SCHEMA,
    )

    drivers = spark.createDataFrame(
        [
            (d.driver_id, d.driver_name, d.driver_category, d.license_type, float(d.aggression))
            for d in reference.drivers
        ],
        schema=DIM_DRIVER_SCHEMA,
    )

    routes = spark.createDataFrame(
        [
            (
                r.route_id,
                r.route_name,
                float(r.distance_km),
                len(r.waypoints),
                float(r.waypoints[0][0]),
                float(r.waypoints[0][1]),
                float(r.waypoints[-1][0]),
                float(r.waypoints[-1][1]),
            )
            for r in reference.routes
        ],
        schema=DIM_ROUTE_SCHEMA,
    )

    counts: dict[str, int] = {}
    for name, df in (("dim_vehicle", vehicles), ("dim_driver", drivers), ("dim_route", routes)):
        table = f"{cat}.{ns}.{name}"
        df.writeTo(table).using("iceberg").createOrReplace()
        counts[name] = df.count()
        logger.info("wrote %d rows to %s", counts[name], table)
    return counts


def main() -> int:
    configure_logging()
    settings = get_settings()
    spark = build_spark("fleetstream-seed-dimensions", settings)
    try:
        bootstrap(spark, settings)
        counts = seed(spark, settings)
        logger.info("dimensions seeded: %s", counts)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
