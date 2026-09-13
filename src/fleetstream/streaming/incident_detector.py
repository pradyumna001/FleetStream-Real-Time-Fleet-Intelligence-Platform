"""Real-time incident detection: Kafka to Kafka.

The analytical path (Bronze, Silver, Gold) answers questions about what happened.
It is not the right place to raise an alert: it batches, it validates, it merges,
and by the time an overheating engine appears in a Gold table the truck has been
running hot for minutes.

So detection reads the telemetry topic directly and publishes to
``fleet.incidents``, from which an operations service can page a dispatcher or open
a maintenance ticket. It is intentionally simple - a stateless filter with a short
trigger - because latency is the only thing it optimises for. Correctness,
deduplication and history are the analytical path's job, and the same event is
still processed there independently.

Publishing to Kafka rather than writing a table keeps the platform decoupled: the
consumer of an incident is an operational system, not an analyst.
"""

from __future__ import annotations

import logging
import sys

from fleetstream.common.config import get_settings
from fleetstream.common.logging import configure_logging
from fleetstream.common.schemas import telemetry_struct
from fleetstream.common.spark import build_spark

logger = logging.getLogger(__name__)

JOB_NAME = "incident-detector"
#: Short, because this path exists for latency. Bronze can afford 30s; an alert cannot.
TRIGGER_INTERVAL = "5 seconds"

OVERHEAT_THRESHOLD_C = 100.0
CRITICAL_OVERHEAT_THRESHOLD_C = 115.0
LOW_FUEL_THRESHOLD_PCT = 10.0
SEVERE_OVERSPEED_KMH = 140.0
LOW_BATTERY_THRESHOLD_PCT = 20.0


def detect(parsed):
    """Classify readings into incidents. Rows that are not incidents are filtered out."""
    from pyspark.sql import functions as F

    incident_type = (
        F.when(
            F.col("engine_temperature") > CRITICAL_OVERHEAT_THRESHOLD_C, "engine_critical_overheat"
        )
        .when(F.col("engine_temperature") > OVERHEAT_THRESHOLD_C, "engine_overheat")
        .when(F.col("speed") > SEVERE_OVERSPEED_KMH, "severe_overspeed")
        .when(F.col("fuel_level") < LOW_FUEL_THRESHOLD_PCT, "low_fuel")
        .when(F.col("battery_level") < LOW_BATTERY_THRESHOLD_PCT, "low_battery")
        .otherwise(F.lit(None))
    )

    severity = (
        F.when(F.col("incident_type").isin("engine_critical_overheat"), "critical")
        .when(F.col("incident_type").isin("engine_overheat", "severe_overspeed"), "high")
        .otherwise("medium")
    )

    return (
        parsed.withColumn("incident_type", incident_type)
        .filter(F.col("incident_type").isNotNull())
        .withColumn("severity", severity)
        .withColumn("detected_at", F.current_timestamp())
    )


def main() -> int:
    configure_logging()
    settings = get_settings()
    spark = build_spark(f"fleetstream-{JOB_NAME}", settings)

    from pyspark.sql import functions as F

    try:
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
            .option("subscribe", settings.kafka.telemetry_topic)
            # Alerting on a backlog of hours-old readings would page people about
            # problems that have already passed, so this path starts at the live
            # edge rather than replaying history.
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed = raw.select(
            F.from_json(F.col("value").cast("string"), telemetry_struct()).alias("t")
        ).select("t.*")

        incidents = detect(parsed)

        payload = F.to_json(
            F.struct(
                F.col("event_id"),
                F.col("vehicle_id"),
                F.col("driver_id"),
                F.col("trip_id"),
                F.col("event_time"),
                F.col("incident_type"),
                F.col("severity"),
                F.col("speed"),
                F.col("engine_temperature"),
                F.col("fuel_level"),
                F.col("battery_level"),
                F.col("latitude"),
                F.col("longitude"),
                F.col("detected_at"),
            )
        )

        query = (
            incidents.select(
                # Keyed by vehicle so a consumer sees one vehicle's incidents in order.
                F.col("vehicle_id").cast("string").alias("key"),
                payload.alias("value"),
            )
            .writeStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
            .option("topic", settings.kafka.incidents_topic)
            .option("checkpointLocation", settings.storage.checkpoint_uri(JOB_NAME))
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )

        logger.info(
            "detecting incidents on %s -> %s",
            settings.kafka.telemetry_topic,
            settings.kafka.incidents_topic,
        )
        query.awaitTermination()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
