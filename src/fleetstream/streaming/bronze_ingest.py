"""Kafka to Iceberg Bronze: raw, traceable, replayable.

Bronze does as little as possible. It parses the JSON payload into typed columns
for convenience, but it keeps the verbatim payload alongside them, so a parsing bug
discovered next month can still be fixed by reprocessing rather than by apologising
for lost data. Kafka topic, partition and offset are carried through as well, which
makes any row traceable back to its exact position in the log.

APPEND-ONLY - THIS IS LOAD-BEARING
    The Silver job consumes this table as a *stream*. Iceberg's streaming source
    reads snapshots in order and aborts when it meets one produced by an overwrite,
    delete or MERGE. So Bronze must only ever be appended to. Compaction rewrites
    files and creates exactly such a snapshot, which is why it is confined to the
    maintenance DAG, and why the Silver reader sets
    ``streaming-skip-overwrite-snapshots`` so scheduled maintenance does not kill
    the stream.

No validation happens here. A record that fails every quality rule is still written
to Bronze, because the raw layer's job is to preserve what arrived, not to judge it.
Validation and the quarantine split belong to Silver.
"""

from __future__ import annotations

import logging
import sys
import uuid

from fleetstream.common.config import Settings, get_settings
from fleetstream.common.iceberg import bootstrap, bronze_table
from fleetstream.common.logging import configure_logging
from fleetstream.common.schemas import telemetry_struct
from fleetstream.common.spark import build_spark

logger = logging.getLogger(__name__)

JOB_NAME = "bronze-ingest"
#: Micro-batch interval. Long enough that each batch writes reasonably sized
#: Parquet files rather than a flood of tiny ones; short enough to stay "real time"
#: for monitoring. The small-file problem is managed here first and compacted second.
TRIGGER_INTERVAL = "30 seconds"


def build_stream(spark, settings: Settings, run_id: str):
    """Read Kafka and shape it into the Bronze schema."""
    from pyspark.sql import functions as F

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", settings.kafka.telemetry_topic)
        # earliest so a fresh deployment picks up everything already produced.
        # The checkpoint takes precedence on restart, so this only applies to a
        # genuinely new query, never to a resumed one.
        .option("startingOffsets", "earliest")
        # Bound the work per micro-batch. Without this, a query restarting after a
        # long outage tries to process the entire backlog in a single batch and
        # usually dies of memory exhaustion - the classic "it never catches up".
        .option("maxOffsetsPerTrigger", 200_000)
        # A lost/compacted offset should be loud, not silently skipped.
        .option("failOnDataLoss", "true")
        .load()
    )

    payload = F.col("value").cast("string")

    return (
        raw.select(
            payload.alias("raw_payload"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        # PERMISSIVE (the default): a record whose JSON is entirely unparseable
        # yields nulls rather than killing the batch. It still lands in Bronze with
        # its raw payload intact, and Silver's rules reject it with a reason.
        .withColumn("parsed", F.from_json(F.col("raw_payload"), telemetry_struct()))
        .select(
            F.col("parsed.*"),
            F.current_timestamp().alias("ingestion_time"),
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "raw_payload",
            F.lit(run_id).alias("ingest_run_id"),
        )
    )


def main() -> int:
    configure_logging()
    settings = get_settings()
    run_id = str(uuid.uuid4())

    spark = build_spark(
        f"fleetstream-{JOB_NAME}",
        settings,
        # One shuffle partition per Kafka partition: this stage is a straight
        # projection with no wide dependency, so more would only create small files.
        **{"spark.sql.shuffle.partitions": str(settings.kafka.partitions)},
    )

    try:
        bootstrap(spark, settings)
        target = bronze_table(settings)
        stream = build_stream(spark, settings, run_id)

        logger.info("run_id=%s writing %s -> %s", run_id, settings.kafka.telemetry_topic, target)

        query = (
            stream.writeStream.format("iceberg")
            .outputMode("append")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .option("checkpointLocation", settings.storage.checkpoint_uri(JOB_NAME))
            # fanout-enabled lets one task write to several date partitions without
            # requiring the input to be sorted by partition first. Late events mean
            # a single batch routinely spans multiple days, and without this the
            # write fails on unsorted partition values.
            .option("fanout-enabled", "true")
            .toTable(target)
        )

        query.awaitTermination()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
