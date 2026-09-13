"""Bronze to Silver: validate, deduplicate, enrich — idempotently.

This is where the platform's correctness claims are actually implemented.

WATERMARK AND DEDUPLICATION
    A 10-minute watermark bounds how long deduplication state is retained. Without
    it, the set of seen ``event_id`` values would grow without limit until the job
    died. ``dropDuplicatesWithinWatermark`` removes repeats that arrive inside that
    window.

WHY THE WATERMARK IS NOT ENOUGH
    Dedup state expires with the watermark. An event replayed an hour later - after
    a Kafka reset, a backfill, or a checkpoint rollback - is no longer remembered
    and would be written a second time. Structured Streaming's checkpoint does not
    prevent this either: it guarantees the *source* is re-read consistently, not
    that the *sink* applied each record once.

    So the sink is made idempotent instead. Each micro-batch is applied with
    ``MERGE INTO ... ON event_id``, which is why ``event_id`` is derived
    deterministically at the source rather than randomly generated. Re-processing
    the same events updates rows in place instead of appending duplicates, and the
    row count does not move. The verification suite asserts exactly this by
    replaying a batch.

    Watermark + MERGE are complementary: the watermark keeps the common case cheap,
    the MERGE makes correctness permanent.

QUARANTINE
    Invalid records are diverted with the rule they broke, the reason, and the raw
    payload, rather than being dropped or allowed to poison the table.
"""

from __future__ import annotations

import logging
import sys
import uuid

from fleetstream.common.config import Settings, get_settings
from fleetstream.common.iceberg import (
    bootstrap,
    bronze_table,
    quarantine_table,
    silver_table,
)
from fleetstream.common.logging import configure_logging
from fleetstream.common.spark import build_spark
from fleetstream.quality import engine as dq
from fleetstream.quality.reconciliation import Counts, record

logger = logging.getLogger(__name__)

JOB_NAME = "silver-transform"
TRIGGER_INTERVAL = "60 seconds"

#: How much event-time lateness deduplication state is retained for. Sized from the
#: simulator's injected lateness (5-20 minutes); in production it would come from
#: measured p99 arrival delay, not a guess.
WATERMARK_DELAY = "10 minutes"

LOW_FUEL_THRESHOLD = 15.0
OVERHEAT_THRESHOLD = 100.0
#: Fraction of the vehicle's rated maximum above which a reading counts as speeding.
OVERSPEED_RATIO = 1.0


def load_dimensions(spark, settings: Settings):
    """Load the vehicle and driver dimensions for enrichment.

    Both are small - hundreds of rows - so they are broadcast. Broadcasting turns
    the enrichment into a map-side join with no shuffle at all, which matters when
    it runs once per micro-batch forever.
    """
    from pyspark.sql import functions as F

    cat, ns = settings.catalog.name, settings.catalog.silver

    vehicles = F.broadcast(
        spark.table(f"{cat}.{ns}.dim_vehicle").select(
            "vehicle_id",
            "vehicle_type",
            "manufacturer",
            "model",
            "region",
            "fleet_owner",
            "max_speed_kmh",
        )
    )
    drivers = F.broadcast(
        spark.table(f"{cat}.{ns}.dim_driver").select(
            "driver_id",
            "driver_name",
            "driver_category",
        )
    )
    return vehicles, drivers


def build_stream(spark, settings: Settings):
    """Stream Bronze, applying the watermark and deduplication."""
    source = (
        spark.readStream.format("iceberg")
        # Scheduled compaction rewrites Bronze files, producing a snapshot the
        # streaming reader would otherwise refuse to read. Skipping those snapshots
        # lets maintenance run without killing this query - the data they contain
        # was already consumed from the append snapshots they replaced.
        .option("streaming-skip-overwrite-snapshots", "true")
        .option("streaming-skip-delete-snapshots", "true")
        # Bound the per-batch work so a restart after downtime catches up in steady
        # increments instead of attempting the whole backlog at once.
        .option("streaming-max-files-per-micro-batch", 64)
        .load(bronze_table(settings))
    )

    return (
        source.withWatermark("event_time", WATERMARK_DELAY)
        # Cross-batch deduplication, bounded by the watermark. Repeats arriving
        # later than that are caught by the MERGE in the sink instead.
        .dropDuplicatesWithinWatermark(["event_id"])
    )


def enrich(batch_df, vehicles, drivers):
    """Join reference data and derive the operational flags.

    Left joins on purpose: a vehicle missing from the dimension must not make its
    telemetry disappear. The reading is still real, and losing it would be a far
    worse outcome than a null vehicle_type.
    """
    from pyspark.sql import functions as F

    enriched = batch_df.join(vehicles, on="vehicle_id", how="left").join(
        drivers, on="driver_id", how="left"
    )

    return (
        enriched.withColumn(
            "is_overspeed",
            F.when(
                F.col("speed").isNotNull() & F.col("max_speed_kmh").isNotNull(),
                F.col("speed") > F.col("max_speed_kmh") * F.lit(OVERSPEED_RATIO),
            ).otherwise(F.lit(None).cast("boolean")),
        )
        .withColumn(
            "is_overheating",
            F.when(
                F.col("engine_temperature").isNotNull(),
                F.col("engine_temperature") > F.lit(OVERHEAT_THRESHOLD),
            ).otherwise(F.lit(None).cast("boolean")),
        )
        .withColumn(
            "is_low_fuel",
            F.when(
                F.col("fuel_level").isNotNull(),
                F.col("fuel_level") < F.lit(LOW_FUEL_THRESHOLD),
            ).otherwise(F.lit(None).cast("boolean")),
        )
        .drop("max_speed_kmh")
    )


def _dedupe_within_batch(batch_df):
    """Keep one row per event_id within the batch.

    Iceberg's MERGE aborts when several source rows match the same target row, so a
    duplicate surviving into a single batch would fail the write outright. The
    streaming dedup should already have removed them; this makes the guarantee local
    to the write rather than dependent on upstream state being intact.

    The newest ingestion wins, which is also the right answer for a corrected replay.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ordering = Window.partitionBy("event_id").orderBy(
        F.col("ingestion_time").desc(), F.col("kafka_offset").desc()
    )
    return (
        batch_df.withColumn("_rn", F.row_number().over(ordering))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def write_batch(
    batch_df,
    batch_id: int,
    settings: Settings,
    run_id: str,
) -> None:
    """Apply one micro-batch: split, enrich, MERGE, quarantine, reconcile.

    Everything here runs against ``batch_df.sparkSession`` rather than the session
    that started the query. foreachBatch hands over a DataFrame belonging to a CLONED
    session, and two things break if that is ignored:

    * temp views are per-session, so a view registered on the clone is invisible to
      the original session and the MERGE fails with TABLE_OR_VIEW_NOT_FOUND;
    * DataFrames built from a different session cannot be reliably joined to the
      batch, which is why the dimensions are loaded here rather than passed in.

    Re-reading the dimensions each batch costs little - they are a few hundred rows,
    broadcast - and it means a dimension update takes effect without restarting the
    query.
    """
    from pyspark.sql import functions as F

    target = silver_table(settings)
    quarantine = quarantine_table(settings)
    session = batch_df.sparkSession
    vehicles, drivers = load_dimensions(session, settings)

    batch_df = batch_df.persist()
    try:
        source_count = batch_df.count()
        if source_count == 0:
            logger.info("batch %d is empty, nothing to do", batch_id)
            return

        valid, rejected = dq.split(batch_df)

        # ---- quarantine -------------------------------------------------
        rejected = rejected.persist()
        rejected_events = rejected.select("event_id").distinct().count()
        if rejected_events:
            (
                rejected.select(
                    F.sha2(
                        F.concat_ws(
                            "|", F.coalesce(F.col("event_id"), F.lit("")), F.col("rule_name")
                        ),
                        256,
                    ).alias("quarantine_id"),
                    "event_id",
                    "vehicle_id",
                    "event_time",
                    "rule_name",
                    "rule_severity",
                    "failure_reason",
                    "raw_payload",
                    F.current_timestamp().alias("quarantined_at"),
                    F.lit(run_id).alias("run_id"),
                )
                .writeTo(quarantine)
                .option("fanout-enabled", "true")
                .append()
            )
            logger.info(
                "batch %d quarantined %d records (%d rule violations)",
                batch_id,
                rejected_events,
                rejected.count(),
            )

        # ---- enrich and merge -------------------------------------------
        enriched = (
            enrich(valid, vehicles, drivers)
            .withColumn("silver_run_id", F.lit(run_id))
            # Stamped here rather than at read time so that any caller producing a
            # Silver row - the stream or a replay - goes through the identical path.
            .withColumn("processed_at", F.current_timestamp())
        )
        deduped = _dedupe_within_batch(enriched)

        # Align to the target's column order so MERGE's INSERT */UPDATE * are
        # unambiguous. Reading the column list from the table rather than repeating
        # it here means adding a Silver column cannot silently break the merge.
        target_columns = session.table(target).columns
        prepared = deduped.select(*[F.col(c) for c in target_columns])

        # Sanitised: a hyphen is illegal in a view identifier, and callers outside
        # the streaming loop (the replay in the verification suite) pass negative
        # batch ids. "n" rather than dropping the sign so -1 and 1 stay distinct.
        view = f"silver_updates_{str(batch_id).replace('-', 'n')}"
        prepared.createOrReplaceTempView(view)

        assignments = ", ".join(f"t.{c} = s.{c}" for c in target_columns)
        columns = ", ".join(target_columns)
        values = ", ".join(f"s.{c}" for c in target_columns)

        # The idempotency guarantee. Re-running this batch updates the same rows
        # instead of inserting new ones, so the row count is unchanged.
        session.sql(
            f"""
            MERGE INTO {target} t
            USING {view} s
            ON t.event_id = s.event_id
            WHEN MATCHED THEN UPDATE SET {assignments}
            WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})
            """
        )
        output_count = prepared.count()

        record(
            session,
            Counts(
                run_id=run_id,
                stage="silver",
                batch_id=batch_id,
                source_count=source_count,
                duplicate_count=source_count - rejected_events - output_count,
                rejected_count=rejected_events,
                output_count=output_count,
            ),
            settings,
        )

        session.catalog.dropTempView(view)
        logger.info(
            "batch %d merged %d rows into %s (%d quarantined)",
            batch_id,
            output_count,
            target,
            rejected_events,
        )
    finally:
        batch_df.unpersist()


def main() -> int:
    configure_logging()
    settings = get_settings()
    run_id = str(uuid.uuid4())

    spark = build_spark(f"fleetstream-{JOB_NAME}", settings)

    try:
        bootstrap(spark, settings)
        logger.info("run_id=%s\n%s", run_id, dq.rule_summary())

        stream = build_stream(spark, settings)

        query = (
            stream.writeStream.outputMode("append")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .option("checkpointLocation", settings.storage.checkpoint_uri(JOB_NAME))
            .foreachBatch(lambda df, bid: write_batch(df, bid, settings, run_id))
            .start()
        )

        query.awaitTermination()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
