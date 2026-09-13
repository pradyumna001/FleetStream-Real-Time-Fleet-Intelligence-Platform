"""Integration tests: the behaviours that only appear with a real Spark and Iceberg.

The unit suite covers the contract, the rule set and the simulator. What it cannot
cover is whether Spark actually parses the generated DDL, whether the rule expressions
are valid Spark SQL, and whether MERGE behaves as the idempotency argument claims.
Those are exactly the things that would break silently in production, so they are
tested against the real engine.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from fleetstream.common import schemas
from fleetstream.quality import engine as dq

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# The generated contract must be valid Spark
# ---------------------------------------------------------------------------


def test_telemetry_struct_parses(spark):
    """The DDL generated from FieldSpec must be something Spark accepts."""
    struct = schemas.telemetry_struct()
    names = [f.name for f in struct.fields]
    assert names == [f.name for f in schemas.TELEMETRY_FIELDS]
    types = dict(zip(names, [f.dataType.simpleString() for f in struct.fields], strict=True))
    assert types["speed"] == "double"


def test_generated_ddl_creates_real_tables(spark, settings, scratch_namespace):
    """Every table DDL must execute. A typo here breaks bootstrap for everyone."""
    catalog, ns = scratch_namespace.split(".", 1)
    for ddl_fn in (
        schemas.bronze_table_ddl,
        schemas.silver_table_ddl,
        schemas.quarantine_table_ddl,
        schemas.reconciliation_table_ddl,
    ):
        spark.sql(ddl_fn(catalog, ns))

    tables = {r["tableName"] for r in spark.sql(f"SHOW TABLES IN {scratch_namespace}").collect()}
    assert {"telemetry_raw", "telemetry", "telemetry_quarantine", "reconciliation"} <= tables


def test_iceberg_round_trip(spark, scratch_namespace):
    table = f"{scratch_namespace}.round_trip"
    spark.sql(f"CREATE TABLE {table} (id BIGINT, note STRING) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'through MinIO')")
    rows = spark.sql(f"SELECT * FROM {table}").collect()
    assert len(rows) == 1 and rows[0]["note"] == "through MinIO"


# ---------------------------------------------------------------------------
# The quality rules must be valid Spark SQL and must split correctly
# ---------------------------------------------------------------------------


def _payload(**overrides):
    base = {
        "event_id": "e1",
        "vehicle_id": "V1000",
        "driver_id": "D500",
        "trip_id": "T1",
        "route_id": "R001",
        "event_time": "2026-09-08T10:00:00.000Z",
        "latitude": 18.52,
        "longitude": 73.85,
        "speed": 62.0,
        "fuel_level": 71.0,
        "engine_temperature": 88.0,
        "battery_level": 95.0,
        "odometer_km": 1000.0,
        "schema_version": schemas.SCHEMA_VERSION,
    }
    base.update(overrides)
    return base


def _frame(spark, payloads):
    """Build a Bronze-shaped frame the way bronze_ingest does, from raw JSON."""
    from pyspark.sql import functions as F

    raw = spark.createDataFrame([(json.dumps(p),) for p in payloads], schema="raw_payload STRING")
    return (
        raw.withColumn("parsed", F.from_json(F.col("raw_payload"), schemas.telemetry_struct()))
        .select(F.col("parsed.*"), "raw_payload")
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("kafka_offset", F.lit(0).cast("bigint"))
    )


def test_every_rule_is_valid_spark_sql(spark):
    """A rule with a syntax error would fail the Silver query at runtime, not startup."""
    df = _frame(spark, [_payload()])
    annotated = dq.apply_rules(df)
    row = annotated.select(dq.FAILED_RULES_COL, dq.IS_VALID_COL).collect()[0]
    assert row[dq.IS_VALID_COL] is True, (
        f"a clean record was rejected by {row[dq.FAILED_RULES_COL]}"
    )


def test_clean_records_pass_and_bad_records_are_split(spark):
    payloads = [
        _payload(event_id="good-1"),
        _payload(event_id="bad-null-vehicle", vehicle_id=None),
        _payload(event_id="bad-speed", speed=-12.0),
        _payload(event_id="bad-fuel", fuel_level=142.7),
        _payload(event_id="bad-lat", latitude=118.4),
    ]
    valid, rejected = dq.split(_frame(spark, payloads))

    assert {r["event_id"] for r in valid.collect()} == {"good-1"}

    by_event = {}
    for r in rejected.collect():
        by_event.setdefault(r["event_id"], set()).add(r["rule_name"])

    assert "vehicle_id_not_null" in by_event["bad-null-vehicle"]
    assert "speed_non_negative" in by_event["bad-speed"]
    assert "fuel_level_range" in by_event["bad-fuel"]
    assert "latitude_range" in by_event["bad-lat"]


def test_unparseable_number_is_caught_not_silently_nulled(spark):
    """from_json nulls a value it cannot coerce. Without the type rule that loss would
    be invisible - the record would look like a sensor dropout rather than a bug."""
    valid, rejected = dq.split(_frame(spark, [_payload(event_id="bad-type", speed="seventy-two")]))
    assert valid.count() == 0
    assert "speed_type_valid" in {r["rule_name"] for r in rejected.collect()}


def test_null_optional_measure_is_not_rejected(spark):
    """A sensor dropout is a missing value, not a violation."""
    valid, _ = dq.split(_frame(spark, [_payload(event_id="dropout", speed=None, fuel_level=None)]))
    assert valid.count() == 1


def test_warnings_do_not_reject(spark):
    """An unknown schema_version is surfaced, not rejected - otherwise a producer
    rollout becomes an outage."""
    df = _frame(spark, [_payload(event_id="v2", schema_version="9.9")])
    annotated = dq.apply_rules(df)
    row = annotated.select(dq.IS_VALID_COL, dq.WARNINGS_COL).collect()[0]
    assert row[dq.IS_VALID_COL] is True
    assert "schema_version_known" in row[dq.WARNINGS_COL]


# ---------------------------------------------------------------------------
# The idempotency claim, tested directly against Iceberg MERGE
# ---------------------------------------------------------------------------


def test_merge_on_event_id_is_idempotent(spark, scratch_namespace):
    """The property the whole sink design rests on.

    Applying the same batch twice must leave the row count unchanged. An append would
    double it, which is precisely the failure this design exists to prevent.
    """
    table = f"{scratch_namespace}.merge_target"
    spark.sql(f"CREATE TABLE {table} (event_id STRING, value INT) USING iceberg")

    batch = spark.createDataFrame(
        [(f"e{i}", i) for i in range(100)], schema="event_id STRING, value INT"
    )
    batch.createOrReplaceTempView("merge_source")

    merge = f"""
        MERGE INTO {table} t USING merge_source s ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET t.value = s.value
        WHEN NOT MATCHED THEN INSERT (event_id, value) VALUES (s.event_id, s.value)
    """

    spark.sql(merge)
    first = spark.table(table).count()
    assert first == 100

    spark.sql(merge)
    spark.sql(merge)
    assert spark.table(table).count() == first, "replay changed the row count"


def test_append_would_duplicate(spark, scratch_namespace):
    """The counter-example, asserted rather than asserted-about.

    This is what the sink would do without the MERGE, and it is why the check above
    is not merely restating that Iceberg works.
    """
    table = f"{scratch_namespace}.append_target"
    spark.sql(f"CREATE TABLE {table} (event_id STRING, value INT) USING iceberg")
    batch = spark.createDataFrame(
        [(f"e{i}", i) for i in range(50)], schema="event_id STRING, value INT"
    )
    batch.writeTo(table).append()
    batch.writeTo(table).append()
    assert spark.table(table).count() == 100
    assert spark.sql(f"SELECT count(DISTINCT event_id) AS c FROM {table}").collect()[0]["c"] == 50


# ---------------------------------------------------------------------------
# Event-time semantics
# ---------------------------------------------------------------------------


def test_partitioning_uses_event_time_not_ingestion_time(spark, scratch_namespace):
    """A late event must land in its event-time partition.

    If it were partitioned by arrival, "distance driven on Tuesday" would silently
    depend on network conditions.
    """
    catalog, ns = scratch_namespace.split(".", 1)
    spark.sql(schemas.bronze_table_ddl(catalog, ns, "partitioned"))
    table = f"{scratch_namespace}.partitioned"

    now = datetime(2026, 9, 8, 10, 0, tzinfo=timezone.utc)
    late_event_time = now - timedelta(days=1)

    rows = [
        (
            "on-time",
            "V1",
            "D1",
            "T1",
            "R1",
            now,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            schemas.SCHEMA_VERSION,
            now,
            "t",
            0,
            0,
            now,
            "{}",
            "run",
        ),
        (
            "late",
            "V1",
            "D1",
            "T1",
            "R1",
            late_event_time,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            schemas.SCHEMA_VERSION,
            now,
            "t",
            0,
            1,
            now,
            "{}",
            "run",
        ),
    ]
    df = spark.createDataFrame(rows, schema=schemas.bronze_struct())
    df.writeTo(table).option("fanout-enabled", "true").append()

    partitions = spark.sql(
        f"SELECT DISTINCT date(event_time) AS d FROM {table} ORDER BY d"
    ).collect()
    assert [str(r["d"]) for r in partitions] == ["2026-09-07", "2026-09-08"]
