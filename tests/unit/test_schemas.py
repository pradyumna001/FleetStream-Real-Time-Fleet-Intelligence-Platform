"""The event contract must stay internally consistent across its representations."""

from __future__ import annotations

import pytest

from fleetstream.common import schemas


def test_field_names_are_unique():
    names = [f.name for f in schemas.TELEMETRY_FIELDS]
    assert len(names) == len(set(names))


def test_bronze_metadata_does_not_collide_with_payload_fields():
    """A metadata column shadowing a payload column would silently overwrite source
    data during ingestion."""
    payload = {f.name for f in schemas.TELEMETRY_FIELDS}
    metadata = {f.name for f in schemas.BRONZE_METADATA_FIELDS}
    assert payload & metadata == set()


def test_required_fields_are_the_identity_and_time_anchors():
    required = {f.name for f in schemas.TELEMETRY_FIELDS if f.required}
    assert required == {
        "event_id",
        "vehicle_id",
        "driver_id",
        "trip_id",
        "event_time",
        "schema_version",
    }


def test_json_schema_matches_the_contract():
    js = schemas.telemetry_json_schema()
    assert js["additionalProperties"] is False
    assert set(js["properties"]) == {f.name for f in schemas.TELEMETRY_FIELDS}
    assert set(js["required"]) == {f.name for f in schemas.TELEMETRY_FIELDS if f.required}
    assert js["properties"]["event_time"]["format"] == "date-time"


def test_optional_numeric_fields_admit_null_in_json_schema():
    """Optional readings must be expressible as null, or a sensor dropout would be
    a schema violation rather than a missing value."""
    for name in ("speed", "fuel_level", "latitude", "battery_level"):
        assert "null" in schemas.telemetry_json_schema()["properties"][name]["type"]


@pytest.mark.parametrize(
    "ddl_fn,table",
    [
        (schemas.bronze_table_ddl, "telemetry_raw"),
        (schemas.silver_table_ddl, "telemetry"),
        (schemas.quarantine_table_ddl, "telemetry_quarantine"),
        (schemas.reconciliation_table_ddl, "reconciliation"),
    ],
)
def test_ddl_is_spark_typed_and_idempotent(ddl_fn, table):
    """DDL is executed by Spark, so it must use Spark type names, and every job may
    run it at startup, so it must be safe to re-run."""
    ddl = ddl_fn("fleetstream", "ns", table)
    assert "CREATE TABLE IF NOT EXISTS fleetstream.ns." + table in ddl
    assert "USING iceberg" in ddl
    # Trino spellings must not leak into statements Spark will parse.
    assert "TIMESTAMP(6) WITH TIME ZONE" not in ddl
    assert "VARCHAR" not in ddl


def test_bronze_is_partitioned_by_event_time_not_vehicle():
    """Partitioning by vehicle_id would create one partition per vehicle - thousands
    of tiny partitions and unusable metadata."""
    ddl = schemas.bronze_table_ddl("c", "bronze")
    assert "PARTITIONED BY (days(event_time))" in ddl
    assert "vehicle_id)" not in ddl.split("PARTITIONED BY")[1].split(")")[0] + ")"


def test_bronze_retains_raw_payload_and_kafka_coordinates():
    """Provenance is what makes replay and backfill possible."""
    ddl = schemas.bronze_table_ddl("c", "bronze")
    for column in ("raw_payload", "kafka_partition", "kafka_offset", "ingestion_time"):
        assert column in ddl


def test_silver_drops_raw_payload_but_quarantine_keeps_it():
    """Silver is the cleaned layer; the quarantine table must keep the original so a
    rejected record can be corrected and replayed."""
    assert "raw_payload" not in schemas.silver_table_ddl("c", "silver")
    assert "raw_payload" in schemas.quarantine_table_ddl("c", "silver")


def test_silver_uses_merge_on_read():
    """Silver is written by MERGE on every micro-batch; copy-on-write would rewrite
    whole files each time."""
    ddl = schemas.silver_table_ddl("c", "silver")
    assert "'write.merge.mode'" in ddl
    assert "merge-on-read" in ddl


def test_quarantine_records_rule_reason_and_run():
    ddl = schemas.quarantine_table_ddl("c", "silver")
    for column in ("rule_name", "failure_reason", "run_id", "quarantined_at"):
        assert column in ddl
