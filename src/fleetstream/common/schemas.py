"""The telemetry event contract — the single source of truth for the platform.

One list of :class:`FieldSpec` drives everything downstream:

* the Spark ``StructType`` used to parse Kafka payloads,
* the JSON Schema the producer validates against,
* the Iceberg DDL for the Bronze, Silver and quarantine tables.

They are generated rather than hand-maintained so they cannot drift apart. A
field added here appears in every layer at once, which is what makes the schema
evolution story in the design document actually true rather than aspirational.

PySpark is imported lazily inside :func:`telemetry_struct` so this module stays
importable on the host interpreter (Python 3.13, no PySpark) for unit tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql.types import StructType

#: Bumped whenever the contract changes in a way consumers must notice.
#: Carried on every event so Bronze records remain interpretable after evolution.
SCHEMA_VERSION = "1.0"


@dataclass(frozen=True)
class FieldSpec:
    """One field of the telemetry contract, expressed once for every target system."""

    name: str
    spark_type: str
    sql_type: str
    json_type: str | list[str]
    required: bool
    description: str

    @property
    def spark_ddl(self) -> str:
        return f"{self.name} {self.spark_type}"


# ---------------------------------------------------------------------------
# The contract
# ---------------------------------------------------------------------------

TELEMETRY_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec(
        "event_id",
        "STRING",
        "VARCHAR",
        "string",
        True,
        "Globally unique, deterministic event identifier. The idempotency key for the "
        "whole platform: Silver MERGEs on it, so a replay cannot create duplicates.",
    ),
    FieldSpec(
        "vehicle_id",
        "STRING",
        "VARCHAR",
        "string",
        True,
        "Vehicle emitting the event; also the Kafka partition key.",
    ),
    FieldSpec(
        "driver_id",
        "STRING",
        "VARCHAR",
        "string",
        True,
        "Driver operating the vehicle at event time.",
    ),
    FieldSpec("trip_id", "STRING", "VARCHAR", "string", True, "Trip this event belongs to."),
    FieldSpec(
        "route_id",
        "STRING",
        "VARCHAR",
        "string",
        False,
        "Planned route, when the trip follows one.",
    ),
    FieldSpec(
        "event_time",
        "TIMESTAMP",
        "TIMESTAMP(6) WITH TIME ZONE",
        "string",
        True,
        "When the event actually occurred on the vehicle. Drives watermarking and all "
        "date partitioning. Never to be confused with ingestion_time.",
    ),
    FieldSpec(
        "latitude", "DOUBLE", "DOUBLE", ["number", "null"], False, "WGS84 latitude, -90..90."
    ),
    FieldSpec(
        "longitude", "DOUBLE", "DOUBLE", ["number", "null"], False, "WGS84 longitude, -180..180."
    ),
    FieldSpec("speed", "DOUBLE", "DOUBLE", ["number", "null"], False, "Ground speed in km/h."),
    FieldSpec(
        "fuel_level",
        "DOUBLE",
        "DOUBLE",
        ["number", "null"],
        False,
        "Fuel remaining as a percentage, 0..100.",
    ),
    FieldSpec(
        "engine_temperature",
        "DOUBLE",
        "DOUBLE",
        ["number", "null"],
        False,
        "Engine coolant temperature in Celsius.",
    ),
    FieldSpec(
        "battery_level",
        "DOUBLE",
        "DOUBLE",
        ["number", "null"],
        False,
        "Battery charge as a percentage, 0..100.",
    ),
    FieldSpec(
        "odometer_km",
        "DOUBLE",
        "DOUBLE",
        ["number", "null"],
        False,
        "Lifetime distance in km; monotonically non-decreasing per vehicle.",
    ),
    FieldSpec(
        "schema_version",
        "STRING",
        "VARCHAR",
        "string",
        True,
        "Contract version the producer emitted against.",
    ),
)

TELEMETRY_FIELDS_BY_NAME: dict[str, FieldSpec] = {f.name: f for f in TELEMETRY_FIELDS}

#: Columns Bronze adds on top of the source contract. These carry the provenance
#: that makes raw data replayable and auditable.
BRONZE_METADATA_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec(
        "ingestion_time",
        "TIMESTAMP",
        "TIMESTAMP(6) WITH TIME ZONE",
        "string",
        True,
        "When the platform received the event.",
    ),
    FieldSpec("kafka_topic", "STRING", "VARCHAR", "string", True, "Source topic."),
    FieldSpec(
        "kafka_partition",
        "INT",
        "INTEGER",
        "integer",
        True,
        "Source partition; makes skew analysable from the table itself.",
    ),
    FieldSpec(
        "kafka_offset",
        "BIGINT",
        "BIGINT",
        "integer",
        True,
        "Source offset, for exact replay positioning.",
    ),
    FieldSpec(
        "kafka_timestamp",
        "TIMESTAMP",
        "TIMESTAMP(6) WITH TIME ZONE",
        "string",
        True,
        "Broker append time.",
    ),
    FieldSpec(
        "raw_payload",
        "STRING",
        "VARCHAR",
        "string",
        True,
        "Verbatim source bytes as UTF-8. Retained so a parsing bug is always recoverable.",
    ),
    FieldSpec(
        "ingest_run_id",
        "STRING",
        "VARCHAR",
        "string",
        True,
        "Streaming query run that wrote the row.",
    ),
)


# ---------------------------------------------------------------------------
# Derived representations
# ---------------------------------------------------------------------------


def telemetry_ddl() -> str:
    """Spark DDL string for the parsed source payload."""
    return ", ".join(f.spark_ddl for f in TELEMETRY_FIELDS)


def _struct_from_ddl(ddl: str) -> StructType:
    """Parse a Spark DDL string into a ``StructType``.

    ``StructType.fromDDL`` only exists from PySpark 4.0. This project targets Spark
    3.5 (chosen for its mature Iceberg runtime and for
    ``dropDuplicatesWithinWatermark``), where the equivalent is the private
    ``_parse_datatype_string``. Both paths are kept so the code runs unchanged if the
    image is later moved to Spark 4.
    """
    from pyspark.sql.types import StructType

    from_ddl = getattr(StructType, "fromDDL", None)
    if from_ddl is not None:  # PySpark >= 4.0
        return from_ddl(ddl)

    from pyspark.sql.types import _parse_datatype_string

    return _parse_datatype_string(ddl)


def telemetry_struct() -> StructType:
    """Spark ``StructType`` for parsing the Kafka JSON payload.

    All fields are nullable at parse time on purpose: a malformed record must
    survive into the quarantine table with its failure explained, rather than
    being dropped by ``from_json`` before any rule has a chance to report it.
    """
    return _struct_from_ddl(telemetry_ddl())


def bronze_struct() -> StructType:
    fields = ", ".join(f.spark_ddl for f in (*TELEMETRY_FIELDS, *BRONZE_METADATA_FIELDS))
    return _struct_from_ddl(fields)


def telemetry_json_schema() -> dict[str, Any]:
    """JSON Schema for producer-side validation of a single telemetry event."""
    properties: dict[str, Any] = {}
    for spec in TELEMETRY_FIELDS:
        prop: dict[str, Any] = {"type": spec.json_type, "description": spec.description}
        if spec.name == "event_time":
            prop["format"] = "date-time"
        properties[spec.name] = prop
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "FleetStream telemetry event",
        "type": "object",
        "properties": properties,
        "required": [f.name for f in TELEMETRY_FIELDS if f.required],
        "additionalProperties": False,
    }


def _columns(fields: tuple[FieldSpec, ...], dialect: str = "spark", indent: str = "    ") -> str:
    """Render a column list.

    The ``CREATE TABLE`` statements below are executed by Spark, so they default to
    Spark types. ``FieldSpec.sql_type`` holds the equivalent Trino/ANSI type and is
    used for documentation and the dbt layer, where Trino is the engine. Spark's
    ``TIMESTAMP`` maps to Iceberg ``timestamptz``, which is what Trino then reads
    back as ``TIMESTAMP(6) WITH TIME ZONE`` - the two spellings describe one type.
    """
    attr = "spark_type" if dialect == "spark" else "sql_type"
    width = max(len(f.name) for f in fields)
    return ",\n".join(f"{indent}{f.name.ljust(width)} {getattr(f, attr)}" for f in fields)


def bronze_table_ddl(catalog: str, namespace: str, table: str = "telemetry_raw") -> str:
    """Bronze holds the source record plus provenance, and is strictly append-only.

    Append-only is load-bearing rather than stylistic: the Silver job consumes this
    table as a stream, and Iceberg streaming reads abort when they encounter a
    snapshot produced by an overwrite or delete.
    """
    columns = _columns((*TELEMETRY_FIELDS, *BRONZE_METADATA_FIELDS))
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.{table} (
{columns}
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'write.format.default'          = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'  = '134217728',
    'format-version'                = '2',
    'comment'                       = 'Bronze: raw telemetry, append-only, replayable'
)
""".strip()


def silver_table_ddl(catalog: str, namespace: str, table: str = "telemetry") -> str:
    """Silver: cleaned, deduplicated, enriched. Written by MERGE on event_id."""
    enrichment = (
        FieldSpec("vehicle_type", "STRING", "VARCHAR", "string", False, "From dim_vehicle."),
        FieldSpec("manufacturer", "STRING", "VARCHAR", "string", False, "From dim_vehicle."),
        FieldSpec("model", "STRING", "VARCHAR", "string", False, "From dim_vehicle."),
        FieldSpec("region", "STRING", "VARCHAR", "string", False, "From dim_vehicle."),
        FieldSpec("fleet_owner", "STRING", "VARCHAR", "string", False, "From dim_vehicle."),
        FieldSpec("driver_name", "STRING", "VARCHAR", "string", False, "From dim_driver."),
        FieldSpec("driver_category", "STRING", "VARCHAR", "string", False, "From dim_driver."),
        FieldSpec(
            "is_overspeed",
            "BOOLEAN",
            "BOOLEAN",
            "boolean",
            False,
            "Speed above the vehicle's limit.",
        ),
        FieldSpec(
            "is_overheating",
            "BOOLEAN",
            "BOOLEAN",
            "boolean",
            False,
            "Engine temperature above threshold.",
        ),
        FieldSpec(
            "is_low_fuel",
            "BOOLEAN",
            "BOOLEAN",
            "boolean",
            False,
            "Fuel below the low-fuel threshold.",
        ),
        FieldSpec(
            "processed_at",
            "TIMESTAMP",
            "TIMESTAMP(6) WITH TIME ZONE",
            "string",
            True,
            "When Silver last wrote this row.",
        ),
        FieldSpec(
            "silver_run_id", "STRING", "VARCHAR", "string", True, "Run that last wrote this row."
        ),
    )
    metadata = tuple(f for f in BRONZE_METADATA_FIELDS if f.name != "raw_payload")
    columns = _columns((*TELEMETRY_FIELDS, *metadata, *enrichment))
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.{table} (
{columns}
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'write.format.default'          = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'  = '134217728',
    'format-version'                = '2',
    'write.merge.mode'              = 'merge-on-read',
    'write.delete.mode'             = 'merge-on-read',
    'write.update.mode'             = 'merge-on-read',
    'comment'                       = 'Silver: validated, deduplicated, enriched telemetry'
)
""".strip()


def quarantine_table_ddl(catalog: str, namespace: str, table: str = "telemetry_quarantine") -> str:
    """Rejected rows, kept with enough context to diagnose and replay them.

    Partitioned by quarantine date rather than event date: the access pattern is
    "what did we reject in the last run", not "what happened on this event day".
    """
    quarantine_fields = (
        FieldSpec(
            "quarantine_id",
            "STRING",
            "VARCHAR",
            "string",
            True,
            "Surrogate key: event_id + rule_name.",
        ),
        FieldSpec(
            "event_id",
            "STRING",
            "VARCHAR",
            "string",
            False,
            "May be null - that is itself a rejection reason.",
        ),
        FieldSpec("vehicle_id", "STRING", "VARCHAR", "string", False, "Best-effort, for triage."),
        FieldSpec(
            "event_time",
            "TIMESTAMP",
            "TIMESTAMP(6) WITH TIME ZONE",
            "string",
            False,
            "Best-effort, for triage.",
        ),
        FieldSpec(
            "rule_name",
            "STRING",
            "VARCHAR",
            "string",
            True,
            "Which validation rule rejected the row.",
        ),
        FieldSpec(
            "rule_severity",
            "STRING",
            "VARCHAR",
            "string",
            True,
            "Severity declared in quality/rules.yml.",
        ),
        FieldSpec(
            "failure_reason",
            "STRING",
            "VARCHAR",
            "string",
            True,
            "Human-readable explanation with the offending value.",
        ),
        FieldSpec(
            "raw_payload",
            "STRING",
            "VARCHAR",
            "string",
            True,
            "Verbatim original record, so it can be corrected and replayed.",
        ),
        FieldSpec(
            "quarantined_at",
            "TIMESTAMP",
            "TIMESTAMP(6) WITH TIME ZONE",
            "string",
            True,
            "When the row was rejected.",
        ),
        FieldSpec("run_id", "STRING", "VARCHAR", "string", True, "Pipeline run that rejected it."),
    )
    columns = _columns(quarantine_fields)
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.{table} (
{columns}
)
USING iceberg
PARTITIONED BY (days(quarantined_at))
TBLPROPERTIES (
    'write.format.default'          = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'format-version'                = '2',
    'comment'                       = 'Quarantine: records failing validation, retained for replay'
)
""".strip()


def reconciliation_table_ddl(catalog: str, namespace: str, table: str = "reconciliation") -> str:
    """Per-run counts, so "rows went missing" is an answerable question."""
    fields = (
        FieldSpec("run_id", "STRING", "VARCHAR", "string", True, "Pipeline run identifier."),
        FieldSpec("stage", "STRING", "VARCHAR", "string", True, "bronze | silver | gold."),
        FieldSpec(
            "batch_id",
            "BIGINT",
            "BIGINT",
            "integer",
            False,
            "Structured Streaming micro-batch id, when applicable.",
        ),
        FieldSpec(
            "source_count", "BIGINT", "BIGINT", "integer", True, "Rows read from the source."
        ),
        FieldSpec(
            "duplicate_count", "BIGINT", "BIGINT", "integer", True, "Rows removed by deduplication."
        ),
        FieldSpec(
            "rejected_count", "BIGINT", "BIGINT", "integer", True, "Rows sent to quarantine."
        ),
        FieldSpec(
            "output_count", "BIGINT", "BIGINT", "integer", True, "Rows written to the target."
        ),
        FieldSpec(
            "recorded_at",
            "TIMESTAMP",
            "TIMESTAMP(6) WITH TIME ZONE",
            "string",
            True,
            "When the counts were recorded.",
        ),
    )
    columns = _columns(fields)
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.{table} (
{columns}
)
USING iceberg
PARTITIONED BY (days(recorded_at))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'format-version'       = '2',
    'comment'              = 'Reconciliation counters: source vs duplicate vs rejected vs output'
)
""".strip()
