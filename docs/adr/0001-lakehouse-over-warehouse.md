# ADR 0001: Iceberg lakehouse as the system of record

**Status:** accepted

## Context

Telemetry arrives continuously and must serve two consumers with different needs:
streaming jobs that append constantly, and analysts who run ad-hoc SQL. The obvious
alternatives are a warehouse-first design (stream straight into Snowflake) or a
plain data lake (Parquet files on S3 with a Hive-style catalog).

## Decision

Apache Iceberg on object storage is the system of record. Compute engines - Spark for
processing, Trino for serving - read and write the same tables.

## Why

**Against warehouse-first.** Streaming into a warehouse couples ingestion to a vendor
and to compute cost that scales with ingest volume rather than with query value.
Reprocessing history means re-loading it. Raw data ends up living somewhere else
anyway, which is the lake we were trying to avoid.

**Against a plain lake.** Parquet on S3 with no table format has no atomic commits, so
a reader can see a half-written batch; no schema evolution beyond convention; and no
way to delete or update a row, which makes GDPR erasure and bug fixes intractable.

**For Iceberg.** Snapshot isolation gives readers a consistent view while writers
commit. MERGE makes the idempotent sink possible, which is the platform's central
correctness property. Schema and partition evolution are metadata operations. And
storage stays decoupled from compute: Spark and Trino query one copy of the data with
no extract, no sync, and no chance of the two disagreeing.

## Costs

Table maintenance becomes our responsibility - compaction and snapshot expiry are
scheduled jobs that would not exist in a warehouse (see ADR 0004). The catalog is a
new stateful component to operate. And Iceberg's streaming source constrains Bronze to
append-only, which shapes the design (see ADR 0002).
