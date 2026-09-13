# Data model

Dimensional, with four fact tables at four explicitly declared grains. The grain of a
fact table is the single most important thing about it: it determines what a row
means, what may be summed, and what may be joined without corrupting the answer.

## The event contract

Every layer derives from one list of field definitions in
`src/fleetstream/common/schemas.py`. That list generates the Spark `StructType` used
to parse Kafka payloads, the JSON Schema the producer validates against, and the
Iceberg DDL for Bronze, Silver and quarantine.

Generated rather than hand-maintained, because three hand-written copies of a schema
diverge — usually silently, and usually discovered when a column is quietly full of
nulls. Adding a field here adds it everywhere at once.

| Field                | Type      | Required | Notes                                        |
|----------------------|-----------|----------|----------------------------------------------|
| `event_id`           | STRING    | yes      | UUIDv5 over (vehicle, trip, sequence)        |
| `vehicle_id`         | STRING    | yes      | Also the Kafka partition key                 |
| `driver_id`          | STRING    | yes      |                                              |
| `trip_id`            | STRING    | yes      |                                              |
| `route_id`           | STRING    | no       | Null when the trip follows no planned route  |
| `event_time`         | TIMESTAMP | yes      | When it happened on the vehicle              |
| `latitude`           | DOUBLE    | no       | −90..90                                      |
| `longitude`          | DOUBLE    | no       | −180..180                                    |
| `speed`              | DOUBLE    | no       | km/h                                         |
| `fuel_level`         | DOUBLE    | no       | percent, 0..100                              |
| `engine_temperature` | DOUBLE    | no       | Celsius                                      |
| `battery_level`      | DOUBLE    | no       | percent, 0..100                              |
| `odometer_km`        | DOUBLE    | no       | monotonically non-decreasing per vehicle     |
| `schema_version`     | STRING    | yes      | Contract version the producer emitted        |

Measures are nullable on purpose: a sensor dropout is a missing value, not a schema
violation, and rejecting the whole record would lose the readings that *did* arrive.
Identity and time fields are required, because a record without them cannot be
attributed, deduplicated or timed.

### `event_id` is deterministic

The most consequential decision in the model. `uuid5(namespace, "vehicle|trip|seq")`
means the same logical reading always hashes to the same id, no matter how many times
it is produced or replayed. That is what makes the Silver `MERGE` idempotent. A random
UUID would make every replay look like new data.

The namespace UUID is fixed forever — changing it would make every previously produced
id irreproducible.

## Layers

### Bronze — `bronze.telemetry_raw`

Grain: one row per Kafka message (duplicates included — they are the evidence that
deduplication is doing something).

Source fields plus provenance: `ingestion_time`, `kafka_topic`, `kafka_partition`,
`kafka_offset`, `kafka_timestamp`, `raw_payload`, `ingest_run_id`.

Partitioned by `days(event_time)`. Append-only, always.

`raw_payload` is the verbatim source bytes. It costs storage and buys the ability to
fix a parsing bug retroactively — the difference between a reprocess and a permanent
data gap.

### Silver — `silver.telemetry`

Grain: **one row per `event_id`**, enforced by the MERGE key.

Validated, deduplicated, enriched with vehicle and driver attributes, and carrying
three derived flags (`is_overspeed`, `is_overheating`, `is_low_fuel`) computed against
that vehicle's own limits rather than a fleet-wide constant.

Written merge-on-read, because it is updated by MERGE on every micro-batch and
copy-on-write would rewrite whole files each time.

### Quarantine — `silver.telemetry_quarantine`

Grain: one row per (rejected record × broken rule). A record failing three rules
produces three rows, which makes "how often does each rule fire" a plain `GROUP BY`
instead of an array-unnesting exercise.

Partitioned by `days(quarantined_at)` — the access pattern is "what did we reject in
the last run", not "what happened on this event day".

### Gold

Built by dbt: `staging → intermediate → marts`.

**Dimensions**: `dim_vehicle`, `dim_driver`, `dim_route`, `dim_date`.

`dim_date` exists so that a day with no activity appears as a zero rather than a
missing row. In a chart those look identical, and only one of them is good news.

**Facts**:

| Table                  | Grain                 | Merge key         | Rows per trip |
|------------------------|-----------------------|-------------------|---------------|
| `fact_telemetry`       | one reading           | `event_id`        | ~1000         |
| `fact_trip`            | one completed trip    | `trip_id`         | 1             |
| `fact_vehicle_daily`   | one vehicle × one day | `vehicle_day_key` | —             |
| `fact_driver_incident` | one incident          | `incident_id`     | varies        |

## Fanout: the failure this model is shaped to prevent

`fact_trip` has one row per trip. `fact_telemetry` has about a thousand. Join them and
sum `fuel_consumed_l`, and the answer is roughly a thousand times too large.

The number will still look like a number. It will have the right units, a plausible
order of magnitude for a fleet, and no error message anywhere. That is why grain is
enforced by tests rather than trusted to reviewers:

```yaml
- name: fact_trip
  columns:
    - name: trip_id
      data_tests: [not_null, unique]
```

and, for the composite natural key behind a surrogate:

```yaml
- dbt_utils.unique_combination_of_columns:
    arguments:
      combination_of_columns: [vehicle_id, event_date]
```

If a join ever duplicates rows, these fail on the next run rather than at the next
board meeting.

`fact_trip` also carries `distinct_vehicles`, tested to always equal 1. A trip
spanning two vehicles would mean `trip_id` is not the key we believe it is — a
condition uniqueness alone would not catch.

## Two independent measures of distance

`fact_trip` derives distance twice: the odometer delta, and the summed great-circle
distance between consecutive GPS fixes. `distance_discrepancy_km` is the gap.

One measure has to be trusted. Two that disagree reveal a stuck odometer or dropped
readings, and the disagreement is visible rather than inferred.

## Daily aggregates come from events, not from trips

`int_vehicle_daily_metrics` aggregates event-grain telemetry by `event_date`. It does
**not** sum `fact_trip`, because a trip starting at 23:40 and ending at 00:20 belongs
to two days, and attributing its distance to either one is wrong. Aggregating readings
by their own date puts every kilometre on the day it was actually driven.

## Incremental strategy

Every incremental fact reprocesses a trailing window and merges:

```sql
where event_time >= current_date - interval '2' day
```

with `incremental_strategy='merge'` on a deterministic key.

A high-water-mark filter (`event_time > last_max`) would be cheaper and would
permanently lose late arrivals: an event generated yesterday and delivered today has
an `event_time` already older than the mark. Reprocessing a window and merging costs
a little compute and is correct.

The window is a single `var` (`lookback_days`). It should be set from the measured p99
of `ingestion_time - event_time`, which `stg_telemetry.ingestion_lag_seconds` exposes,
not from a guess.

## Business questions and where they are answered

| Question                              | Model                                     |
|---------------------------------------|-------------------------------------------|
| Which vehicles overheated yesterday?  | `fact_driver_incident`                    |
| Fuel consumption by vehicle           | `vehicle_performance`                     |
| Which drivers speed most?             | `driver_performance` (per 100 km)         |
| Which routes are unreliable?          | `route_performance` (p95 − median spread) |
| Fleet KPIs by day                     | `fleet_daily_summary`                     |
| Which vehicles need maintenance?      | `vehicle_performance.needs_maintenance_review` |
| Is data quality degrading?            | `data_quality_summary`                    |

Driver metrics are normalised per 100 km. Raw counts would rank drivers by how far
they drive, which is a rota question, not a behaviour one.

Route reliability uses the p95-minus-median spread rather than an average duration. A
route with a 90-minute median and a 300-minute p95 is unreliable in a way that no
average will ever show.
