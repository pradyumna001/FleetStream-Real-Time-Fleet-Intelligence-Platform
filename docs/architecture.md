# FleetStream architecture

## The problem

A logistics operator runs a fleet of vehicles that continuously emit telemetry:
position, speed, fuel, engine temperature, battery. They need answers to two very
different kinds of question, and the difference is what shapes the whole platform.

**Operational, now.** Which engine is overheating *this minute*? A dispatcher needs
to act while the truck is still moving. Latency is the only thing that matters, and
an answer that arrives ten minutes late is worthless.

**Analytical, over time.** What is fuel consumption per route this quarter? Which
drivers speed most per 100 km? Which vehicles need maintenance? These need
correctness, history, and the ability to recompute when a bug is found. Latency is
almost irrelevant; being *wrong* is unacceptable.

A single pipeline optimised for one of these serves the other badly. FleetStream runs
both paths off one ingestion point.

## Shape of the system

```
                     vehicles / simulator
                              |
                              v
                    ┌───────────────────┐
                    │  Kafka            │  keyed by vehicle_id
                    │  fleet.telemetry  │  12 partitions
                    └───────────────────┘
                         |          |
        operational path |          | analytical path
                         v          v
              ┌────────────────┐  ┌──────────────────────┐
              │ incident       │  │ bronze_ingest        │
              │ detector       │  │ (Spark Structured    │
              │ (stateless)    │  │  Streaming)          │
              └────────────────┘  └──────────────────────┘
                       |                     |
                       v                     v
              fleet.incidents        ┌────────────────┐
              (Kafka topic)          │ ICEBERG BRONZE │  append-only, raw,
                       |             │ telemetry_raw  │  replayable
                       v             └────────────────┘
              ops / paging                   |
                                             v
                                   ┌────────────────────┐
                                   │ silver_transform   │  watermark + dedup
                                   │ (Spark, MERGE)     │  validate + enrich
                                   └────────────────────┘
                                        |          |
                                        v          v
                              ┌──────────────┐  ┌──────────────┐
                              │ ICEBERG      │  │ QUARANTINE   │
                              │ SILVER       │  │ + reason     │
                              └──────────────┘  └──────────────┘
                                        |
                                        v
                                    ┌───────┐
                                    │  dbt  │  staging → intermediate → marts
                                    └───────┘
                                        |
                                        v
                              ┌──────────────────┐
                              │  ICEBERG GOLD    │  facts + dimensions
                              └──────────────────┘
                                        |
                                        v
                              ┌──────────────────┐
                              │ Trino  →  Grafana│
                              └──────────────────┘
```

Around it: Airflow orchestrates the batch path, Prometheus and Grafana observe it,
Terraform describes the AWS target, and CI checks all of it on every change.

## The two paths, and why they are separate

The incident detector reads Kafka directly, filters on thresholds, and publishes to
`fleet.incidents`. It is stateless, triggers every 5 seconds, and does no validation
or deduplication. That is not laziness — those steps cost latency, and this path
exists only to be fast. A duplicate alert is a minor annoyance; a late alert is a
failure.

The analytical path does the opposite. It buffers into 30- and 60-second batches,
validates every record, deduplicates, and writes transactionally. It processes the
same events independently, so the operational path being wrong or restarted never
corrupts the history.

## Layers

**Bronze** is raw and append-only. It stores the parsed columns *and* the verbatim
payload, plus Kafka topic/partition/offset. If a parsing bug is found next month, the
fix is a reprocess, not an apology. Append-only is load-bearing rather than stylistic:
Silver consumes this table as a stream, and Iceberg's streaming source aborts on
snapshots produced by overwrite or delete.

**Silver** is validated, deduplicated and enriched. It is the correctness layer, and
section "Idempotency" below is really about this table.
 
**Gold** is business-shaped: conformed dimensions and four facts at four explicit
grains. Built by dbt in SQL, because this layer is where business logic lives and SQL
is what the people who own that logic can read and change.

## Idempotency: the central claim

Structured Streaming's checkpoint guarantees that the *source* is re-read
consistently. It does not guarantee the *sink* applied each record once. If Spark
writes a batch and dies before the checkpoint advances, the batch is reprocessed on
restart, and a naive appending sink duplicates every row in it.

Deduplication does not fix this either. `dropDuplicatesWithinWatermark` bounds state
by the watermark — necessary, because unbounded state eventually kills the job — but
that means state *expires*. An event replayed an hour later is no longer remembered.

So the sink is made idempotent instead:

```sql
MERGE INTO silver.telemetry t
USING batch s ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

This only works because `event_id` is **deterministic** — a UUIDv5 over
`(vehicle_id, trip_id, sequence)`, computed at the source. A random UUID would make
the same logical reading look new on every replay, and the MERGE would insert
duplicates just as an append would.

Watermark and MERGE are complementary, not alternatives: the watermark keeps the
common case cheap, the MERGE makes correctness permanent.

`make verify` proves this by replaying events already in Silver and asserting the row
count does not move.

## Event time vs ingestion time

A vehicle loses signal at 10:05 and delivers its buffered readings at 10:20. The
event happened at 10:05, and every aggregate must say so, or "distance driven on
Tuesday" silently depends on network conditions.

Both timestamps are carried through every layer. `event_time` drives watermarking,
partitioning and all date logic. `ingestion_time` is kept for diagnostics — the gap
between them is the lateness distribution, which is what should size the watermark
and the dbt lookback window.

Downstream, dbt incremental models reprocess a trailing window
(`vars.lookback_days`) rather than filtering on "newer than last run". A high-water
mark on `event_time` would skip late arrivals permanently.

## Data quality: split, don't fail

A malformed record must not kill a batch, and it must not silently vanish either.
Rules in `src/fleetstream/quality/rules.yml` compile to Spark expressions and split
each batch in one pass: valid rows to Silver, invalid rows to a quarantine table
carrying the rule name, the reason, the run id and the original payload.

Rules are declarative because the streaming job and the Airflow batch check must
apply *the same* rules; expressed twice in code, they drift, and "the batch job
disagrees with the stream" becomes an unfalsifiable bug report.

This is also why the engine is hand-written rather than Great Expectations or Soda.
Those tools answer "did this dataset pass?", which is a different question. Neither
expresses a row-level *split*, and forcing one would mean either failing the whole
micro-batch on one bad row or scanning the data twice to find the culprits.

## Grain, and why it is tested

Four facts, four grains:

| Fact                   | Grain                  |
|------------------------|------------------------|
| `fact_telemetry`       | one telemetry reading  |
| `fact_trip`            | one completed trip     |
| `fact_vehicle_daily`   | one vehicle × one day  |
| `fact_driver_incident` | one detected incident  |

There are roughly a thousand telemetry rows per trip. Joining `fact_trip` to
`fact_telemetry` and summing a trip measure multiplies it by that factor — the
classic fanout error, and it produces numbers that look plausible enough to reach a
report before anyone checks.

So each grain is declared in `schema.yml` **and enforced** by a uniqueness test. A
documented grain that nothing checks is a comment.

## Partitioning and skew

Kafka is keyed by `vehicle_id`, which gives per-vehicle ordering — necessary, because
a fuel reading that arrives out of order would make consumption look negative. The
cost is that one high-volume vehicle concentrates on one partition, and no number of
extra consumers can help, since a partition is consumed by exactly one member of a
group.

The simulator reproduces this deliberately: `V9999` reports 25× more often than any
other vehicle, making its partition roughly twice the average depth. It is visible on
the Grafana platform dashboard, which is the point — it is a real property of this
keying choice, not a defect to hide.

Iceberg tables partition by `days(event_time)`. Not by `vehicle_id`: that would create
one partition per vehicle, thousands of tiny partitions, and metadata overhead far
larger than the data.

## Small files

Streaming writes one file per partition per micro-batch. At a 30-second trigger that
is thousands of small files a day. The cost is not storage; it is that every query
opens, plans and reads all of them, so the table gets slower for reasons no single
query explains.

Handled in two places: trigger intervals sized so each batch writes a reasonable file,
and a scheduled maintenance DAG running `rewrite_data_files` then `expire_snapshots`.
Order matters — compaction is what makes the old files unreferenced, and expiry is
what reclaims them. Expiry first simply does less work while storage keeps growing.

## Local stand-ins

| Local            | AWS              | Why it substitutes cleanly            |
|------------------|------------------|---------------------------------------|
| Kafka KRaft      | MSK              | Same protocol and client code         |
| MinIO            | S3               | Same API; endpoint is the only change |
| Iceberg REST     | Glue Catalog     | Same Iceberg catalog interface        |
| Trino            | Athena           | Athena is Presto-derived; same SQL    |
| Docker Compose   | EMR / ECS        | —                                     |

The dbt project also defines a Snowflake target. It is written but untested — there
is no Snowflake account behind it — and is documented as a starting point rather than
presented as working.

## Known limits

* **Single Kafka broker.** Replication cannot be demonstrated on one machine.
  Partition count is what matters here, and that is realistic at 12.
* **Terraform is validate-only.** No AWS account, so nothing has been applied. See
  `terraform/README.md`.
* **The Snowflake dbt target is untested.**
* **Trip completion is heuristic.** A trip is "complete" when its last reading is over
  30 minutes old. Real telemetry usually carries explicit ignition-on/off events.
* **The catalog is a single Postgres instance.** Fine here; a real deployment would
  use Glue or a replicated RDS instance.
