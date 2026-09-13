# FleetStream — Real-Time Fleet Intelligence Platform

A working lakehouse for high-volume vehicle telemetry: **Kafka → Spark Structured
Streaming → Apache Iceberg → dbt → Trino**, with orchestration, data quality,
observability and infrastructure-as-code around it.

It runs end to end on a laptop with `make demo`. Managed services are replaced by
local equivalents that speak the same interfaces — MinIO for S3, Trino for Athena, an
Iceberg REST catalog for Glue — so the design stays portable to AWS.

## What makes this different from a demo pipeline

Most streaming examples show the happy path. This one is built so its correctness
claims are **falsifiable**: the simulator deliberately injects the failures the
platform says it handles, and `make verify` asserts that each was caught.

```
[PASS] bronze ingested          311,138 rows, 308,116 distinct event_id
[PASS] duplicates injected      3,022 duplicate rows reached Bronze (0.97%)
[PASS] silver deduplicated      306,607 rows, all event_id distinct
[PASS] rows conserved           306,607 in Silver + 1,509 quarantined = 308,116 of
                                308,116 distinct Bronze events (0 still in flight)
[PASS] quarantine populated     1,545 violations across 9 rules
[PASS] quarantine is exclusive  no critically-invalid record reached Silver
[PASS] reconciliation balances  all recorded batches balance
[PASS] event_time preserved     event_time identical to Bronze for every row
[PASS] business query answers   top overheating vehicles: V9999=38816, V1164=444, ...
[PASS] manifest reconciliation  injected 1,509 invalid records, quarantined 1,509
[PASS] replay is idempotent     replayed 505 events; Silver row count unchanged
```

Every number above is checked, not printed. The injected count and the quarantined
count match exactly because they are the same records.

## Quick start

Requires Docker (~10 GB available) and `make`.

```bash
make preflight   # checks Docker, .env, available memory
make demo        # core → bootstrap → seed → streaming → simulate → serving
make dbt         # build the Gold models
make verify      # run the assertion suite
```

| Service       | URL                    | Notes                       |
|---------------|------------------------|-----------------------------|
| Grafana       | http://localhost:3000  | platform + business boards; `admin` / `fleetstream` |
| Trino         | http://localhost:8090  | query the lakehouse         |
| Spark master  | http://localhost:8080  | streaming queries           |
| Prometheus    | http://localhost:9090  | metrics + alert rules       |
| MinIO console | http://localhost:9001  | the object store            |
| Airflow       | http://localhost:8082  | `admin` / `admin`; `make orchestrate` first |

Running less: `make up` (core, ~1.5 GB), then `stream`, `serve`, `orchestrate` as
needed — they are Compose profiles.

## The three ideas worth reading the code for

### 1. The sink is idempotent, and the watermark alone is not enough

Structured Streaming's checkpoint guarantees the *source* is re-read consistently. It
does **not** guarantee the *sink* applied each record once — a batch written just
before a crash is reprocessed on restart, and an appending sink duplicates every row
in it.

Deduplication does not close the gap either. `dropDuplicatesWithinWatermark` bounds
state by the watermark, which is necessary (unbounded state kills the job) and means
that state *expires*: an event replayed an hour later is no longer remembered.

So Silver is written with `MERGE INTO ... ON event_id`, and `event_id` is a **UUIDv5
over (vehicle, trip, sequence)** computed at the source. Deterministic ids are what
make the merge key meaningful — with random UUIDs, MERGE would insert on every replay
and behave exactly like the append it replaced.

`make verify` replays real events through the real write path and asserts the row
count does not move. See [ADR 0003](docs/adr/0003-idempotent-sink.md).

### 2. Bad data is split, not dropped and not fatal

Rules in [`quality/rules.yml`](src/fleetstream/quality/rules.yml) compile to Spark
expressions and split each batch in one pass: valid rows to Silver, invalid rows to a
quarantine table carrying the rule, the reason, the run id and the original payload.

They are declarative because the streaming job and the Airflow batch check must apply
*the same* rules — written twice in code they drift, and "the batch disagrees with the
stream" becomes an unfalsifiable bug report.

This is also why the engine is hand-written rather than Great Expectations or Soda:
those answer "did this dataset pass?", which is a different question. Neither expresses
a row-level *split*.

### 3. Grain is declared **and enforced**

`fact_telemetry` holds ~1000 rows per trip; `fact_trip` holds one. Join them, sum a
trip measure, and the answer is a thousand times too large — with the right units, a
plausible magnitude, and no error anywhere.

So every fact declares its grain in `schema.yml` and enforces it with a uniqueness
test. A documented grain that nothing checks is a comment.

## Repository layout

```
src/fleetstream/
  common/       event contract, config, Spark session, Iceberg bootstrap
  simulator/    vehicle state machine + deliberate fault injection
  streaming/    bronze_ingest, silver_transform, incident_detector
  quality/      declarative rules, split engine, reconciliation
  jobs/         bootstrap, seed_dimensions, verify
  maintenance/  compaction and snapshot expiry
dbt/fleetstream/  staging → intermediate → marts → gold
airflow/dags/     daily pipeline + maintenance
compose/          base + spark / serving / orchestration overlays
docker/           spark, simulator, dbt, airflow, iceberg-rest images
observability/    prometheus config, alert rules, Grafana dashboards
terraform/        AWS target (validated, never applied)
docs/             architecture, data model, runbook, ADRs
tests/            unit (no Spark) + integration (in-container)
```

## The simulator is part of the test strategy

It models vehicles as state machines — fuel falls with distance, the odometer never
decreases, engine temperature drifts toward a load-dependent steady state — so trip
and daily facts mean something arithmetically rather than just looking plausible.

On top of that it injects, at configurable rates:

| Injection                          | Default | Exercises                       |
|------------------------------------|---------|---------------------------------|
| Duplicate `event_id` re-sends      | 1%      | dedup + MERGE idempotency       |
| Late arrivals (5–20 min delayed)   | 2%      | watermark, dbt lookback window  |
| Out-of-range / wrong-typed fields  | 0.5%    | quality rules + quarantine      |
| One vehicle at 25× the rate        | on      | Kafka hot partition             |

The hot vehicle (`V9999`) makes its partition roughly twice the average depth, visible
on the Grafana platform dashboard. That is a genuine consequence of keying by
`vehicle_id` — kept and shown, not hidden.

## Documentation

- [Architecture](docs/architecture.md) — the two paths, layers, and why each exists
- [Data model](docs/data_model.md) — the contract, grains, and the fanout failure
- [Runbook](docs/runbook.md) — lag, missing data, checkpoint recovery, backfills
- [ADRs](docs/adr/) — decisions with their costs, including two that were reversed

## Honest limitations

- **Terraform is validated, never applied.** No AWS account. `fmt`, `init` and
  `validate` pass in CI; nothing beyond that is proven. See
  [terraform/README.md](terraform/README.md).
- **The Snowflake dbt target is defined but untested.** It exists to show the serving
  engine is a profile change, not a modelling change — but it has never run.
- **Single Kafka broker.** Replication cannot be demonstrated on one machine;
  partition count (12) is the property that matters here and is realistic.
- **Trip completion is heuristic** — last reading older than 30 minutes. Real
  telemetry carries explicit ignition events.
- **Windows/Git Bash** rewrites container paths in `docker exec` arguments. Use the
  Makefile targets, or prefix with `MSYS_NO_PATHCONV=1`.

## Testing

```bash
make test              # unit tests, no Spark needed (54 tests)
make test-integration  # runs inside the Spark container against real Iceberg
make verify            # end-to-end assertions against live data
```

Application modules import PySpark lazily, which is what keeps the contract, the rule
engine and the simulator testable on a machine with no JVM.
