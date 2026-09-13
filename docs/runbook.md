# Runbook

Operational procedures for FleetStream. Each entry names a symptom, then how to
distinguish causes that look identical from the outside.

## Starting and stopping

```bash
make preflight   # Docker running? .env present? memory available?
make up          # Kafka, MinIO, catalog DB, Iceberg REST
make bootstrap   # create namespaces + tables, round-trip smoke test
make seed        # load vehicle/driver/route dimensions
make stream      # Spark cluster + the three streaming queries
make serve       # Trino, Prometheus, Grafana
make simulate    # produce telemetry
make dbt         # build Gold
make verify      # end-to-end assertions
```

`make down` stops everything and keeps data. `make clean` deletes volumes and prompts
first — it destroys the Iceberg catalog, so every table has to be recreated.

### Endpoints

| Service        | URL                     | Notes |
|----------------|-------------------------|-------|
| Spark master   | http://localhost:8080   |  |
| Bronze driver  | http://localhost:4040   |  |
| Silver driver  | http://localhost:4041   |  |
| Trino          | http://localhost:8090   |  |
| Grafana        | http://localhost:3000   | `admin` / `fleetstream`, anonymous viewing enabled |
| Prometheus     | http://localhost:9090   |  |
| MinIO console  | http://localhost:9001   | `fleetstream` / `fleetstream-local-secret` |
| Airflow        | http://localhost:8082   | login `admin` / `admin` |

## Cannot log in to Airflow

Username `admin`, password `admin` (set by `AIRFLOW_ADMIN_PASSWORD` in `.env`).

Airflow 3 removed `airflow users create` - it raises `AttributeError:
'AirflowSecurityManagerV2' object has no attribute 'find_role'`. Credentials now come
from an auth manager, and the default SimpleAuthManager *generates a random password*
on first start unless one is seeded.

`airflow-init` seeds `/opt/airflow/auth/passwords.json` on a shared volume, and
SimpleAuthManager only fills in users missing from that file, so the seeded value
stands. To check what is actually in effect:

```bash
docker exec fleetstream-airflow-apiserver cat /opt/airflow/auth/passwords.json
```

If that file is missing or empty, the volume permissions are wrong - it is created
root-owned while Airflow runs as uid 50000, which is what `airflow-auth-init` fixes.
Look for "Permission denied" in `docker logs fleetstream-airflow-init`.

To change the password, set `AIRFLOW_ADMIN_PASSWORD` in `.env` and recreate, removing
the volume so the seed runs again:

```bash
make down
docker volume rm fleetstream_airflow-auth
make orchestrate
```

## Consumer lag is growing

**Read the derivative, not the absolute value.** A large but shrinking backlog is a
system recovering correctly. Only sustained growth means arrival exceeds processing.

Order of investigation — the first two steps rule out the cases where adding capacity
would make things worse:

1. **Is one partition much deeper than the rest?**
   `make lag`, or the "Consumer lag by partition" panel. If one line sits far above
   the others, this is a hot key, not a capacity problem. A partition is consumed by
   exactly one member of a group, so more consumers cannot help. `V9999` in the
   simulator reproduces this deliberately.

2. **Is the sink the bottleneck?** Check Silver batch duration on the Spark UI. If the
   MERGE dominates, more input parallelism just enlarges the queue behind it.

3. **Is batch duration above the trigger interval?** A batch taking longer than its
   trigger can never catch up — the next batch is due before this one finishes.

4. **Is there shuffle spill or skew?** Spark UI, stage detail, task duration
   distribution. If 99% of tasks finish in seconds and one runs for minutes, that is
   skew; look at AQE skew handling or the join strategy before scaling.

5. **Only now**, increase parallelism: `spark.cores.max` per query, then Kafka
   partition count if the ceiling is genuinely partition-bound.

Raising `maxOffsetsPerTrigger` when the sink is slow makes recovery worse, not better.

## Data appears to be missing

The reconciliation table answers this directly:

```sql
SELECT run_id, stage, batch_id, source_count, duplicate_count,
       rejected_count, output_count,
       source_count - duplicate_count - rejected_count - output_count AS unaccounted
FROM iceberg.platform.reconciliation
WHERE unaccounted <> 0
ORDER BY recorded_at DESC;
```

`unaccounted = 0` means every row is explained: it was written, deduplicated, or
quarantined. Non-zero means genuine loss, and the stage where it first appears is the
one to investigate.

If the counts balance but a number still looks low, the rows were probably rejected:

```sql
SELECT rule_name, count(*) AS violations
FROM iceberg.silver.telemetry_quarantine
WHERE quarantined_at > current_timestamp - interval '1' day
GROUP BY rule_name ORDER BY violations DESC;
```

A rule that suddenly fires on a large fraction of records usually means an upstream
producer changed, not that the data went bad.

## Duplicate rows appeared in Silver

This should be impossible — `event_id` is the MERGE key. If it happens:

1. Confirm it: `SELECT count(*), count(DISTINCT event_id) FROM iceberg.silver.telemetry;`
2. If the counts differ, the MERGE is not running. Check whether the Silver job fell
   back to an append path, and check `write_batch` for an exception being swallowed.
3. Check that `event_id` is genuinely deterministic — a producer regenerating random
   ids per send defeats the whole mechanism, and it looks exactly like this.

## A streaming query will not restart

**Checkpoint corruption.** Symptoms are an offset log that will not parse, or a state
store that fails to load.

Checkpoints live at `s3a://fleetstream/checkpoints/<job>`. Deleting one makes the
query restart from `startingOffsets` and reprocess. That is safe for Silver — the
MERGE deduplicates — and safe for Bronze only if you accept duplicate raw rows, which
Silver will then collapse anyway.

```bash
docker run --rm --network fleetstream -e MC_HOST_local=http://fleetstream:fleetstream-local-secret@minio:9000 \
  minio/mc rm --recursive --force local/fleetstream/checkpoints/silver-transform
```

**Expired snapshot.** If the Silver query was down longer than the snapshot retention
window (72h), the Bronze snapshot it wants to resume from may have been expired by
maintenance. It cannot resume; it has to be restarted from a current snapshot and the
gap backfilled. This is why retention is kept comfortably longer than any expected
outage.

## Iceberg queries got slow

Almost always small files. Measure before acting:

```sql
SELECT count(*) AS files, avg(file_size_in_bytes)/1024/1024 AS avg_mb
FROM iceberg.bronze."telemetry_raw$files";
```

Thousands of files averaging a few MB means compaction is overdue:

```bash
make maintenance
```

which runs `rewrite_data_files` then `expire_snapshots`, in that order. Compaction
makes the old files unreferenced; expiry reclaims them. Running expiry first does less
work while storage keeps growing.

Compacting Bronze produces an overwrite snapshot. The Silver reader sets
`streaming-skip-overwrite-snapshots`, so this is safe — but it is a real coupling, and
it is why maintenance is scheduled rather than run ad hoc.

## dbt fails with "Failed to create namespace" or a commit error

Catalog contention. The Iceberg catalog is shared mutable state written by three
streaming queries plus Trino and dbt at once.

This is exactly why the catalog is backed by Postgres rather than the stock SQLite
fixture: SQLite takes a database-wide write lock and fails concurrent writers with
`SQLITE_BUSY`. If these errors reappear, check that `iceberg-rest` is actually using
the Postgres image:

```bash
docker inspect fleetstream-iceberg-rest --format '{{.Config.Image}}'   # fleetstream/iceberg-rest:1.10.1-pg
docker logs fleetstream-iceberg-rest | grep -i "sqlite\|busy"          # should be empty
```

Reducing dbt threads is a workaround, not a fix.

## Trino cannot see a table Spark just wrote

Both engines share one catalog, so this is nearly always a namespace or catalog-name
mismatch rather than a sync problem — there is nothing to sync.

```sql
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.silver;
```

Spark writes to catalog `fleetstream`; Trino calls the same catalog `iceberg`. Two
names, one catalog. That is configuration, not duplication.

## Source schema changed

1. Is the change backward compatible? A new nullable field is; a renamed or retyped
   field is not.
2. Add the field to `TELEMETRY_FIELDS` in `common/schemas.py`. Every representation —
   Spark schema, JSON Schema, Iceberg DDL — updates from that one edit.
3. Bump `SCHEMA_VERSION`. Old records keep their old value, so mixed-version data
   stays interpretable.
4. `schema_version_known` is a **warning**, not a critical rule, so a producer rolling
   out a new version does not cause an outage. It surfaces without rejecting.
5. Evolve the Iceberg tables (`ALTER TABLE ... ADD COLUMN`) and update dbt models.

## Backfilling

Raw data is retained in Bronze, which is what makes this possible at all.

1. Stop the Silver query.
2. Delete its checkpoint (see above).
3. Restart. It reprocesses from the beginning of Bronze.
4. The MERGE makes this safe: rows are updated in place, not duplicated. This is the
   same property `make verify` asserts.
5. Re-run dbt with a wider window: `dbt build --vars '{lookback_days: 30}'`.

## Resource pressure

The full stack needs roughly 10 GB. Compose profiles let you run less:

```bash
make up                 # core only, ~1.5 GB
make stream             # + Spark, ~6 GB
make serve              # + Trino/Grafana, ~9 GB
make orchestrate        # + Airflow, ~11 GB
```

If Spark executors are being killed, lower `SPARK_WORKER_MEMORY` in `.env` or run
fewer profiles. Docker Desktop's memory limit is set in its own settings, not here.

## A note for Windows users

Git Bash rewrites absolute paths in `docker exec`/`docker run` arguments, turning
`/opt/spark/bin/spark-submit` into a `C:/Program Files/Git/...` path that does not
exist in the container. Prefix such commands with `MSYS_NO_PATHCONV=1`, or use the
Makefile targets, which avoid passing container paths on the command line.
