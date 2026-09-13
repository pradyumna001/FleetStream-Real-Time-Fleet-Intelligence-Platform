"""Iceberg table maintenance.

Streaming ingestion writes one file per partition per micro-batch. At a 30-second
trigger that is thousands of small files a day, and the cost is not storage - it is
that every query must open, plan and read all of them. Metadata grows, planning
slows, and the table becomes gradually slower to read for reasons no single query
explains.

ORDER MATTERS
    Compaction runs before expiry. Compaction is what makes the pre-compaction files
    unreferenced; expiry is what actually reclaims them. Run expiry first and it
    simply has less to do, while storage keeps growing.

COUPLING WITH THE STREAM
    Compacting Bronze produces an overwrite snapshot, which the Silver streaming
    reader would normally refuse. That reader sets
    ``streaming-skip-overwrite-snapshots``, so maintenance is safe - but the coupling
    is real, which is why this is scheduled rather than run ad hoc, and why snapshot
    retention (72h) is kept comfortably longer than any expected streaming outage. A
    query resuming from a snapshot that has been expired cannot proceed at all.

Scheduled off-peak, and separately from the daily transformation DAG, so a long
compaction cannot delay the business numbers.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

SPARK_IMAGE = "fleetstream/spark:3.5.9"
NETWORK = os.environ.get("FLEETSTREAM_NETWORK", "fleetstream")
DOCKER_URL = "unix://var/run/docker.sock"

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "fleetstream")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")

SPARK_ENV = {
    "S3_ENDPOINT": "http://minio:9000",
    "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY", "fleetstream"),
    "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY", "fleetstream-local-secret"),
    "AWS_ACCESS_KEY_ID": os.environ.get("S3_ACCESS_KEY", "fleetstream"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("S3_SECRET_KEY", "fleetstream-local-secret"),
    "AWS_REGION": "us-east-1",
    "WAREHOUSE_BUCKET": "fleetstream",
    "ICEBERG_REST_URI": "http://iceberg-rest:8181",
    "CATALOG_NAME": "fleetstream",
    "CHECKPOINT_ROOT": "s3a://fleetstream/checkpoints",
    # PYTHONPATH intentionally omitted - the Spark image sets it correctly.
}

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="fleetstream_maintenance",
    description="Iceberg compaction, snapshot expiry and orphan cleanup",
    # 03:00, after the 01:00 transformation DAG has finished.
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fleetstream", "maintenance", "iceberg"],
    doc_md=__doc__,
)
def fleetstream_maintenance() -> None:
    @task
    def report_file_counts() -> dict[str, int]:
        """Record small-file counts before maintenance, so the run has a baseline.

        Without a before-and-after number, "maintenance ran" is unfalsifiable. The
        Iceberg `$files` metadata table makes the problem directly measurable.
        """
        from trino.dbapi import connect  # noqa: PLC0415

        conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG)
        counts: dict[str, int] = {}
        try:
            cursor = conn.cursor()
            for schema, table in (("bronze", "telemetry_raw"), ("silver", "telemetry")):
                try:
                    cursor.execute(
                        f'SELECT count(*), coalesce(avg(file_size_in_bytes), 0) '
                        f'FROM {TRINO_CATALOG}.{schema}."{table}$files"'
                    )
                    files, avg_size = cursor.fetchone()
                    counts[f"{schema}.{table}.files"] = int(files)
                    counts[f"{schema}.{table}.avg_mb"] = int((avg_size or 0) / 1024 / 1024)
                    logger.info(
                        "%s.%s: %d data files, average %.1f MB",
                        schema, table, files, (avg_size or 0) / 1024 / 1024,
                    )
                except Exception:  # noqa: BLE001 - a missing table must not fail the DAG
                    logger.warning("could not read file metadata for %s.%s", schema, table)
        finally:
            conn.close()
        return counts

    # Compaction and expiry run together in one Spark job: they share a session, and
    # splitting them across containers would pay the Spark startup cost twice for no
    # isolation benefit, since a failure in either wants the same investigation.
    compact = DockerOperator(
        task_id="compact_and_expire",
        image=SPARK_IMAGE,
        command=[
            "/opt/spark/bin/spark-submit",
            "--master", "local[2]",
            "--driver-memory", "1g",
            "/opt/fleetstream/src/fleetstream/maintenance/table_maintenance.py",
            # Orphan removal lists the entire warehouse prefix, which is expensive
            # and races with in-flight writes. Weekly is plenty; the daily run skips it.
            "--skip-orphans",
        ],
        docker_url=DOCKER_URL,
        network_mode=NETWORK,
        environment=SPARK_ENV,
        auto_remove="success",
        mount_tmp_dir=False,
    )

    before = report_file_counts()
    after = report_file_counts.override(task_id="report_file_counts_after")()

    before >> compact >> after


fleetstream_maintenance()
