"""Daily FleetStream batch pipeline.

    validate source -> dbt build -> dbt test -> quality report -> reconcile -> notify

Two principles shape this DAG.

ORCHESTRATE, DO NOT PROCESS
    Airflow starts containers and checks results. It never loads a dataframe. The
    heavy work happens in the dbt image, launched through the Docker API. A
    transformation that runs out of memory kills its own container, not the
    scheduler.

EVERY TASK IS IDEMPOTENT
    Airflow retries, and a retry is only safe if re-running a task is
    indistinguishable from running it once. Nothing here appends: dbt models merge on
    deterministic keys, and the checks are read-only. This is also why a failed run
    can simply be cleared and re-run rather than needing manual cleanup first.

XCom carries only small control values - counts, dates, statuses. Data stays in the
lakehouse, which is what XCom's size limit is trying to tell you.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

DBT_IMAGE = "fleetstream/dbt:1.12.4"
NETWORK = os.environ.get("FLEETSTREAM_NETWORK", "fleetstream")
DOCKER_URL = "unix://var/run/docker.sock"

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "fleetstream")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")

#: Fail the run if Silver has not been written to within this window. The streaming
#: layer is meant to be continuous, so staleness is a real incident rather than a
#: scheduling quirk.
MAX_SOURCE_STALENESS = timedelta(hours=6)

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-platform",
    "retries": 2,
    # Exponential backoff: a transient warehouse restart resolves in seconds, but
    # retrying a genuinely overloaded cluster every 30s makes the overload worse.
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "depends_on_past": False,
}

DBT_ENV = {
    "DBT_TARGET": "trino",
    "TRINO_HOST": TRINO_HOST,
    "TRINO_PORT": str(TRINO_PORT),
    "TRINO_USER": TRINO_USER,
    "TRINO_CATALOG": TRINO_CATALOG,
}


def query(sql: str) -> list[tuple]:
    """Run a read-only query against Trino and return all rows."""
    from trino.dbapi import connect  # noqa: PLC0415 - provider import, keep it local

    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        conn.close()


def dbt_task(task_id: str, command: list[str]) -> DockerOperator:
    """A dbt invocation in its own container.

    ``auto_remove`` keeps the host clean across hundreds of scheduled runs, and
    ``mount_tmp_dir=False`` is required because Airflow's default temp mount does not
    exist on the Docker host when Airflow is itself containerised - a mismatch that
    otherwise fails every task with an unhelpful mount error.
    """
    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        command=command,
        docker_url=DOCKER_URL,
        network_mode=NETWORK,
        environment=DBT_ENV,
        auto_remove="success",
        mount_tmp_dir=False,
        retrieve_output=False,
    )


@dag(
    dag_id="fleetstream_daily",
    description="Daily transformation, quality and reconciliation pipeline",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # Catchup off: these models reprocess a trailing window on every run, so a
    # backfill of thirty separate days would recompute the same rows thirty times
    # and finish with exactly the state a single run produces.
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fleetstream", "daily", "gold"],
    doc_md=__doc__,
)
def fleetstream_daily() -> None:
    @task
    def validate_source_freshness() -> dict[str, Any]:
        """Confirm the streaming layer is actually delivering before transforming.

        Running dbt against a stalled source produces a Gold layer that looks healthy
        and is quietly hours out of date - far worse than a failed run, because
        nobody investigates a green pipeline.
        """
        rows = query(
            f"""
            SELECT max(event_time), max(processed_at), count(*)
            FROM {TRINO_CATALOG}.silver.telemetry
            """
        )
        latest_event, latest_processed, total = rows[0]
        if total == 0:
            raise ValueError("silver.telemetry is empty - the streaming layer has not run")

        now = pendulum.now("UTC")
        processed = pendulum.instance(latest_processed) if latest_processed else None
        staleness = now - processed if processed else None

        if staleness and staleness > MAX_SOURCE_STALENESS:
            raise ValueError(
                f"silver.telemetry last written {staleness.in_words()} ago, "
                f"which exceeds the {MAX_SOURCE_STALENESS} threshold"
            )

        logger.info("source is fresh: %s rows, latest event %s", f"{total:,}", latest_event)
        return {
            "row_count": int(total),
            "latest_event_time": str(latest_event),
            "staleness_seconds": int(staleness.total_seconds()) if staleness else None,
        }

    @task
    def check_reconciliation() -> dict[str, Any]:
        """Assert the streaming layer accounted for every row it read.

        source - duplicates - rejected == output, per batch. A batch that fails this
        lost rows, and that must surface before those numbers are published.
        """
        rows = query(
            f"""
            SELECT
                count(*)                                                        AS batches,
                count_if(source_count - duplicate_count - rejected_count <> output_count) AS unbalanced,
                coalesce(sum(source_count), 0)                                   AS total_source,
                coalesce(sum(rejected_count), 0)                                 AS total_rejected,
                coalesce(sum(output_count), 0)                                   AS total_output
            FROM {TRINO_CATALOG}.platform.reconciliation
            WHERE recorded_at >= current_timestamp - interval '2' day
            """
        )
        batches, unbalanced, source, rejected, output = rows[0]
        if batches == 0:
            logger.warning("no reconciliation rows in the last 2 days - nothing to verify")
            return {"batches": 0}

        if unbalanced:
            raise ValueError(
                f"{unbalanced} of {batches} batches do not reconcile: rows were lost "
                "between the source and the target"
            )

        logger.info(
            "reconciliation clean across %d batches: %s source, %s rejected, %s output",
            batches, f"{source:,}", f"{rejected:,}", f"{output:,}",
        )
        return {
            "batches": int(batches),
            "source": int(source),
            "rejected": int(rejected),
            "output": int(output),
        }

    @task
    def quality_report() -> dict[str, Any]:
        """Summarise rejections. Reports rather than fails.

        A rising rejection rate usually means an upstream producer changed, which is
        a conversation, not a reason to withhold today's numbers. The pipeline
        surfaces it and keeps going; the alert is what escalates.
        """
        rows = query(
            f"""
            SELECT rule_name, rule_severity, count(*) AS violations
            FROM {TRINO_CATALOG}.silver.telemetry_quarantine
            WHERE quarantined_at >= current_timestamp - interval '1' day
            GROUP BY rule_name, rule_severity
            ORDER BY violations DESC
            """
        )
        if not rows:
            logger.info("no records quarantined in the last day")
            return {"total_violations": 0, "rules_fired": 0}

        total = sum(r[2] for r in rows)
        for rule, severity, count in rows:
            logger.info("  [%s] %s: %s", severity, rule, f"{count:,}")
        return {
            "total_violations": int(total),
            "rules_fired": len(rows),
            "top_rule": rows[0][0],
        }

    @task
    def publish_summary(
        freshness: dict[str, Any],
        reconciliation: dict[str, Any],
        quality: dict[str, Any],
    ) -> str:
        """Final report. In production this would post to Slack or PagerDuty."""
        gold = query(
            f"""
            SELECT event_date, active_vehicles, total_trips,
                   total_distance_km, total_incidents
            FROM {TRINO_CATALOG}.gold.fleet_daily_summary
            ORDER BY event_date DESC
            LIMIT 1
            """
        )
        lines = [
            "FleetStream daily pipeline complete",
            f"  source rows      : {freshness['row_count']:,}",
            f"  batches verified : {reconciliation.get('batches', 0)}",
            f"  DQ violations    : {quality['total_violations']:,} "
            f"across {quality['rules_fired']} rules",
        ]
        if gold:
            date, vehicles, trips, distance, incidents = gold[0]
            lines += [
                f"  latest gold day  : {date}",
                f"    active vehicles: {vehicles:,}",
                f"    trips          : {trips:,}",
                f"    distance km    : {distance:,.0f}" if distance else "    distance km    : n/a",
                f"    incidents      : {incidents:,}",
            ]
        summary = "\n".join(lines)
        logger.info(summary)
        return summary

    # dbt build runs models and their tests together, so a model that produces bad
    # data fails before anything downstream of it is built on top.
    build = dbt_task("dbt_build", ["build", "--target", "trino"])
    test = dbt_task("dbt_test", ["test", "--target", "trino"])

    freshness = validate_source_freshness()
    reconciliation = check_reconciliation()
    quality = quality_report()

    freshness >> reconciliation >> build >> test >> quality
    publish_summary(freshness, reconciliation, quality)


fleetstream_daily()
