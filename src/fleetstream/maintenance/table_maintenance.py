"""Iceberg table maintenance: compaction, snapshot expiry, orphan cleanup.

Streaming ingestion writes a file per partition per micro-batch. At a 30-second
trigger that is thousands of small files a day, and the cost is not mainly storage -
it is that every query must open, plan and read them. Metadata grows, planning slows,
and eventually the table becomes slow to read for reasons no query can explain.

Three operations keep it healthy:

``rewrite_data_files``
    Combines small files into target-sized ones.

``expire_snapshots``
    Drops old snapshots and the data files only they referenced. Without this,
    compaction *increases* storage forever, because the pre-compaction files stay
    alive for time travel.

``remove_orphan_files``
    Deletes files no metadata references - the debris of failed writes.

ORDERING MATTERS
    Compaction must come before expiry: compaction is what makes the old files
    unreferenced, and expiry is what actually reclaims them. Running expiry first
    simply does less work.

A NOTE ON BRONZE
    Compacting Bronze produces a snapshot the Silver streaming reader would refuse.
    That is handled by the reader setting ``streaming-skip-overwrite-snapshots``, but
    it means maintenance and the stream are coupled: run this on a schedule, not
    ad hoc, and keep the retention window comfortably longer than the streaming
    query's maximum downtime, or a restart may find its position expired.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from fleetstream.common.config import Settings, get_settings
from fleetstream.common.iceberg import (
    bronze_table,
    quarantine_table,
    reconciliation_table,
    silver_table,
)
from fleetstream.common.logging import configure_logging
from fleetstream.common.spark import build_spark

logger = logging.getLogger(__name__)

#: Files below this are candidates for rewriting.
DEFAULT_TARGET_FILE_SIZE_MB = 128
#: How long snapshots are retained. Must exceed the longest expected streaming
#: outage: a query resuming from a snapshot that has been expired cannot proceed.
DEFAULT_SNAPSHOT_RETENTION_HOURS = 72
#: Orphan files must be older than this. A short window would race with in-flight
#: writes and delete files a running job is still committing.
DEFAULT_ORPHAN_RETENTION_HOURS = 24


@dataclass
class MaintenanceResult:
    table: str
    rewritten_files: int = 0
    added_files: int = 0
    expired_snapshots: int = 0
    removed_orphans: int = 0
    error: str | None = None


def compact(spark, table: str, target_mb: int) -> tuple[int, int]:
    """Rewrite small files into larger ones. Returns (rewritten, added)."""
    target_bytes = target_mb * 1024 * 1024
    rows = spark.sql(
        f"""
        CALL {table.split(".")[0]}.system.rewrite_data_files(
            table => '{".".join(table.split(".")[1:])}',
            options => map(
                'target-file-size-bytes', '{target_bytes}',
                'min-input-files', '5',
                'partial-progress.enabled', 'true'
            )
        )
        """
    ).collect()
    if not rows:
        return 0, 0
    row = rows[0]
    return int(row[0] or 0), int(row[1] or 0)


def expire(spark, table: str, retention_hours: int) -> int:
    """Expire snapshots older than the retention window. Returns files deleted."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)
    stamp = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    rows = spark.sql(
        f"""
        CALL {table.split(".")[0]}.system.expire_snapshots(
            table => '{".".join(table.split(".")[1:])}',
            older_than => TIMESTAMP '{stamp}',
            retain_last => 5
        )
        """
    ).collect()
    return int(rows[0][0] or 0) if rows else 0


def remove_orphans(spark, table: str, retention_hours: int) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)
    stamp = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    rows = spark.sql(
        f"""
        CALL {table.split(".")[0]}.system.remove_orphan_files(
            table => '{".".join(table.split(".")[1:])}',
            older_than => TIMESTAMP '{stamp}'
        )
        """
    ).collect()
    return len(rows)


def maintain(
    spark,
    table: str,
    target_mb: int,
    snapshot_hours: int,
    orphan_hours: int,
    skip_orphans: bool,
) -> MaintenanceResult:
    result = MaintenanceResult(table=table)
    try:
        result.rewritten_files, result.added_files = compact(spark, table, target_mb)
        logger.info(
            "%s: compacted %d files into %d", table, result.rewritten_files, result.added_files
        )

        result.expired_snapshots = expire(spark, table, snapshot_hours)
        logger.info("%s: expiry removed %d files", table, result.expired_snapshots)

        if not skip_orphans:
            result.removed_orphans = remove_orphans(spark, table, orphan_hours)
            logger.info("%s: removed %d orphan files", table, result.removed_orphans)
    except Exception as exc:
        result.error = str(exc)
        logger.exception("maintenance failed for %s", table)
    return result


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    settings: Settings = get_settings()

    parser = argparse.ArgumentParser(description="Run Iceberg table maintenance")
    parser.add_argument("--tables", nargs="*", default=None, help="fully qualified table names")
    parser.add_argument("--target-file-size-mb", type=int, default=DEFAULT_TARGET_FILE_SIZE_MB)
    parser.add_argument(
        "--snapshot-retention-hours", type=int, default=DEFAULT_SNAPSHOT_RETENTION_HOURS
    )
    parser.add_argument(
        "--orphan-retention-hours", type=int, default=DEFAULT_ORPHAN_RETENTION_HOURS
    )
    parser.add_argument(
        "--skip-orphans",
        action="store_true",
        help="skip orphan removal (it lists the whole prefix and is expensive)",
    )
    args = parser.parse_args(argv)

    tables = args.tables or [
        bronze_table(settings),
        silver_table(settings),
        quarantine_table(settings),
        reconciliation_table(settings),
    ]

    spark = build_spark("fleetstream-maintenance", settings)
    try:
        results = [
            maintain(
                spark,
                table,
                args.target_file_size_mb,
                args.snapshot_retention_hours,
                args.orphan_retention_hours,
                args.skip_orphans,
            )
            for table in tables
        ]

        logger.info("--- maintenance summary ---")
        for r in results:
            status = (
                f"ERROR: {r.error}"
                if r.error
                else (
                    f"compacted {r.rewritten_files}->{r.added_files}, "
                    f"expired {r.expired_snapshots} files, {r.removed_orphans} orphans"
                )
            )
            logger.info("  %s: %s", r.table, status)

        failed = [r for r in results if r.error]
        if failed:
            logger.error("%d of %d tables failed maintenance", len(failed), len(results))
            return 1
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
