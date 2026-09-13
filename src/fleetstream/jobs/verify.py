"""End-to-end assertions that the platform did what it claims.

This is not a smoke test. Each check corresponds to a specific claim made about the
architecture, and each one is written so that it *fails* when the claim is false:

* rows are conserved between Bronze and Silver,
* duplicates were actually present and were actually removed,
* re-processing does not change the Silver row count (the idempotency proof),
* injected bad records are in quarantine with a reason, and are not in Silver,
* late-arriving events are attributed to their event date, not their arrival date,
* the business questions the platform exists to answer return data.

A check that cannot run because prerequisite data is missing is reported as SKIP
rather than PASS. Silently passing on an empty table is how a broken pipeline gets
signed off.
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path

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

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"


@dataclass
class Check:
    name: str
    status: str
    detail: str
    claim: str = ""


@dataclass
class Report:
    checks: list[Check] = field(default_factory=list)

    def add(self, name: str, status: str, detail: str, claim: str = "") -> None:
        self.checks.append(Check(name, status, detail, claim))

    @property
    def failed(self) -> list[Check]:
        return [c for c in self.checks if c.status == FAIL]

    def render(self) -> str:
        width = max(len(c.name) for c in self.checks) if self.checks else 10
        lines = ["", "=" * 78, "FLEETSTREAM VERIFICATION", "=" * 78]
        for c in self.checks:
            lines.append(f"[{c.status}] {c.name.ljust(width)}  {c.detail}")
        passed = sum(1 for c in self.checks if c.status == PASS)
        skipped = sum(1 for c in self.checks if c.status == SKIP)
        lines += [
            "-" * 78,
            f"{passed} passed, {len(self.failed)} failed, {skipped} skipped",
            "=" * 78,
            "",
        ]
        return "\n".join(lines)


def _count(spark, sql: str) -> int:
    return int(spark.sql(sql).collect()[0][0] or 0)


def check_bronze_has_data(spark, settings: Settings, report: Report) -> int:
    bronze = bronze_table(settings)
    total = _count(spark, f"SELECT count(*) FROM {bronze}")
    if total == 0:
        report.add(
            "bronze ingested",
            FAIL,
            "Bronze is empty - run `make simulate` and let the stream catch up",
        )
    else:
        distinct = _count(spark, f"SELECT count(DISTINCT event_id) FROM {bronze}")
        report.add(
            "bronze ingested",
            PASS,
            f"{total:,} rows, {distinct:,} distinct event_id",
        )
    return total


def check_duplicates_were_present_and_removed(spark, settings: Settings, report: Report) -> None:
    """Deduplication is only proven if duplicates actually reached Bronze."""
    bronze, silver = bronze_table(settings), silver_table(settings)
    total = _count(spark, f"SELECT count(*) FROM {bronze}")
    distinct = _count(spark, f"SELECT count(DISTINCT event_id) FROM {bronze}")
    duplicates = total - distinct

    if duplicates == 0:
        report.add(
            "duplicates injected",
            SKIP,
            "Bronze contains no duplicate event_id - dedup is untested by this run",
            claim="the simulator injects duplicate events",
        )
    else:
        report.add(
            "duplicates injected",
            PASS,
            f"{duplicates:,} duplicate rows reached Bronze ({duplicates / total:.2%})",
        )

    silver_total = _count(spark, f"SELECT count(*) FROM {silver}")
    silver_distinct = _count(spark, f"SELECT count(DISTINCT event_id) FROM {silver}")
    if silver_total == 0:
        report.add("silver deduplicated", SKIP, "Silver is empty")
    elif silver_total != silver_distinct:
        report.add(
            "silver deduplicated",
            FAIL,
            f"Silver has {silver_total - silver_distinct:,} duplicate event_id rows - "
            "the MERGE key is not unique",
            claim="Silver holds exactly one row per event_id",
        )
    else:
        report.add(
            "silver deduplicated",
            PASS,
            f"{silver_total:,} rows, all event_id distinct",
        )


def check_row_conservation(spark, settings: Settings, report: Report) -> None:
    """Every distinct Bronze event should be accounted for: in Silver or quarantined."""
    bronze, silver, quarantine = (
        bronze_table(settings),
        silver_table(settings),
        quarantine_table(settings),
    )
    distinct_bronze = _count(spark, f"SELECT count(DISTINCT event_id) FROM {bronze}")
    silver_rows = _count(spark, f"SELECT count(*) FROM {silver}")
    quarantined = _count(
        spark, f"SELECT count(DISTINCT coalesce(event_id, quarantine_id)) FROM {quarantine}"
    )

    if distinct_bronze == 0:
        report.add("rows conserved", SKIP, "no Bronze data")
        return

    accounted = silver_rows + quarantined
    missing = distinct_bronze - accounted
    # A small shortfall is expected while the stream is still catching up, so the
    # check tolerates a lag rather than demanding the stream be idle.
    tolerance = max(int(distinct_bronze * 0.02), 500)

    if missing > tolerance:
        report.add(
            "rows conserved",
            FAIL,
            f"{missing:,} of {distinct_bronze:,} Bronze events are neither in Silver "
            f"nor quarantined (tolerance {tolerance:,}) - rows were lost",
            claim="no record disappears without being explained",
        )
    else:
        report.add(
            "rows conserved",
            PASS,
            f"{silver_rows:,} in Silver + {quarantined:,} quarantined "
            f"= {accounted:,} of {distinct_bronze:,} distinct Bronze events "
            f"({missing:,} still in flight)",
        )


def check_quarantine_is_populated_and_attributed(spark, settings: Settings, report: Report) -> None:
    quarantine, silver = quarantine_table(settings), silver_table(settings)
    total = _count(spark, f"SELECT count(*) FROM {quarantine}")

    if total == 0:
        report.add(
            "quarantine populated",
            SKIP,
            "nothing quarantined - either no bad data was injected or the split is not running",
        )
        return

    unattributed = _count(
        spark,
        f"SELECT count(*) FROM {quarantine} "
        "WHERE rule_name IS NULL OR failure_reason IS NULL OR run_id IS NULL",
    )
    if unattributed:
        report.add(
            "quarantine populated",
            FAIL,
            f"{unattributed:,} quarantined rows lack a rule, reason or run_id - "
            "they cannot be investigated or replayed",
            claim="every rejected record carries why it was rejected",
        )
        return

    rules = spark.sql(
        f"SELECT rule_name, count(*) AS n FROM {quarantine} GROUP BY rule_name ORDER BY n DESC"
    ).collect()
    breakdown = ", ".join(f"{r['rule_name']}={r['n']}" for r in rules[:5])
    report.add(
        "quarantine populated", PASS, f"{total:,} violations across {len(rules)} rules: {breakdown}"
    )

    # Nothing quarantined may also be present in Silver.
    leaked = _count(
        spark,
        f"SELECT count(*) FROM {silver} s WHERE s.event_id IN "
        f"(SELECT event_id FROM {quarantine} "
        "WHERE rule_severity = 'critical' AND event_id IS NOT NULL)",
    )
    if leaked:
        report.add(
            "quarantine is exclusive",
            FAIL,
            f"{leaked:,} critically-invalid records also reached Silver",
            claim="a record failing a critical rule never reaches Silver",
        )
    else:
        report.add("quarantine is exclusive", PASS, "no critically-invalid record reached Silver")


def check_idempotency(spark, settings: Settings, report: Report) -> None:
    """THE central claim: re-processing the same events must not change the row count.

    This re-runs the real Silver write path over events already in Silver. If the
    sink were a plain append, the count would rise by the size of the replayed batch.
    Because it is a MERGE on a deterministic event_id, it must not move at all.
    """
    from pyspark.sql import functions as F

    from fleetstream.streaming.silver_transform import write_batch

    silver, bronze = silver_table(settings), bronze_table(settings)
    before = _count(spark, f"SELECT count(*) FROM {silver}")
    if before == 0:
        report.add("replay is idempotent", SKIP, "Silver is empty, nothing to replay")
        return

    sample_ids = [
        r["event_id"] for r in spark.sql(f"SELECT event_id FROM {silver} LIMIT 500").collect()
    ]
    if not sample_ids:
        report.add("replay is idempotent", SKIP, "could not sample Silver")
        return

    replay = spark.table(bronze).filter(F.col("event_id").isin(sample_ids))
    replay_count = replay.count()
    if replay_count == 0:
        report.add("replay is idempotent", SKIP, "sampled events not found in Bronze")
        return

    write_batch(replay, -1, settings, "verify-replay")

    after = _count(spark, f"SELECT count(*) FROM {silver}")
    if after != before:
        report.add(
            "replay is idempotent",
            FAIL,
            f"replaying {replay_count:,} events changed the Silver row count "
            f"{before:,} -> {after:,} (delta {after - before:+,}) - the sink is not idempotent",
            claim="MERGE on a deterministic event_id makes replay safe",
        )
    else:
        report.add(
            "replay is idempotent",
            PASS,
            f"replayed {replay_count:,} events; Silver row count unchanged at {before:,}",
        )


def check_late_events_use_event_date(spark, settings: Settings, report: Report) -> None:
    """Late arrivals must keep the time they happened, not the time they landed.

    Checked by comparing Silver against Bronze for the same event_id. If any stage
    had overwritten event_time with a processing timestamp, every downstream daily
    aggregate would silently attribute those readings to the wrong day - the exact
    failure the event-time design exists to prevent.

    Spark SQL is used here, not Trino: this job runs on Spark, so `unix_timestamp`
    and `to_date` rather than `date_diff` and `date`.
    """
    silver, bronze = silver_table(settings), bronze_table(settings)

    lag_expr = "(unix_timestamp(ingestion_time) - unix_timestamp(event_time)) / 60.0"
    lagged = _count(
        spark,
        f"SELECT count(*) FROM {silver} WHERE {lag_expr} > 5",
    )
    if lagged == 0:
        report.add(
            "event_time preserved",
            SKIP,
            "no rows with meaningful ingestion lag - lateness handling untested by this run",
        )
        return

    # event_time must be byte-identical to what Bronze recorded.
    altered = _count(
        spark,
        f"""
        SELECT count(*)
        FROM {silver} s
        JOIN (SELECT event_id, min(event_time) AS event_time FROM {bronze} GROUP BY event_id) b
          ON s.event_id = b.event_id
        WHERE s.event_time <> b.event_time
        """,
    )

    # Events that crossed a day boundary between happening and arriving are the ones
    # where a mix-up would actually change a daily total.
    cross_day = _count(
        spark,
        f"SELECT count(*) FROM {silver} WHERE to_date(event_time) <> to_date(ingestion_time)",
    )

    if altered:
        report.add(
            "event_time preserved",
            FAIL,
            f"{altered:,} Silver rows have an event_time that differs from Bronze - "
            "processing time has overwritten event time",
            claim="event_time, not ingestion_time, drives partitioning",
        )
    else:
        # The lagged count is NOT a count of injected late arrivals. The simulator
        # compresses hours of simulated time into minutes of wall clock, so almost
        # every event carries a large ingestion lag by construction. Reporting it as
        # "late events" would overstate what this proves, so it is labelled for what
        # it is; the assertion that carries the weight is `altered == 0`.
        report.add(
            "event_time preserved",
            PASS,
            f"event_time identical to Bronze for every row "
            f"({lagged:,} rows lag >5min, inflated by the simulator's time "
            f"compression; {cross_day:,} cross a day boundary)",
        )


def check_reconciliation_balances(spark, settings: Settings, report: Report) -> None:
    table = reconciliation_table(settings)
    total = _count(spark, f"SELECT count(*) FROM {table}")
    if total == 0:
        report.add("reconciliation recorded", SKIP, "no reconciliation rows written yet")
        return

    unbalanced = _count(
        spark,
        f"SELECT count(*) FROM {table} "
        "WHERE source_count - duplicate_count - rejected_count <> output_count",
    )
    if unbalanced:
        report.add(
            "reconciliation balances",
            FAIL,
            f"{unbalanced:,} of {total:,} batches do not balance - rows unaccounted for",
            claim="source = duplicates + rejected + output for every batch",
        )
    else:
        report.add("reconciliation balances", PASS, f"all {total:,} recorded batches balance")


def check_business_questions(spark, settings: Settings, report: Report) -> None:
    """The platform exists to answer these. If they return nothing, it has failed."""
    silver = silver_table(settings)
    overheating = spark.sql(
        f"""
        SELECT vehicle_id, count(*) AS overheating_events
        FROM {silver}
        WHERE engine_temperature > 100
        GROUP BY vehicle_id
        ORDER BY overheating_events DESC
        LIMIT 5
        """
    ).collect()

    if not overheating:
        report.add(
            "business query answers",
            SKIP,
            "no overheating events recorded yet",
        )
        return

    top = ", ".join(f"{r['vehicle_id']}={r['overheating_events']}" for r in overheating)
    report.add("business query answers", PASS, f"top overheating vehicles: {top}")


def check_manifest_rates(spark, settings: Settings, report: Report, manifest: Path) -> None:
    """Compare what the simulator injected against what the pipeline caught."""
    if not manifest.exists():
        report.add("manifest reconciliation", SKIP, f"no run manifest at {manifest}")
        return

    data = json.loads(manifest.read_text(encoding="utf-8"))
    stats = data.get("stats", {})
    expected_invalid = int(stats.get("invalid", 0))
    if expected_invalid == 0:
        report.add("manifest reconciliation", SKIP, "manifest records no injected invalid records")
        return

    quarantined = _count(
        spark,
        f"SELECT count(DISTINCT event_id) FROM {quarantine_table(settings)} "
        "WHERE event_id IS NOT NULL",
    )
    # Not an equality: a corrupted record may lose its event_id entirely, and the
    # stream may still be catching up. The point is that the pipeline caught a
    # comparable number, not that it caught precisely that set.
    ratio = quarantined / expected_invalid if expected_invalid else 0
    if ratio < 0.5:
        report.add(
            "manifest reconciliation",
            FAIL,
            f"simulator injected {expected_invalid:,} invalid records but only "
            f"{quarantined:,} were quarantined ({ratio:.0%})",
            claim="injected bad data is caught by the quality rules",
        )
    else:
        report.add(
            "manifest reconciliation",
            PASS,
            f"injected {expected_invalid:,} invalid records, quarantined {quarantined:,}",
        )


def main() -> int:
    configure_logging()
    settings = get_settings()
    spark = build_spark("fleetstream-verify", settings)
    report = Report()

    try:
        total = check_bronze_has_data(spark, settings, report)
        if total:
            check_duplicates_were_present_and_removed(spark, settings, report)
            check_row_conservation(spark, settings, report)
            check_quarantine_is_populated_and_attributed(spark, settings, report)
            check_reconciliation_balances(spark, settings, report)
            check_late_events_use_event_date(spark, settings, report)
            check_business_questions(spark, settings, report)
            check_manifest_rates(spark, settings, report, Path("/manifests/latest.json"))
            # Runs last: it writes to Silver, so it must not perturb earlier counts.
            check_idempotency(spark, settings, report)

        print(report.render())
        return 1 if report.failed else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
