"""Per-run counters that make "rows went missing" an answerable question.

When a downstream count looks wrong, the useful question is not "is data missing?"
but "which stage lost it, and did it get rejected, deduplicated, or dropped?".
Recording source / duplicate / rejected / output at each stage turns that from an
investigation into a query:

    source_count - duplicate_count - rejected_count == output_count

A run where that identity fails has genuinely lost rows, and the stage where it
first fails is the one to investigate.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from fleetstream.common.config import Settings, get_settings
from fleetstream.common.iceberg import reconciliation_table

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Counts:
    """Row counts for one stage of one run."""

    run_id: str
    stage: str
    source_count: int
    duplicate_count: int = 0
    rejected_count: int = 0
    output_count: int = 0
    batch_id: int | None = None

    @property
    def balances(self) -> bool:
        """Whether the counts account for every source row."""
        return self.source_count - self.duplicate_count - self.rejected_count == self.output_count

    @property
    def unaccounted(self) -> int:
        """Rows that vanished without being explained. Non-zero means data loss."""
        return self.source_count - self.duplicate_count - self.rejected_count - self.output_count


RECONCILIATION_SCHEMA = (
    "run_id STRING, stage STRING, batch_id BIGINT, source_count BIGINT, "
    "duplicate_count BIGINT, rejected_count BIGINT, output_count BIGINT, "
    "recorded_at TIMESTAMP"
)


def record(spark: SparkSession, counts: Counts, settings: Settings | None = None) -> None:
    """Append one stage's counters to the reconciliation table.

    Failures here are logged but never raised. Losing an observability row must not
    fail an otherwise healthy pipeline - that would turn the monitoring into the
    outage it exists to detect.
    """
    cfg = settings or get_settings()
    try:
        row = (
            counts.run_id,
            counts.stage,
            counts.batch_id,
            int(counts.source_count),
            int(counts.duplicate_count),
            int(counts.rejected_count),
            int(counts.output_count),
            datetime.now(timezone.utc),
        )
        df = spark.createDataFrame([row], schema=RECONCILIATION_SCHEMA)
        df.writeTo(reconciliation_table(cfg)).append()

        if not counts.balances:
            logger.warning(
                "reconciliation does not balance for run=%s stage=%s: "
                "source=%d duplicates=%d rejected=%d output=%d (unaccounted=%d)",
                counts.run_id,
                counts.stage,
                counts.source_count,
                counts.duplicate_count,
                counts.rejected_count,
                counts.output_count,
                counts.unaccounted,
            )
        else:
            logger.info(
                "reconciled run=%s stage=%s: source=%d duplicates=%d rejected=%d output=%d",
                counts.run_id,
                counts.stage,
                counts.source_count,
                counts.duplicate_count,
                counts.rejected_count,
                counts.output_count,
            )
    except Exception:
        logger.exception("failed to record reconciliation counters for run=%s", counts.run_id)
