"""Iceberg namespace and table bootstrap.

Creating tables explicitly, rather than letting a write infer them, is what lets
the platform control partitioning, file sizing and the merge-on-read settings the
Silver MERGE depends on. Every statement is ``IF NOT EXISTS``, so bootstrap is
idempotent and safe to run at the start of any job or DAG.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fleetstream.common.config import Settings, get_settings
from fleetstream.common.schemas import (
    bronze_table_ddl,
    quarantine_table_ddl,
    reconciliation_table_ddl,
    silver_table_ddl,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

BRONZE_TELEMETRY = "telemetry_raw"
SILVER_TELEMETRY = "telemetry"
SILVER_QUARANTINE = "telemetry_quarantine"
PLATFORM_RECONCILIATION = "reconciliation"


def table_name(settings: Settings, namespace: str, table: str) -> str:
    """Fully qualified ``catalog.namespace.table`` identifier."""
    return f"{settings.catalog.name}.{namespace}.{table}"


def bootstrap(spark: SparkSession, settings: Settings | None = None) -> None:
    """Create namespaces and core tables if they do not already exist."""
    cfg = settings or get_settings()
    cat = cfg.catalog.name

    for namespace in (
        cfg.catalog.bronze,
        cfg.catalog.silver,
        cfg.catalog.gold,
        cfg.catalog.platform,
    ):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{namespace}")
        logger.info("namespace ready: %s.%s", cat, namespace)

    statements = (
        bronze_table_ddl(cat, cfg.catalog.bronze, BRONZE_TELEMETRY),
        silver_table_ddl(cat, cfg.catalog.silver, SILVER_TELEMETRY),
        quarantine_table_ddl(cat, cfg.catalog.silver, SILVER_QUARANTINE),
        reconciliation_table_ddl(cat, cfg.catalog.platform, PLATFORM_RECONCILIATION),
    )
    for ddl in statements:
        spark.sql(ddl)
    logger.info("iceberg bootstrap complete")


def bronze_table(settings: Settings | None = None) -> str:
    cfg = settings or get_settings()
    return table_name(cfg, cfg.catalog.bronze, BRONZE_TELEMETRY)


def silver_table(settings: Settings | None = None) -> str:
    cfg = settings or get_settings()
    return table_name(cfg, cfg.catalog.silver, SILVER_TELEMETRY)


def quarantine_table(settings: Settings | None = None) -> str:
    cfg = settings or get_settings()
    return table_name(cfg, cfg.catalog.silver, SILVER_QUARANTINE)


def reconciliation_table(settings: Settings | None = None) -> str:
    cfg = settings or get_settings()
    return table_name(cfg, cfg.catalog.platform, PLATFORM_RECONCILIATION)
