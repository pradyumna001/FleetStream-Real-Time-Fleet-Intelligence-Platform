"""Shared fixtures for tests that need a real Spark session and Iceberg catalog.

These run inside the Spark container (`make test-integration`), not on the host: the
host has no JVM and Python 3.13, which PySpark 3.5 does not target.

Every test writes into its own throwaway namespace so a failure cannot leave state
that makes the next run pass or fail for the wrong reason.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest

from fleetstream.common.config import Settings, get_settings
from fleetstream.common.spark import build_spark


@pytest.fixture(scope="session")
def settings() -> Settings:
    return get_settings()


@pytest.fixture(scope="session")
def spark(settings: Settings) -> Iterator:
    """One session for the whole run - creating a SparkSession costs ~10 seconds."""
    session = build_spark("fleetstream-integration-tests", settings)
    yield session
    session.stop()


@pytest.fixture
def scratch_namespace(spark, settings: Settings) -> Iterator[str]:
    """A unique namespace, dropped afterwards.

    Randomised rather than a fixed name so that concurrent runs, or a previous run
    that died before cleanup, cannot interfere with this one.
    """
    name = f"{settings.catalog.name}.it_{uuid.uuid4().hex[:10]}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name}")
    yield name
    try:
        for row in spark.sql(f"SHOW TABLES IN {name}").collect():
            spark.sql(f"DROP TABLE IF EXISTS {name}.{row['tableName']}")
        spark.sql(f"DROP NAMESPACE IF EXISTS {name}")
    except Exception:
        pass
