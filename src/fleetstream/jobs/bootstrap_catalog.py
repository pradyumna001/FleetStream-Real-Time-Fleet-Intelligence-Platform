"""Create the Iceberg namespaces and core tables, then prove the stack works.

Run as a one-shot before the streaming jobs start. Two things are checked, because
"the catalog responded" and "data actually reached object storage" are different
failures with the same symptom until you look:

1. the REST catalog accepts DDL, and
2. a round-trip write/read lands bytes in MinIO and reads them back.

The smoke table is created and dropped in the platform namespace rather than
written into Bronze, which must stay append-only and free of synthetic rows.
"""

from __future__ import annotations

import logging
import sys

from fleetstream.common.config import get_settings
from fleetstream.common.iceberg import bootstrap
from fleetstream.common.logging import configure_logging
from fleetstream.common.spark import build_spark

logger = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    settings = get_settings()
    spark = build_spark("fleetstream-bootstrap", settings)

    try:
        bootstrap(spark, settings)

        cat = settings.catalog.name
        smoke = f"{cat}.{settings.catalog.platform}.smoke_test"
        spark.sql(f"DROP TABLE IF EXISTS {smoke}")
        spark.sql(f"CREATE TABLE {smoke} (id BIGINT, note STRING) USING iceberg")
        spark.sql(f"INSERT INTO {smoke} VALUES (1, 'round-trip through MinIO')")
        rows = spark.sql(f"SELECT id, note FROM {smoke}").collect()

        if len(rows) != 1 or rows[0]["id"] != 1:
            logger.error("smoke test read back unexpected data: %s", rows)
            return 1
        logger.info("smoke test passed: %s", rows[0]["note"])
        spark.sql(f"DROP TABLE {smoke}")

        logger.info("--- tables now registered ---")
        for namespace in (
            settings.catalog.bronze,
            settings.catalog.silver,
            settings.catalog.platform,
        ):
            for row in spark.sql(f"SHOW TABLES IN {cat}.{namespace}").collect():
                logger.info("  %s.%s.%s", cat, namespace, row["tableName"])
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
