"""Spark session construction, with the Iceberg REST catalog wired in.

Every job builds its session here so catalog configuration exists in exactly one
place. A job that configured its own catalog would be one rename away from
writing to a different warehouse than the rest of the platform.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fleetstream.common.config import Settings, get_settings

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def build_spark(app_name: str, settings: Settings | None = None, **extra: str) -> SparkSession:
    """Create a Spark session configured for the FleetStream Iceberg catalog.

    ``extra`` overrides or adds raw Spark configuration; streaming jobs use it to
    set shuffle partitions appropriate to their batch size.
    """
    from pyspark.sql import SparkSession

    cfg = settings or get_settings()
    cat = cfg.catalog.name
    prefix = f"spark.sql.catalog.{cat}"

    conf: dict[str, str] = {
        # --- Iceberg catalog ---------------------------------------------------
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": cfg.catalog.rest_uri,
        f"{prefix}.warehouse": cfg.storage.warehouse_uri,
        # Iceberg talks to object storage through its own S3FileIO rather than the
        # Hadoop S3A client - fewer moving parts and no AWS SDK version conflict.
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.s3.endpoint": cfg.storage.endpoint,
        # MinIO serves bucket-in-path URLs, not virtual-host style.
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.s3.access-key-id": cfg.storage.access_key,
        f"{prefix}.s3.secret-access-key": cfg.storage.secret_key,
        f"{prefix}.client.region": cfg.storage.region,
        "spark.sql.defaultCatalog": cat,
        # --- Hadoop S3A, used ONLY for streaming checkpoints -------------------
        # Iceberg reads and writes table data through its own S3FileIO above. These
        # settings are for Spark's filesystem layer, which writes the checkpoint
        # directory - a separate code path with separate configuration.
        "spark.hadoop.fs.s3a.endpoint": cfg.storage.endpoint,
        "spark.hadoop.fs.s3a.access.key": cfg.storage.access_key,
        "spark.hadoop.fs.s3a.secret.key": cfg.storage.secret_key,
        # MinIO serves bucket-in-path URLs rather than virtual-host style.
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        # The warehouse URI uses the s3:// scheme (Iceberg's S3FileIO accepts it),
        # but Hadoop registers only s3a://. Table maintenance procedures such as
        # remove_orphan_files list the table location through Hadoop rather than
        # through FileIO, and without this mapping they fail with
        # "No FileSystem for scheme s3".
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        # --- Correctness -------------------------------------------------------
        # Pin the session timezone. Without this Spark uses the JVM default, which
        # differs between the host, the containers and CI, and silently shifts
        # every event_time-derived date partition.
        "spark.sql.session.timeZone": "UTC",
        # --- Performance -------------------------------------------------------
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        # AQE skew handling: the simulator deliberately creates a hot vehicle, so
        # this is exercised rather than decorative.
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.shuffle.partitions": "16",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        # Streaming state: RocksDB keeps dedup state off the JVM heap, which matters
        # once the watermark window holds millions of event_ids.
        "spark.sql.streaming.stateStore.providerClass": (
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
        ),
        "spark.sql.streaming.metricsEnabled": "true",
    }
    conf.update(extra)

    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session '%s' ready against catalog '%s'", app_name, cat)
    return spark
