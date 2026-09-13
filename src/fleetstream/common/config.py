"""Central configuration, sourced from environment variables.

Deliberately stdlib-only. This module is imported on the host, inside the Spark
containers and inside Airflow; a settings library would have to be pinned into
three separate images to buy very little.

Every value has a default that matches ``.env.example`` running under Docker
Compose, so a fresh clone works with no configuration at all.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache


def _env(key: str, default: str) -> str:
    value = os.environ.get(key, "").strip()
    return value or default


def _env_int(key: str, default: int) -> int:
    raw = _env(key, str(default))
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - misconfiguration should be loud
        raise ValueError(f"Environment variable {key}={raw!r} is not an integer") from exc


def _env_float(key: str, default: float) -> float:
    raw = _env(key, str(default))
    try:
        return float(raw)
    except ValueError as exc:  # pragma: no cover
        raise ValueError(f"Environment variable {key}={raw!r} is not a float") from exc


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    telemetry_topic: str = field(default_factory=lambda: _env("TELEMETRY_TOPIC", "fleet.telemetry"))
    incidents_topic: str = field(default_factory=lambda: _env("INCIDENTS_TOPIC", "fleet.incidents"))
    dlq_topic: str = field(default_factory=lambda: _env("DLQ_TOPIC", "fleet.telemetry.dlq"))
    partitions: int = field(default_factory=lambda: _env_int("TELEMETRY_PARTITIONS", 12))


@dataclass(frozen=True)
class StorageConfig:
    """MinIO locally; the same settings point at real S3 unchanged apart from the endpoint."""

    endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://minio:9000"))
    access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "fleetstream"))
    secret_key: str = field(
        default_factory=lambda: _env("S3_SECRET_KEY", "fleetstream-local-secret")
    )
    region: str = field(default_factory=lambda: _env("S3_REGION", "us-east-1"))
    bucket: str = field(default_factory=lambda: _env("WAREHOUSE_BUCKET", "fleetstream"))
    #: See :meth:`checkpoint_uri` for why this is a local volume and not s3a://.
    checkpoint_root: str = field(
        default_factory=lambda: _env("CHECKPOINT_ROOT", "s3a://fleetstream/checkpoints")
    )

    @property
    def warehouse_uri(self) -> str:
        return f"s3://{self.bucket}/warehouse"

    def checkpoint_uri(self, job: str) -> str:
        """Where a streaming query keeps its checkpoint.

        Object storage, and that is forced rather than merely preferred.

        The Silver query streams *from* an Iceberg table, and Iceberg's streaming
        source writes its offset metadata into the checkpoint directory using the
        catalog's ``S3FileIO``. Handed a ``file:`` URI, S3FileIO fails outright with
        "Invalid S3 URI, cannot determine scheme". The checkpoint location must
        therefore be a URI that BOTH Spark's filesystem layer and Iceberg's FileIO
        accept, which means ``s3a://``.

        A Windows bind mount stays categorically wrong here: it does not provide
        reliable rename semantics, and the corruption shows up as intermittent,
        hard-to-diagnose query failures rather than a clean error.

        On the usual objection that object stores lack atomic rename: the hazard that
        describes is two writers racing on one checkpoint. Exactly one query owns each
        checkpoint path, so there is no race to lose, and MinIO is strongly consistent.
        This is also what managed Spark services do against real S3.

        ``CHECKPOINT_ROOT`` overrides it, which is how a deployment points at HDFS.
        """
        return f"{self.checkpoint_root.rstrip('/')}/{job}"


@dataclass(frozen=True)
class CatalogConfig:
    rest_uri: str = field(
        default_factory=lambda: _env("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    )
    name: str = field(default_factory=lambda: _env("CATALOG_NAME", "fleetstream"))

    # Namespaces (medallion layers plus a platform namespace for operational tables)
    bronze: str = "bronze"
    silver: str = "silver"
    gold: str = "gold"
    platform: str = "platform"


@dataclass(frozen=True)
class TrinoConfig:
    host: str = field(default_factory=lambda: _env("TRINO_HOST", "trino"))
    port: int = field(default_factory=lambda: _env_int("TRINO_PORT", 8080))
    user: str = field(default_factory=lambda: _env("TRINO_USER", "fleetstream"))


@dataclass(frozen=True)
class SimulatorConfig:
    vehicle_count: int = field(default_factory=lambda: _env_int("SIM_VEHICLE_COUNT", 200))
    events_per_vehicle_per_min: int = field(
        default_factory=lambda: _env_int("SIM_EVENTS_PER_VEHICLE_PER_MIN", 10)
    )
    #: Wall-clock acceleration. 60 means one simulated minute per real second.
    speedup: float = field(default_factory=lambda: _env_float("SIM_SPEEDUP", 60.0))
    seed: int = field(default_factory=lambda: _env_int("SIM_SEED", 20260908))

    duplicate_rate: float = field(default_factory=lambda: _env_float("SIM_DUPLICATE_RATE", 0.01))
    late_rate: float = field(default_factory=lambda: _env_float("SIM_LATE_RATE", 0.02))
    invalid_rate: float = field(default_factory=lambda: _env_float("SIM_INVALID_RATE", 0.005))

    hot_vehicle: str = field(default_factory=lambda: _env("SIM_HOT_VEHICLE", "V9999"))
    #: How many times more often the skewed vehicle reports.
    #:
    #: Tuned against the default 200-vehicle fleet: 25 makes this one vehicle about
    #: 11% of all events, so its Kafka partition carries roughly twice the average -
    #: a clearly visible hot partition. Much higher (50+) and a single vehicle becomes
    #: half the dataset, which distorts every fleet-level aggregate and turns a
    #: skew demonstration into a data-quality problem.
    hot_vehicle_multiplier: int = field(
        default_factory=lambda: _env_int("SIM_HOT_VEHICLE_MULTIPLIER", 25)
    )

    def __post_init__(self) -> None:
        for name in ("duplicate_rate", "late_rate", "invalid_rate"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"SimulatorConfig.{name} must be within [0, 1], got {value}")
        if self.vehicle_count < 1:
            raise ValueError("SIM_VEHICLE_COUNT must be at least 1")


@dataclass(frozen=True)
class Settings:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    catalog: CatalogConfig = field(default_factory=CatalogConfig)
    trino: TrinoConfig = field(default_factory=TrinoConfig)
    simulator: SimulatorConfig = field(default_factory=SimulatorConfig)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings. Cached, so env changes require a fresh process."""
    return Settings()
