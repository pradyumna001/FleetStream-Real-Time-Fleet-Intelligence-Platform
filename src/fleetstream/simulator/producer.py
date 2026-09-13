"""Kafka producer for simulated telemetry.

Producer settings follow the durability story the platform depends on:

``acks=all``
    The write is acknowledged only once every in-sync replica has it. With
    ``acks=1`` a leader failure immediately after the ack loses the record, and no
    amount of downstream idempotency can recover data that was never stored.

``enable.idempotence=true``
    Kafka de-duplicates producer retries, so a retried send does not append a
    second copy. This handles duplicates *introduced by the transport*. It says
    nothing about duplicates that are re-sent later as a new produce request, which
    is why Silver still merges on ``event_id`` - the two mechanisms cover different
    failures and neither replaces the other.

``key=vehicle_id``
    Guarantees per-vehicle ordering, because one key always maps to one partition.
    It also means a single high-volume vehicle concentrates on one partition, which
    is the hot-partition behaviour the simulator deliberately reproduces.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import FrameType
from typing import Any

from fleetstream.common.config import Settings, get_settings
from fleetstream.common.logging import configure_logging
from fleetstream.simulator.generator import TICK_SECONDS, TelemetryGenerator
from fleetstream.simulator.reference import build_reference

logger = logging.getLogger(__name__)

_shutdown = False


def _handle_signal(signum: int, _frame: FrameType | None) -> None:
    """Ask the main loop to stop, so buffered records are still flushed."""
    global _shutdown
    logger.info("received signal %s, finishing current tick and flushing", signum)
    _shutdown = True


def build_producer(settings: Settings) -> Any:
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            # Batching trades a few ms of latency for far better throughput. At
            # 10ms the simulator can saturate the broker without the per-record
            # overhead of unbatched sends.
            "linger.ms": 10,
            "batch.size": 64 * 1024,
            "retries": 10,
            "retry.backoff.ms": 200,
            # Idempotence caps this at 5; being explicit documents that ordering
            # per partition is still guaranteed despite pipelining.
            "max.in.flight.requests.per.connection": 5,
            "client.id": "fleetstream-simulator",
        }
    )


def _delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        logger.error("delivery failed for key=%s: %s", msg.key(), err)


def run(
    settings: Settings,
    ticks: int,
    realtime: bool,
    manifest_path: Path | None,
) -> int:
    cfg = settings.simulator

    # Start in the past so the run finishes at roughly "now". A run that produced
    # only future-dated events would leave every "yesterday" dashboard query empty.
    span = timedelta(seconds=ticks * TICK_SECONDS)
    start_time = datetime.now(timezone.utc) - span

    reference = build_reference(cfg.vehicle_count, cfg.seed, cfg.hot_vehicle)
    generator = TelemetryGenerator(cfg, reference, start_time)
    producer = build_producer(settings)
    topic = settings.kafka.telemetry_topic

    logger.info(
        "producing to %s | %d vehicles | %d ticks (%s of simulated time) | speedup x%.0f",
        topic,
        len(reference.vehicles),
        ticks,
        span,
        cfg.speedup,
    )

    # One tick of simulated time compressed into this much real time.
    sleep_per_tick = TICK_SECONDS / cfg.speedup if cfg.speedup > 0 else 0.0
    sent = 0
    started = time.monotonic()

    for tick_index in range(ticks):
        if _shutdown:
            logger.info("stopping early at tick %d", tick_index)
            break

        tick_started = time.monotonic()
        for key, payload in generator.tick():
            producer.produce(
                topic=topic,
                key=key.encode("utf-8") if key else None,
                value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                on_delivery=_delivery_report,
            )
            sent += 1
            # Serve delivery callbacks and make room in the local queue. Without
            # this the buffer fills and produce() starts raising BufferError.
            producer.poll(0)

        if tick_index % 100 == 0 and tick_index:
            logger.info(
                "tick %d/%d | %d records produced | %d late events pending",
                tick_index,
                ticks,
                sent,
                generator.pending_late,
            )

        if realtime and sleep_per_tick:
            elapsed = time.monotonic() - tick_started
            remaining = sleep_per_tick - elapsed
            if remaining > 0:
                time.sleep(remaining)

    # Flush anything still held in the late buffer, otherwise those events are
    # simply lost and the produced count never reconciles with what was injected.
    for key, payload in generator.flush_late():
        producer.produce(
            topic=topic,
            key=key.encode("utf-8") if key else None,
            value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            on_delivery=_delivery_report,
        )
        sent += 1
        producer.poll(0)

    logger.info("flushing producer buffer")
    outstanding = producer.flush(timeout=60)
    if outstanding:
        logger.error("%d messages were not delivered before the flush timeout", outstanding)
        return 1

    duration = time.monotonic() - started
    stats = generator.stats.as_dict()
    logger.info(
        "produced %d records in %.1fs (%.0f rec/s)", sent, duration, sent / max(duration, 0.001)
    )
    logger.info("injection summary: %s", stats)

    if sent != stats["total_produced"]:
        logger.error(
            "produced %d records but the generator counted %d - reconciliation would fail",
            sent,
            stats["total_produced"],
        )
        return 1

    if manifest_path is not None:
        manifest = {
            "seed": cfg.seed,
            "vehicle_count": len(reference.vehicles),
            "ticks": ticks,
            "tick_seconds": TICK_SECONDS,
            "sim_start": start_time.isoformat(),
            "sim_end": generator.sim_time.isoformat(),
            "topic": topic,
            "records_sent": sent,
            "hot_vehicle": cfg.hot_vehicle,
            "rates": {
                "duplicate": cfg.duplicate_rate,
                "late": cfg.late_rate,
                "invalid": cfg.invalid_rate,
            },
            "stats": stats,
        }
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        logger.info("run manifest written to %s", manifest_path)

    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    settings = get_settings()

    parser = argparse.ArgumentParser(description="Produce simulated fleet telemetry to Kafka")
    parser.add_argument(
        "--minutes",
        type=float,
        default=10.0,
        help="minutes of real time to run for (default: 10)",
    )
    parser.add_argument(
        "--ticks",
        type=int,
        default=None,
        help="explicit tick count; overrides --minutes",
    )
    parser.add_argument(
        "--no-realtime",
        action="store_true",
        help="produce as fast as possible instead of pacing to SIM_SPEEDUP",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("/manifests/latest.json"),
        help="where to write the run manifest the verification suite reads",
    )
    parser.add_argument("--no-manifest", action="store_true", help="skip writing the manifest")
    args = parser.parse_args(argv)

    if args.ticks is not None:
        ticks = args.ticks
    else:
        # Each tick is TICK_SECONDS of simulated time, compressed by the speedup.
        real_seconds_per_tick = TICK_SECONDS / max(settings.simulator.speedup, 1e-9)
        ticks = max(1, int(args.minutes * 60 / real_seconds_per_tick))

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    return run(
        settings=settings,
        ticks=ticks,
        realtime=not args.no_realtime,
        manifest_path=None if args.no_manifest else args.manifest,
    )


if __name__ == "__main__":
    sys.exit(main())
