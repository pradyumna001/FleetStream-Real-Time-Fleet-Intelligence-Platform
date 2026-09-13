"""Uniform logging setup for every entry point."""

from __future__ import annotations

import logging
import os
import sys


def configure_logging(level: str | None = None) -> None:
    """Configure root logging once, writing to stdout for container log capture."""
    resolved = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    root = logging.getLogger()
    if root.handlers:  # Spark installs handlers of its own; do not duplicate them
        root.setLevel(resolved)
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-5s %(name)s: %(message)s", "%H:%M:%S")
    )
    root.addHandler(handler)
    root.setLevel(resolved)
