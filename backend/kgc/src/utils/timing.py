"""Lightweight timing context manager for pipeline step instrumentation."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator


@contextmanager
def log_duration(label: str, log: logging.Logger | None = None) -> Generator[None]:
    """Log elapsed wall-clock time for a block of code.

    Usage::

        with log_duration("Load entities", logger):
            store = EntityStore(path)
    """
    _log = log or logging.getLogger(__name__)
    _log.info("[START] %s", label)
    t0 = time.monotonic()
    yield
    elapsed = time.monotonic() - t0
    _log.info("[DONE]  %s (%.1fs)", label, elapsed)
