"""Lightweight timing context manager for pipeline step instrumentation."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator

_depth = 0


@contextmanager
def log_duration(label: str, log: logging.Logger | None = None) -> Generator[None]:
    """Log elapsed wall-clock time for a block of code.

    Nesting depth is shown via repeated ``>`` / ``<`` markers so that
    nested steps are visually distinct regardless of log-prefix width.

    Usage::

        with log_duration("Load entities", logger):
            store = EntityStore(path)
    """
    global _depth  # noqa: PLW0603
    _depth += 1
    prefix = ">" * (_depth + 1)
    _log = log or logging.getLogger(__name__)
    _log.info("%s [START] %s", prefix, label)
    t0 = time.monotonic()
    yield
    elapsed = time.monotonic() - t0
    _log.info("%s [DONE]  %s (%.1fs)", prefix, label, elapsed)
    _depth -= 1
