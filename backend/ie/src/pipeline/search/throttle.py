"""Request pacing + backoff shared by the on-demand NCBI fetchers."""

from __future__ import annotations

import random
import threading
import time


class RateLimiter:
    """Cap the global request-start rate to 1/min_interval across all threads.

    NCBI throttles the BioC OA service by request rate per IP; unpaced workers
    burst past its ceiling and it answers HTTP 200 with an HTML throttle page.
    """

    def __init__(self, min_interval: float) -> None:
        self._min_interval = min_interval
        self._lock = threading.Lock()
        self._next_slot = 0.0

    def wait(self) -> None:
        with self._lock:
            slot = max(time.monotonic(), self._next_slot)
            self._next_slot = slot + self._min_interval
        delay = slot - time.monotonic()
        if delay > 0:
            time.sleep(delay)


def backoff_seconds(attempt: int) -> float:
    """Exponential backoff with jitter: ~1s, 2s, 4s, ... (+0-1s)."""
    return 2.0**attempt + random.uniform(0.0, 1.0)
