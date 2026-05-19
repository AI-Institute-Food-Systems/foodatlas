"""Per-API-key rate limiting for the public /v1/ API.

A small in-memory token-bucket limiter keyed off
``request.state.api_key_email`` (populated by :func:`verify_v1_key`). The
``"internal"`` sentinel — assigned to the frontend's SSR calls — always
bypasses; every distinct public key gets its own bucket.

Storage is per-process. When the API runs on more than one Fargate task
(see issue #187), each task tracks its own buckets, so the effective
cluster-wide rate is N times the configured per-task rate. Acceptable while
the task count is small; swap to a Redis-backed limiter if/when that
ceases to be true.

Wired into the `/v1/` router as the second router-level dependency
(after :func:`verify_v1_key`) in :mod:`src.routes.v1`.
"""

from __future__ import annotations

import time
from threading import Lock

from fastapi import HTTPException, Request

INTERNAL_KEY_EMAIL = "internal"


class TokenBucketLimiter:
    """Per-key token bucket with continuous refill.

    ``sustained_per_min`` sets the long-term rate; ``burst`` is the
    bucket capacity (the largest spike a single key can land before
    being throttled). ``check(key)`` returns 0.0 when the request is
    admitted and a positive ``retry_after`` (seconds) otherwise.
    """

    def __init__(self, sustained_per_min: int, burst: int) -> None:
        if sustained_per_min <= 0 or burst <= 0:
            raise ValueError("rate-limit settings must be positive")
        self._refill_per_sec = sustained_per_min / 60.0
        self._capacity = float(burst)
        self._state: dict[str, tuple[float, float]] = {}
        self._lock = Lock()

    def check(self, key: str) -> float:
        """Try to reserve one token for ``key``. Returns retry-after seconds."""
        if key == INTERNAL_KEY_EMAIL:
            return 0.0
        now = time.monotonic()
        with self._lock:
            tokens, last = self._state.get(key, (self._capacity, now))
            tokens = min(
                self._capacity,
                tokens + (now - last) * self._refill_per_sec,
            )
            if tokens >= 1.0:
                self._state[key] = (tokens - 1.0, now)
                return 0.0
            need = 1.0 - tokens
            self._state[key] = (tokens, now)
            return need / self._refill_per_sec


async def enforce_rate_limit(request: Request) -> None:
    """Throttle /v1/* requests per API key. 429 on overage with Retry-After.

    No-op when the app has no limiter installed (debug or feature-flagged
    off), so this dependency is safe to declare unconditionally on the
    /v1/ router.
    """
    limiter: TokenBucketLimiter | None = getattr(
        request.app.state, "rate_limiter", None
    )
    if limiter is None:
        return
    key = getattr(request.state, "api_key_email", "anonymous")
    retry_after = limiter.check(key)
    if retry_after > 0.0:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": str(max(1, int(retry_after) + 1))},
        )
