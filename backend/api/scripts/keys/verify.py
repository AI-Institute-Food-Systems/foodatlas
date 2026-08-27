"""Confirm a freshly merged key is live on *every* task before handing it out.

Each Fargate task holds its own :class:`~src.public_keys.PublicKeyStore` with
an independent refresh timer (``public_keys_refresh_seconds``, default 300s)
and the ALB round-robins between them. So for up to one full refresh interval
after a merge, a new key does not fail cleanly — it fails *intermittently*, as
one task has picked it up and another has not. Emailing the key during that
window makes the recipient's first calls fail at random, which reads as a
broken key.

The fix is to require a run of consecutive successes, not a single one.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from collections.abc import Callable

PROBE_PATH = "/v1/stats"
# Long enough to cover a full refresh interval plus deploy jitter.
DEFAULT_DEADLINE_S = 360.0
# With 2 tasks and round-robin routing, 12 consecutive hits make it very
# unlikely that a stale task simply was not sampled. Raise this if the
# service ever autoscales well beyond a handful of tasks.
DEFAULT_STREAK = 12
DEFAULT_INTERVAL_S = 5.0


@dataclass(frozen=True)
class ProbeResult:
    """Outcome of waiting for a key to converge across tasks."""

    live: bool
    attempts: int
    streak: int
    last_status: int
    elapsed_s: float


def wait_until_live(
    base_url: str,
    plaintext: str,
    *,
    streak: int = DEFAULT_STREAK,
    interval_s: float = DEFAULT_INTERVAL_S,
    deadline_s: float = DEFAULT_DEADLINE_S,
    sleep: Callable[[float], None] = time.sleep,
    probe: Callable[[str, str], int] | None = None,
    on_attempt: Callable[[int, int, int], None] | None = None,
) -> ProbeResult:
    """Poll until ``streak`` consecutive 200s, or the deadline passes.

    Any non-200 resets the run: a single stale task must not be averaged away.
    """
    send = probe or _probe
    url = base_url.rstrip("/") + PROBE_PATH
    started = time.monotonic()
    attempts = 0
    run = 0
    status = 0
    while time.monotonic() - started < deadline_s:
        status = send(url, plaintext)
        attempts += 1
        run = run + 1 if status == 200 else 0
        if on_attempt is not None:
            on_attempt(attempts, run, status)
        if run >= streak:
            return ProbeResult(True, attempts, run, status, time.monotonic() - started)
        sleep(interval_s)
    return ProbeResult(False, attempts, run, status, time.monotonic() - started)


def _probe(url: str, plaintext: str) -> int:
    try:
        resp = httpx.get(
            url,
            headers={"Authorization": f"Bearer {plaintext}"},
            timeout=10.0,
        )
    except httpx.HTTPError:
        return 0
    return resp.status_code
