"""Mirror external ``/v1/`` traffic into umami as ``api_request`` events.

The structured access log (:mod:`src.access_log`) stays the authoritative,
per-email record in CloudWatch. This module is the dashboard view of the same
requests: :class:`UmamiSink` is fed by the middleware's ``sink`` hook, builds
one umami custom event per request and posts it from a background task so the
request path never waits on umami. Sampling and a bounded queue keep a slow or
dead umami from ever affecting the API — events are dropped, never awaited.

Identity is the key **prefix + org**, never the email: umami is a shared
dashboard, the ledger is not. Internal (frontend) and unauthenticated requests
are filtered out, so the counts are exactly "external key holders".

No ``ip`` is sent on purpose. umami keys sessions on hash(website, ip, ua); with
the task IP and a fixed, browser-shaped ``userAgent`` every API client lands in
one handful of phantom sessions instead of inflating the site's Visitors
count one-per-client. Browser-shaped because ``/api/send`` runs ``isbot()`` on
the UA and would silently drop ``python-requests`` or ``curl``. The real client
UA still travels in ``data.ua`` for the Properties breakdown.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import httpx

from src.access_log import UNAUTHENTICATED

if TYPE_CHECKING:
    from collections.abc import Callable, MutableMapping

    from src.config import APISettings

logger = logging.getLogger("foodatlas.umami")

EVENT_NAME = "api_request"
# Fixed session identity for every API event — see the module docstring.
SESSION_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
# Only external key holders are counted; the frontend's own SSR traffic and
# rejected requests are noise for a "who uses the API" dashboard.
EXCLUDED_EMAILS = frozenset({"internal", UNAUTHENTICATED})
# Keys issued before the ledger have no prefix (it derives from the plaintext,
# which was never stored); give them one bucket instead of an empty tag.
NO_PREFIX = "no-prefix"
# umami's own column limits: tag 50, everything else 500.
MAX_TAG_LEN = 50
MAX_FIELD_LEN = 500
SEND_TIMEOUT_S = 2.0
# Bound on how long ``stop()`` waits for the queue to drain before cancelling.
STOP_DRAIN_TIMEOUT_S = 5.0


@dataclass
class UmamiSink:
    """Bounded queue + single worker posting events to ``<host_url>/api/send``.

    ``enqueue`` is synchronous and never raises or awaits, so it is safe to
    call from the access-log middleware after the response has gone out.
    Counters are plain ints for a ``/health``-style peek and for tests.
    """

    website_id: str
    host_url: str
    hostname: str
    sample_rate: float = 1.0
    queue_size: int = 1000
    sent: int = 0
    failed: int = 0
    dropped: int = 0
    _queue: asyncio.Queue[dict[str, Any]] | None = None
    _task: asyncio.Task[None] | None = None
    _warned: bool = False
    # Test injection points, like ``PublicKeyStore._client_factory``.
    _transport: httpx.AsyncBaseTransport | None = None
    _random: Callable[[], float] = field(default=random.random)

    @property
    def send_url(self) -> str:
        return f"{self.host_url.rstrip('/')}/api/send"

    async def start(self) -> None:
        """Create the queue and start the worker. Call from app lifespan."""
        if self._task is not None:
            return
        self._queue = asyncio.Queue(maxsize=self.queue_size)
        self._task = asyncio.create_task(self._worker(), name="umami_sink")

    async def stop(self) -> None:
        """Drain what is queued (bounded), then cancel the worker. Idempotent."""
        task, queue = self._task, self._queue
        self._task = None
        if task is None:
            return
        if queue is not None:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(queue.join(), timeout=STOP_DRAIN_TIMEOUT_S)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    def enqueue(self, entry: dict[str, Any], scope: MutableMapping[str, Any]) -> None:
        """Middleware sink: filter, sample, build and queue — never block."""
        if entry.get("email") in EXCLUDED_EMAILS:
            return
        if entry.get("method") == "OPTIONS":
            return
        if self.sample_rate < 1.0 and self._random() >= self.sample_rate:
            return
        event = build_event(
            entry, scope, website_id=self.website_id, hostname=self.hostname
        )
        queue = self._queue
        if queue is None:
            self.dropped += 1
            return
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1

    async def _worker(self) -> None:
        assert self._queue is not None
        async with httpx.AsyncClient(
            transport=self._transport,
            timeout=SEND_TIMEOUT_S,
            headers={"User-Agent": SESSION_USER_AGENT},
        ) as client:
            while True:
                event = await self._queue.get()
                try:
                    await self._send(client, event)
                finally:
                    self._queue.task_done()

    async def _send(self, client: httpx.AsyncClient, event: dict[str, Any]) -> None:
        try:
            response = await client.post(self.send_url, json=event)
        except httpx.HTTPError as exc:
            self._record_failure(f"{type(exc).__name__}: {exc}")
            return
        if response.is_success:
            self.sent += 1
            self._warned = False
        else:
            self._record_failure(f"HTTP {response.status_code}")

    def _record_failure(self, reason: str) -> None:
        """One warning per failure streak; the rest at debug so logs stay quiet."""
        self.failed += 1
        if self._warned:
            logger.debug("umami_sink: send failed (%s)", reason)
            return
        self._warned = True
        logger.warning("umami_sink: send to %s failed (%s)", self.send_url, reason)


def build_event(
    entry: dict[str, Any],
    scope: MutableMapping[str, Any],
    *,
    website_id: str,
    hostname: str,
) -> dict[str, Any]:
    """Shape one access-log entry as an umami ``/api/send`` event body.

    ``tag`` carries the key prefix so the Insights report can filter a single
    key's time series; the same prefix sits in ``data`` for the Properties
    breakdown. The email is deliberately absent from every field.
    """
    prefix = str(entry.get("key_prefix") or "") or NO_PREFIX
    route = str(entry.get("route") or "")
    payload: dict[str, Any] = {
        "website": website_id,
        "hostname": hostname,
        "url": route[:MAX_FIELD_LEN],
        "name": EVENT_NAME,
        "tag": prefix[:MAX_TAG_LEN],
        "userAgent": SESSION_USER_AGENT,
        "data": {
            "route": route,
            "key_prefix": prefix,
            "org": str(entry.get("org") or "")[:MAX_FIELD_LEN],
            "method": entry.get("method", ""),
            "status": entry.get("status"),
            "duration_ms": entry.get("duration_ms"),
            "ua": str(entry.get("ua") or "")[:MAX_FIELD_LEN],
        },
    }
    referrer = _referrer(scope)
    if referrer:
        payload["referrer"] = referrer[:MAX_FIELD_LEN]
    return {"type": "event", "payload": payload}


def build_sink(settings: APISettings) -> UmamiSink | None:
    """Sink from settings, or ``None`` when no website id is configured."""
    if not settings.umami_website_id:
        return None
    return UmamiSink(
        website_id=settings.umami_website_id,
        host_url=settings.umami_host_url,
        hostname=settings.umami_hostname,
        sample_rate=settings.umami_sample_rate,
        queue_size=settings.umami_queue_size,
    )


def _referrer(scope: MutableMapping[str, Any]) -> str:
    for name, value in scope.get("headers") or []:
        if name.lower() == b"referer":
            return str(value.decode("latin-1"))
    return ""
