"""Structured per-request access log for the public ``/v1/`` API.

One JSON line per request on stdout, which the awslogs driver already ships to
the API's CloudWatch log group (``infra/aws/stacks/api_stack.py``). Every line
carries ``"log": "v1_access"`` so Logs Insights can separate them from ordinary
application output; see ``docs/contact-form-runbook.md`` for canned queries.

Attribution comes from ``request.state.api_key_email`` / ``api_key_prefix``,
which :func:`src.dependencies.verify_v1_key` sets on the matched record. That
happens *inside* the endpoint call, so this is a pure ASGI middleware that
reads ``scope["state"]`` after the downstream app returns — ``request.state``
is backed by that same dict.

Never logged: the ``Authorization`` header, and any query parameter whose name
looks like a credential. A rejected request logs only the first
:data:`~src.public_keys.PREFIX_LEN` characters of whatever was presented, so
repeated bad-key traffic stays attributable without recording a usable secret.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sys
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlencode

from src.public_keys import PREFIX_LEN

if TYPE_CHECKING:
    from collections.abc import Callable, MutableMapping

LOG_MARKER = "v1_access"
LOGGER_NAME = "foodatlas.access"
UNAUTHENTICATED = "unauthenticated"

# Query params redacted before logging. The API takes none of these today; the
# list is here so a future auth-by-query-param never silently starts logging
# credentials.
REDACTED_PARAMS = frozenset({"key", "token", "api_key", "apikey", "access_token"})

logger = logging.getLogger(LOGGER_NAME)


class AccessLogHandler(logging.StreamHandler):
    """Marker subclass so :func:`configure_access_logger` stays idempotent."""


def configure_access_logger() -> logging.Logger:
    """Attach a stdout handler that emits the bare JSON line.

    Self-contained on purpose: uvicorn configures only its own loggers and
    leaves root without a handler, so an unconfigured logger would drop these
    at INFO. Idempotent — repeated ``create_app`` calls (every test) must not
    stack handlers.
    """
    if not any(isinstance(h, AccessLogHandler) for h in logger.handlers):
        handler = AccessLogHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


class AccessLogMiddleware:
    """Emit one structured line per request under ``path_prefix``."""

    def __init__(
        self,
        app: Any,
        *,
        path_prefix: str = "/v1",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.app = app
        self.path_prefix = path_prefix
        self._emit = emit or _emit

    async def __call__(
        self, scope: MutableMapping[str, Any], receive: Any, send: Any
    ) -> None:
        if scope.get("type") != "http" or not str(scope.get("path", "")).startswith(
            self.path_prefix
        ):
            await self.app(scope, receive, send)
            return

        started = time.perf_counter()
        status = 500  # stands if the app raises before sending a response

        async def send_wrapper(message: MutableMapping[str, Any]) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message["status"])
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            self._emit(build_entry(scope, status=status, duration_ms=duration_ms))


def build_entry(
    scope: MutableMapping[str, Any],
    *,
    status: int,
    duration_ms: float,
) -> dict[str, Any]:
    """Assemble the log record for one request."""
    headers = _headers(scope)
    state = scope.get("state") or {}
    email = state.get("api_key_email") or UNAUTHENTICATED
    # Only fall back to what was presented when nothing authenticated. An
    # authenticated request with no recorded prefix is the internal frontend
    # key, and echoing 8 chars of that shared secret into a six-month log is
    # exactly the exposure this module is supposed to avoid.
    prefix = (
        _presented_prefix(headers)
        if email == UNAUTHENTICATED
        else str(state.get("api_key_prefix") or "")
    )
    path = str(scope.get("path", ""))
    return {
        "log": LOG_MARKER,
        "ts": dt.datetime.now(dt.UTC).isoformat(timespec="milliseconds"),
        "email": email,
        "key_prefix": prefix,
        "method": scope.get("method", ""),
        "route": route_template(path, scope.get("path_params") or {}),
        "path": path,
        "query": _scrub_query(scope.get("query_string", b"")),
        "status": status,
        "duration_ms": duration_ms,
        "client_ip": _client_ip(scope, headers),
        "ua": headers.get("user-agent", ""),
        "request_id": headers.get("x-amzn-trace-id", ""),
    }


def route_template(path: str, path_params: dict[str, Any]) -> str:
    """Re-derive the templated route so log cardinality stays bounded.

    Starlette puts the matched params in the scope but not the route itself,
    so substitute the values back out of the concrete path. Matching is
    segment-wise and each param is consumed once: a plain ``str.replace``
    turns ``/v1/foods/1`` into ``/v{food_id}/foods/{food_id}``, because the
    value also occurs inside ``/v1/``.

    A value spanning several segments (a ``:path`` converter, which no route
    uses today) simply will not match, and the concrete path is returned.
    """
    remaining = [
        (name, str(value)) for name, value in path_params.items() if str(value)
    ]
    if not remaining:
        return path
    out: list[str] = []
    for segment in path.split("/"):
        index = next((i for i, (_, v) in enumerate(remaining) if v == segment), None)
        if index is None:
            out.append(segment)
        else:
            name, _ = remaining.pop(index)
            out.append("{" + name + "}")
    return "/".join(out)


def _headers(scope: MutableMapping[str, Any]) -> dict[str, str]:
    return {
        k.decode("latin-1").lower(): v.decode("latin-1")
        for k, v in scope.get("headers") or []
    }


def _presented_prefix(headers: dict[str, str]) -> str:
    """First few characters of a rejected bearer token, for abuse triage."""
    auth = headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        return ""
    return auth[len("Bearer ") :][:PREFIX_LEN]


def _client_ip(scope: MutableMapping[str, Any], headers: dict[str, str]) -> str:
    forwarded = headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    client = scope.get("client")
    return str(client[0]) if client else ""


def _scrub_query(query_string: bytes) -> str:
    if not query_string:
        return ""
    pairs = parse_qsl(query_string.decode("latin-1"), keep_blank_values=True)
    return urlencode(
        [(k, "[redacted]" if k.lower() in REDACTED_PARAMS else v) for k, v in pairs]
    )


def _emit(entry: dict[str, Any]) -> None:
    logger.info(json.dumps(entry, separators=(",", ":")))
