"""Baseline security headers on every API response.

The site sets these in next.config.mjs; the API answered with none. No
Content-Security-Policy: /docs and /redoc load Swagger and ReDoc from a CDN,
and the JSON responses have nothing for a CSP to protect.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

SECURITY_HEADERS: tuple[tuple[bytes, bytes], ...] = (
    (b"x-content-type-options", b"nosniff"),
    (b"strict-transport-security", b"max-age=31536000"),
    (b"referrer-policy", b"no-referrer"),
    (b"x-frame-options", b"DENY"),
)


class SecurityHeadersMiddleware:
    """Pure ASGI, so it adds no per-request task the way BaseHTTPMiddleware does."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                present = {name.lower() for name, _ in headers}
                headers += [h for h in SECURITY_HEADERS if h[0] not in present]
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_headers)
