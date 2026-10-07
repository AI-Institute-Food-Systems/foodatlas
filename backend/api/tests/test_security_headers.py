"""Every API response carries the baseline security headers."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from src.security_headers import SECURITY_HEADERS, SecurityHeadersMiddleware

if TYPE_CHECKING:
    from fastapi.testclient import TestClient
    from starlette.types import Message, Receive, Scope, Send

EXPECTED = {name.decode(): value.decode() for name, value in SECURITY_HEADERS}


def _assert_headers(headers: dict[str, str]) -> None:
    for name, value in EXPECTED.items():
        assert headers.get(name) == value, name


def test_route_response(client: TestClient) -> None:
    _assert_headers(dict(client.get("/health").headers))


def test_unknown_path_404(client: TestClient) -> None:
    response = client.get("/no-such-path")
    assert response.status_code == 404
    _assert_headers(dict(response.headers))


def test_cors_preflight(client: TestClient) -> None:
    # CORSMiddleware answers a preflight itself, without reaching a route.
    response = client.options(
        "/health",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
        },
    )
    _assert_headers(dict(response.headers))


@pytest.mark.asyncio
async def test_keeps_a_header_the_app_already_set() -> None:
    async def app(_scope: Scope, _receive: Receive, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"x-frame-options", b"SAMEORIGIN")],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    sent: list[Message] = []

    async def capture(message: Message) -> None:
        sent.append(message)

    async def receive() -> Message:
        return {"type": "http.request"}

    middleware = SecurityHeadersMiddleware(app)
    await middleware({"type": "http"}, receive, capture)
    headers = sent[0]["headers"]
    assert headers.count((b"x-frame-options", b"SAMEORIGIN")) == 1
    assert (b"x-frame-options", b"DENY") not in headers
