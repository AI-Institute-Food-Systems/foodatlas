"""The access-log middleware's ``sink`` hook and its wiring into the app."""

# No ``from __future__ import annotations`` here: FastAPI resolves the
# ``request: Request`` annotation on the dependency override at runtime.

import json
from collections.abc import AsyncGenerator, MutableMapping
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from src.access_log import AccessLogMiddleware
from src.app import create_app
from src.config import APISettings
from src.dependencies import get_db, get_settings, verify_v1_key
from src.umami_sink import UmamiSink
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

WEBSITE = "00000000-0000-0000-0000-000000000000"
STATS = {
    "foods": 1,
    "chemicals": 1,
    "diseases": 1,
    "publications": 1,
    "connections": 1,
}


async def _stats(request: Request) -> JSONResponse:
    request.state.api_key_email = "alice@u.edu"
    request.state.api_key_prefix = "Ky3mAa7Q"
    request.state.api_key_org = "UC Davis"
    return JSONResponse({"ok": True})


async def _internal(_request: Request) -> PlainTextResponse:
    return PlainTextResponse("ui")


class TestMiddlewareSink:
    @pytest.fixture()
    def calls(self) -> dict[str, list[Any]]:
        return {"emit": [], "sink": []}

    @pytest.fixture()
    def client(self, calls: dict[str, list[Any]]) -> TestClient:
        def sink(entry: dict[str, Any], scope: MutableMapping[str, Any]) -> None:
            calls["sink"].append((entry, scope))

        app = Starlette(
            routes=[Route("/v1/stats", _stats), Route("/food/x", _internal)]
        )
        app.add_middleware(AccessLogMiddleware, emit=calls["emit"].append, sink=sink)
        return TestClient(app)

    def test_v1_request_reaches_emit_and_sink(
        self, client: TestClient, calls: dict[str, list[Any]]
    ) -> None:
        client.get("/v1/stats")
        assert len(calls["emit"]) == 1
        assert len(calls["sink"]) == 1
        entry, scope = calls["sink"][0]
        assert entry is calls["emit"][0]
        assert scope["path"] == "/v1/stats"
        assert entry["org"] == "UC Davis"

    def test_internal_route_reaches_neither(
        self, client: TestClient, calls: dict[str, list[Any]]
    ) -> None:
        client.get("/food/x")
        assert calls == {"emit": [], "sink": []}

    def test_org_defaults_to_empty(self, calls: dict[str, list[Any]]) -> None:
        async def no_org(request: Request) -> PlainTextResponse:
            request.state.api_key_email = "alice@u.edu"
            return PlainTextResponse("ok")

        app = Starlette(routes=[Route("/v1/x", no_org)])
        app.add_middleware(AccessLogMiddleware, emit=calls["emit"].append)
        TestClient(app).get("/v1/x")
        assert calls["emit"][0]["org"] == ""


def _settings(**overrides: Any) -> APISettings:
    settings = APISettings(**{"_env_file": None}, **overrides)
    settings.debug = True
    return settings


def _app(settings: APISettings) -> FastAPI:
    app = create_app(settings)

    async def _override_db() -> AsyncGenerator[AsyncMock]:
        yield AsyncMock()

    async def _verify(request: Request) -> None:
        request.state.api_key_email = "alice@u.edu"
        request.state.api_key_prefix = "Ky3mAa7Q"
        request.state.api_key_org = "UC Davis"

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[verify_v1_key] = _verify
    app.dependency_overrides[get_settings] = lambda: settings
    return app


class TestAppWiring:
    def test_sink_is_none_without_website_id(self) -> None:
        app = create_app(_settings())
        assert app.state.umami_sink is None

    def test_sink_is_built_from_settings(self) -> None:
        app = create_app(_settings(umami_website_id=WEBSITE))
        assert isinstance(app.state.umami_sink, UmamiSink)
        assert app.state.umami_sink.website_id == WEBSITE

    def test_end_to_end_posts_one_event(self) -> None:
        received: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            received.append(request)
            return httpx.Response(200)

        app = _app(_settings(umami_website_id=WEBSITE))
        sink: UmamiSink = app.state.umami_sink
        sink._transport = httpx.MockTransport(handler)

        with (
            patch(
                "src.repositories.v1.search.get_stats",
                return_value=STATS,
                new_callable=AsyncMock,
            ),
            TestClient(app) as client,
        ):
            assert client.get("/v1/stats").status_code == 200
            assert client.get("/health").status_code == 200
        # Leaving the ``with`` runs lifespan shutdown, which drains the queue.

        assert len(received) == 1
        body = json.loads(received[0].content)
        assert body["payload"]["name"] == "api_request"
        assert body["payload"]["tag"] == "Ky3mAa7Q"
        assert body["payload"]["data"]["route"] == "/v1/stats"
        assert sink.sent == 1

    def test_lifespan_stops_sink(self) -> None:
        app = _app(_settings(umami_website_id=WEBSITE))
        sink: UmamiSink = app.state.umami_sink
        sink._transport = httpx.MockTransport(lambda _r: httpx.Response(200))
        with TestClient(app):
            assert sink._task is not None
        assert sink._task is None
