"""Tests for the structured /v1 access log."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from src.access_log import (
    LOG_MARKER,
    UNAUTHENTICATED,
    AccessLogMiddleware,
    build_entry,
    configure_access_logger,
    logger,
    route_template,
)
from src.app import create_app
from src.config import APISettings
from src.dependencies import get_db, get_settings, verify_v1_key
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


async def _stats(request: Request) -> JSONResponse:
    request.state.api_key_email = "alice@u.edu"
    request.state.api_key_prefix = "Ky3mAa7Q"
    return JSONResponse({"ok": True})


async def _food(request: Request) -> JSONResponse:
    request.state.api_key_email = "alice@u.edu"
    return JSONResponse({"id": request.path_params["food_id"]})


async def _denied(_request: Request) -> PlainTextResponse:
    return PlainTextResponse("nope", status_code=401)


async def _boom(_request: Request) -> PlainTextResponse:
    raise RuntimeError("kaboom")


async def _internal(_request: Request) -> PlainTextResponse:
    return PlainTextResponse("ui")


async def _internal_auth(request: Request) -> PlainTextResponse:
    request.state.api_key_email = "internal"
    request.state.api_key_prefix = ""
    return PlainTextResponse("ok")


def _client(entries: list[dict[str, Any]]) -> TestClient:
    app = Starlette(
        routes=[
            Route("/v1/stats", _stats),
            Route("/v1/foods/{food_id}", _food),
            Route("/v1/denied", _denied),
            Route("/v1/boom", _boom),
            Route("/food/composition", _internal),
        ]
    )
    app.add_middleware(AccessLogMiddleware, emit=entries.append)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def entries() -> list[dict[str, Any]]:
    return []


class TestScope:
    def test_v1_request_is_logged(self, entries: list[dict[str, Any]]) -> None:
        _client(entries).get("/v1/stats")
        assert len(entries) == 1
        assert entries[0]["log"] == LOG_MARKER
        assert entries[0]["status"] == 200
        assert entries[0]["method"] == "GET"

    def test_internal_routes_are_not_logged(
        self, entries: list[dict[str, Any]]
    ) -> None:
        # The UI router is far higher volume and is not what we attribute.
        _client(entries).get("/food/composition")
        assert entries == []

    def test_duration_is_recorded(self, entries: list[dict[str, Any]]) -> None:
        _client(entries).get("/v1/stats")
        assert entries[0]["duration_ms"] >= 0


class TestAttribution:
    def test_email_and_prefix_come_from_request_state(
        self, entries: list[dict[str, Any]]
    ) -> None:
        _client(entries).get("/v1/stats")
        assert entries[0]["email"] == "alice@u.edu"
        assert entries[0]["key_prefix"] == "Ky3mAa7Q"

    def test_rejected_request_logs_presented_prefix(
        self, entries: list[dict[str, Any]]
    ) -> None:
        _client(entries).get(
            "/v1/denied", headers={"Authorization": "Bearer badkey0123456789"}
        )
        assert entries[0]["email"] == UNAUTHENTICATED
        assert entries[0]["key_prefix"] == "badkey01"
        assert entries[0]["status"] == 401

    def test_internal_key_is_never_echoed(self, entries: list[dict[str, Any]]) -> None:
        # The frontend's shared internal key authenticates but records no
        # prefix. Falling back to the presented token here would write 8
        # characters of that shared secret into every line.
        app = Starlette(routes=[Route("/v1/internal", _internal_auth)])
        app.add_middleware(AccessLogMiddleware, emit=entries.append)
        TestClient(app).get(
            "/v1/internal", headers={"Authorization": "Bearer internal-shared-key"}
        )
        assert entries[0]["email"] == "internal"
        assert entries[0]["key_prefix"] == ""

    def test_no_bearer_header_gives_empty_prefix(
        self, entries: list[dict[str, Any]]
    ) -> None:
        _client(entries).get("/v1/denied", headers={"Authorization": "Basic abc"})
        assert entries[0]["key_prefix"] == ""

    def test_authorization_header_never_appears_in_the_line(
        self, entries: list[dict[str, Any]]
    ) -> None:
        secret_value = "super-secret-key-value"
        _client(entries).get(
            "/v1/denied", headers={"Authorization": f"Bearer {secret_value}"}
        )
        assert secret_value not in json.dumps(entries[0])

    def test_client_ip_prefers_forwarded_header(
        self, entries: list[dict[str, Any]]
    ) -> None:
        _client(entries).get(
            "/v1/stats", headers={"X-Forwarded-For": "203.0.113.7, 10.0.0.1"}
        )
        assert entries[0]["client_ip"] == "203.0.113.7"


class TestRouteTemplate:
    def test_path_params_are_collapsed(self, entries: list[dict[str, Any]]) -> None:
        _client(entries).get("/v1/foods/f_12345")
        assert entries[0]["route"] == "/v1/foods/{food_id}"
        assert entries[0]["path"] == "/v1/foods/f_12345"

    def test_short_value_does_not_corrupt_the_prefix(self) -> None:
        # A blanket str.replace turns this into "/v{short}/a/..." because "1"
        # also occurs inside "/v1/". Only whole segments may be substituted.
        template = route_template("/v1/a/12345/b/1", {"long": "12345", "short": "1"})
        assert template == "/v1/a/{long}/b/{short}"

    def test_id_equal_to_the_version_segment(self) -> None:
        assert route_template("/v1/foods/1", {"food_id": "1"}) == "/v1/foods/{food_id}"

    def test_repeated_values_consume_one_param_each(self) -> None:
        template = route_template("/v1/a/7/b/7", {"x": "7", "y": "7"})
        assert template == "/v1/a/{x}/b/{y}"

    def test_multi_segment_value_falls_back_to_the_path(self) -> None:
        assert route_template("/v1/f/a/b", {"p": "a/b"}) == "/v1/f/a/b"

    def test_empty_values_are_ignored(self) -> None:
        assert route_template("/v1/stats", {"unused": ""}) == "/v1/stats"

    def test_no_params_is_identity(self) -> None:
        assert route_template("/v1/stats", {}) == "/v1/stats"


class TestQueryRedaction:
    def test_credential_params_are_redacted(
        self, entries: list[dict[str, Any]]
    ) -> None:
        _client(entries).get("/v1/stats?limit=5&api_key=leaked&token=alsoleaked")
        query = entries[0]["query"]
        assert "leaked" not in query
        assert "limit=5" in query
        assert query.count("%5Bredacted%5D") == 2

    def test_ordinary_params_survive(self, entries: list[dict[str, Any]]) -> None:
        _client(entries).get("/v1/stats?q=onion&page=2")
        assert entries[0]["query"] == "q=onion&page=2"

    def test_empty_query_is_empty_string(self, entries: list[dict[str, Any]]) -> None:
        _client(entries).get("/v1/stats")
        assert entries[0]["query"] == ""


class TestFailures:
    def test_unhandled_exception_is_still_logged_as_500(
        self, entries: list[dict[str, Any]]
    ) -> None:
        _client(entries).get("/v1/boom")
        assert entries[0]["status"] == 500

    def test_exception_still_propagates(self) -> None:
        entries: list[dict[str, Any]] = []
        app = Starlette(routes=[Route("/v1/boom", _boom)])
        app.add_middleware(AccessLogMiddleware, emit=entries.append)
        with (
            TestClient(app, raise_server_exceptions=True) as client,
            pytest.raises(RuntimeError, match="kaboom"),
        ):
            client.get("/v1/boom")
        assert entries[0]["status"] == 500


class TestStateIsSharedWithTheScope:
    """The invariant the middleware is built on.

    ``request.state`` is backed by ``scope["state"]``, so a value written by a
    dependency inside the endpoint is visible to the middleware afterwards. If
    Starlette ever changed that, attribution would silently degrade to
    "unauthenticated" rather than fail loudly — hence a test on the mechanism.
    """

    def test_state_written_through_a_request_reaches_build_entry(self) -> None:
        scope: dict[str, Any] = {
            "type": "http",
            "method": "GET",
            "path": "/v1/stats",
            "headers": [],
        }
        request = Request(scope)
        request.state.api_key_email = "dana@u.edu"
        request.state.api_key_prefix = "Dd44eeff"
        entry = build_entry(scope, status=200, duration_ms=0.0)
        assert entry["email"] == "dana@u.edu"
        assert entry["key_prefix"] == "Dd44eeff"


class TestBuildEntryDirectly:
    def test_missing_state_falls_back_to_unauthenticated(self) -> None:
        entry = build_entry(
            {"type": "http", "method": "GET", "path": "/v1/stats", "headers": []},
            status=200,
            duration_ms=1.0,
        )
        assert entry["email"] == UNAUTHENTICATED
        assert entry["client_ip"] == ""

    def test_client_tuple_used_when_unproxied(self) -> None:
        entry = build_entry(
            {
                "type": "http",
                "method": "GET",
                "path": "/v1/stats",
                "headers": [],
                "client": ("198.51.100.4", 5555),
            },
            status=200,
            duration_ms=1.0,
        )
        assert entry["client_ip"] == "198.51.100.4"


class TestLoggerConfiguration:
    def test_is_idempotent(self) -> None:
        before = list(logger.handlers)
        try:
            configure_access_logger()
            after_one = len(logger.handlers)
            configure_access_logger()
            assert len(logger.handlers) == after_one
        finally:
            logger.handlers = before

    def test_emits_bare_json_to_stdout(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        before = list(logger.handlers)
        try:
            configure_access_logger()
            logger.info(json.dumps({"log": LOG_MARKER}))
            line = capsys.readouterr().out.strip()
            assert json.loads(line) == {"log": LOG_MARKER}
        finally:
            logger.handlers = before

    def test_level_allows_info(self) -> None:
        before, level = list(logger.handlers), logger.level
        try:
            configure_access_logger()
            assert logger.isEnabledFor(logging.INFO)
        finally:
            logger.handlers, logger.level = before, level


class TestWiredIntoTheApp:
    """The production path: create_app registers the middleware when not in debug."""

    @staticmethod
    def _settings() -> APISettings:
        settings = APISettings(**{"_env_file": None})
        settings.debug = False
        settings.key = "x"
        settings.rate_limit_enabled = False
        settings.public_keys_secret_name = ""
        return settings

    def _client(self, settings: APISettings) -> TestClient:
        app = create_app(settings)

        async def _db() -> AsyncGenerator[AsyncMock]:
            yield AsyncMock()

        async def _auth(request: Request) -> None:
            request.state.api_key_email = "alice@u.edu"
            request.state.api_key_prefix = "Ky3mAa7Q"

        app.dependency_overrides[get_db] = _db
        app.dependency_overrides[get_settings] = lambda: settings
        app.dependency_overrides[verify_v1_key] = _auth
        return TestClient(app, raise_server_exceptions=False)

    def test_v1_request_writes_a_json_line(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure_access_logger()
        self._client(self._settings()).get("/v1/stats")
        lines = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.startswith("{")
        ]
        entry = next(e for e in lines if e.get("log") == LOG_MARKER)
        assert entry["route"] == "/v1/stats"
        assert entry["email"] == "alice@u.edu"
        assert entry["key_prefix"] == "Ky3mAa7Q"

    def test_disabled_by_setting(self, capsys: pytest.CaptureFixture[str]) -> None:
        settings = self._settings()
        settings.access_log_enabled = False
        self._client(settings).get("/v1/stats")
        assert LOG_MARKER not in capsys.readouterr().out
