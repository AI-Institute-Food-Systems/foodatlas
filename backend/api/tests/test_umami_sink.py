"""Tests for the umami ``api_request`` sink."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx
import pytest
from src.access_log import UNAUTHENTICATED
from src.config import APISettings
from src.umami_sink import (
    EVENT_NAME,
    MAX_TAG_LEN,
    NO_PREFIX,
    SESSION_USER_AGENT,
    UmamiSink,
    build_event,
    build_sink,
)

WEBSITE = "00000000-0000-0000-0000-000000000000"


def _entry(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "log": "v1_access",
        "ts": "2026-09-18T10:00:00.000+00:00",
        "email": "alice@u.edu",
        "key_prefix": "Ky3mAa7Q",
        "org": "UC Davis",
        "method": "GET",
        "route": "/v1/foods/{food_id}",
        "path": "/v1/foods/1",
        "query": "",
        "status": 200,
        "duration_ms": 12.5,
        "client_ip": "203.0.113.9",
        "ua": "python-requests/2.31",
        "request_id": "",
    }
    base.update(overrides)
    return base


def _scope(**headers: str) -> dict[str, Any]:
    return {
        "type": "http",
        "headers": [(k.encode(), v.encode()) for k, v in headers.items()],
    }


def _sink(**overrides: Any) -> UmamiSink:
    kwargs: dict[str, Any] = {
        "website_id": WEBSITE,
        "host_url": "http://umami.test",
        "hostname": "api.foodatlas.ai",
    }
    kwargs.update(overrides)
    return UmamiSink(**kwargs)


class TestBuildEvent:
    def test_public_key_event_shape(self) -> None:
        event = build_event(
            _entry(), _scope(), website_id=WEBSITE, hostname="api.foodatlas.ai"
        )
        assert event["type"] == "event"
        payload = event["payload"]
        assert payload["website"] == WEBSITE
        assert payload["hostname"] == "api.foodatlas.ai"
        assert payload["url"] == "/v1/foods/{food_id}"
        assert payload["name"] == EVENT_NAME
        assert payload["tag"] == "Ky3mAa7Q"
        assert payload["userAgent"] == SESSION_USER_AGENT
        assert payload["data"] == {
            "route": "/v1/foods/{food_id}",
            "key_prefix": "Ky3mAa7Q",
            "org": "UC Davis",
            "method": "GET",
            "status": 200,
            "duration_ms": 12.5,
            "ua": "python-requests/2.31",
        }

    def test_no_ip_is_sent(self) -> None:
        # Deliberate: umami would otherwise create one visitor per API client.
        event = build_event(_entry(), _scope(), website_id=WEBSITE, hostname="h")
        assert "ip" not in event["payload"]
        assert "203.0.113.9" not in json.dumps(event)

    def test_email_never_appears(self) -> None:
        event = build_event(_entry(), _scope(), website_id=WEBSITE, hostname="h")
        assert "@" not in json.dumps(event)

    def test_missing_prefix_gets_a_bucket(self) -> None:
        event = build_event(
            _entry(key_prefix=""), _scope(), website_id=WEBSITE, hostname="h"
        )
        assert event["payload"]["tag"] == NO_PREFIX
        assert event["payload"]["data"]["key_prefix"] == NO_PREFIX

    def test_tag_is_truncated_to_umami_limit(self) -> None:
        long_prefix = "x" * (MAX_TAG_LEN + 20)
        event = build_event(
            _entry(key_prefix=long_prefix), _scope(), website_id=WEBSITE, hostname="h"
        )
        assert len(event["payload"]["tag"]) == MAX_TAG_LEN
        assert event["payload"]["data"]["key_prefix"] == long_prefix

    def test_referrer_comes_from_scope_headers(self) -> None:
        event = build_event(
            _entry(),
            _scope(referer="https://example.org/notebook"),
            website_id=WEBSITE,
            hostname="h",
        )
        assert event["payload"]["referrer"] == "https://example.org/notebook"

    def test_no_referrer_key_without_header(self) -> None:
        event = build_event(_entry(), _scope(), website_id=WEBSITE, hostname="h")
        assert "referrer" not in event["payload"]


class TestEnqueue:
    @pytest.mark.asyncio
    async def test_public_key_request_is_queued(self) -> None:
        sink = _sink()
        await sink.start()
        try:
            sink.enqueue(_entry(), _scope())
            assert sink._queue is not None
            assert sink._queue.qsize() == 1
        finally:
            await sink.stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("email", ["internal", UNAUTHENTICATED])
    async def test_internal_and_rejected_requests_are_skipped(self, email: str) -> None:
        sink = _sink()
        await sink.start()
        try:
            sink.enqueue(_entry(email=email), _scope())
            assert sink._queue is not None
            assert sink._queue.qsize() == 0
        finally:
            await sink.stop()

    @pytest.mark.asyncio
    async def test_options_preflight_is_skipped(self) -> None:
        sink = _sink()
        await sink.start()
        try:
            sink.enqueue(_entry(method="OPTIONS"), _scope())
            assert sink._queue is not None
            assert sink._queue.qsize() == 0
        finally:
            await sink.stop()

    @pytest.mark.asyncio
    async def test_sampling_uses_injected_random(self) -> None:
        rolls = iter([0.1, 0.9, 0.49, 0.5])
        sink = _sink(sample_rate=0.5, _random=lambda: next(rolls))
        await sink.start()
        try:
            for _ in range(4):
                sink.enqueue(_entry(), _scope())
            assert sink._queue is not None
            assert sink._queue.qsize() == 2
        finally:
            await sink.stop()

    @pytest.mark.asyncio
    async def test_full_queue_drops_and_counts(self) -> None:
        sink = _sink(queue_size=1)
        await sink.start()
        try:
            sink.enqueue(_entry(), _scope())
            sink.enqueue(_entry(), _scope())
            assert sink.dropped == 1
        finally:
            await sink.stop()

    def test_enqueue_before_start_is_counted_not_raised(self) -> None:
        sink = _sink()
        sink.enqueue(_entry(), _scope())
        assert sink.dropped == 1


class TestWorker:
    @pytest.mark.asyncio
    async def test_posts_events_with_session_user_agent(self) -> None:
        received: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            received.append(request)
            return httpx.Response(200, text="ok")

        sink = _sink(_transport=httpx.MockTransport(handler))
        await sink.start()
        for _ in range(3):
            sink.enqueue(_entry(), _scope())
        await sink.stop()

        assert len(received) == 3
        assert sink.sent == 3
        assert all(r.url == "http://umami.test/api/send" for r in received)
        assert all(r.headers["user-agent"] == SESSION_USER_AGENT for r in received)
        assert all(json.loads(r.content)["type"] == "event" for r in received)

    @pytest.mark.asyncio
    async def test_http_error_counts_failed_and_keeps_going(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        statuses = iter([500, 200])

        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(next(statuses))

        sink = _sink(_transport=httpx.MockTransport(handler))
        with caplog.at_level(logging.WARNING, logger="foodatlas.umami"):
            await sink.start()
            sink.enqueue(_entry(), _scope())
            sink.enqueue(_entry(), _scope())
            await sink.stop()
        assert (sink.failed, sink.sent) == (1, 1)
        assert any("HTTP 500" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_connect_error_does_not_kill_the_worker(self) -> None:
        calls = 0

        def handler(_request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise httpx.ConnectError("refused")
            return httpx.Response(200)

        sink = _sink(_transport=httpx.MockTransport(handler))
        await sink.start()
        sink.enqueue(_entry(), _scope())
        sink.enqueue(_entry(), _scope())
        await sink.stop()
        assert (sink.failed, sink.sent) == (1, 1)

    @pytest.mark.asyncio
    async def test_one_warning_per_failure_streak(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused")

        sink = _sink(_transport=httpx.MockTransport(handler))
        with caplog.at_level(logging.WARNING, logger="foodatlas.umami"):
            await sink.start()
            for _ in range(5):
                sink.enqueue(_entry(), _scope())
            await sink.stop()
        assert sink.failed == 5
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(self) -> None:
        sink = _sink(_transport=httpx.MockTransport(lambda _r: httpx.Response(200)))
        await sink.start()
        await sink.stop()
        await sink.stop()
        assert sink._task is None

    @pytest.mark.asyncio
    async def test_start_is_idempotent(self) -> None:
        sink = _sink(_transport=httpx.MockTransport(lambda _r: httpx.Response(200)))
        await sink.start()
        task = sink._task
        await sink.start()
        assert sink._task is task
        await sink.stop()

    @pytest.mark.asyncio
    async def test_stop_gives_up_on_a_stuck_worker(self, monkeypatch: Any) -> None:
        # A transport that never answers: drain must time out, not hang.
        monkeypatch.setattr("src.umami_sink.STOP_DRAIN_TIMEOUT_S", 0.05)

        async def never(_request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(3600)
            return httpx.Response(200)

        sink = _sink(_transport=httpx.MockTransport(never))
        await sink.start()
        sink.enqueue(_entry(), _scope())
        await asyncio.wait_for(sink.stop(), timeout=2)
        assert sink._task is None


class TestBuildSink:
    def test_empty_website_id_disables(self) -> None:
        settings = APISettings(**{"_env_file": None})
        assert build_sink(settings) is None

    def test_settings_are_forwarded(self) -> None:
        settings = APISettings(
            **{"_env_file": None},
            umami_website_id=WEBSITE,
            umami_host_url="https://umami.example/",
            umami_hostname="api.example",
            umami_sample_rate=0.25,
            umami_queue_size=7,
        )
        sink = build_sink(settings)
        assert sink is not None
        assert sink.website_id == WEBSITE
        assert sink.send_url == "https://umami.example/api/send"
        assert sink.hostname == "api.example"
        assert sink.sample_rate == 0.25
        assert sink.queue_size == 7
