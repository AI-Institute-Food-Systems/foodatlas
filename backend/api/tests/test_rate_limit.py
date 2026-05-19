"""Tests for the /v1/ token-bucket rate limiter."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient
from src.app import create_app
from src.config import APISettings
from src.dependencies import get_db, get_settings, verify_v1_key
from src.rate_limit import TokenBucketLimiter, enforce_rate_limit

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class TestTokenBucketLimiter:
    def test_allows_up_to_burst(self) -> None:
        lim = TokenBucketLimiter(sustained_per_min=60, burst=3)
        assert lim.check("a@x.edu") == 0.0
        assert lim.check("a@x.edu") == 0.0
        assert lim.check("a@x.edu") == 0.0

    def test_throttles_past_burst(self) -> None:
        lim = TokenBucketLimiter(sustained_per_min=60, burst=2)
        lim.check("a@x.edu")
        lim.check("a@x.edu")
        retry_after = lim.check("a@x.edu")
        assert retry_after > 0.0

    def test_retry_after_reflects_refill_rate(self) -> None:
        # 60/min = 1 token/sec, so retry_after for 1 missing token ~= 1s.
        lim = TokenBucketLimiter(sustained_per_min=60, burst=1)
        lim.check("a@x.edu")
        retry_after = lim.check("a@x.edu")
        assert 0.5 < retry_after < 1.5

    def test_independent_buckets(self) -> None:
        lim = TokenBucketLimiter(sustained_per_min=60, burst=1)
        assert lim.check("a@x.edu") == 0.0
        # Different key keeps its own bucket, still has a token.
        assert lim.check("b@x.edu") == 0.0
        # Original key is now empty.
        assert lim.check("a@x.edu") > 0.0

    def test_internal_key_bypasses(self) -> None:
        lim = TokenBucketLimiter(sustained_per_min=60, burst=1)
        for _ in range(100):
            assert lim.check("internal") == 0.0

    def test_refills_over_time(self) -> None:
        # 600/min = 10 tokens/sec, so a 0.2s wait fully refills a burst-of-1.
        lim = TokenBucketLimiter(sustained_per_min=600, burst=1)
        lim.check("a@x.edu")
        assert lim.check("a@x.edu") > 0.0
        time.sleep(0.2)
        assert lim.check("a@x.edu") == 0.0

    def test_rejects_nonpositive_config(self) -> None:
        with pytest.raises(ValueError):
            TokenBucketLimiter(sustained_per_min=0, burst=10)
        with pytest.raises(ValueError):
            TokenBucketLimiter(sustained_per_min=60, burst=0)


class TestEnforceRateLimitDependency:
    @pytest.mark.asyncio
    async def test_no_limiter_installed_is_noop(self) -> None:
        request = MagicMock()
        request.app.state = MagicMock(spec=[])  # no rate_limiter attribute
        await enforce_rate_limit(request)

    @pytest.mark.asyncio
    async def test_allows_under_burst(self) -> None:
        request = MagicMock()
        request.app.state.rate_limiter = TokenBucketLimiter(
            sustained_per_min=60, burst=3
        )
        request.state.api_key_email = "a@x.edu"
        await enforce_rate_limit(request)
        await enforce_rate_limit(request)
        await enforce_rate_limit(request)

    @pytest.mark.asyncio
    async def test_raises_429_with_retry_after(self) -> None:
        request = MagicMock()
        request.app.state.rate_limiter = TokenBucketLimiter(
            sustained_per_min=60, burst=1
        )
        request.state.api_key_email = "a@x.edu"
        await enforce_rate_limit(request)
        with pytest.raises(HTTPException) as exc_info:
            await enforce_rate_limit(request)
        assert exc_info.value.status_code == 429
        assert "Retry-After" in exc_info.value.headers
        assert int(exc_info.value.headers["Retry-After"]) >= 1

    @pytest.mark.asyncio
    async def test_internal_key_bypasses_in_dependency(self) -> None:
        request = MagicMock()
        request.app.state.rate_limiter = TokenBucketLimiter(
            sustained_per_min=60, burst=1
        )
        request.state.api_key_email = "internal"
        for _ in range(50):
            await enforce_rate_limit(request)


# -- HTTP-layer integration: 429 surfaces on /v1/ when limit is hit -----------


def _enabled_settings() -> APISettings:
    s = APISettings(**{"_env_file": None})
    s.debug = False
    s.key = "x"
    s.rate_limit_enabled = True
    s.rate_limit_per_minute = 60
    s.rate_limit_burst = 2
    s.public_keys_secret_name = ""
    return s


def _build_v1_client(settings: APISettings, mock_db: AsyncMock) -> TestClient:
    app = create_app(settings)

    async def _override_db() -> AsyncGenerator[AsyncMock]:
        yield mock_db

    async def _override_verify_v1(request: Request) -> None:
        request.state.api_key_email = "alice@u.edu"

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[verify_v1_key] = _override_verify_v1
    app.dependency_overrides[get_settings] = lambda: settings
    return TestClient(app, raise_server_exceptions=False)


class TestV1RateLimitHttp:
    def test_third_request_429s(self) -> None:
        mock_db = AsyncMock()
        client = _build_v1_client(_enabled_settings(), mock_db)
        with patch(
            "src.repositories.v1.search.get_stats",
            return_value={
                "foods": 0,
                "chemicals": 0,
                "diseases": 0,
                "publications": 0,
                "connections": 0,
            },
            new_callable=AsyncMock,
        ):
            r1 = client.get("/v1/stats")
            r2 = client.get("/v1/stats")
            r3 = client.get("/v1/stats")
        assert r1.status_code == 200
        assert r2.status_code == 200
        assert r3.status_code == 429
        assert "Retry-After" in r3.headers
