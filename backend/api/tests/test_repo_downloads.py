"""Tests for the downloads manifest repository."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from src.repositories import downloads


def _mock_async_client(
    *,
    json_payload: object | None = None,
    raise_for_status_exc: Exception | None = None,
    json_exc: Exception | None = None,
) -> AsyncMock:
    """Build a mock httpx.AsyncClient suitable for `async with` usage."""
    response = MagicMock()
    if raise_for_status_exc is not None:
        response.raise_for_status = MagicMock(side_effect=raise_for_status_exc)
    else:
        response.raise_for_status = MagicMock()
    if json_exc is not None:
        response.json = MagicMock(side_effect=json_exc)
    else:
        response.json = MagicMock(return_value=json_payload)

    client = AsyncMock()
    client.get = AsyncMock(return_value=response)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)

    factory = MagicMock(return_value=client)
    return factory


def test_manifest_url_is_public_https() -> None:
    assert (
        downloads.manifest_url("my-bucket", "us-west-1")
        == "https://my-bucket.s3.us-west-1.amazonaws.com/bundles/index.json"
    )


@pytest.mark.asyncio
async def test_fetch_manifest_returns_list_on_success() -> None:
    payload = [{"version": "v1.0"}]
    with patch.object(httpx, "AsyncClient", _mock_async_client(json_payload=payload)):
        result = await downloads.fetch_manifest("bucket", "us-west-1")
    assert result == payload


@pytest.mark.asyncio
async def test_fetch_manifest_returns_empty_on_network_error() -> None:
    factory = _mock_async_client(
        raise_for_status_exc=httpx.HTTPError("boom"),
    )
    with patch.object(httpx, "AsyncClient", factory):
        assert await downloads.fetch_manifest("bucket", "us-west-1") == []


@pytest.mark.asyncio
async def test_fetch_manifest_returns_empty_on_bad_json() -> None:
    factory = _mock_async_client(json_exc=json.JSONDecodeError("bad", "", 0))
    with patch.object(httpx, "AsyncClient", factory):
        assert await downloads.fetch_manifest("bucket", "us-west-1") == []


@pytest.mark.asyncio
async def test_fetch_manifest_returns_empty_when_payload_is_not_array() -> None:
    with patch.object(
        httpx, "AsyncClient", _mock_async_client(json_payload={"oops": 1})
    ):
        assert await downloads.fetch_manifest("bucket", "us-west-1") == []
