"""Tests for verify_v1_key auth dependency."""

from __future__ import annotations

import hashlib
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from src.config import APISettings
from src.dependencies import verify_v1_key
from src.public_keys import KeyRecord, PublicKeyStore, set_store_for_tests


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _settings(debug: bool = False, internal_key: str = "internal-key") -> APISettings:
    s = APISettings(_env_file=None)  # type: ignore[call-arg]
    s.debug = debug
    s.key = internal_key
    return s


class TestVerifyV1Key:
    @pytest.mark.asyncio
    async def test_debug_bypasses_auth(self) -> None:
        request = MagicMock()
        request.headers.get.return_value = ""
        await verify_v1_key(request, _settings(debug=True))

    @pytest.mark.asyncio
    async def test_internal_key_accepted(self) -> None:
        request = MagicMock()
        request.headers.get.return_value = "Bearer internal-key"
        request.state = MagicMock()
        await verify_v1_key(request, _settings())
        assert request.state.api_key_email == "internal"

    @pytest.mark.asyncio
    async def test_public_key_accepted(self) -> None:
        store = PublicKeyStore(secret_name="s", region="us-west-1")
        store._keys = {_hash("pub-key"): KeyRecord(email="alice@u.edu")}
        prev = None
        set_store_for_tests(store)
        try:
            request = MagicMock()
            request.headers.get.return_value = "Bearer pub-key"
            request.state = MagicMock()
            await verify_v1_key(request, _settings())
            assert request.state.api_key_email == "alice@u.edu"
        finally:
            set_store_for_tests(prev)

    @pytest.mark.asyncio
    async def test_missing_header_rejected(self) -> None:
        request = MagicMock()
        request.headers.get.return_value = ""
        with pytest.raises(HTTPException) as exc_info:
            await verify_v1_key(request, _settings())
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_non_bearer_rejected(self) -> None:
        request = MagicMock()
        request.headers.get.return_value = "Basic abc"
        with pytest.raises(HTTPException):
            await verify_v1_key(request, _settings())

    @pytest.mark.asyncio
    async def test_unknown_token_rejected(self) -> None:
        prev = None
        set_store_for_tests(PublicKeyStore(secret_name="s", region="us-west-1"))
        try:
            request = MagicMock()
            request.headers.get.return_value = "Bearer not-known"
            with pytest.raises(HTTPException):
                await verify_v1_key(request, _settings())
        finally:
            set_store_for_tests(prev)

    @pytest.mark.asyncio
    async def test_no_store_rejects_non_internal(self) -> None:
        prev = None
        set_store_for_tests(None)
        try:
            request = MagicMock()
            request.headers.get.return_value = "Bearer some-key"
            with pytest.raises(HTTPException):
                await verify_v1_key(request, _settings())
        finally:
            set_store_for_tests(prev)


class TestVerifyV1KeyLedger:
    @pytest.mark.asyncio
    async def test_prefix_stashed_for_access_log(self) -> None:
        store = PublicKeyStore(secret_name="s", region="us-west-1")
        store._keys = {
            _hash("pub-key"): KeyRecord(email="alice@u.edu", prefix="pub-key1")
        }
        set_store_for_tests(store)
        try:
            request = MagicMock()
            request.headers.get.return_value = "Bearer pub-key"
            request.state = MagicMock()
            await verify_v1_key(request, _settings())
            assert request.state.api_key_prefix == "pub-key1"
        finally:
            set_store_for_tests(None)

    @pytest.mark.asyncio
    async def test_internal_key_has_no_prefix(self) -> None:
        request = MagicMock()
        request.headers.get.return_value = "Bearer internal-key"
        request.state = MagicMock()
        await verify_v1_key(request, _settings())
        assert request.state.api_key_prefix == ""

    @pytest.mark.asyncio
    async def test_revoked_key_rejected(self) -> None:
        store = PublicKeyStore(secret_name="s", region="us-west-1")
        store._keys = {_hash("gone"): KeyRecord(email="bob@u.edu", status="revoked")}
        set_store_for_tests(store)
        try:
            request = MagicMock()
            request.headers.get.return_value = "Bearer gone"
            with pytest.raises(HTTPException) as exc_info:
                await verify_v1_key(request, _settings())
            assert exc_info.value.status_code == 401
        finally:
            set_store_for_tests(None)
