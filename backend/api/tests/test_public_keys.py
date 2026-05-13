"""Tests for the PublicKeyStore (AWS Secrets Manager-backed)."""

from __future__ import annotations

import asyncio
import hashlib
import json
from unittest.mock import MagicMock

import pytest
from src.config import APISettings
from src.public_keys import (
    KeyRecord,
    PublicKeyStore,
    _parse_secret_payload,
    get_store,
    init_store,
    set_store_for_tests,
)


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class TestParseSecretPayload:
    def test_returns_records_keyed_by_hash(self) -> None:
        payload = json.dumps(
            {
                _hash("k1"): {"email": "a@x", "created": "2026-05-13", "notes": "n"},
            }
        )
        result = _parse_secret_payload(payload)
        assert result[_hash("k1")] == KeyRecord(
            email="a@x", created="2026-05-13", notes="n"
        )

    def test_empty_string_returns_empty(self) -> None:
        assert _parse_secret_payload("") == {}

    def test_skips_non_hash_keys(self) -> None:
        payload = json.dumps({"not-a-hash": {"email": "x"}})
        assert _parse_secret_payload(payload) == {}

    def test_skips_non_dict_meta(self) -> None:
        payload = json.dumps({_hash("k"): "scalar"})
        assert _parse_secret_payload(payload) == {}

    def test_invalid_json_returns_empty(self) -> None:
        # CDK provisions the secret with a non-JSON placeholder; the loader
        # must treat that as "no keys configured" rather than crashing.
        assert _parse_secret_payload("{not json") == {}

    def test_non_object_returns_empty(self) -> None:
        assert _parse_secret_payload("[]") == {}


class TestPublicKeyStoreVerify:
    def test_returns_record_for_known_hash(self) -> None:
        store = PublicKeyStore(secret_name="s", region="us-west-1")
        store._keys = {_hash("plain"): KeyRecord(email="a@x")}
        record = store.verify("plain")
        assert record is not None
        assert record.email == "a@x"

    def test_returns_none_for_unknown(self) -> None:
        store = PublicKeyStore(secret_name="s", region="us-west-1")
        assert store.verify("anything") is None

    def test_returns_none_for_empty(self) -> None:
        store = PublicKeyStore(secret_name="s", region="us-west-1")
        store._keys = {_hash("x"): KeyRecord(email="a")}
        assert store.verify("") is None


class TestPublicKeyStoreLoad:
    @pytest.mark.asyncio
    async def test_load_populates_keys(self) -> None:
        payload = {_hash("k1"): {"email": "a@x"}}
        client = MagicMock()
        client.get_secret_value.return_value = {"SecretString": json.dumps(payload)}
        store = PublicKeyStore(
            secret_name="s",
            region="us-west-1",
            _client_factory=lambda: client,
        )
        await store.load()
        assert store.verify("k1") is not None

    @pytest.mark.asyncio
    async def test_load_replaces_previous(self) -> None:
        client = MagicMock()
        client.get_secret_value.return_value = {
            "SecretString": json.dumps({_hash("new"): {"email": "n@x"}})
        }
        store = PublicKeyStore(
            secret_name="s",
            region="us-west-1",
            _client_factory=lambda: client,
        )
        store._keys = {_hash("old"): KeyRecord(email="o@x")}
        await store.load()
        assert store.verify("old") is None
        assert store.verify("new") is not None

    @pytest.mark.asyncio
    async def test_load_handles_missing_secret_string(self) -> None:
        client = MagicMock()
        client.get_secret_value.return_value = {}
        store = PublicKeyStore(
            secret_name="s",
            region="us-west-1",
            _client_factory=lambda: client,
        )
        await store.load()
        assert store._keys == {}


class TestPublicKeyStoreLifecycle:
    @pytest.mark.asyncio
    async def test_start_does_nothing_when_no_secret_name(self) -> None:
        store = PublicKeyStore(secret_name="", region="us-west-1")
        await store.start()
        assert store._task is None

    @pytest.mark.asyncio
    async def test_start_loads_and_launches_refresh_task(self) -> None:
        client = MagicMock()
        client.get_secret_value.return_value = {"SecretString": "{}"}
        store = PublicKeyStore(
            secret_name="s",
            region="us-west-1",
            refresh_seconds=3600,
            _client_factory=lambda: client,
        )
        await store.start()
        try:
            assert store._task is not None
            assert not store._task.done()
        finally:
            await store.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_task(self) -> None:
        client = MagicMock()
        client.get_secret_value.return_value = {"SecretString": "{}"}
        store = PublicKeyStore(
            secret_name="s",
            region="us-west-1",
            refresh_seconds=3600,
            _client_factory=lambda: client,
        )
        await store.start()
        await store.stop()
        assert store._task is None

    @pytest.mark.asyncio
    async def test_refresh_loop_swallows_failure(self) -> None:
        """A refresh failure must keep the old map and not crash the loop."""
        calls = {"n": 0}

        def factory() -> MagicMock:
            client = MagicMock()
            if calls["n"] == 0:
                client.get_secret_value.return_value = {
                    "SecretString": json.dumps({_hash("k"): {"email": "a@x"}})
                }
            else:
                client.get_secret_value.side_effect = RuntimeError("boom")
            calls["n"] += 1
            return client

        store = PublicKeyStore(
            secret_name="s",
            region="us-west-1",
            refresh_seconds=0,  # 0 → loop fires immediately
            _client_factory=factory,
        )
        await store.start()
        # Yield several times so the refresh tick has a chance to run + fail.
        for _ in range(5):
            await asyncio.sleep(0)
        await store.stop()
        # Original map preserved despite the refresh failure.
        assert store.verify("k") is not None


class TestInitStore:
    def test_creates_store_from_settings(self) -> None:
        prev = get_store()
        try:
            settings = APISettings(_env_file=None)  # type: ignore[call-arg]
            settings.public_keys_secret_name = "test/secret"
            settings.aws_region = "us-east-1"
            settings.public_keys_refresh_seconds = 60
            store = init_store(settings)
            assert store.secret_name == "test/secret"
            assert store.region == "us-east-1"
            assert store.refresh_seconds == 60
            assert get_store() is store
        finally:
            set_store_for_tests(prev)
