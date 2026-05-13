"""Public /v1/ API key store, backed by AWS Secrets Manager.

The secret named by :attr:`APISettings.public_keys_secret_name` holds a JSON
object keyed by ``sha256_hex(plaintext_key)``::

    {
        "<sha256-of-key>": {
            "email":   "alice@uni.edu",
            "created": "2026-05-13",
            "notes":   "DMD paper figure code"
        },
        ...
    }

Plaintext keys never live on disk or in this process — only their hashes do.
A new key is issued by ``scripts/issue_public_key.py`` and pasted into the
secret via ``aws secretsmanager update-secret``; the running API picks it up
on the next refresh tick (default 300s) or restart.

Failure model: the initial load on startup is fail-closed (the app refuses
to come up) unless ``debug=True`` or ``public_keys_secret_name`` is empty.
After the initial load succeeds, refresh failures log a warning but keep
the previously-loaded map so a transient AWS outage doesn't lock everyone
out mid-flight.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from src.config import APISettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KeyRecord:
    """Metadata for an issued key. Plaintext is never stored — only the hash."""

    email: str
    created: str = ""
    notes: str = ""


@dataclass
class PublicKeyStore:
    """In-memory map of sha256(key) → KeyRecord, refreshed from Secrets Manager.

    The store is safe to share across requests: ``verify`` only reads
    :attr:`_keys`, and refreshes swap the dict atomically.
    """

    secret_name: str
    region: str
    refresh_seconds: int = 300
    _keys: dict[str, KeyRecord] = field(default_factory=dict)
    _task: asyncio.Task[None] | None = None
    # Test injection point: when set, the loader calls this instead of boto3.
    _client_factory: Callable[[], Any] | None = None

    def verify(self, token: str) -> KeyRecord | None:
        """Return the matching record (and identify the caller) or None."""
        if not token:
            return None
        return self._keys.get(_hash(token))

    async def load(self) -> None:
        """Fetch the secret once and replace the in-memory map atomically."""
        raw = await asyncio.to_thread(self._fetch_secret)
        self._keys = _parse_secret_payload(raw)

    async def start(self) -> None:
        """Initial load + start the refresh loop. Call from app lifespan."""
        if not self.secret_name:
            logger.info("public_keys: secret name empty; public key path disabled")
            return
        await self.load()
        self._task = asyncio.create_task(
            self._refresh_loop(), name="public_keys_refresh"
        )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None

    async def _refresh_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.refresh_seconds)
                await self.load()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "public_keys: refresh failed, keeping previous map: %s", exc
                )

    def _fetch_secret(self) -> str:
        if self._client_factory is not None:
            client = self._client_factory()
        else:
            import boto3  # noqa: PLC0415

            client = boto3.client("secretsmanager", region_name=self.region)
        resp = client.get_secret_value(SecretId=self.secret_name)
        return str(resp.get("SecretString") or "{}")


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _parse_secret_payload(raw: str) -> dict[str, KeyRecord]:
    """Parse the secret JSON; drop malformed entries with a warning."""
    try:
        payload = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        msg = f"public_keys secret is not valid JSON: {exc}"
        raise ValueError(msg) from exc
    if not isinstance(payload, dict):
        msg = "public_keys secret must be a JSON object keyed by key hash"
        raise ValueError(msg)
    out: dict[str, KeyRecord] = {}
    for key_hash, meta in payload.items():
        if not isinstance(key_hash, str) or len(key_hash) != 64:
            logger.warning("public_keys: skipping non-hash key %r", key_hash)
            continue
        if not isinstance(meta, dict):
            logger.warning("public_keys: skipping non-object metadata for %s", key_hash)
            continue
        out[key_hash] = KeyRecord(
            email=str(meta.get("email", "")),
            created=str(meta.get("created", "")),
            notes=str(meta.get("notes", "")),
        )
    return out


_store: PublicKeyStore | None = None


def init_store(settings: APISettings) -> PublicKeyStore:
    """Build (or rebuild) the module-level store from settings."""
    global _store  # noqa: PLW0603
    _store = PublicKeyStore(
        secret_name=settings.public_keys_secret_name,
        region=settings.aws_region,
        refresh_seconds=settings.public_keys_refresh_seconds,
    )
    return _store


def get_store() -> PublicKeyStore | None:
    return _store


def set_store_for_tests(store: PublicKeyStore | None) -> None:
    """Test-only override; production code uses ``init_store``."""
    global _store  # noqa: PLW0603
    _store = store
