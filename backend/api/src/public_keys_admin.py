"""Ledger operations over the public-keys secret payload.

The payload is the JSON object described in :mod:`src.public_keys`:
``{sha256_hex(key): {email, created, notes, prefix, org, status, ...}}``.

Everything here is a pure dict-in/dict-out transform. All AWS calls live in
``scripts/keys/store.py`` so this module stays unit-testable (and counted by
the ``--cov=src`` gate) without touching Secrets Manager.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import secrets
from dataclasses import dataclass

from src.public_keys import ACTIVE, PREFIX_LEN, REVOKED

Payload = dict[str, dict[str, str]]

# Fields a record is allowed to carry. Anything else is dropped on write so a
# stray key from a hand-edit does not silently become part of the schema.
RECORD_FIELDS = (
    "email",
    "created",
    "notes",
    "prefix",
    "org",
    "status",
    "revoked_at",
    "issued_by",
)


class LedgerError(ValueError):
    """A ledger operation that must not be retried blindly (dup, unknown key)."""


@dataclass(frozen=True)
class MintedKey:
    """A freshly generated key. ``plaintext`` is never persisted anywhere."""

    plaintext: str
    key_hash: str
    prefix: str


@dataclass(frozen=True)
class LedgerRow:
    """One flattened record, for display by the ``list`` command."""

    key_hash: str
    email: str
    prefix: str
    created: str
    status: str
    org: str
    notes: str
    revoked_at: str


def mint() -> MintedKey:
    """Generate a key and derive its hash + non-secret prefix."""
    plaintext = secrets.token_urlsafe(32)
    return MintedKey(
        plaintext=plaintext,
        key_hash=hash_key(plaintext),
        prefix=plaintext[:PREFIX_LEN],
    )


def hash_key(plaintext: str) -> str:
    """sha256 hex of a plaintext key — the payload's record key."""
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


def build_record(
    *,
    email: str,
    notes: str = "",
    org: str = "",
    issued_by: str = "",
    prefix: str = "",
    created: str | None = None,
) -> dict[str, str]:
    """Assemble the metadata object stored under a key's hash."""
    return {
        "email": email,
        "created": created or dt.date.today().isoformat(),
        "notes": notes,
        "prefix": prefix,
        "org": org,
        "status": ACTIVE,
        "revoked_at": "",
        "issued_by": issued_by,
    }


def merge_record(payload: Payload, key_hash: str, record: dict[str, str]) -> Payload:
    """Return ``payload`` plus ``record``, refusing to clobber anything.

    A duplicate hash means we regenerated an existing key (a 256-bit
    coincidence, so in practice a bug or a replayed merge); a duplicate prefix
    would make the fingerprint ambiguous in ``list`` and in the access log.
    Both are refused rather than overwritten — this write is how the ledger
    stays trustworthy.
    """
    if key_hash in payload:
        raise LedgerError(f"hash {key_hash[:12]}… is already in the ledger")
    prefix = record.get("prefix", "")
    if prefix and _find_by_prefix(payload, prefix):
        raise LedgerError(f"prefix {prefix!r} is already in the ledger")
    merged = dict(payload)
    merged[key_hash] = {k: record.get(k, "") for k in RECORD_FIELDS}
    return merged


def revoke_record(
    payload: Payload,
    selector: str,
    *,
    revoked_at: str | None = None,
) -> tuple[Payload, LedgerRow]:
    """Flip a record to revoked, keeping it in the ledger. Returns the new payload."""
    key_hash = resolve(payload, selector)
    meta = dict(payload[key_hash])
    if (meta.get("status") or ACTIVE) == REVOKED:
        raise LedgerError(
            f"{selector!r} was already revoked on {meta.get('revoked_at')}"
        )
    meta["status"] = REVOKED
    meta["revoked_at"] = revoked_at or dt.date.today().isoformat()
    revised = dict(payload)
    revised[key_hash] = meta
    return revised, _row(key_hash, meta)


def resolve(payload: Payload, selector: str) -> str:
    """Find one record's hash by full hash, key prefix, or unique email.

    Legacy records carry no prefix, so the hash is the only stable handle for
    them — accepting all three keeps those revocable too.
    """
    if not selector:
        raise LedgerError("no key selector given")
    if selector in payload:
        return selector
    matches = _find_by_prefix(payload, selector) or _find_by_email(payload, selector)
    if not matches:
        raise LedgerError(f"no ledger record matches {selector!r}")
    if len(matches) > 1:
        raise LedgerError(
            f"{selector!r} matches {len(matches)} records; "
            "use the full hash from `list`"
        )
    return matches[0]


def format_ledger(payload: Payload, *, include_revoked: bool = True) -> list[LedgerRow]:
    """Flatten the payload into display rows, newest first."""
    rows = [_row(h, meta) for h, meta in payload.items()]
    if not include_revoked:
        rows = [r for r in rows if r.status != REVOKED]
    return sorted(rows, key=lambda r: (r.created, r.email), reverse=True)


def _row(key_hash: str, meta: dict[str, str]) -> LedgerRow:
    return LedgerRow(
        key_hash=key_hash,
        email=str(meta.get("email", "")),
        prefix=str(meta.get("prefix", "")),
        created=str(meta.get("created", "")),
        status=str(meta.get("status", "") or ACTIVE),
        org=str(meta.get("org", "")),
        notes=str(meta.get("notes", "")),
        revoked_at=str(meta.get("revoked_at", "")),
    )


def _find_by_prefix(payload: Payload, prefix: str) -> list[str]:
    if not prefix:
        return []
    return [h for h, meta in payload.items() if meta.get("prefix") == prefix]


def _find_by_email(payload: Payload, email: str) -> list[str]:
    if "@" not in email:
        return []
    return [h for h, meta in payload.items() if meta.get("email") == email]
