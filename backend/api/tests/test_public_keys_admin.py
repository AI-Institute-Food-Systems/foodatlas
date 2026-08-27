"""Tests for the pure ledger operations behind the key-admin CLI."""

from __future__ import annotations

import pytest
from src.public_keys_admin import (
    LedgerError,
    build_record,
    format_ledger,
    hash_key,
    merge_record,
    mint,
    resolve,
    revoke_record,
)


def _payload(*records: tuple[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    return dict(records)


class TestMint:
    def test_hash_and_prefix_derive_from_plaintext(self) -> None:
        minted = mint()
        assert minted.key_hash == hash_key(minted.plaintext)
        assert minted.plaintext.startswith(minted.prefix)
        assert len(minted.prefix) == 8

    def test_keys_are_unique(self) -> None:
        assert mint().plaintext != mint().plaintext


class TestBuildRecord:
    def test_defaults_to_active_today(self) -> None:
        record = build_record(email="a@x", created="2026-08-27")
        assert record["status"] == "active"
        assert record["created"] == "2026-08-27"
        assert record["revoked_at"] == ""

    def test_created_defaults_to_today_when_omitted(self) -> None:
        assert build_record(email="a@x")["created"]


class TestMergeRecord:
    def test_adds_record_without_touching_existing(self) -> None:
        existing = _payload(("h1", {"email": "a@x", "prefix": "aaaa1111"}))
        merged = merge_record(existing, "h2", build_record(email="b@x", prefix="bbbb"))
        assert set(merged) == {"h1", "h2"}
        assert merged["h1"] == existing["h1"]

    def test_does_not_mutate_the_input(self) -> None:
        existing = _payload(("h1", {"email": "a@x"}))
        merge_record(existing, "h2", build_record(email="b@x"))
        assert set(existing) == {"h1"}

    def test_duplicate_hash_refused(self) -> None:
        existing = _payload(("h1", {"email": "a@x"}))
        with pytest.raises(LedgerError, match="already in the ledger"):
            merge_record(existing, "h1", build_record(email="b@x"))

    def test_duplicate_prefix_refused(self) -> None:
        # An ambiguous prefix would break `revoke <prefix>` and make the
        # access log attribute requests to the wrong record.
        existing = _payload(("h1", {"email": "a@x", "prefix": "dupe1234"}))
        with pytest.raises(LedgerError, match="prefix"):
            merge_record(existing, "h2", build_record(email="b@x", prefix="dupe1234"))

    def test_blank_prefix_does_not_collide(self) -> None:
        # Legacy records have no prefix; that must not block new issuance.
        existing = _payload(("h1", {"email": "a@x", "prefix": ""}))
        merged = merge_record(existing, "h2", build_record(email="b@x", prefix=""))
        assert set(merged) == {"h1", "h2"}

    def test_unknown_fields_are_dropped(self) -> None:
        record = {**build_record(email="a@x"), "sneaky": "value"}
        merged = merge_record({}, "h1", record)
        assert "sneaky" not in merged["h1"]


class TestResolve:
    PAYLOAD = _payload(
        ("h" * 64, {"email": "a@x", "prefix": "aaaa1111"}),
        ("b" * 64, {"email": "b@x", "prefix": "bbbb2222"}),
        ("c" * 64, {"email": "b@x", "prefix": "cccc3333"}),
    )

    def test_by_full_hash(self) -> None:
        assert resolve(self.PAYLOAD, "h" * 64) == "h" * 64

    def test_by_prefix(self) -> None:
        assert resolve(self.PAYLOAD, "bbbb2222") == "b" * 64

    def test_by_unique_email(self) -> None:
        assert resolve(self.PAYLOAD, "a@x") == "h" * 64

    def test_ambiguous_email_refused(self) -> None:
        with pytest.raises(LedgerError, match="matches 2 records"):
            resolve(self.PAYLOAD, "b@x")

    def test_unknown_selector_refused(self) -> None:
        with pytest.raises(LedgerError, match="no ledger record"):
            resolve(self.PAYLOAD, "zzzz9999")

    def test_empty_selector_refused(self) -> None:
        with pytest.raises(LedgerError, match="no key selector"):
            resolve(self.PAYLOAD, "")


class TestRevokeRecord:
    def test_keeps_the_record_and_flips_status(self) -> None:
        payload = _payload(
            ("h1", {"email": "a@x", "prefix": "aaaa1111", "status": "active"})
        )
        revised, row = revoke_record(payload, "aaaa1111", revoked_at="2026-08-28")
        assert set(revised) == {"h1"}
        assert revised["h1"]["status"] == "revoked"
        assert revised["h1"]["revoked_at"] == "2026-08-28"
        assert row.email == "a@x"

    def test_does_not_mutate_the_input(self) -> None:
        payload = _payload(("h1", {"email": "a@x", "prefix": "aaaa1111"}))
        revoke_record(payload, "aaaa1111")
        assert payload["h1"].get("status", "") in ("", "active")

    def test_legacy_record_revocable_by_hash(self) -> None:
        payload = _payload(("h" * 64, {"email": "a@x", "created": "2026-01-01"}))
        revised, _ = revoke_record(payload, "h" * 64)
        assert revised["h" * 64]["status"] == "revoked"

    def test_double_revoke_refused(self) -> None:
        payload = _payload(
            (
                "h1",
                {
                    "email": "a@x",
                    "prefix": "p",
                    "status": "revoked",
                    "revoked_at": "2026-08-01",
                },
            )
        )
        with pytest.raises(LedgerError, match="already revoked"):
            revoke_record(payload, "p")


class TestFormatLedger:
    PAYLOAD = _payload(
        ("h1", {"email": "a@x", "created": "2026-01-01", "status": "active"}),
        ("h2", {"email": "b@x", "created": "2026-08-01", "status": "revoked"}),
    )

    def test_newest_first(self) -> None:
        assert [r.email for r in format_ledger(self.PAYLOAD)] == ["b@x", "a@x"]

    def test_can_hide_revoked(self) -> None:
        rows = format_ledger(self.PAYLOAD, include_revoked=False)
        assert [r.email for r in rows] == ["a@x"]

    def test_missing_status_reads_as_active(self) -> None:
        rows = format_ledger(_payload(("h", {"email": "a@x"})))
        assert rows[0].status == "active"
