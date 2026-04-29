"""Tests for trust runner pure helpers (selection, merge, response parsing).

End-to-end runner is exercised via the smoke test, not in unit tests, since
it needs a live LLM call. These tests cover the deterministic helpers that
gate correctness: id derivation, response → row translation, parquet merge.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import pytest
from src.pipeline.trust.llm.base import TrustLLMRequest, TrustLLMResponse
from src.pipeline.trust.runner import (
    _merge_for_run,
    _resolve_sentence,
    _responses_to_rows,
    signal_id,
)
from src.pipeline.trust.versions import VersionBundle


def _bundle() -> VersionBundle:
    return VersionBundle(
        signal_kind="llm_plausibility",
        provider="gemini",
        model="gemini-2.5-flash-lite",
        prompts={"system": "sys", "user": "user {food}"},
        response_schema={
            "type": "object",
            "required": ["score", "reason"],
            "properties": {
                "score": {"type": "number"},
                "reason": {"type": "string"},
            },
        },
    )


def _row(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "signal_id": "x" * 64,
        "attestation_id": "atabc",
        "signal_kind": "llm_plausibility",
        "version": "v1",
        "config_hash": "h" * 64,
        "model": "gemini:gemini-2.5-flash-lite",
        "score": 0.8,
        "reason": "ok",
        "error_text": "",
        "created_at": datetime.now(UTC),
    }
    base.update(overrides)
    return base


class TestSignalId:
    def test_deterministic(self):
        a = signal_id("at1", "llm_plausibility", "v1", "h" * 64)
        b = signal_id("at1", "llm_plausibility", "v1", "h" * 64)
        assert a == b
        assert len(a) == 64

    def test_changes_on_any_field(self):
        base = signal_id("at1", "llm_plausibility", "v1", "h" * 64)
        assert signal_id("at2", "llm_plausibility", "v1", "h" * 64) != base
        assert signal_id("at1", "range_check", "v1", "h" * 64) != base
        assert signal_id("at1", "llm_plausibility", "v2", "h" * 64) != base
        assert signal_id("at1", "llm_plausibility", "v1", "g" * 64) != base


class TestResolveSentence:
    def test_returns_text_field_from_reference_json(self):
        ev = pd.DataFrame(
            {"reference": [json.dumps({"pmcid": 1, "text": "tomato is red."})]},
            index=["ev1"],
        )
        assert _resolve_sentence(ev, "ev1") == "tomato is red."

    def test_missing_text_returns_none(self):
        ev = pd.DataFrame(
            {"reference": [json.dumps({"url": "http://x"})]},
            index=["ev1"],
        )
        assert _resolve_sentence(ev, "ev1") is None

    def test_unknown_evidence_id_returns_none(self):
        ev = pd.DataFrame(
            {"reference": [json.dumps({"text": "x"})]},
            index=["ev1"],
        )
        assert _resolve_sentence(ev, "missing") is None

    def test_malformed_reference_returns_none(self):
        ev = pd.DataFrame({"reference": ["not-json"]}, index=["ev1"])
        assert _resolve_sentence(ev, "ev1") is None

    def test_empty_dataframe(self):
        assert _resolve_sentence(pd.DataFrame(), "ev1") is None


class TestResponsesToRows:
    def test_success_row(self):
        bundle = _bundle()
        requests = {"at1": TrustLLMRequest(key="at1", user_prompt="...")}
        responses = [
            TrustLLMResponse(
                key="at1",
                raw_text='{"score": 0.9, "reason": "typical"}',
                error=None,
            )
        ]
        df = _responses_to_rows(
            responses,
            requests_by_key=requests,
            bundle=bundle,
            version="v1",
            config_hash="h" * 64,
        )
        assert len(df) == 1
        assert df.iloc[0]["score"] == pytest.approx(0.9)
        assert df.iloc[0]["error_text"] == ""
        assert df.iloc[0]["reason"] == "typical"
        assert df.iloc[0]["model"] == "gemini:gemini-2.5-flash-lite"

    def test_error_response_becomes_minus_one(self):
        bundle = _bundle()
        requests = {"at1": TrustLLMRequest(key="at1", user_prompt="...")}
        responses = [TrustLLMResponse(key="at1", raw_text=None, error="rate limit")]
        df = _responses_to_rows(
            responses,
            requests_by_key=requests,
            bundle=bundle,
            version="v1",
            config_hash="h" * 64,
        )
        assert df.iloc[0]["score"] == -1
        assert df.iloc[0]["error_text"] == "rate limit"

    def test_invalid_json_becomes_error_row(self):
        bundle = _bundle()
        requests = {"at1": TrustLLMRequest(key="at1", user_prompt="...")}
        # Score out of range — fails LLMPlausibilityResponse validation.
        responses = [
            TrustLLMResponse(
                key="at1",
                raw_text='{"score": 1.5, "reason": "x"}',
                error=None,
            )
        ]
        df = _responses_to_rows(
            responses,
            requests_by_key=requests,
            bundle=bundle,
            version="v1",
            config_hash="h" * 64,
        )
        assert df.iloc[0]["score"] == -1
        assert "schema" in df.iloc[0]["error_text"]

    def test_unknown_key_dropped(self):
        bundle = _bundle()
        requests = {"at1": TrustLLMRequest(key="at1", user_prompt="...")}
        responses = [
            TrustLLMResponse(
                key="at_unknown",
                raw_text='{"score": 0.5, "reason": "x"}',
                error=None,
            )
        ]
        df = _responses_to_rows(
            responses,
            requests_by_key=requests,
            bundle=bundle,
            version="v1",
            config_hash="h" * 64,
        )
        assert df.empty


class TestMergeForRun:
    def test_empty_existing_returns_new(self):
        new = pd.DataFrame([_row()])
        merged = _merge_for_run(pd.DataFrame(), new, "llm_plausibility", "v1", "h" * 64)
        assert len(merged) == 1

    def test_empty_new_returns_existing(self):
        existing = pd.DataFrame([_row()])
        merged = _merge_for_run(
            existing, pd.DataFrame(), "llm_plausibility", "v1", "h" * 64
        )
        assert len(merged) == 1

    def test_replaces_old_for_same_signal_id(self):
        sid = signal_id("at1", "llm_plausibility", "v1", "h" * 64)
        old = _row(
            signal_id=sid, attestation_id="at1", score=-1, error_text="rate limit"
        )
        new = _row(signal_id=sid, attestation_id="at1", score=0.85, reason="ok")
        merged = _merge_for_run(
            pd.DataFrame([old]),
            pd.DataFrame([new]),
            "llm_plausibility",
            "v1",
            "h" * 64,
        )
        assert len(merged) == 1
        assert merged.iloc[0]["score"] == pytest.approx(0.85)
        assert merged.iloc[0]["error_text"] == ""

    def test_keeps_other_run_rows_untouched(self):
        # An existing v2 row for the same attestation must not be dropped.
        v1_sid = signal_id("at1", "llm_plausibility", "v1", "h" * 64)
        v2_sid = signal_id("at1", "llm_plausibility", "v2", "k" * 64)
        existing = pd.DataFrame(
            [
                _row(signal_id=v2_sid, version="v2", config_hash="k" * 64, score=0.4),
            ]
        )
        new = pd.DataFrame([_row(signal_id=v1_sid, score=0.85)])
        merged = _merge_for_run(existing, new, "llm_plausibility", "v1", "h" * 64)
        assert len(merged) == 2
        assert set(merged["version"]) == {"v1", "v2"}
