"""Tests for trust-signal pydantic models."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError
from src.models.trust_signal import LLMPlausibilityResponse, TrustSignal


class TestTrustSignal:
    def _row(self, **overrides):
        base = {
            "signal_id": "a" * 64,
            "attestation_id": "atabc",
            "signal_kind": "llm_plausibility",
            "version": "v1",
            "config_hash": "b" * 64,
            "model": "gemini:gemini-2.5-flash-lite",
            "score": 0.7,
            "reason": "within typical range",
            "error_text": "",
            "created_at": datetime.now(UTC),
        }
        base.update(overrides)
        return base

    def test_valid_score(self):
        TrustSignal(**self._row(score=0.7))

    def test_zero_and_one_accepted(self):
        TrustSignal(**self._row(score=0.0))
        TrustSignal(**self._row(score=1.0))

    def test_error_sentinel_accepted(self):
        # -1 is the "this row is an error" sentinel.
        TrustSignal(**self._row(score=-1.0, reason="", error_text="rate limit"))

    def test_score_below_minus_one_rejected(self):
        with pytest.raises(ValidationError):
            TrustSignal(**self._row(score=-1.5))

    def test_score_above_one_rejected(self):
        with pytest.raises(ValidationError):
            TrustSignal(**self._row(score=1.1))

    def test_defaults_for_optional_fields(self):
        row = self._row()
        row.pop("error_text")
        row.pop("reason")
        sig = TrustSignal(**row)
        assert sig.error_text == ""
        assert sig.reason == ""


class TestLLMPlausibilityResponse:
    def test_valid_response(self):
        resp = LLMPlausibilityResponse(score=0.85, reason="typical for this food")
        assert resp.score == 0.85

    def test_score_strict_zero_to_one(self):
        # The LLM never produces -1; that's set by the runner on error.
        with pytest.raises(ValidationError):
            LLMPlausibilityResponse(score=-0.1, reason="x")
        with pytest.raises(ValidationError):
            LLMPlausibilityResponse(score=1.5, reason="x")

    def test_reason_max_length(self):
        # The validator is intentionally more lenient than the prompt asks
        # for (prompt: ≤ 200 chars; validator: 500). 500 accepts, 501 rejects.
        LLMPlausibilityResponse(score=0.5, reason="x" * 500)
        with pytest.raises(ValidationError):
            LLMPlausibilityResponse(score=0.5, reason="x" * 501)

    def test_reason_default_empty(self):
        resp = LLMPlausibilityResponse(score=0.5)
        assert resp.reason == ""

    def test_parses_from_json(self):
        resp = LLMPlausibilityResponse.model_validate_json(
            '{"score": 0.42, "reason": "borderline"}'
        )
        assert resp.score == 0.42
        assert resp.reason == "borderline"
