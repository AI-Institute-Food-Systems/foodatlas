"""Tests for the per-attestation trust-score filter applied to compositions."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories.trust_filter import apply_trust_filter


def _ext(att_id: str, **kwargs: object) -> dict:
    base = {
        "attestation_id": att_id,
        "extracted_food_name": "tomato",
        "extracted_chemical_name": "lycopene",
        "extracted_concentration": "5 mg/100g",
        "converted_concentration": {"value": 5.0, "unit": "mg/100g"},
        "method": "lit2kg:gpt-5.2",
    }
    base.update(kwargs)
    return base


def _ev(extractions: list[dict]) -> dict:
    return {
        "premise": "tomato contains lycopene",
        "reference": {"id": "PMC1", "url": "u", "source_name": "FoodAtlas"},
        "extraction": extractions,
    }


def _row(**evidences: object) -> dict:
    return {
        "name": "lycopene",
        "id": "echem",
        "median_concentration": {"unit": "mg/100g", "value": 99.0},
        "fdc_evidences": evidences.get("fdc"),
        "foodatlas_evidences": evidences.get("foodatlas"),
        "dmd_evidences": evidences.get("dmd"),
    }


def _ext_with_conc(att_id: str, value: float) -> dict:
    return _ext(att_id, converted_concentration={"value": value, "unit": "mg/100g"})


def _mock_session_with_scores(scores: dict[str, float]) -> AsyncMock:
    session = AsyncMock()
    result = MagicMock()
    rows = [MagicMock(attestation_id=aid, score=s) for aid, s in scores.items()]
    result.__iter__.return_value = iter(rows)
    session.execute.return_value = result
    return session


class TestShowAllMode:
    @pytest.mark.asyncio
    async def test_annotates_every_extraction(self):
        rows = [
            _row(
                foodatlas=[_ev([_ext("at_low"), _ext("at_high"), _ext("at_unscored")])]
            )
        ]
        session = _mock_session_with_scores({"at_low": 0.1, "at_high": 0.9})
        out = await apply_trust_filter(session, rows, mode="show_all", threshold=0.3)
        kept = out[0]["foodatlas_evidences"][0]["extraction"]
        flags = {e["attestation_id"]: e["trust_low"] for e in kept}
        assert flags == {
            "at_low": True,
            "at_high": False,
            "at_unscored": False,  # no score → not flagged
        }
        # show_all keeps every extraction; just annotates.
        assert len(kept) == 3

    @pytest.mark.asyncio
    async def test_no_db_call_when_no_attestations(self):
        # Empty evidence arrays: nothing to annotate; no DB call.
        rows = [_row()]
        session = AsyncMock()
        out = await apply_trust_filter(session, rows, mode="show_all", threshold=0.3)
        assert out == rows
        session.execute.assert_not_called()


class TestDefaultMode:
    @pytest.mark.asyncio
    async def test_drops_low_trust_extractions(self):
        rows = [
            _row(
                foodatlas=[
                    _ev([_ext("at_low"), _ext("at_high")]),
                ]
            )
        ]
        session = _mock_session_with_scores({"at_low": 0.1, "at_high": 0.9})
        out = await apply_trust_filter(session, rows, mode="default", threshold=0.3)
        kept = out[0]["foodatlas_evidences"][0]["extraction"]
        assert [e["attestation_id"] for e in kept] == ["at_high"]

    @pytest.mark.asyncio
    async def test_drops_row_when_all_evidences_filtered_out(self):
        rows = [_row(foodatlas=[_ev([_ext("at_low")])])]
        session = _mock_session_with_scores({"at_low": 0.1})
        out = await apply_trust_filter(session, rows, mode="default", threshold=0.3)
        assert out == []

    @pytest.mark.asyncio
    async def test_keeps_row_when_other_evidences_remain(self):
        # foodatlas filtered out, but fdc has no trust signal so passes.
        rows = [
            _row(
                fdc=[_ev([_ext("at_fdc")])],
                foodatlas=[_ev([_ext("at_low")])],
            )
        ]
        session = _mock_session_with_scores({"at_low": 0.1})
        out = await apply_trust_filter(session, rows, mode="default", threshold=0.3)
        assert len(out) == 1
        assert out[0]["foodatlas_evidences"] is None
        assert out[0]["fdc_evidences"] is not None  # fdc default 1.0 ≥ 0.3

    @pytest.mark.asyncio
    async def test_attestations_without_signal_treated_as_high_trust(self):
        rows = [_row(foodatlas=[_ev([_ext("at_unscored")])])]
        session = _mock_session_with_scores({})  # no scores returned
        out = await apply_trust_filter(session, rows, mode="default", threshold=0.3)
        # default 1.0 ≥ 0.3 → kept
        assert len(out) == 1


class TestLowOnlyMode:
    @pytest.mark.asyncio
    async def test_keeps_only_low_trust_extractions(self):
        rows = [
            _row(
                foodatlas=[_ev([_ext("at_low"), _ext("at_high")])],
            )
        ]
        session = _mock_session_with_scores({"at_low": 0.1, "at_high": 0.9})
        out = await apply_trust_filter(session, rows, mode="low_only", threshold=0.3)
        kept = out[0]["foodatlas_evidences"][0]["extraction"]
        assert [e["attestation_id"] for e in kept] == ["at_low"]

    @pytest.mark.asyncio
    async def test_drops_unscored_attestations(self):
        # Unscored default to 1.0; under low_only those don't qualify.
        rows = [_row(foodatlas=[_ev([_ext("at_unscored")])])]
        session = _mock_session_with_scores({})
        out = await apply_trust_filter(session, rows, mode="low_only", threshold=0.3)
        assert out == []

    @pytest.mark.asyncio
    async def test_drops_row_when_no_low_trust_evidence(self):
        rows = [_row(foodatlas=[_ev([_ext("at_high")])])]
        session = _mock_session_with_scores({"at_high": 0.9})
        out = await apply_trust_filter(session, rows, mode="low_only", threshold=0.3)
        assert out == []


class TestMedianRecompute:
    @pytest.mark.asyncio
    async def test_default_recomputes_median_from_surviving(self):
        # 5 + 50000 high-trust pair; the 50000 outlier is low-trust.
        # Pre-filter median (across 3 values) = 5; surviving median = 5.
        # The point: after filtering we recompute, dropping the bad value.
        rows = [
            _row(
                foodatlas=[
                    _ev(
                        [
                            _ext_with_conc("at_a", 4.0),
                            _ext_with_conc("at_b", 5.0),
                            _ext_with_conc("at_low", 50000.0),
                        ]
                    )
                ]
            )
        ]
        rows[0]["median_concentration"] = {"unit": "mg/100g", "value": 99.0}
        session = _mock_session_with_scores({"at_a": 0.9, "at_b": 0.9, "at_low": 0.05})
        out = await apply_trust_filter(session, rows, mode="default", threshold=0.3)
        assert out[0]["median_concentration"]["value"] == 4.5  # median of [4, 5]

    @pytest.mark.asyncio
    async def test_low_only_recomputes_median_from_low(self):
        rows = [
            _row(
                foodatlas=[
                    _ev(
                        [
                            _ext_with_conc("at_high", 5.0),
                            _ext_with_conc("at_low", 50000.0),
                        ]
                    )
                ]
            )
        ]
        session = _mock_session_with_scores({"at_high": 0.9, "at_low": 0.05})
        out = await apply_trust_filter(session, rows, mode="low_only", threshold=0.3)
        assert out[0]["median_concentration"]["value"] == 50000.0


class TestThreshold:
    @pytest.mark.asyncio
    async def test_score_at_threshold_is_low_trust(self):
        # Boundary inclusive on the LOW side: score == threshold is "low trust".
        # Default mode drops it; low_only mode keeps it.
        rows = [_row(foodatlas=[_ev([_ext("at_edge")])])]
        session = _mock_session_with_scores({"at_edge": 0.3})
        out = await apply_trust_filter(session, rows, mode="default", threshold=0.3)
        assert out == []

    @pytest.mark.asyncio
    async def test_score_just_above_threshold_kept_default(self):
        rows = [_row(foodatlas=[_ev([_ext("at_edge")])])]
        session = _mock_session_with_scores({"at_edge": 0.301})
        out = await apply_trust_filter(session, rows, mode="default", threshold=0.3)
        assert len(out) == 1

    @pytest.mark.asyncio
    async def test_low_only_includes_threshold_value(self):
        rows = [_row(foodatlas=[_ev([_ext("at_edge")])])]
        session = _mock_session_with_scores({"at_edge": 0.3})
        out = await apply_trust_filter(session, rows, mode="low_only", threshold=0.3)
        assert len(out) == 1
