"""HOTFIX 2026-06-26 — delete this whole file when the upstream cleanup
lands and `_bioact_hotfix.py` is removed (see that module's docstring).
"""

from __future__ import annotations

from src.repositories._bioact_hotfix import (
    clean_endpoint_options,
    clean_measurements,
    normalize_unit,
    should_drop,
)


class TestNormalizeUnit:
    def test_micromolar_aliases_fold_to_uM(self) -> None:
        for raw in ("uM", "UM", "MICROMOLAR", "microM", "µM", "μM"):
            assert normalize_unit(raw) == "uM"

    def test_ug_ml_aliases_fold(self) -> None:
        for raw in ("ug.mL-1", "ug ml-1", "ug/ml", "UG/ML"):
            assert normalize_unit(raw) == "ug/mL"

    def test_empty_and_none_become_literal_None(self) -> None:
        assert normalize_unit(None) == "None"
        assert normalize_unit("") == "None"
        assert normalize_unit("   ") == "None"
        assert normalize_unit("NONE") == "None"
        assert normalize_unit("none") == "None"

    def test_unknown_units_pass_through_trimmed(self) -> None:
        assert normalize_unit("  nM  ") == "nM"
        assert normalize_unit("mol/L") == "mol/L"


class TestShouldDrop:
    def test_leaked_assay_endpoints_drop(self) -> None:
        for ep in (
            "LUCIFERASE INFECTION ASSAY - IC50",
            "HEPG2TOX ASSAY - CC50",
            "Maximum test concentration that did not exhibit cytotoxicity",
        ):
            assert should_drop(ep) is True

    def test_outcome_endpoints_drop_case_insensitive(self) -> None:
        for ep in ("Mitotic index", "MITOTIC INDEX", "  cytotoxicity  "):
            assert should_drop(ep) is True

    def test_legit_endpoints_keep(self) -> None:
        for ep in ("IC50", "EC50", "Ki", "IC25", "EC90", None, ""):
            assert should_drop(ep) is False


class TestCleanMeasurements:
    def test_drops_dirty_and_folds_units(self) -> None:
        out = clean_measurements(
            [
                {"endpoint": "IC50", "unit": "MICROMOLAR", "value": 1.0},
                {"endpoint": "Mitotic index", "unit": "uM", "value": 2.0},
                {"endpoint": "HEPG2TOX ASSAY - CC50", "unit": "uM", "value": 3.0},
                {"endpoint": "EC50", "unit": "", "value": 4.0},
            ]
        )
        assert out == [
            {"endpoint": "IC50", "unit": "uM", "value": 1.0},
            {"endpoint": "EC50", "unit": "None", "value": 4.0},
        ]

    def test_empty_input(self) -> None:
        assert clean_measurements(None) == []
        assert clean_measurements([]) == []


class TestCleanEndpointOptions:
    def test_drops_dirty_and_folds_duplicate_keys(self) -> None:
        out = clean_endpoint_options(
            [
                {"endpoint": "IC50", "unit": "uM", "count": 100},
                {"endpoint": "IC50", "unit": "MICROMOLAR", "count": 50},
                {"endpoint": "IC50", "unit": "microM", "count": 5},
                {"endpoint": "Mitotic index", "unit": "uM", "count": 200},
                {"endpoint": "EC50", "unit": "nM", "count": 30},
            ]
        )
        # IC50·uM folds three aliases into one entry of 155; outcome
        # endpoint dropped entirely; sorted by count desc.
        assert out == [
            {"endpoint": "IC50", "unit": "uM", "count": 155},
            {"endpoint": "EC50", "unit": "nM", "count": 30},
        ]
