"""Tests for corrections applier."""

import pandas as pd
from src.config.corrections import (
    CdnoCorrections,
    CdnoXrefRemap,
    ChebiCorrections,
    Corrections,
    FdcCorrections,
    FdcNutrientOverrides,
)
from src.construct.corrections_applier import apply_corrections


def _default_corrections(**kwargs: object) -> Corrections:
    return Corrections(**kwargs)


def test_chebi_drop_nodes() -> None:
    nodes = pd.DataFrame(
        [
            {"source_id": "chebi", "native_id": "100", "name": "a"},
            {"source_id": "chebi", "native_id": "200", "name": "b"},
        ]
    )
    sources = {"chebi": {"nodes": nodes}}
    corrections = _default_corrections(chebi=ChebiCorrections(drop_nodes=["100"]))
    apply_corrections(sources, corrections)
    assert len(sources["chebi"]["nodes"]) == 1
    assert sources["chebi"]["nodes"].iloc[0]["native_id"] == "200"


def test_chebi_rename_nodes() -> None:
    nodes = pd.DataFrame(
        [
            {"source_id": "chebi", "native_id": "100", "name": "old_name"},
        ]
    )
    sources = {"chebi": {"nodes": nodes}}
    corrections = _default_corrections(
        chebi=ChebiCorrections(rename_nodes={"100": {"name": "new_name"}})
    )
    apply_corrections(sources, corrections)
    assert sources["chebi"]["nodes"].iloc[0]["name"] == "new_name"


def test_cdno_remap_xrefs() -> None:
    xrefs = pd.DataFrame(
        [
            {
                "source_id": "cdno",
                "native_id": "X",
                "target_source": "chebi",
                "target_id": "OLD",
            },
            {
                "source_id": "cdno",
                "native_id": "Y",
                "target_source": "chebi",
                "target_id": "KEEP",
            },
        ]
    )
    sources = {"cdno": {"xrefs": xrefs}}
    corrections = _default_corrections(
        cdno=CdnoCorrections(
            remap_xrefs=[
                CdnoXrefRemap(target_source="chebi", old_target="OLD", new_target="NEW")
            ]
        )
    )
    apply_corrections(sources, corrections)
    result = sources["cdno"]["xrefs"]
    assert result.iloc[0]["target_id"] == "NEW"
    assert result.iloc[1]["target_id"] == "KEEP"


def test_fdc_drop_nutrients() -> None:
    nodes = pd.DataFrame(
        [
            {
                "source_id": "fdc",
                "native_id": "nutrient:100",
                "name": "a",
                "synonyms": ["a"],
            },
            {
                "source_id": "fdc",
                "native_id": "nutrient:200",
                "name": "b",
                "synonyms": ["b"],
            },
        ]
    )
    sources = {"fdc": {"nodes": nodes}}
    corrections = _default_corrections(
        fdc=FdcCorrections(nutrient_overrides=FdcNutrientOverrides(drop_ids=[100]))
    )
    apply_corrections(sources, corrections)
    assert len(sources["fdc"]["nodes"]) == 1
    assert sources["fdc"]["nodes"].iloc[0]["native_id"] == "nutrient:200"


def test_fdc_rename_nutrient() -> None:
    nodes = pd.DataFrame(
        [
            {
                "source_id": "fdc",
                "native_id": "nutrient:2047",
                "name": "old",
                "synonyms": ["old"],
            },
        ]
    )
    sources = {"fdc": {"nodes": nodes}}
    corrections = _default_corrections(
        fdc=FdcCorrections(
            nutrient_overrides=FdcNutrientOverrides(renames={2047: "energy"})
        )
    )
    apply_corrections(sources, corrections)
    assert sources["fdc"]["nodes"].iloc[0]["name"] == "energy"


def test_apply_corrections_missing_source() -> None:
    """Corrections for missing sources should be silently skipped."""
    sources: dict[str, dict[str, pd.DataFrame]] = {}
    corrections = _default_corrections(chebi=ChebiCorrections(drop_nodes=["100"]))
    apply_corrections(sources, corrections)
