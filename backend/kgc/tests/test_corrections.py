"""Tests for corrections config loader."""

from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING

from src.config.corrections import (
    CdnoChebiAssignment,
    CdnoXrefRemap,
    Corrections,
    load_corrections,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_load_default_corrections() -> None:
    c = load_corrections()
    assert isinstance(c, Corrections)
    assert "194466" in c.chebi.drop_nodes
    assert "221398" in c.chebi.rename_nodes
    assert c.ontology_roots.chebi_molecular_entity == 23367


def test_load_corrections_chebi_rename() -> None:
    c = load_corrections()
    assert c.chebi.rename_nodes["221398"]["name"] == "15G256nu"
    assert c.chebi.rename_nodes["224404"]["name"] == "15G256omicron"


def test_load_corrections_cdno_remap() -> None:
    c = load_corrections()
    assert len(c.cdno.remap_xrefs) == 1
    remap = c.cdno.remap_xrefs[0]
    assert isinstance(remap, CdnoXrefRemap)
    assert remap.target_source == "chebi"
    assert "CHEBI_80096" in remap.old_target
    assert "CHEBI_166888" in remap.new_target


def test_load_corrections_cdno_disambiguate() -> None:
    c = load_corrections()
    assert c.cdno.disambiguate_fdc["1162"] == 0
    assert c.cdno.skip_fdc_ids == ["1071"]


def test_load_corrections_cdno_chebi_fix() -> None:
    c = load_corrections()
    assert len(c.cdno.fix_chebi_assignments) == 1
    fix = c.cdno.fix_chebi_assignments[0]
    assert isinstance(fix, CdnoChebiAssignment)
    assert fix.label == "nitrogen atom"
    assert fix.chebi_id == 29351


def test_load_corrections_fdc() -> None:
    c = load_corrections()
    assert 2512381 in c.fdc.food_overrides
    assert 323121 in c.fdc.multi_foodon_resolution
    assert c.fdc.nutrient_overrides.drop_ids == [2048, 1008, 1062]
    assert c.fdc.nutrient_overrides.renames[2047] == "energy"


def test_load_corrections_chebi_lut() -> None:
    c = load_corrections()
    assert "ash" in c.chebi_lut.drop_names


def test_load_corrections_ontology_roots() -> None:
    c = load_corrections()
    assert "FOODON_00002381" in c.ontology_roots.foodon_is_food
    assert "OBI_0100026" in c.ontology_roots.foodon_is_organism


def test_load_corrections_from_custom_path(tmp_path: Path) -> None:
    yaml_content = dedent("""\
        chebi:
          drop_nodes: ["999"]
        ontology_roots:
          chebi_molecular_entity: 42
    """)
    p = tmp_path / "custom.yaml"
    p.write_text(yaml_content)
    c = load_corrections(p)
    assert c.chebi.drop_nodes == ["999"]
    assert c.ontology_roots.chebi_molecular_entity == 42
    assert c.cdno.skip_fdc_ids == []


def test_load_corrections_empty_file(tmp_path: Path) -> None:
    p = tmp_path / "empty.yaml"
    p.write_text("")
    c = load_corrections(p)
    assert isinstance(c, Corrections)
    assert c.chebi.drop_nodes == []
