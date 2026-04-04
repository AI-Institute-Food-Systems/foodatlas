"""Typed loader for corrections.yaml — centralized manual corrections."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_DEFAULT_PATH = Path(__file__).resolve().parent / "corrections.yaml"


@dataclass(frozen=True)
class ChebiCorrections:
    drop_nodes: list[str] = field(default_factory=list)
    rename_nodes: dict[str, dict[str, str]] = field(default_factory=dict)


@dataclass(frozen=True)
class CdnoXrefRemap:
    target_source: str
    old_target: str
    new_target: str


@dataclass(frozen=True)
class CdnoChebiAssignment:
    label: str
    chebi_id: int


@dataclass(frozen=True)
class CdnoCorrections:
    remap_xrefs: list[CdnoXrefRemap] = field(default_factory=list)
    disambiguate_fdc: dict[str, int] = field(default_factory=dict)
    skip_fdc_ids: list[str] = field(default_factory=list)
    fix_chebi_assignments: list[CdnoChebiAssignment] = field(default_factory=list)


@dataclass(frozen=True)
class FdcNutrientOverrides:
    drop_ids: list[int] = field(default_factory=list)
    renames: dict[int, str] = field(default_factory=dict)


@dataclass(frozen=True)
class FdcCorrections:
    food_overrides: dict[int, str] = field(default_factory=dict)
    multi_foodon_resolution: dict[int, str] = field(default_factory=dict)
    nutrient_overrides: FdcNutrientOverrides = field(
        default_factory=FdcNutrientOverrides
    )


@dataclass(frozen=True)
class ChebiLutCorrections:
    drop_names: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class OntologyRoots:
    foodon_is_food: str = "http://purl.obolibrary.org/obo/FOODON_00002381"
    foodon_is_organism: str = "http://purl.obolibrary.org/obo/OBI_0100026"
    chebi_molecular_entity: int = 23367


@dataclass(frozen=True)
class Corrections:
    chebi: ChebiCorrections = field(default_factory=ChebiCorrections)
    cdno: CdnoCorrections = field(default_factory=CdnoCorrections)
    fdc: FdcCorrections = field(default_factory=FdcCorrections)
    chebi_lut: ChebiLutCorrections = field(default_factory=ChebiLutCorrections)
    ontology_roots: OntologyRoots = field(default_factory=OntologyRoots)


def _build_cdno(raw: dict[str, Any]) -> CdnoCorrections:
    return CdnoCorrections(
        remap_xrefs=[CdnoXrefRemap(**r) for r in raw.get("remap_xrefs", [])],
        disambiguate_fdc={
            str(k): v for k, v in raw.get("disambiguate_fdc", {}).items()
        },
        skip_fdc_ids=[str(s) for s in raw.get("skip_fdc_ids", [])],
        fix_chebi_assignments=[
            CdnoChebiAssignment(**a) for a in raw.get("fix_chebi_assignments", [])
        ],
    )


def _build_fdc(raw: dict[str, Any]) -> FdcCorrections:
    nutrient_raw = raw.get("nutrient_overrides", {})
    return FdcCorrections(
        food_overrides={int(k): v for k, v in raw.get("food_overrides", {}).items()},
        multi_foodon_resolution={
            int(k): v for k, v in raw.get("multi_foodon_resolution", {}).items()
        },
        nutrient_overrides=FdcNutrientOverrides(
            drop_ids=nutrient_raw.get("drop_ids", []),
            renames={int(k): v for k, v in nutrient_raw.get("renames", {}).items()},
        ),
    )


def load_corrections(path: Path | None = None) -> Corrections:
    """Load and validate corrections from YAML."""
    p = path or _DEFAULT_PATH
    with p.open() as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    chebi_raw = raw.get("chebi", {})
    roots_raw = raw.get("ontology_roots", {})

    return Corrections(
        chebi=ChebiCorrections(
            drop_nodes=chebi_raw.get("drop_nodes", []),
            rename_nodes=chebi_raw.get("rename_nodes", {}),
        ),
        cdno=_build_cdno(raw.get("cdno", {})),
        fdc=_build_fdc(raw.get("fdc", {})),
        chebi_lut=ChebiLutCorrections(
            drop_names=raw.get("chebi_lut", {}).get("drop_names", []),
        ),
        ontology_roots=OntologyRoots(
            foodon_is_food=roots_raw.get(
                "foodon_is_food",
                "http://purl.obolibrary.org/obo/FOODON_00002381",
            ),
            foodon_is_organism=roots_raw.get(
                "foodon_is_organism",
                "http://purl.obolibrary.org/obo/OBI_0100026",
            ),
            chebi_molecular_entity=roots_raw.get("chebi_molecular_entity", 23367),
        ),
    )
