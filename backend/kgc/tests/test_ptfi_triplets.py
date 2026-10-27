"""Tests for the PTFI triplets builder — attestation shape, per-sample
granularity, resolution by whichever public id keys a chemical, verbatim
evidence, and narrowing an ambiguous PubChem CID by the name PTFI gave."""

from __future__ import annotations

import json

import pandas as pd
from src.pipeline.triplets.food_chemical.ptfi import (
    _attestation_rows,
    _prefer_named_chemical,
    _resolve_entities,
    merge_ptfi_triplets,
)

_ENTITIES = pd.DataFrame(
    [
        {
            "foodatlas_id": "e10",
            "entity_type": "food",
            "common_name": "pepper",
            "external_ids": {"ptfi": ["GGB100", "GGB101"]},
        },
        {
            "foodatlas_id": "e20",
            "entity_type": "chemical",
            "common_name": "formate",
            "external_ids": {"pubchem_compound": [283]},
        },
        {
            "foodatlas_id": "e21",
            "entity_type": "chemical",
            "common_name": "monocarboxylic acid anion",
            "external_ids": {"pubchem_compound": [283]},
        },
        {
            "foodatlas_id": "e30",
            "entity_type": "chemical",
            "common_name": "quercitrin",
            "external_ids": {"pubchem_compound": [5192]},
        },
        {
            "foodatlas_id": "e40",
            "entity_type": "chemical",
            "common_name": "protein",
            "external_ids": {"chebi": [36080], "fdc_nutrient": [1003]},
        },
    ]
).set_index("foodatlas_id")

_REL = {"source": "ptfi", "dataset": "PTFI vX", "platform": "LC-MS"}
_PRX = {"source": "ptfi", "panel": "proximates", "unit": "g/100g"}


def _edge(
    sample_id: str,
    chemical_key: str,
    name: str,
    value: float,
    unit: str = "relative_abundance",
    evidence: dict = _REL,
) -> dict:
    return {
        "source_id": "ptfi",
        "head_native_id": "PTFI_FOOD:GGB100",
        "tail_native_id": f"PTFI_CHEM:{chemical_key}",
        "edge_type": "contains",
        "raw_attrs": {
            "sample_id": sample_id,
            "chemical_key": chemical_key,
            "chemical_name": name,
            "conc_value": value,
            "conc_unit": unit,
            "evidence": evidence,
        },
    }


class _Entities:
    def __init__(self, entities: pd.DataFrame) -> None:
        self._entities = entities


class _Evidence:
    def __init__(self) -> None:
        self.created: pd.DataFrame | None = None

    def create(self, rows: pd.DataFrame) -> pd.DataFrame:
        self.created = rows
        out = rows.copy()
        out.index = pd.Index("ev_" + rows["reference"].map(hash).astype(str))
        return out


class _Attestations:
    def __init__(self) -> None:
        self.rows: pd.DataFrame | None = None

    def create(self, rows: pd.DataFrame) -> pd.DataFrame:
        self.rows = rows
        out = rows.copy()
        out.index = pd.Index([f"at{i}" for i in range(len(rows))])
        return out


class _Triplets:
    def __init__(self) -> None:
        self.rows: pd.DataFrame | None = None

    def create(self, rows: pd.DataFrame) -> pd.DataFrame:
        self.rows = rows
        return rows.drop_duplicates(subset=["head_id", "tail_id"])


class _KG:
    def __init__(self, entities: pd.DataFrame = _ENTITIES) -> None:
        self.entities = _Entities(entities)
        self.evidence = _Evidence()
        self.attestations = _Attestations()
        self.triplets = _Triplets()


def _sources(edges: list[dict]) -> dict[str, dict[str, pd.DataFrame]]:
    return {"ptfi": {"edges": pd.DataFrame(edges)}}


class TestAmbiguityNarrowing:
    def test_ambiguous_cid_narrows_to_the_chemical_ptfi_named(self) -> None:
        """CID 283 is both `formate` and a ChEBI class; PTFI said formate."""
        kg = _KG()
        resolved = _resolve_entities(
            kg, pd.DataFrame([_edge("GGB100", "pubchem:283", "formate", 1.0)])
        )
        assert list(resolved["_tail_id"]) == ["e20"]

    def test_name_match_is_case_insensitive(self) -> None:
        kg = _KG()
        resolved = _resolve_entities(
            kg, pd.DataFrame([_edge("GGB100", "pubchem:283", "FORMATE", 1.0)])
        )
        assert list(resolved["_tail_id"]) == ["e20"]

    def test_unmatched_name_keeps_every_candidate(self) -> None:
        """Both entities are the same compound; PTFI cannot settle it, so keep both."""
        kg = _KG()
        resolved = _resolve_entities(
            kg, pd.DataFrame([_edge("GGB100", "pubchem:283", "methanoate", 1.0)])
        )
        assert set(resolved["_tail_id"]) == {"e20", "e21"}

    def test_narrowing_is_per_measurement(self) -> None:
        """One sample, one chemical, two measurements: the LC-MS row names
        formate and narrows to it; the reference-panel row's analyte code
        matches nothing and must keep both candidates, not vanish."""
        kg = _KG()
        edges = [
            _edge("GGB100", "pubchem:283", "formate", 1.0),
            _edge("GGB100", "pubchem:283", "FAA_FORMATE", 2.0, "mg/100g", _PRX),
        ]
        resolved = _resolve_entities(kg, pd.DataFrame(edges))
        by_name = resolved.groupby(
            resolved["raw_attrs"].map(lambda a: a["chemical_name"])
        )["_tail_id"].apply(set)
        assert by_name["formate"] == {"e20"}
        assert by_name["FAA_FORMATE"] == {"e20", "e21"}
        assert "_edge" not in resolved.columns

    def test_unambiguous_cid_is_untouched(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "_tail_id": "e30",
                    "tail_candidates": ["e30"],
                    "_sample_id": "GGB100",
                    "_chemical_key": "pubchem:5192",
                    "raw_attrs": {"chemical_name": "quercitrin"},
                }
            ]
        )
        assert _prefer_named_chemical(df, _ENTITIES).equals(df)


class TestChemicalKeys:
    def test_chemical_keyed_by_chebi_resolves(self) -> None:
        """Nutrient totals have no CID; the key names the id that resolves them."""
        kg = _KG()
        resolved = _resolve_entities(
            kg,
            pd.DataFrame([_edge("GGB100", "chebi:36080", "PRX_PROTEIN", 2000.0)]),
        )
        assert list(resolved["_tail_id"]) == ["e40"]

    def test_chemical_keyed_by_fdc_nutrient_resolves(self) -> None:
        kg = _KG()
        resolved = _resolve_entities(
            kg, pd.DataFrame([_edge("GGB100", "fdc_nutrient:1003", "protein", 1.0)])
        )
        assert list(resolved["_tail_id"]) == ["e40"]

    def test_unknown_key_yields_nothing(self) -> None:
        kg = _KG()
        merge_ptfi_triplets(kg, _sources([_edge("GGB100", "chebi:1", "x", 1.0)]))
        assert kg.attestations.rows is None


class TestAttestations:
    def test_one_attestation_per_sample_not_per_food(self) -> None:
        """Two samples of one food must stay two measurements, not collapse."""
        kg = _KG()
        edges = [
            _edge("GGB100", "pubchem:5192", "quercitrin", 12.5),
            _edge("GGB101", "pubchem:5192", "quercitrin", 30.0),
        ]
        merge_ptfi_triplets(kg, _sources(edges))
        assert len(kg.attestations.rows) == 2
        assert sorted(kg.attestations.rows["head_name_raw"]) == ["GGB100", "GGB101"]
        assert sorted(kg.attestations.rows["conc_value"]) == [12.5, 30.0]

    def test_concentration_passes_through_unconverted(self) -> None:
        kg = _KG()
        edges = [
            _edge("GGB100", "pubchem:5192", "quercitrin", 12.5),
            _edge("GGB100", "chebi:36080", "PRX_PROTEIN", 2000.0, "mg/100g", _PRX),
        ]
        rows = _attestation_rows(kg, _resolve_entities(kg, pd.DataFrame(edges)))
        rows = rows.set_index("_chemical_key")
        assert rows.at["pubchem:5192", "conc_unit"] == "relative_abundance"
        assert rows.at["pubchem:5192", "conc_value"] == 12.5
        assert rows.at["pubchem:5192", "conc_value_raw"] == "12.5"
        assert rows.at["chebi:36080", "conc_unit"] == "mg/100g"
        assert rows.at["chebi:36080", "conc_value"] == 2000.0
        assert set(rows["source"]) == {"ptfi"}
        assert rows["validated"].all()

    def test_evidence_is_the_adapters_record_verbatim(self) -> None:
        """One evidence row per PTFI panel, not one hand-built row for all."""
        kg = _KG()
        edges = [
            _edge("GGB100", "pubchem:5192", "quercitrin", 1.0),
            _edge("GGB100", "chebi:36080", "PRX_PROTEIN", 2000.0, "mg/100g", _PRX),
        ]
        rows = _attestation_rows(kg, _resolve_entities(kg, pd.DataFrame(edges)))
        references = [json.loads(r) for r in kg.evidence.created["reference"]]
        assert references == [_REL, _PRX]
        assert set(kg.evidence.created["source_type"]) == {"ptfi"}
        assert rows["evidence_id"].nunique() == 2


class TestTriplets:
    def test_samples_of_one_food_collapse_to_one_triplet(self) -> None:
        kg = _KG()
        edges = [
            _edge("GGB100", "pubchem:5192", "quercitrin", 12.5),
            _edge("GGB101", "pubchem:5192", "quercitrin", 30.0),
        ]
        merge_ptfi_triplets(kg, _sources(edges))
        assert len(kg.triplets.rows.drop_duplicates(["head_id", "tail_id"])) == 1
        assert set(kg.triplets.rows["relationship_id"]) == {"r1"}


class TestNoData:
    def test_absent_source_is_a_no_op(self) -> None:
        kg = _KG()
        merge_ptfi_triplets(kg, {})
        assert kg.attestations.rows is None

    def test_no_contains_edges_is_a_no_op(self) -> None:
        kg = _KG()
        merge_ptfi_triplets(kg, _sources([]))
        assert kg.attestations.rows is None

    def test_unresolvable_sample_yields_nothing(self) -> None:
        kg = _KG()
        merge_ptfi_triplets(
            kg, _sources([_edge("GGB_UNKNOWN", "pubchem:5192", "x", 1.0)])
        )
        assert kg.attestations.rows is None
