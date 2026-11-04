"""Tests for converting the PTFI delta into the adapter's source files: the
CID → ChEBI → FDC nutrient key priority, per-row evidence, and the hard
failures on anything the delta cites that cannot be keyed."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.pipeline.ingest.adapters.ptfi import SOURCE_ID, PTFIAdapter
from src.pipeline.ingest.prepare.ptfi import prepare_ptfi

if TYPE_CHECKING:
    from pathlib import Path

_IRI = "http://purl.obolibrary.org/obo/FOODON_1"
_REL = {"source": "ptfi", "dataset": "PTFI vX", "platform": "LC-MS"}
_PRX = {"source": "ptfi", "panel": "proximates", "unit": "g/100g"}


def _entity(eid: str, kind: str, name: str, external: dict, **extra: object) -> dict:
    return {
        "foodatlas_id": eid,
        "entity_type": kind,
        "common_name": name,
        "scientific_name": extra.get("scientific_name", float("nan")),
        "synonyms": json.dumps(extra.get("synonyms", [name])),
        "external_ids": json.dumps(external),
        "attributes": json.dumps(extra.get("attributes", {})),
    }


def _attestation(
    aid: str, sample: str, name: str, value: float, unit: str, ev: str
) -> dict:
    return {
        "attestation_id": aid,
        "evidence_id": ev,
        "head_name_raw": sample,
        "tail_name_raw": name,
        "conc_value": value,
        "conc_unit": unit,
    }


def _delta(
    triplets: list[tuple[str, str, list[str]]] | None = None,
    attestations: list[dict] | None = None,
) -> dict[str, pd.DataFrame]:
    """Two foods and one described chemical; cited chemicals live in the snapshot."""
    entities = [
        _entity(
            "e1",
            "food",
            "pepper (raw)",
            {"ptfi": ["GGB200", "GGB100"], "foodon": [_IRI]},
            scientific_name="Capsicum annuum",
            synonyms=["pepper (raw)", "Red Pepper"],
            attributes={"food_groups": ["plant food product"]},
        ),
        _entity("e2", "food", "falafel wrap", {"ptfi": ["GGB900"]}),
        _entity(
            "e3",
            "chemical",
            "quercitrin",
            {"pubchem_compound": [5192], "ptfi": ["MET_PTF1"], "inchikey": ["AAA-B-C"]},
        ),
    ]
    triplets = triplets or [
        ("e1", "e3", ["at1", "at2"]),
        ("e1", "e20", ["at3"]),
        ("e2", "e21", ["at4"]),
        ("e1", "e22", ["at5"]),
    ]
    attestations = attestations or [
        _attestation(
            "at1", "GGB100", "quercitrin", 12.5, "relative_abundance", "ev_rel"
        ),
        _attestation(
            "at2", "GGB200", "quercitrin", 30.0, "relative_abundance", "ev_rel"
        ),
        _attestation(
            "at3", "GGB100", "Salicylic acid", 4.0, "relative_abundance", "ev_rel"
        ),
        _attestation("at4", "GGB900", "PRX_PROTEIN", 2000.0, "mg/100g", "ev_prx"),
        _attestation("at5", "GGB100", "PRX_ASH", 500.0, "mg/100g", "ev_prx"),
    ]
    return {
        "entities": pd.DataFrame(entities),
        "triplets": pd.DataFrame(
            [
                {
                    "head_id": h,
                    "relationship_id": "r1",
                    "tail_id": t,
                    "attestation_ids": json.dumps(a),
                }
                for h, t, a in triplets
            ]
        ),
        "attestations": pd.DataFrame(attestations),
        "evidence": pd.DataFrame(
            [
                {
                    "evidence_id": "ev_rel",
                    "source_type": "ptfi",
                    "reference": json.dumps(_REL),
                },
                {
                    "evidence_id": "ev_prx",
                    "source_type": "ptfi",
                    "reference": json.dumps(_PRX),
                },
            ]
        ),
    }


def _snapshot() -> pd.DataFrame:
    rows = [
        {
            "foodatlas_id": "e20",
            "entity_type": "chemical",
            "common_name": "salicylic acid",
            "synonyms": ["salicylic acid"],
            "external_ids": {"pubchem_compound": [77.0], "chebi": [16914]},
        },
        {
            "foodatlas_id": "e21",
            "entity_type": "chemical",
            "common_name": "protein",
            "synonyms": [],
            "external_ids": {"chebi": [36080], "fdc_nutrient": [1003]},
        },
        {
            "foodatlas_id": "e22",
            "entity_type": "chemical",
            "common_name": "ash",
            "synonyms": [],
            "external_ids": {"fdc_nutrient": [1007]},
        },
        {
            "foodatlas_id": "e23",
            "entity_type": "chemical",
            "common_name": "mystery",
            "synonyms": [],
            "external_ids": {"mesh": ["D000001"]},
        },
        {
            "foodatlas_id": "e30",
            "entity_type": "food",
            "common_name": "pepper",
            "synonyms": [],
            "external_ids": {"foodon": [_IRI]},
        },
    ]
    return pd.DataFrame(rows).set_index("foodatlas_id")


class TestChemicals:
    def test_keyed_cid_then_chebi_then_fdc_nutrient(self) -> None:
        chemicals = prepare_ptfi(_delta(), _snapshot()).chemicals.set_index(
            "common_name"
        )
        assert chemicals.at["quercitrin", "chemical_key"] == "pubchem:5192"
        assert chemicals.at["salicylic acid", "chemical_key"] == "pubchem:77"
        assert chemicals.at["salicylic acid", "chebi"] == "16914"
        assert chemicals.at["protein", "chemical_key"] == "chebi:36080"
        assert chemicals.at["protein", "fdc_nutrient"] == "1003"
        assert chemicals.at["ash", "chemical_key"] == "fdc_nutrient:1007"

    def test_described_chemical_keeps_ptfi_id_and_inchikey(self) -> None:
        chemicals = prepare_ptfi(_delta(), _snapshot()).chemicals.set_index(
            "common_name"
        )
        assert chemicals.at["quercitrin", "ptfi_id"] == "MET_PTF1"
        assert chemicals.at["quercitrin", "inchikey"] == "AAA-B-C"
        assert chemicals.at["salicylic acid", "ptfi_id"] == ""


class TestMeasurements:
    def test_one_row_per_attestation_with_key_and_evidence(self) -> None:
        measurements = prepare_ptfi(_delta(), _snapshot()).measurements
        assert len(measurements) == 5
        protein = measurements[measurements["sample_id"] == "GGB900"].iloc[0]
        assert protein["chemical_key"] == "chebi:36080"
        assert protein["chemical_name"] == "PRX_PROTEIN"
        assert protein["conc_unit"] == "mg/100g"
        assert protein["evidence_id"] == "ev_prx"

    def test_evidence_is_the_deltas_verbatim(self) -> None:
        source = prepare_ptfi(_delta(), _snapshot())
        assert list(source.evidence["evidence_id"]) == ["ev_rel", "ev_prx"]
        assert json.loads(source.evidence["reference"].iloc[1]) == _PRX


class TestFoods:
    def test_keyed_on_iri_and_sorted_sample_ids(self) -> None:
        foods = prepare_ptfi(_delta(), _snapshot()).foods.set_index("common_name")
        assert json.loads(foods.at["pepper (raw)", "ptfi_sample_ids"]) == [
            "GGB100",
            "GGB200",
        ]
        assert foods.at["pepper (raw)", "foodon_iri"] == _IRI
        assert json.loads(foods.at["pepper (raw)", "food_groups"]) == [
            "plant food product"
        ]
        assert foods.at["falafel wrap", "foodon_iri"] == ""
        assert foods.at["falafel wrap", "scientific_name"] == ""


class TestFailures:
    def test_cited_chemical_with_no_key_fails(self) -> None:
        delta = _delta(triplets=[("e1", "e23", ["at1"])])
        with pytest.raises(ValueError, match="e23 \\(mystery\\)"):
            prepare_ptfi(delta, _snapshot())

    def test_cited_id_that_is_not_a_chemical_fails(self) -> None:
        delta = _delta(triplets=[("e1", "e30", ["at1"])])
        with pytest.raises(ValueError, match="e30: not a chemical"):
            prepare_ptfi(delta, _snapshot())

    def test_cited_attestation_that_is_absent_fails(self) -> None:
        delta = _delta(triplets=[("e1", "e3", ["at1", "at99"])])
        with pytest.raises(ValueError, match="1 attestation id"):
            prepare_ptfi(delta, _snapshot())

    def test_sample_belonging_to_no_food_fails(self) -> None:
        delta = _delta(
            triplets=[("e1", "e3", ["at1"])],
            attestations=[
                _attestation(
                    "at1", "GGB777", "quercitrin", 1.0, "relative_abundance", "ev_rel"
                )
            ],
        )
        with pytest.raises(ValueError, match="GGB777"):
            prepare_ptfi(delta, _snapshot())

    def test_unknown_evidence_id_fails(self) -> None:
        delta = _delta(
            triplets=[("e1", "e3", ["at1"])],
            attestations=[
                _attestation(
                    "at1", "GGB100", "quercitrin", 1.0, "relative_abundance", "ev_nope"
                )
            ],
        )
        with pytest.raises(ValueError, match="ev_nope"):
            prepare_ptfi(delta, _snapshot())


class TestWrite:
    def test_written_source_is_what_the_adapter_ingests(self, tmp_path: Path) -> None:
        """The contract between the two halves: prepare writes, the adapter reads."""
        prepare_ptfi(_delta(), _snapshot()).write(tmp_path / "raw" / "PTFI")
        manifest = PTFIAdapter().ingest(tmp_path / "raw", tmp_path / "out")
        assert manifest.node_count == 2 + 4
        assert manifest.edge_count == 5

        xrefs = pd.read_parquet(tmp_path / "out" / f"{SOURCE_ID}_xrefs.parquet")
        by_target = xrefs.groupby("target_source")["target_id"].apply(set).to_dict()
        assert by_target["chebi"] == {"16914", "36080"}
        assert by_target["fdc_nutrient"] == {"1003", "1007"}

        edges = pd.read_parquet(tmp_path / "out" / f"{SOURCE_ID}_edges.parquet")
        attrs = [json.loads(a) for a in edges["raw_attrs"]]
        protein = next(a for a in attrs if a["chemical_key"] == "chebi:36080")
        assert protein["evidence"] == _PRX
