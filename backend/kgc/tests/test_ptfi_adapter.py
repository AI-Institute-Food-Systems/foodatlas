"""Tests for the PTFI ingest adapter — node/edge/xref shape, the food-group
native id, multi-key chemicals, and the rule that only measured entities are
emitted."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.pipeline.ingest.adapters import ptfi as mod
from src.pipeline.ingest.adapters.ptfi import SOURCE_ID, PTFIAdapter
from src.pipeline.ingest.adapters.ptfi_source import _parse_list

if TYPE_CHECKING:
    from pathlib import Path

_REL = {"source": "ptfi", "dataset": "PTFI vX", "platform": "LC-MS"}
_PRX = {"source": "ptfi", "panel": "proximates", "unit": "g/100g"}


def _write_source(ptfi_dir: Path) -> None:
    """Two foods (one unmeasured) and four chemicals (one unmeasured), keyed
    by CID, CID, ChEBI and CID respectively."""
    ptfi_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "ptfi_sample_ids": json.dumps(["GGB200", "GGB100"]),
                "foodon_iri": "http://purl.obolibrary.org/obo/FOODON_1",
                "common_name": "pepper (raw)",
                "scientific_name": "Capsicum annuum",
                "synonyms": json.dumps(["pepper (raw)", "Red Pepper"]),
                "food_groups": json.dumps(["plant food product"]),
            },
            {
                "ptfi_sample_ids": json.dumps(["GGB900"]),
                "foodon_iri": "",
                "common_name": "falafel wrap",
                "scientific_name": "",
                "synonyms": json.dumps(["falafel wrap"]),
                "food_groups": json.dumps([]),
            },
        ]
    ).to_csv(ptfi_dir / "ptfi_foods.csv", index=False)
    pd.DataFrame(
        [
            _chemical(
                "pubchem:5192",
                cid="5192",
                ptfi_id="MET_PTF1",
                inchikey="AAA-B-C",
                name="quercitrin",
            ),
            _chemical("pubchem:77", cid="77", chebi="16914", name="salicylic acid"),
            _chemical(
                "chebi:36080", chebi="36080", fdc_nutrient="1003", name="protein"
            ),
            _chemical(
                "pubchem:999", cid="999", ptfi_id="MET_PTF9", name="never measured"
            ),
        ]
    ).to_csv(ptfi_dir / "ptfi_chemicals.csv", index=False)
    pd.DataFrame(
        [
            _measurement("GGB100", "pubchem:5192", "quercitrin", 12.5),
            _measurement("GGB200", "pubchem:5192", "quercitrin", 30.0),
            _measurement("GGB100", "pubchem:77", "salicylic acid", 4.0),
            _measurement(
                "GGB100", "chebi:36080", "PRX_PROTEIN", 2000.0, "mg/100g", "ev_prx"
            ),
        ]
    ).to_csv(ptfi_dir / "ptfi_measurements.csv", index=False)
    pd.DataFrame(
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
    ).to_csv(ptfi_dir / "ptfi_evidence.csv", index=False)


def _chemical(key: str, name: str, **ids: str) -> dict:
    row = {
        "chemical_key": key,
        "cid": "",
        "chebi": "",
        "fdc_nutrient": "",
        "ptfi_id": "",
        "inchikey": "",
    }
    row.update(ids)
    row["common_name"] = name
    return row


def _measurement(
    sample: str,
    key: str,
    name: str,
    value: float,
    unit: str = "relative_abundance",
    evidence_id: str = "ev_rel",
) -> dict:
    return {
        "sample_id": sample,
        "chemical_key": key,
        "chemical_name": name,
        "conc_value": value,
        "conc_unit": unit,
        "evidence_id": evidence_id,
    }


@pytest.fixture
def raw_dir(tmp_path: Path) -> Path:
    _write_source(tmp_path / "raw" / "PTFI")
    return tmp_path / "raw"


def _ingest(raw_dir: Path, out_dir: Path) -> dict[str, pd.DataFrame]:
    PTFIAdapter().ingest(raw_dir, out_dir)
    return {
        kind: pd.read_parquet(out_dir / f"{SOURCE_ID}_{kind}.parquet")
        for kind in ("nodes", "edges", "xrefs")
    }


class TestNodes:
    def test_unmeasured_entities_are_not_emitted(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        out = _ingest(raw_dir, tmp_path / "out")
        nodes = out["nodes"]
        assert set(nodes[nodes.node_type == "food"]["name"]) == {"pepper (raw)"}
        assert set(nodes[nodes.node_type == "chemical"]["name"]) == {
            "quercitrin",
            "salicylic acid",
            "protein",
        }

    def test_food_native_id_is_the_lowest_sample_id(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        """Lowest, not first, so the id does not depend on CSV row order."""
        nodes = _ingest(raw_dir, tmp_path / "out")["nodes"]
        food = nodes[nodes.node_type == "food"].iloc[0]
        assert food["native_id"] == "PTFI_FOOD:GGB100"

    def test_food_carries_its_whole_sample_group(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        nodes = _ingest(raw_dir, tmp_path / "out")["nodes"]
        attrs = json.loads(nodes[nodes.node_type == "food"].iloc[0]["raw_attrs"])
        assert attrs["sample_ids"] == ["GGB200", "GGB100"]
        assert attrs["scientific_name"] == "Capsicum annuum"

    def test_including_unmeasured_emits_everything(
        self, raw_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(mod, "INCLUDE_UNMEASURED", True)
        nodes = _ingest(raw_dir, tmp_path / "out")["nodes"]
        assert len(nodes[nodes.node_type == "food"]) == 2
        assert len(nodes[nodes.node_type == "chemical"]) == 4

    def test_chemical_native_id_is_its_key_and_attrs_carry_every_id(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        nodes = _ingest(raw_dir, tmp_path / "out")["nodes"]
        protein = nodes[nodes["name"] == "protein"].iloc[0]
        assert protein["native_id"] == "PTFI_CHEM:chebi:36080"
        attrs = json.loads(protein["raw_attrs"])
        assert attrs == {
            "cid": "",
            "chebi": "36080",
            "fdc_nutrient": "1003",
            "ptfi_id": "",
            "inchikey": "",
        }


class TestEdges:
    def test_one_edge_per_measurement_with_the_sample_id_kept(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        edges = _ingest(raw_dir, tmp_path / "out")["edges"]
        assert len(edges) == 4
        assert set(edges["edge_type"]) == {"contains"}
        samples = [json.loads(a)["sample_id"] for a in edges["raw_attrs"]]
        assert sorted(samples) == ["GGB100", "GGB100", "GGB100", "GGB200"]

    def test_head_resolves_to_the_food_group_not_the_sample(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        """Both GGB100 and GGB200 belong to one food, so both heads are the group."""
        edges = _ingest(raw_dir, tmp_path / "out")["edges"]
        assert set(edges["head_native_id"]) == {"PTFI_FOOD:GGB100"}

    def test_edge_carries_the_concentration_and_its_evidence_record(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        edges = _ingest(raw_dir, tmp_path / "out")["edges"]
        attrs = [json.loads(a) for a in edges["raw_attrs"]]
        quercitrin = next(a for a in attrs if a["chemical_key"] == "pubchem:5192")
        assert quercitrin["conc_unit"] == "relative_abundance"
        assert quercitrin["evidence"] == _REL
        protein = next(a for a in attrs if a["chemical_key"] == "chebi:36080")
        assert protein["conc_unit"] == "mg/100g"
        assert protein["conc_value"] == 2000.0
        assert protein["evidence"] == _PRX

    def test_tail_is_the_chemicals_key(self, raw_dir: Path, tmp_path: Path) -> None:
        edges = _ingest(raw_dir, tmp_path / "out")["edges"]
        assert set(edges["tail_native_id"]) == {
            "PTFI_CHEM:pubchem:5192",
            "PTFI_CHEM:pubchem:77",
            "PTFI_CHEM:chebi:36080",
        }


class TestXrefs:
    def test_keys_resolution_links_on(self, raw_dir: Path, tmp_path: Path) -> None:
        xrefs = _ingest(raw_dir, tmp_path / "out")["xrefs"]
        by_target = xrefs.groupby("target_source")["target_id"].apply(set).to_dict()
        assert by_target["foodon"] == {"http://purl.obolibrary.org/obo/FOODON_1"}
        assert by_target["pubchem"] == {"5192", "77"}
        assert by_target["chebi"] == {"16914", "36080"}
        assert by_target["fdc_nutrient"] == {"1003"}
        # Only the PTFI-described chemical carries an InChIKey.
        assert by_target["inchikey"] == {"AAA-B-C"}

    def test_food_without_an_iri_gets_no_foodon_xref(
        self, raw_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(mod, "INCLUDE_UNMEASURED", True)
        xrefs = _ingest(raw_dir, tmp_path / "out")["xrefs"]
        foodon = xrefs[xrefs.target_source == "foodon"]
        assert "PTFI_FOOD:GGB900" not in set(foodon["native_id"])


class TestManifest:
    def test_counts_match_the_emitted_files(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        out_dir = tmp_path / "out"
        manifest = PTFIAdapter().ingest(raw_dir, out_dir)
        assert manifest.source_id == SOURCE_ID
        assert manifest.node_count == 4  # 1 food + 3 chemicals
        assert manifest.edge_count == 4
        assert (out_dir / f"{SOURCE_ID}_manifest.json").exists()


class TestParseList:
    def test_handles_json_blank_and_garbage(self) -> None:
        assert _parse_list('["a","b"]') == ["a", "b"]
        assert _parse_list("") == []
        assert _parse_list("not json") == []
        assert _parse_list(["a"]) == ["a"]
        assert _parse_list('{"a": 1}') == []
