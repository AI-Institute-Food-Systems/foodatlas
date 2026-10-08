"""Tests for the PTFI ingest adapter — node/edge/xref shape, the food-group
native id, and the rule that only measured entities are emitted."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.pipeline.ingest.adapters import ptfi as mod
from src.pipeline.ingest.adapters.ptfi import SOURCE_ID, PTFIAdapter

if TYPE_CHECKING:
    from pathlib import Path


def _write_source(ptfi_dir: Path) -> None:
    """Two foods (one unmeasured) and three chemicals (one unmeasured)."""
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
            {
                "cid": "5192",
                "ptfi_id": "MET_PTF1",
                "inchikey": "AAA-B-C",
                "common_name": "quercitrin",
            },
            {
                "cid": "77",
                "ptfi_id": "",
                "inchikey": "",
                "common_name": "salicylic acid",
            },
            {
                "cid": "999",
                "ptfi_id": "MET_PTF9",
                "inchikey": "ZZZ-B-C",
                "common_name": "never measured",
            },
        ]
    ).to_csv(ptfi_dir / "ptfi_chemicals.csv", index=False)
    pd.DataFrame(
        [
            {
                "sample_id": "GGB100",
                "cid": "5192",
                "chemical_name": "quercitrin",
                "conc_value": 12.5,
                "conc_unit": "relative_abundance",
            },
            {
                "sample_id": "GGB200",
                "cid": "5192",
                "chemical_name": "quercitrin",
                "conc_value": 30.0,
                "conc_unit": "relative_abundance",
            },
            {
                "sample_id": "GGB100",
                "cid": "77",
                "chemical_name": "salicylic acid",
                "conc_value": 4.0,
                "conc_unit": "relative_abundance",
            },
        ]
    ).to_csv(ptfi_dir / "ptfi_measurements.csv", index=False)
    (ptfi_dir / "ptfi_evidence.json").write_text(
        json.dumps({"source": "ptfi", "dataset": "PTFI vX", "platform": "LC-MS"})
    )


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
        assert len(nodes[nodes.node_type == "chemical"]) == 3


class TestEdges:
    def test_one_edge_per_measurement_with_the_sample_id_kept(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        edges = _ingest(raw_dir, tmp_path / "out")["edges"]
        assert len(edges) == 3
        assert set(edges["edge_type"]) == {"contains"}
        samples = [json.loads(a)["sample_id"] for a in edges["raw_attrs"]]
        assert sorted(samples) == ["GGB100", "GGB100", "GGB200"]

    def test_head_resolves_to_the_food_group_not_the_sample(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        """Both GGB100 and GGB200 belong to one food, so both heads are the group."""
        edges = _ingest(raw_dir, tmp_path / "out")["edges"]
        assert set(edges["head_native_id"]) == {"PTFI_FOOD:GGB100"}

    def test_edge_carries_the_concentration_and_provenance(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        edges = _ingest(raw_dir, tmp_path / "out")["edges"]
        attrs = json.loads(edges.iloc[0]["raw_attrs"])
        assert attrs["conc_unit"] == "relative_abundance"
        assert attrs["dataset"] == "PTFI vX"
        assert attrs["platform"] == "LC-MS"


class TestXrefs:
    def test_keys_resolution_links_on(self, raw_dir: Path, tmp_path: Path) -> None:
        xrefs = _ingest(raw_dir, tmp_path / "out")["xrefs"]
        by_target = xrefs.groupby("target_source")["target_id"].apply(set).to_dict()
        assert by_target["foodon"] == {"http://purl.obolibrary.org/obo/FOODON_1"}
        assert by_target["pubchem"] == {"5192", "77"}
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
        assert manifest.node_count == 3  # 1 food + 2 chemicals
        assert manifest.edge_count == 3
        assert (out_dir / f"{SOURCE_ID}_manifest.json").exists()


class TestParseList:
    def test_handles_json_blank_and_garbage(self) -> None:
        assert mod._parse_list('["a","b"]') == ["a", "b"]
        assert mod._parse_list("") == []
        assert mod._parse_list("not json") == []
        assert mod._parse_list(["a"]) == ["a"]
        assert mod._parse_list('{"a": 1}') == []
