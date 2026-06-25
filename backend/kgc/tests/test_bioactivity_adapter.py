"""Tests for the bioactivity ingest adapter — covers helpers + the
end-to-end ingest with a small synthetic CSV fixture set."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.pipeline.ingest.adapters.bioactivity import (
    SOURCE_ID,
    BioactivityAdapter,
    _build_edges,
    _build_nodes,
    _build_xrefs,
    _clean_str,
    _parse_comma_list,
    _parse_json_list,
)


class TestParsers:
    def test_parse_json_list_handles_string(self) -> None:
        assert _parse_json_list('["a","b"]') == ["a", "b"]

    def test_parse_json_list_empty_and_nan(self) -> None:
        assert _parse_json_list("") == []
        assert _parse_json_list("   ") == []
        assert _parse_json_list(float("nan")) == []  # type: ignore[arg-type]

    def test_parse_comma_list(self) -> None:
        assert _parse_comma_list("a, b , ,c") == ["a", "b", "c"]
        assert _parse_comma_list("") == []
        assert _parse_comma_list(None) == []  # type: ignore[arg-type]

    def test_clean_str(self) -> None:
        assert _clean_str("hello ") == "hello"
        assert _clean_str(float("nan")) == ""
        assert _clean_str(3.0) == "3"
        assert _clean_str(3.14) == "3.14"


class TestBuilders:
    @pytest.fixture
    def concepts(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "bioactivity_id": "B1",
                    "common_name": "Antioxidant",
                    "Synonyms": '["radical scavenger","oxidation inhibitor"]',
                    "Description": "neutralizes free radicals",
                    "last_modified": "2026-01-01",
                    "External_database_IDs": '["MESH:D000975","ChEBI:22586"]',
                    "parent_label_ids": "B0",
                },
                {
                    "bioactivity_id": "B2",
                    "common_name": "Anticancer",
                    "Synonyms": "",
                    "Description": "",
                    "last_modified": "",
                    "External_database_IDs": "",
                    "parent_label_ids": "",
                },
            ]
        )

    def test_build_nodes_carries_synonyms_and_xrefs(
        self, concepts: pd.DataFrame
    ) -> None:
        nodes = _build_nodes(concepts)
        assert len(nodes) == 2
        row = nodes[nodes["native_id"] == "B1"].iloc[0]
        assert row["node_type"] == "bioactivity"
        assert row["source_id"] == SOURCE_ID
        assert row["name"] == "antioxidant"
        assert "radical scavenger" in row["synonyms"]
        assert row["raw_attrs"]["description"] == "neutralizes free radicals"
        assert "MESH:D000975" in row["raw_attrs"]["external_database_ids"]

    def test_build_xrefs_splits_on_colon(self, concepts: pd.DataFrame) -> None:
        xrefs = _build_xrefs(concepts)
        assert len(xrefs) == 2
        assert set(xrefs["target_source"]) == {"mesh", "chebi"}
        assert "D000975" in xrefs["target_id"].tolist()

    def test_build_edges_combines_assoc_and_hierarchy(
        self, concepts: pd.DataFrame
    ) -> None:
        food_raw = pd.DataFrame(
            [
                {
                    "foodatlas_id": "F1",
                    "bioactivity_id": "B1",
                    "bioactivity_metadata_ids": '["bm1","bm2"]',
                }
            ]
        )
        chem_raw = pd.DataFrame(
            [
                {
                    "CID": "123",
                    "bioactivity_id": "B2",
                    "bioactivity_metadata_ids": '["bm3"]',
                }
            ]
        )
        edges = _build_edges(concepts, food_raw, chem_raw)
        types = sorted(edges["edge_type"].unique())
        assert types == ["exhibits", "is_a", "measured"]
        # is_a edge B1 -> B0 (from parent_label_ids on concepts row 1)
        is_a = edges[edges["edge_type"] == "is_a"]
        assert is_a.iloc[0]["head_native_id"] == "B1"
        assert is_a.iloc[0]["tail_native_id"] == "B0"


class TestAdapterEndToEnd:
    @pytest.fixture
    def raw_dir(self, tmp_path: Path) -> Path:
        """Write the minimum set of CSVs the adapter expects."""
        bio = tmp_path / "raw" / "Bioactivity"
        bio.mkdir(parents=True)

        pd.DataFrame(
            [
                {
                    "bioactivity_id": "B1",
                    "common_name": "Antioxidant",
                    "Synonyms": '["radical scavenger"]',
                    "Description": "neutralizes free radicals",
                    "last_modified": "2026-01-01",
                    "External_database_IDs": '["MESH:D000975"]',
                    "parent_label_ids": "",
                }
            ]
        ).to_csv(bio / "bioactivity_entities.csv", index=False)
        pd.DataFrame(
            [{"foodatlas_id": "F1", "bioactivity_id": "B1",
              "bioactivity_metadata_ids": '["bm1"]'}]
        ).to_csv(bio / "food_bioactivity_triplets.csv", index=False)
        pd.DataFrame(
            [{"CID": "123", "bioactivity_id": "B1",
              "bioactivity_metadata_ids": '["bm1"]'}]
        ).to_csv(bio / "chemical_bioactivity_triplets.csv", index=False)
        pd.DataFrame(
            [
                {
                    "bioactivity_metadata_id": "bm1",
                    "source_assay_id": "A1",
                    "reported_activity_outcome": "active",
                    "evidence_endpoint_type": "IC50",
                    "evidence_relation": "=",
                    "evidence_value_potency_value": 1.5,
                    "evidence_value_potency_unit": "uM",
                    "evidence_value_efficacy_zeroactivity": None,
                    "evidence_value_efficacy_infiniteactivity": None,
                    "evidence_value_efficacy_logac50_value": None,
                    "evidence_value_efficacy_hillslope": None,
                    "exhibit_type": "binding",
                    "evidence_source": "ChEMBL",
                    "evidence_type": "literature",
                    "evidence_fit_r2": None,
                    "evidence_fit_curveclass": None,
                }
            ]
        ).to_csv(bio / "bioactivity_metadata.csv", index=False)
        pd.DataFrame(
            [
                {
                    "source_assay_id": "A1",
                    "source": "ChEMBL",
                    "n_measurements": 3,
                    "assay_description": "binding",
                    "target_id": "T1",
                    "target_name": "TGT",
                    "target_organism": "human",
                    "target_uniprot": "P12345",
                    "target_entrez_gene": "1234",
                    "bioactivity_ids": '["B1"]',
                }
            ]
        ).to_csv(bio / "bioassay_metadata.csv", index=False)
        pd.DataFrame(
            [{"foodatlas_id": "D1", "bioactivity_id": "B1",
              "relationship": '["treats"]',
              "bioactivity_disease_metadata_id": '["dm1"]'}]
        ).to_csv(bio / "disease_bioactivity_triplets.csv", index=False)
        pd.DataFrame(
            [{"bioactivity_disease_metadata_id": "dm1", "target_ids": '["T1"]'}]
        ).to_csv(bio / "bioactivity_disease_metadata.csv", index=False)
        return tmp_path / "raw"

    def test_ingest_writes_all_outputs_and_manifest(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        out_dir = tmp_path / "out"
        manifest = BioactivityAdapter().ingest(raw_dir, out_dir)
        assert manifest.source_id == SOURCE_ID
        assert manifest.node_count == 1
        # 1 exhibits + 1 measured = 2 (no is_a since parent_label_ids empty)
        assert manifest.edge_count == 2
        # All 7 output parquets should exist
        for name in [
            "nodes",
            "edges",
            "xrefs",
            "measurements",
            "bioassays",
            "disease",
            "disease_targets",
        ]:
            assert (out_dir / f"bioactivity_{name}.parquet").exists()
        # Manifest written (file is named "{source_id}_manifest.json")
        assert (out_dir / f"{SOURCE_ID}_manifest.json").exists()

    def test_progress_callback_called(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        calls: list[tuple[int, int]] = []

        def progress(done: int, total: int) -> None:
            calls.append((done, total))

        BioactivityAdapter().ingest(raw_dir, tmp_path / "out", progress=progress)
        assert (0, calls[0][1]) in calls  # start
        assert calls[-1][0] == calls[-1][1]  # ends at total

    def test_measurement_columns_renamed(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        out_dir = tmp_path / "out"
        BioactivityAdapter().ingest(raw_dir, out_dir)
        measurements = pd.read_parquet(out_dir / "bioactivity_measurements.parquet")
        assert "potency_value" in measurements.columns
        assert "evidence_value_potency_value" not in measurements.columns

    def test_bioassay_ids_decoded_to_list(
        self, raw_dir: Path, tmp_path: Path
    ) -> None:
        out_dir = tmp_path / "out"
        BioactivityAdapter().ingest(raw_dir, out_dir)
        bioassays = pd.read_parquet(out_dir / "bioactivity_bioassays.parquet")
        assert bioassays["bioactivity_ids"].iloc[0] == ["B1"]


def test_parse_json_list_returns_typed_list() -> None:
    """Regression: parsed list typed for mypy no-any-return."""
    result = _parse_json_list(json.dumps(["x"]))
    assert isinstance(result, list)
    assert result == ["x"]
