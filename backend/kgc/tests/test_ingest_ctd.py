"""Tests for CTD ingest adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from src.ingest.adapters.ctd import CTDAdapter, _load_ctd_csv, _split_pipe_columns

if TYPE_CHECKING:
    from pathlib import Path


def _write_ctd_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    lines = [
        "# CTD Data\n",
        "# Fields:\n",
        f"# {','.join(header)}\n",
    ]
    for row in rows:
        lines.append(",".join(row) + "\n")
    path.write_text("".join(lines))


def test_load_ctd_csv(tmp_path: Path) -> None:
    header = ["ChemicalID", "DiseaseName", "DirectEvidence", "PubMedIDs"]
    rows = [
        ["MESH:C001", "Diabetes", "therapeutic", "123|456"],
        ["MESH:C002", "Cancer", "", "789"],
    ]
    path = tmp_path / "test.csv"
    _write_ctd_csv(path, header, rows)
    df = _load_ctd_csv(path)
    assert len(df) == 2
    assert isinstance(df.iloc[0]["PubMedIDs"], list)
    assert df.iloc[0]["PubMedIDs"] == [123, 456]


def test_split_pipe_columns() -> None:
    df = pd.DataFrame({"PubMedIDs": ["1|2|3", pd.NA], "Other": ["a", "b"]})
    result = _split_pipe_columns(df)
    assert result.iloc[0]["PubMedIDs"] == [1, 2, 3]
    assert result.iloc[1]["PubMedIDs"] == []


def test_ctd_adapter_full(tmp_path: Path) -> None:
    ctd_dir = tmp_path / "CTD"
    ctd_dir.mkdir()

    chemdis_header = [
        "ChemicalName",
        "ChemicalID",
        "CasRN",
        "DiseaseName",
        "DiseaseID",
        "DirectEvidence",
        "InferenceGeneSymbol",
        "InferenceScore",
        "OmimIDs",
        "PubMedIDs",
    ]
    chemdis_rows = [
        [
            "Caffeine",
            "MESH:C001",
            "",
            "Diabetes",
            "MESH:D001",
            "therapeutic",
            "",
            "",
            "",
            "123",
        ],
    ]
    _write_ctd_csv(ctd_dir / "CTD_chemicals_diseases.csv", chemdis_header, chemdis_rows)

    disease_header = [
        "DiseaseName",
        "DiseaseID",
        "AltDiseaseIDs",
        "Definition",
        "ParentIDs",
        "TreeNumbers",
        "ParentTreeNumbers",
        "Synonyms",
        "SlimMappings",
    ]
    disease_rows = [
        [
            "Diabetes",
            "MESH:D001",
            "OMIM:123",
            "A disease",
            "",
            "",
            "",
            "diabetes mellitus",
            "",
        ],
    ]
    _write_ctd_csv(ctd_dir / "CTD_diseases.csv", disease_header, disease_rows)

    adapter = CTDAdapter()
    out_dir = tmp_path / "output"
    manifest = adapter.ingest(tmp_path, out_dir)
    assert manifest.source_id == "ctd"
    assert manifest.node_count >= 1
    assert manifest.edge_count >= 1

    nodes = pd.read_parquet(out_dir / "ctd_nodes.parquet")
    assert len(nodes) >= 1
    assert nodes.iloc[0]["node_type"] == "disease"

    xrefs = pd.read_parquet(out_dir / "ctd_xrefs.parquet")
    assert len(xrefs) >= 1
