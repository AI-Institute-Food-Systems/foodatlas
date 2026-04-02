"""Lightweight tests for ingest adapter protocol compliance."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from src.pipeline.ingest.adapters.cdno import CDNOAdapter
from src.pipeline.ingest.adapters.chebi import ChEBIAdapter
from src.pipeline.ingest.adapters.ctd import CTDAdapter
from src.pipeline.ingest.adapters.fdc import FDCAdapter
from src.pipeline.ingest.adapters.flavordb import FlavorDBAdapter
from src.pipeline.ingest.adapters.foodon import FoodOnAdapter
from src.pipeline.ingest.adapters.mesh import MeSHAdapter
from src.pipeline.ingest.adapters.pubchem import PubChemAdapter
from src.pipeline.ingest.protocol import (
    EDGES_COLUMNS,
    NODES_COLUMNS,
    XREFS_COLUMNS,
    SourceAdapter,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_all_adapters_satisfy_protocol() -> None:
    adapters = [
        FoodOnAdapter(),
        ChEBIAdapter(),
        CDNOAdapter(),
        CTDAdapter(),
        MeSHAdapter(),
        PubChemAdapter(),
        FlavorDBAdapter(),
        FDCAdapter(),
    ]
    for adapter in adapters:
        assert isinstance(adapter, SourceAdapter)


def test_all_adapters_have_unique_source_ids() -> None:
    adapters = [
        FoodOnAdapter(),
        ChEBIAdapter(),
        CDNOAdapter(),
        CTDAdapter(),
        MeSHAdapter(),
        PubChemAdapter(),
        FlavorDBAdapter(),
        FDCAdapter(),
    ]
    ids = [a.source_id for a in adapters]
    assert len(ids) == len(set(ids))


def test_adapter_source_ids() -> None:
    assert FoodOnAdapter().source_id == "foodon"
    assert ChEBIAdapter().source_id == "chebi"
    assert CDNOAdapter().source_id == "cdno"
    assert CTDAdapter().source_id == "ctd"
    assert MeSHAdapter().source_id == "mesh"
    assert PubChemAdapter().source_id == "pubchem"
    assert FlavorDBAdapter().source_id == "flavordb"
    assert FDCAdapter().source_id == "fdc"


def test_column_schemas_non_empty() -> None:
    assert len(NODES_COLUMNS) >= 6
    assert len(EDGES_COLUMNS) >= 4
    assert len(XREFS_COLUMNS) >= 4


def test_chebi_build_nodes_small(tmp_path: Path) -> None:
    """Test ChEBI node building with minimal synthetic data."""
    chebi_dir = tmp_path / "ChEBI"
    chebi_dir.mkdir()

    compounds = pd.DataFrame(
        {
            "ID": [1, 2],
            "NAME": ["Water", "Caffeine"],
            "PARENT_ID": [pd.NA, pd.NA],
            "STAR": [3, 2],
            "STATUS": ["C", "C"],
        }
    )
    compounds.to_csv(chebi_dir / "compounds.tsv", sep="\t", index=False)

    names = pd.DataFrame(
        {
            "COMPOUND_ID": [1, 1],
            "NAME": ["H2O", "oxidane"],
            "TYPE": ["SYNONYM", "IUPAC NAME"],
            "SOURCE": ["ChEBI", "ChEBI"],
            "LANGUAGE": ["en", "en"],
        }
    )
    names.to_csv(chebi_dir / "names.tsv", sep="\t", index=False)

    relations = pd.DataFrame(
        {
            "ID": [1],
            "TYPE": ["is_a"],
            "INIT_ID": [1],
            "FINAL_ID": [2],
            "STATUS": ["C"],
        }
    )
    relations.to_csv(chebi_dir / "relation.tsv", sep="\t", index=False)

    adapter = ChEBIAdapter()
    out_dir = tmp_path / "output"
    manifest = adapter.ingest(tmp_path, out_dir)

    assert manifest.source_id == "chebi"
    assert manifest.node_count == 2
    assert manifest.edge_count == 1

    nodes = pd.read_parquet(out_dir / "chebi_nodes.parquet")
    assert "water" in nodes["name"].values
    assert all(col in nodes.columns for col in NODES_COLUMNS)


def test_pubchem_build_xrefs(tmp_path: Path) -> None:
    """Test PubChem xref building with minimal synthetic data."""
    pc_dir = tmp_path / "PubChem"
    pc_dir.mkdir()

    sid_map = pd.DataFrame(
        {
            "SID": [1, 2, 3],
            "source": ["ChEBI", "ChEBI", "Other"],
            "registry_id": ["CHEBI:100", "CHEBI:200", "X:1"],
            "cid": [10, 20, 30],
        }
    )
    sid_map.to_csv(pc_dir / "SID-Map", sep="\t", index=False, header=False)

    adapter = PubChemAdapter()
    out_dir = tmp_path / "output"
    manifest = adapter.ingest(tmp_path, out_dir)

    assert manifest.source_id == "pubchem"
    assert manifest.xref_count >= 2

    xrefs = pd.read_parquet(out_dir / "pubchem_xrefs.parquet")
    chebi_xrefs = xrefs[xrefs["target_source"] == "chebi"]
    assert len(chebi_xrefs) == 2
