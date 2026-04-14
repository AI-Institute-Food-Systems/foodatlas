"""Lightweight tests for ingest adapter protocol compliance."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from src.pipeline.ingest.adapters.cdno import CDNOAdapter
from src.pipeline.ingest.adapters.chebi import ChEBIAdapter
from src.pipeline.ingest.adapters.ctd import CTDAdapter
from src.pipeline.ingest.adapters.dmd import DMDAdapter, _parse_set_field
from src.pipeline.ingest.adapters.fdc import FDCAdapter
from src.pipeline.ingest.adapters.flavordb import FlavorDBAdapter
from src.pipeline.ingest.adapters.foodon import (
    FoodOnAdapter,
    _remove_brackets,
    _remove_suffix,
)
from src.pipeline.ingest.adapters.mesh import MeSHAdapter, _ensure_list
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
        DMDAdapter(),
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
        DMDAdapter(),
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
    assert DMDAdapter().source_id == "dmd"


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


def test_foodon_remove_brackets() -> None:
    assert _remove_brackets("<http://example.org>") == "http://example.org"
    assert _remove_brackets("no_brackets") == "no_brackets"
    assert pd.isna(_remove_brackets(pd.NA))


def test_foodon_remove_suffix() -> None:
    assert _remove_suffix("apple@en") == "apple"
    assert _remove_suffix("val^^xsd:string") == "val"
    assert _remove_suffix("plain") == "plain"


def test_mesh_ensure_list() -> None:
    assert _ensure_list([1, 2]) == [1, 2]
    assert _ensure_list({"key": "val"}) == [{"key": "val"}]
    assert _ensure_list("text") == ["text"]


def test_dmd_parse_set_field() -> None:
    assert _parse_set_field("{Peptide}") == ["Peptide"]
    assert _parse_set_field("{Peptidomics}") == ["Peptidomics"]
    assert _parse_set_field('{"Untargeted Metabolomics"}') == [
        "Untargeted Metabolomics"
    ]
    assert _parse_set_field('{"Targeted Metabolomics","Untargeted Metabolomics"}') == [
        "Targeted Metabolomics",
        "Untargeted Metabolomics",
    ]
    assert _parse_set_field("") == []
    assert _parse_set_field("{}") == []


def test_dmd_build_nodes_small(tmp_path: Path) -> None:
    """Test DMD node building with minimal synthetic data."""
    dmd_dir = tmp_path / "DMD"
    dmd_dir.mkdir()

    molecules = pd.DataFrame(
        {
            "DMD ID": ["DMD300001", "DMD300002"],
            "Molecule Name": ["CBL_0001", "CBL_0002"],
            "Display Name": ["CBL_0001", "CBL_0002"],
            "Synonyms": [pd.NA, pd.NA],
            "Chemical Composition": ["APFP", "APFPE"],
            "Composition Type": ["Amino Acid Sequence", "Amino Acid Sequence"],
            "Molecular Weight": [430.22, 559.26],
            "Molecular Weight Unit": ["g/mol", "g/mol"],
            "Adduct Type": ["{[M+H]+}", "{[M+H]+}"],
            "Omic Lab": ["{Peptidomics}", "{Peptidomics}"],
            "Molecule Classification": ["{Peptide}", "{Peptide}"],
            "External Database IDs": [
                '{"UniProt": ["P02662"]}',
                '{"UniProt": ["P02662"], "ChEBI": ["12345"]}',
            ],
            "KEGG IDs": ["{282208}", pd.NA],
            "GO IDs": [pd.NA, pd.NA],
            "Isomeric Ambiguity?": ["f", "f"],
            "Entity Type": ["Molecule", "Molecule"],
            "Date Created": ["2022-11-23", "2022-11-23"],
            "Last Updated": ["2023-11-17", "2023-11-17"],
        }
    )
    molecules.to_csv(dmd_dir / "molecule.csv", index=False)

    concentrations = pd.DataFrame(
        {
            "DMD ID": ["DMD600001", "DMD600002"],
            "DMD Product ID": ["DMD100001", "DMD100001"],
            "DMD Sample ID": ["DMD200001", "DMD200001"],
            "DMD Molecule ID": ["DMD300001", "DMD300002"],
            "DMD Experiment Setting ID": ["DMD500001", "DMD500001"],
            "Concentration Value": [0.03, pd.NA],
            "Concentration Value Alt": [0.03, pd.NA],
            "Concentration Unit": ["%", "%"],
            "Concentration Unit Alt": ["%", "%"],
            "Concentration Type": ["Relative Abundance", "Relative Abundance"],
            "Concentration Status": [
                "Detected and Quantified",
                "Not Detected",
            ],
            "Entity Type": ["Concentration", "Concentration"],
            "Date Created": ["2022-11-29", "2022-11-29"],
            "Last Updated": ["2023-01-05", "2023-01-05"],
        }
    )
    concentrations.to_csv(dmd_dir / "concentration.csv", index=False)

    estimated = pd.DataFrame(
        {
            "DMD ID": ["DMDE00001"],
            "DMD Concentration ID": ["DMD600001"],
            "DMD Product ID": ["DMD100001"],
            "DMD Sample ID": ["DMD200001"],
            "DMD Molecule ID": ["DMD300001"],
            "DMD Experiment Setting ID": ["DMD500001"],
            "Estimated Concentration Value": [12.198582],
            "Estimated Concentration Unit": ["µg/240mL"],
            "Concentration Type": ["Absolute Abundance"],
            "Concentration Status": ["Detected and Quantified"],
            "Entity Type": ["Estimated Concentration"],
            "Date Created": ["2023-10-06"],
            "Last Updated": ["2023-10-06"],
        }
    )
    estimated.to_csv(dmd_dir / "estimated_concentration.csv", index=False)

    adapter = DMDAdapter()
    out_dir = tmp_path / "output"
    manifest = adapter.ingest(tmp_path, out_dir)

    assert manifest.source_id == "dmd"
    assert manifest.node_count == 2
    assert manifest.edge_count == 1  # only one row has non-NaN alt value

    nodes = pd.read_parquet(out_dir / "dmd_nodes.parquet")
    assert "CBL_0001" in nodes["name"].values
    assert all(col in nodes.columns for col in NODES_COLUMNS)

    xrefs = pd.read_parquet(out_dir / "dmd_xrefs.parquet")
    assert all(col in xrefs.columns for col in XREFS_COLUMNS)
    chebi_xrefs = xrefs[xrefs["target_source"] == "chebi"]
    assert len(chebi_xrefs) == 1
    kegg_xrefs = xrefs[xrefs["target_source"] == "kegg"]
    assert len(kegg_xrefs) == 1

    edges = pd.read_parquet(out_dir / "dmd_edges.parquet")
    assert all(col in edges.columns for col in EDGES_COLUMNS)
    assert all(edges["head_native_id"] == "milk")
