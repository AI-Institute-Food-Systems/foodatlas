"""Tests for subtree filtering."""

import pandas as pd
from src.config.corrections import OntologyRoots
from src.pipeline.entities.utils.subtree_filter import (
    _compute_descendants,
    filter_sources,
)


def _make_edges(pairs: list[tuple[str, str]], edge_type: str = "is_a") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "source_id": "test",
                "head_native_id": child,
                "tail_native_id": parent,
                "edge_type": edge_type,
                "raw_attrs": {},
            }
            for child, parent in pairs
        ]
    )


def test_compute_descendants_simple() -> None:
    edges = _make_edges([("B", "A"), ("C", "A"), ("D", "B")])
    desc = _compute_descendants(edges, "A")
    assert "A" in desc
    assert "B" in desc
    assert "C" in desc
    assert "D" in desc


def test_compute_descendants_no_match() -> None:
    edges = _make_edges([("B", "A")])
    desc = _compute_descendants(edges, "X")
    assert desc == {"X"}


def test_filter_ctd_direct_evidence() -> None:
    edges = pd.DataFrame(
        [
            {
                "source_id": "ctd",
                "head_native_id": "C1",
                "tail_native_id": "D1",
                "edge_type": "chemical_disease_association",
                "raw_attrs": {"direct_evidence": "therapeutic"},
            },
            {
                "source_id": "ctd",
                "head_native_id": "C2",
                "tail_native_id": "D2",
                "edge_type": "chemical_disease_association",
                "raw_attrs": {"direct_evidence": ""},
            },
            {
                "source_id": "ctd",
                "head_native_id": "D1",
                "tail_native_id": "D0",
                "edge_type": "is_a",
                "raw_attrs": {},
            },
        ]
    )
    sources: dict[str, dict[str, pd.DataFrame]] = {
        "ctd": {"edges": edges, "nodes": pd.DataFrame()}
    }
    filter_sources(sources, OntologyRoots())
    result_edges = sources["ctd"]["edges"]
    chemdis = result_edges[result_edges["edge_type"] == "chemical_disease_association"]
    assert len(chemdis) == 1
    assert chemdis.iloc[0]["head_native_id"] == "C1"


def test_filter_cdno_fdc_nutrient() -> None:
    nodes = pd.DataFrame(
        [
            {
                "source_id": "cdno",
                "native_id": "A",
                "name": "a",
                "synonyms": [],
                "synonym_types": [],
                "node_type": "class",
                "raw_attrs": {},
            },
            {
                "source_id": "cdno",
                "native_id": "B",
                "name": "b",
                "synonyms": [],
                "synonym_types": [],
                "node_type": "class",
                "raw_attrs": {},
            },
        ]
    )
    xrefs = pd.DataFrame(
        [
            {
                "source_id": "cdno",
                "native_id": "A",
                "target_source": "fdc_nutrient",
                "target_id": "1001",
            },
        ]
    )
    sources: dict[str, dict[str, pd.DataFrame]] = {
        "cdno": {"nodes": nodes, "xrefs": xrefs}
    }
    filter_sources(sources, OntologyRoots())
    assert len(sources["cdno"]["nodes"]) == 1
    assert sources["cdno"]["nodes"].iloc[0]["native_id"] == "A"
