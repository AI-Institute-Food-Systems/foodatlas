"""Tests for subtree filtering."""

import pandas as pd
from src.config.corrections import OntologyRoots
from src.pipeline.entities.utils.subtree_filter import (
    _compute_descendants,
    filter_sources,
)

_EDGE_COLUMNS = [
    "source_id",
    "head_native_id",
    "tail_native_id",
    "edge_type",
    "raw_attrs",
]


def _make_edges(pairs: list[tuple[str, str]], edge_type: str = "is_a") -> pd.DataFrame:
    if not pairs:
        return pd.DataFrame(columns=_EDGE_COLUMNS)
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


def _make_cdno_node(native_id: str) -> dict:
    return {
        "source_id": "cdno",
        "native_id": native_id,
        "name": native_id.lower(),
        "synonyms": [],
        "synonym_types": [],
        "node_type": "class",
        "raw_attrs": {},
    }


def test_filter_cdno_fdc_nutrient() -> None:
    nodes = pd.DataFrame([_make_cdno_node("A"), _make_cdno_node("B")])
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
    edges = _make_edges([])
    sources: dict[str, dict[str, pd.DataFrame]] = {
        "cdno": {"nodes": nodes, "edges": edges, "xrefs": xrefs}
    }
    filter_sources(sources, OntologyRoots())
    assert len(sources["cdno"]["nodes"]) == 1
    assert sources["cdno"]["nodes"].iloc[0]["native_id"] == "A"


def test_filter_cdno_keeps_whitelisted_subtree() -> None:
    vitamin_root = "http://purl.obolibrary.org/obo/CDNO_0200179"
    nodes = pd.DataFrame(
        [
            _make_cdno_node(vitamin_root),
            _make_cdno_node("VIT_C"),  # vitamin subtree, no FDC xref
            _make_cdno_node("VIT_B"),  # vitamin subtree, no FDC xref
            _make_cdno_node("UNRELATED"),  # not in subtree, no FDC xref → drop
            _make_cdno_node("WITH_FDC"),  # unrelated but has FDC xref → keep
        ]
    )
    edges = _make_edges(
        [
            ("VIT_C", vitamin_root),
            ("VIT_B", vitamin_root),
        ]
    )
    xrefs = pd.DataFrame(
        [
            {
                "source_id": "cdno",
                "native_id": "WITH_FDC",
                "target_source": "fdc_nutrient",
                "target_id": "2001",
            },
        ]
    )
    sources: dict[str, dict[str, pd.DataFrame]] = {
        "cdno": {"nodes": nodes, "edges": edges, "xrefs": xrefs}
    }
    filter_sources(sources, OntologyRoots())
    kept = set(sources["cdno"]["nodes"]["native_id"])
    assert kept == {vitamin_root, "VIT_C", "VIT_B", "WITH_FDC"}
    # Edges must be preserved between kept nodes.
    kept_edges = sources["cdno"]["edges"]
    assert len(kept_edges) == 2
