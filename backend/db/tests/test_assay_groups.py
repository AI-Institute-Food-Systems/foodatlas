"""Assay groups: each assay of a pair sits in exactly one group."""

import pandas as pd
from src.etl.assay_groups import assay_groups

_EVIDENCE = pd.DataFrame(
    [
        # chemical, disease, assay, bm, relationship, bdm
        ("c1", "d1", "A1", "m1", ["therapeutic"], ["b1"]),
        ("c1", "d1", "A1", "m2", ["therapeutic"], ["b1"]),
        ("c1", "d1", "A2", "m3", ["marker/mechanism"], ["b2"]),
        ("c1", "d1", "A3", "m4", ["therapeutic"], ["b1"]),
        ("c1", "d1", "A4", "m5", ["therapeutic", "marker/mechanism"], []),
        ("c2", "d1", "A1", "m6", ["therapeutic"], ["b1"]),
    ],
    columns=[
        "chemical_id",
        "disease_id",
        "source_assay_id",
        "bm",
        "relationship",
        "bioactivity_disease_metadata_id",
    ],
)
_ASSAY_BIO = pd.DataFrame(
    [("A1", "e1"), ("A2", "e1"), ("A2", "e2"), ("A3", "e1")],
    columns=["source_assay_id", "bioactivity_id"],
)
_NAMES = {"e1": "anticancer", "e2": "antiviral"}
_TARGETS = {"b1": ["NCBIGene: 1"], "b2": ["NCBIGene: 2"]}


def _groups(chem: str) -> list[dict]:
    series = assay_groups(_EVIDENCE, _TARGETS, _ASSAY_BIO, _NAMES)
    return sorted(series.loc[(chem, "d1")], key=lambda g: g["assays"])


def test_assays_split_by_activities_and_relationships() -> None:
    assert _groups("c1") == [
        {
            "activities": ["anticancer"],
            "relationships": ["therapeutic"],
            "n_assays": 2,
            "assays": ["A1", "A3"],
            "target_genes": ["NCBIGene: 1"],
        },
        {
            "activities": ["anticancer", "antiviral"],
            "relationships": ["marker/mechanism"],
            "n_assays": 1,
            "assays": ["A2"],
            "target_genes": ["NCBIGene: 2"],
        },
        {
            "activities": [],
            "relationships": ["marker/mechanism", "therapeutic"],
            "n_assays": 1,
            "assays": ["A4"],
            "target_genes": [],
        },
    ]


def test_group_counts_sum_to_the_pairs_assays() -> None:
    for chem in ("c1", "c2"):
        n = _EVIDENCE[_EVIDENCE["chemical_id"] == chem]["source_assay_id"].nunique()
        assert sum(g["n_assays"] for g in _groups(chem)) == n
