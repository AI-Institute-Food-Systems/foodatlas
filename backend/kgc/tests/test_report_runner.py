"""Tests for report.runner — diff computation logic."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pathlib import Path
from src.pipeline.report.load_old import OldKG
from src.pipeline.report.runner import (
    KGDiffResult,
    compare_entities,
    compare_entity_details,
    compare_sources,
    compare_triplets,
    run_diff,
)


def _make_entities(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    """Helper: build entity DataFrame from (id, type, name) tuples."""
    df = pd.DataFrame(rows, columns=["foodatlas_id", "entity_type", "common_name"])
    return df.set_index("foodatlas_id")


def _make_triplets(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["head_id", "relationship_id", "tail_id"])


class TestCompareEntities:
    def test_basic_diff(self) -> None:
        old = _make_entities([("e1", "food", "a"), ("e2", "chemical", "b")])
        new = _make_entities([("e1", "food", "a"), ("e3", "disease", "c")])
        old_t = _make_triplets([("e1", "r1", "e2")])
        new_t = _make_triplets([("e1", "r1", "e3")])
        s = compare_entities(old, new, old_t, new_t)
        assert s.old_counts == {"food": 1, "chemical": 1}
        assert s.new_counts == {"food": 1, "disease": 1}
        assert s.new_ids == ["e3"]
        assert s.removed_ids == ["e2"]
        assert s.stable_count == 1

    def test_identical(self) -> None:
        old = _make_entities([("e1", "food", "a")])
        new = _make_entities([("e1", "food", "a")])
        empty = _make_triplets([])
        s = compare_entities(old, new, empty, empty)
        assert s.new_ids == []
        assert s.removed_ids == []
        assert s.stable_count == 1

    def test_empty_old(self) -> None:
        old = _make_entities([])
        new = _make_entities([("e1", "food", "a")])
        empty = _make_triplets([])
        s = compare_entities(old, new, empty, empty)
        assert s.new_ids == ["e1"]
        assert s.stable_count == 0

    def test_orphans(self) -> None:
        old = _make_entities(
            [("e1", "food", "a"), ("e2", "chemical", "b"), ("e3", "chemical", "c")]
        )
        new = _make_entities(
            [("e1", "food", "a"), ("e2", "chemical", "b"), ("e4", "disease", "d")]
        )
        old_t = _make_triplets([("e1", "r1", "e2")])
        new_t = _make_triplets([("e1", "r1", "e2")])
        s = compare_entities(old, new, old_t, new_t)
        # e3 is orphan in old; e4 is orphan in new
        assert s.old_orphans_by_type == {"chemical": 1}
        assert s.new_orphans_by_type == {"disease": 1}


class TestCompareTriplets:
    def test_basic_diff(self) -> None:
        old = _make_triplets([("e1", "r1", "e2"), ("e3", "r2", "e4")])
        new = _make_triplets([("e1", "r1", "e2"), ("e5", "r1", "e6")])
        s = compare_triplets(old, new)
        assert s.old_counts == {"r1": 1, "r2": 1}
        assert s.new_counts == {"r1": 2}
        assert s.new_count == 1
        assert s.removed_count == 1
        assert s.stable_count == 1

    def test_empty(self) -> None:
        old = _make_triplets([])
        new = _make_triplets([])
        s = compare_triplets(old, new)
        assert s.stable_count == 0
        assert s.new_count == 0


class TestCompareEntityDetails:
    def test_name_changes(self) -> None:
        old = _make_entities([("e1", "food", "apple"), ("e2", "food", "pear")])
        new = _make_entities([("e1", "food", "Apple"), ("e2", "food", "pear")])
        d = compare_entity_details(old, new)
        assert len(d.name_changes) == 1
        assert d.name_changes[0] == ("e1", "apple", "Apple")

    def test_type_changes(self) -> None:
        old = _make_entities([("e1", "flavor", "vanilla")])
        new = _make_entities([("e1", "chemical", "vanilla")])
        d = compare_entity_details(old, new)
        assert len(d.type_changes) == 1
        assert d.type_changes[0] == ("e1", "flavor", "chemical")

    def test_no_changes(self) -> None:
        old = _make_entities([("e1", "food", "a")])
        new = _make_entities([("e1", "food", "a")])
        d = compare_entity_details(old, new)
        assert d.name_changes == []
        assert d.type_changes == []


class TestCompareSources:
    def test_basic(self) -> None:
        old_mc = pd.Series({"fdc": 100, "dmd": 50}, name="source")
        old_md = pd.Series({"ctd": 200}, name="source")
        new_att = pd.DataFrame({"source": ["fdc", "fdc", "chebi"]})
        new_ev = pd.DataFrame({"source_type": ["pubmed", "pubmed", "fdc"]})

        c = compare_sources(old_mc, old_md, new_att, new_ev)
        assert c.old_contains_by_source == {"fdc": 100, "dmd": 50}
        assert c.old_diseases_by_source == {"ctd": 200}
        assert c.new_attestations_by_source == {"fdc": 2, "chebi": 1}
        assert c.new_evidence_by_type == {"pubmed": 2, "fdc": 1}


class TestRunDiff:
    def test_end_to_end(self, tmp_path: Path) -> None:
        # Build old KG dataclass
        old_ents = pd.DataFrame(
            [("e1", "food", "apple", "", ["apple"], {})],
            columns=[
                "foodatlas_id",
                "entity_type",
                "common_name",
                "scientific_name",
                "synonyms",
                "external_ids",
            ],
        ).set_index("foodatlas_id")
        old_trips = pd.DataFrame(
            [("e1", "r1", "e2")],
            columns=["head_id", "relationship_id", "tail_id"],
        )
        old_kg = OldKG(
            entities=old_ents,
            triplets=old_trips,
            metadata_contains_sources=pd.Series({"fdc": 5}),
            metadata_diseases_sources=pd.Series({"ctd": 3}),
        )

        # Write new KG parquets
        new_ents = pd.DataFrame(
            {
                "foodatlas_id": ["e1", "e3"],
                "entity_type": ["food", "chemical"],
                "common_name": ["Apple", "water"],
                "scientific_name": ["", ""],
                "synonyms": [json.dumps(["apple"]), json.dumps(["water"])],
                "external_ids": [json.dumps({}), json.dumps({})],
            }
        )
        new_ents.to_parquet(tmp_path / "entities.parquet", index=False)

        new_trips = pd.DataFrame(
            {
                "head_id": ["e1", "e3"],
                "relationship_id": ["r1", "r1"],
                "tail_id": ["e2", "e4"],
                "source": ["", ""],
                "attestation_ids": [json.dumps([]), json.dumps([])],
            }
        )
        new_trips.to_parquet(tmp_path / "triplets.parquet", index=False)

        pd.DataFrame({"source": ["fdc", "chebi"]}).to_parquet(
            tmp_path / "attestations.parquet", index=False
        )
        pd.DataFrame({"source_type": ["pubmed"]}).to_parquet(
            tmp_path / "evidence.parquet", index=False
        )

        result = run_diff(old_kg, str(tmp_path))
        assert isinstance(result, KGDiffResult)
        assert result.entity_summary.stable_count == 1
        assert result.entity_summary.new_ids == ["e3"]
        assert result.triplet_summary.new_count == 1
        assert result.triplet_summary.stable_count == 1
        assert len(result.entity_details.name_changes) == 1
