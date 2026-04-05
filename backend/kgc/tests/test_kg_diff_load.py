"""Tests for kg_diff.load_old — loading old v3.3 TSV files."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.pipeline.kg_diff.load_old import (
    OldKG,
    _safe_literal,
    load_old_entities,
    load_old_kg,
    load_old_triplets,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestSafeLiteral:
    def test_list(self) -> None:
        assert _safe_literal("['a', 'b']") == ["a", "b"]

    def test_dict(self) -> None:
        assert _safe_literal("{'k': [1]}") == {"k": [1]}

    def test_invalid(self) -> None:
        assert _safe_literal("not a literal") is None

    def test_nan(self) -> None:
        assert _safe_literal("nan") is None


class TestLoadOldEntities:
    def test_basic(self, tmp_path: Path) -> None:
        tsv = tmp_path / "entities.tsv"
        tsv.write_text(
            "foodatlas_id\tentity_type\tcommon_name\tscientific_name\tsynonyms\texternal_ids\n"
            "e1\tfood\tpectin\t\t['pectin']\t{'foodon': ['URI1']}\n"
            "e2\tchemical\twater\t\t['water', 'H2O']\t{'chebi': [1]}\n"
        )
        df = load_old_entities(tsv)
        assert len(df) == 2
        assert df.index.name == "foodatlas_id"
        assert df.loc["e1", "synonyms"] == ["pectin"]
        assert df.loc["e2", "external_ids"] == {"chebi": [1]}


class TestLoadOldTriplets:
    def test_combines_sources(self, tmp_path: Path) -> None:
        trip = tmp_path / "triplets.tsv"
        trip.write_text(
            "foodatlas_id\thead_id\trelationship_id\ttail_id\tmetadata_ids\n"
            "t1\te1\tr1\te2\t['mc1']\n"
        )
        onto = tmp_path / "food_ontology.tsv"
        onto.write_text(
            "foodatlas_id\thead_id\trelationship_id\ttail_id\tsource\n"
            "fo1\te3\tr2\te4\tfoodon\n"
            "fo2\te5\tr2\te6\tfoodon\n"
        )
        df = load_old_triplets(trip, onto)
        assert len(df) == 3
        assert list(df.columns) == ["head_id", "relationship_id", "tail_id"]
        assert df.iloc[0]["relationship_id"] == "r1"
        assert df.iloc[1]["relationship_id"] == "r2"


class TestLoadOldKG:
    def test_integration(self, tmp_path: Path) -> None:
        v33 = tmp_path / "PreviousFAKG" / "v3.3"
        v33.mkdir(parents=True)

        (v33 / "entities.tsv").write_text(
            "foodatlas_id\tentity_type\tcommon_name\tscientific_name\tsynonyms\texternal_ids\n"
            "e1\tfood\tapple\t\t['apple']\t{}\n"
        )
        (v33 / "triplets.tsv").write_text(
            "foodatlas_id\thead_id\trelationship_id\ttail_id\tmetadata_ids\n"
            "t1\te1\tr1\te2\t['mc1']\n"
        )
        (v33 / "food_ontology.tsv").write_text(
            "foodatlas_id\thead_id\trelationship_id\ttail_id\tsource\n"
        )
        (v33 / "metadata_contains.tsv").write_text(
            "foodatlas_id\tsource\nmc1\tfdc\nmc2\tfdc\nmc3\tdmd\n"
        )
        (v33 / "metadata_diseases.tsv").write_text("foodatlas_id\tsource\nmd1\tctd\n")

        kg = load_old_kg(str(tmp_path))
        assert isinstance(kg, OldKG)
        assert len(kg.entities) == 1
        assert len(kg.triplets) == 1
        assert kg.metadata_contains_sources["fdc"] == 2
        assert kg.metadata_contains_sources["dmd"] == 1
        assert kg.metadata_diseases_sources["ctd"] == 1
