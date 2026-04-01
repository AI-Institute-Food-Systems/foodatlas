"""Tests for the KnowledgeGraph class."""

from pathlib import Path

import pandas as pd
import pytest
from src.constructor.knowledge_graph import KnowledgeGraph
from src.models.settings import KGCSettings


class TestKGLoad:
    def test_loads_entities(self, kg: KnowledgeGraph) -> None:
        assert len(kg.entities._entities) == 2

    def test_loads_triplets(self, kg: KnowledgeGraph) -> None:
        assert len(kg.triplets._triplets) == 1

    def test_loads_metadata(self, kg: KnowledgeGraph) -> None:
        assert len(kg.metadata._records) == 1

    def test_from_fixture_tsvs(self, kg_dir: Path) -> None:
        settings = KGCSettings(kg_dir=str(kg_dir))
        kg = KnowledgeGraph(settings)
        assert kg.entities.get_entity_ids("food", "apple") == ["e0"]
        assert kg.entities.get_entity_ids("chemical", "vitamin c") == ["e1"]


class TestGetTriplets:
    def test_get_all(self, kg: KnowledgeGraph) -> None:
        result = kg.get_triplets()
        assert len(result) == 1
        assert "triplet_id" in result.columns

    def test_filter_by_head(self, kg: KnowledgeGraph) -> None:
        result = kg.get_triplets(head_id="e0")
        assert len(result) == 1

    def test_filter_by_tail(self, kg: KnowledgeGraph) -> None:
        result = kg.get_triplets(tail_id="e1")
        assert len(result) == 1

    def test_no_match_returns_empty(self, kg: KnowledgeGraph) -> None:
        result = kg.get_triplets(head_id="e999")
        assert len(result) == 0


class TestSave:
    def test_save_round_trip(self, kg: KnowledgeGraph, tmp_path: Path) -> None:
        out = tmp_path / "output"
        out.mkdir()
        kg.save(out)

        settings2 = KGCSettings(kg_dir=str(out))
        kg2 = KnowledgeGraph(settings2)
        assert len(kg2.entities._entities) == len(kg.entities._entities)
        assert len(kg2.triplets._triplets) == len(kg.triplets._triplets)


class TestMemoryStats:
    def test_print_stats_runs(self, kg: KnowledgeGraph) -> None:
        kg.print_stats()


class TestAddTripletsFromMetadata:
    @staticmethod
    def _make_metadata() -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "conc_value": 2.0,
                    "conc_unit": "mg/g",
                    "food_part": "flesh",
                    "food_processing": "raw",
                    "source": "fdc",
                    "reference": ["ref2"],
                    "entity_linking_method": "exact",
                    "quality_score": 0.8,
                    "_food_name": "apple",
                    "_chemical_name": "vitamin c",
                    "_conc": "2.0 mg/g",
                    "_food_part": "flesh",
                },
            ]
        )

    def test_adds_metadata_for_existing_entities(self, kg: KnowledgeGraph) -> None:
        meta = self._make_metadata()
        kg.add_triplets_from_metadata(meta)
        assert len(kg.metadata._records) == 2

    def test_deduplicates_existing_triplet(self, kg: KnowledgeGraph) -> None:
        meta = self._make_metadata()
        kg.add_triplets_from_metadata(meta)
        assert len(kg.triplets._triplets) == 1

    def test_unknown_names_are_dropped(self, kg: KnowledgeGraph) -> None:
        meta = pd.DataFrame(
            [
                {
                    "conc_value": 3.0,
                    "conc_unit": "ug/g",
                    "food_part": "",
                    "food_processing": "",
                    "source": "lit",
                    "reference": ["ref3"],
                    "entity_linking_method": "exact",
                    "quality_score": 0.9,
                    "_food_name": "banana",
                    "_chemical_name": "potassium",
                    "_conc": "3.0 ug/g",
                    "_food_part": "",
                },
            ]
        )
        kg.add_triplets_from_metadata(meta)
        assert len(kg.triplets._triplets) == 1


class TestAddTripletsUnsupported:
    def test_raises_for_unknown_relationship(self, kg: KnowledgeGraph) -> None:
        with pytest.raises(NotImplementedError, match="Unsupported"):
            kg.add_triplets_from_metadata(pd.DataFrame(), "unknown")
