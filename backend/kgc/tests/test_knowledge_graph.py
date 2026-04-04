"""Tests for the KnowledgeGraph class."""

from pathlib import Path

from src.models.settings import KGCSettings
from src.pipeline.knowledge_graph import KnowledgeGraph


class TestKGLoad:
    def test_loads_entities(self, kg: KnowledgeGraph) -> None:
        assert len(kg.entities._entities) == 2

    def test_loads_triplets(self, kg: KnowledgeGraph) -> None:
        assert len(kg.triplets._triplets) == 1

    def test_loads_evidence(self, kg: KnowledgeGraph) -> None:
        assert len(kg.evidence) == 1

    def test_loads_attestations(self, kg: KnowledgeGraph) -> None:
        assert len(kg.attestations) == 1

    def test_from_fixture(self, kg_dir: Path) -> None:
        settings = KGCSettings(kg_dir=str(kg_dir))
        kg = KnowledgeGraph(settings)
        assert kg.entities.get_entity_ids("food", "apple") == ["e0"]
        assert kg.entities.get_entity_ids("chemical", "vitamin c") == ["e1"]


class TestGetTriplets:
    def test_get_all(self, kg: KnowledgeGraph) -> None:
        result = kg.get_triplets()
        assert len(result) == 1
        assert "triplet_key" in result.columns

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
